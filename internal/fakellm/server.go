// Package fakellm is a stand-in for an inference server such as vLLM. It speaks the OpenAI
// chat-completions protocol and models the costs a gateway cares about: a fixed number of decode
// slots (requests beyond them queue), prefill time that a prefix cache can skip, and per-token
// latency that grows as the batch fills. The defaults are rough, not measured from a real server.
package fakellm

import (
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byteB8/llm-gateway/internal/openai"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Name             string
	Slots            int           // sequences decoded at once; more requests wait for a slot
	PrefillPerToken  time.Duration // prefill cost of each uncached prompt token
	TokenInterval    time.Duration // time per output token with a single active sequence
	BatchSlowdown    float64       // a full batch makes each token this fraction slower
	CacheEntries     int           // prefixes kept in the prefix cache (0 disables it)
	DefaultMaxTokens int
}

func DefaultConfig(name string) Config {
	return Config{
		Name:             name,
		Slots:            8,
		PrefillPerToken:  200 * time.Microsecond,
		TokenInterval:    20 * time.Millisecond,
		BatchSlowdown:    0.5,
		CacheEntries:     4,
		DefaultMaxTokens: 64,
	}
}

type Stats struct {
	Requests  int64 `json:"requests"`
	CacheHits int64 `json:"cache_hits"`
	Active    int64 `json:"active"`
	Queued    int64 `json:"queued"`
}

type Server struct {
	cfg   Config
	slots chan struct{}
	cache *lru
	stats struct{ requests, hits, active, queued atomic.Int64 }
	fault atomic.Pointer[Fault]
	reg   *prometheus.Registry
	m     struct {
		promptTokens, cachedTokens prometheus.Counter
		faults                     *prometheus.CounterVec
	}
}

func New(cfg Config) *Server {
	if cfg.Slots < 1 {
		cfg.Slots = 1
	}
	s := &Server{cfg: cfg, slots: make(chan struct{}, cfg.Slots), cache: newLRU(cfg.CacheEntries), reg: prometheus.NewRegistry()}
	s.m.promptTokens = prometheus.NewCounter(prometheus.CounterOpts{Name: "fakellm_prefix_cache_queries_total",
		Help: "Prompt tokens looked up in the prefix cache (vLLM's prefix_cache_queries, for the fake)."})
	s.m.cachedTokens = prometheus.NewCounter(prometheus.CounterOpts{Name: "fakellm_prefix_cache_hits_total",
		Help: "Prompt tokens served from the prefix cache."})
	s.m.faults = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fakellm_injected_faults_total",
		Help: "Requests that hit an injected fault."}, []string{"kind"})
	s.reg.MustRegister(s.m.promptTokens, s.m.cachedTokens, s.m.faults,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "fakellm_requests_running", Help: "Requests holding a decode slot."},
			func() float64 { return float64(s.stats.active.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "fakellm_requests_waiting", Help: "Requests queued for a slot."},
			func() float64 { return float64(s.stats.queued.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "fakellm_slots", Help: "Decode slots (vLLM max_num_seqs)."},
			func() float64 { return float64(cfg.Slots) }))
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chat)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/fault", s.faultHandler)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Stats())
	})
	return mux
}

func (s *Server) Stats() Stats {
	return Stats{s.stats.requests.Load(), s.stats.hits.Load(), s.stats.active.Load(), s.stats.queued.Load()}
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	var req openai.ChatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.stats.requests.Add(1)
	ctx := r.Context()

	queuedAt := time.Now()
	s.stats.queued.Add(1)
	select {
	case s.slots <- struct{}{}:
		s.stats.queued.Add(-1)
	case <-ctx.Done():
		s.stats.queued.Add(-1)
		return
	}
	defer func() { <-s.slots }()
	s.stats.active.Add(1)
	defer s.stats.active.Add(-1)

	fault := s.currentFault()
	if chance(fault.ErrorRate) {
		s.m.faults.WithLabelValues("error").Inc()
		http.Error(w, "injected fault", http.StatusServiceUnavailable)
		return
	}
	if !sleep(ctx, time.Duration(fault.ExtraLatencyMs)*time.Millisecond) {
		return
	}

	prompt := openai.PromptTokens(req.Messages)
	prefix := openai.PrefixKey(req.Messages)
	hit := s.cache.touch(hashKey(prefix))
	uncached := prompt
	if hit {
		s.stats.hits.Add(1)
		uncached -= openai.CountTokens(prefix)
	}
	s.m.promptTokens.Add(float64(prompt))
	s.m.cachedTokens.Add(float64(prompt - uncached))
	trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("fakellm.prefix_cache_hit", hit),
		attribute.Int("fakellm.prompt_tokens", prompt), attribute.Int("fakellm.uncached_tokens", uncached),
		attribute.Float64("fakellm.queued_ms", float64(time.Since(queuedAt).Microseconds())/1000))
	if !sleep(ctx, time.Duration(uncached)*s.cfg.PrefillPerToken) {
		return
	}

	n := req.MaxTokens
	if n <= 0 {
		n = s.cfg.DefaultMaxTokens
	}
	w.Header().Set("X-Backend", s.cfg.Name)
	w.Header().Set("X-Prefix-Cache", map[bool]string{true: "hit", false: "miss"}[hit])
	usage := openai.Usage{PromptTokens: prompt, CompletionTokens: n, TotalTokens: prompt + n}
	id := "fake-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if !req.Stream {
		for i := 0; i < n; i++ {
			if !sleep(ctx, s.tokenDelay()) {
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(openai.Completion{
			ID: id, Object: "chat.completion", Created: time.Now().Unix(), Model: req.Model,
			Choices: []openai.Choice{{Message: openai.Message{Role: "assistant", Content: words(n)}, FinishReason: "stop"}},
			Usage:   usage,
		})
		return
	}

	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	send := func(c openai.Chunk) bool {
		b, _ := json.Marshal(c)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	chunk := func(d openai.Delta, finish *string, u *openai.Usage) openai.Chunk {
		return openai.Chunk{ID: id, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: req.Model,
			Choices: []openai.ChunkChoice{{Delta: d, FinishReason: finish}}, Usage: u}
	}
	for i := 0; i < n; i++ {
		if i > 0 && !sleep(ctx, s.tokenDelay()) {
			return
		}
		d := openai.Delta{Content: fmt.Sprintf("tok%d ", i)}
		if i == 0 {
			d.Role = "assistant"
		}
		if !send(chunk(d, nil, nil)) {
			return
		}
		if i == 0 && chance(fault.CutRate) {
			s.m.faults.WithLabelValues("cut").Inc()
			if hj, ok := w.(http.Hijacker); ok { // drop the connection like a crashed server would
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
				}
			}
			return
		}
	}
	stop := "stop"
	send(chunk(openai.Delta{}, &stop, &usage))
	fmt.Fprint(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

// tokenDelay grows linearly from TokenInterval with one active sequence to
// TokenInterval*(1+BatchSlowdown) with every slot busy.
func (s *Server) tokenDelay() time.Duration {
	active := float64(s.stats.active.Load())
	frac := 0.0
	if s.cfg.Slots > 1 {
		frac = (active - 1) / float64(s.cfg.Slots-1)
	}
	return time.Duration(float64(s.cfg.TokenInterval) * (1 + s.cfg.BatchSlowdown*frac))
}

func sleep(ctx interface{ Done() <-chan struct{} }, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func words(n int) string {
	b := make([]byte, 0, n*6)
	for i := 0; i < n; i++ {
		b = fmt.Appendf(b, "tok%d ", i)
	}
	return string(b)
}

func hashKey(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// lru is a fixed-size set of prefix hashes; touch reports whether the key was present and
// marks it most recently used.
type lru struct {
	mu   sync.Mutex
	cap  int
	ll   *list.List
	keys map[uint64]*list.Element
}

func newLRU(capacity int) *lru {
	return &lru{cap: capacity, ll: list.New(), keys: map[uint64]*list.Element{}}
}

func (c *lru) touch(k uint64) bool {
	if c.cap <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.keys[k]; ok {
		c.ll.MoveToFront(e)
		return true
	}
	c.keys[k] = c.ll.PushFront(k)
	if c.ll.Len() > c.cap {
		old := c.ll.Back()
		c.ll.Remove(old)
		delete(c.keys, old.Value.(uint64))
	}
	return false
}
