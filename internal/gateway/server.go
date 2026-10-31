// Package gateway is the HTTP front end: it picks a backend with a routing policy, forwards the
// request and streams the response back as it arrives.
package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/byteB8/llm-gateway/internal/limit"
	"github.com/byteB8/llm-gateway/internal/openai"
	"github.com/byteB8/llm-gateway/internal/router"
	"github.com/byteB8/llm-gateway/internal/usage"
)

type Config struct {
	MaxAttempts           int           // backends tried per request; 0 means all of them
	ResponseHeaderTimeout time.Duration // a backend that sends no headers in this time is skipped
	Tenants               *Tenants      // nil: no authentication, everyone is "anonymous"
	DefaultMaxTokens      int           // reserved for a request that sets no max_tokens
	BackendKey            string        // sent to backends as a bearer token (vLLM --api-key)
	Publisher             usage.Publisher
}

type Server struct {
	cfg      Config
	backends []*router.Backend
	policy   router.Policy
	client   *http.Client
	limiter  *limit.Limiter
	log      *slog.Logger
}

func New(cfg Config, backends []*router.Backend, policy router.Policy, log *slog.Logger) *Server {
	if cfg.MaxAttempts <= 0 || cfg.MaxAttempts > len(backends) {
		cfg.MaxAttempts = len(backends)
	}
	if cfg.ResponseHeaderTimeout <= 0 {
		cfg.ResponseHeaderTimeout = 2 * time.Minute
	}
	if cfg.DefaultMaxTokens <= 0 {
		cfg.DefaultMaxTokens = 256
	}
	if cfg.Publisher == nil {
		cfg.Publisher = usage.Nop{}
	}
	limiter := limit.New(nil)
	if cfg.Tenants != nil {
		for _, t := range cfg.Tenants.list {
			limiter.Set(t.Name, t.Limits)
		}
	}
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
	return &Server{cfg: cfg, backends: backends, policy: policy, client: &http.Client{Transport: transport}, limiter: limiter, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chat)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /backends", s.listBackends)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		out := map[string]any{"policy": s.policy.Name()}
		if k, ok := s.cfg.Publisher.(interface{ Stats() usage.Stats }); ok {
			out["usage_events"] = k.Stats()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	return mux
}

func (s *Server) listBackends(w http.ResponseWriter, _ *http.Request) {
	type row struct {
		Name     string `json:"name"`
		URL      string `json:"url"`
		Inflight int64  `json:"inflight"`
	}
	out := make([]row, len(s.backends))
	for i, b := range s.backends {
		out[i] = row{b.Name, b.URL, b.Inflight()}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"policy": s.policy.Name(), "backends": out})
}

// passHeaders are copied from the backend response to the client.
var passHeaders = []string{"Content-Type", "Cache-Control", "X-Backend", "X-Prefix-Cache"}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := usage.NewRequestID()
	w.Header().Set("X-Request-Id", reqID)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	var req openai.ChatRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "body must be a chat request with at least one message")
		return
	}
	tenant, ok := s.cfg.Tenants.identify(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing or unknown API key")
		return
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = s.cfg.DefaultMaxTokens
	}
	ticket, d := s.limiter.Admit(tenant, openai.PromptTokens(req.Messages)+maxTokens)
	if d.RemainingReqs >= 0 {
		w.Header().Set("X-RateLimit-Remaining-Requests", strconv.Itoa(int(d.RemainingReqs)))
	}
	if d.RemainingTokens >= 0 {
		w.Header().Set("X-RateLimit-Remaining-Tokens", strconv.Itoa(int(d.RemainingTokens)))
	}
	if !d.Allowed {
		if d.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(d.RetryAfter.Seconds()))))
		}
		s.log.Info("rate limited", "tenant", tenant, "reason", d.Reason)
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded: "+d.Reason)
		return
	}

	order := s.policy.Order(openai.PrefixKey(req.Messages))[:s.cfg.MaxAttempts]
	var lastErr string
	for attempt, b := range order {
		b.Acquire()
		resp, err := s.forward(r, b, body)
		if err != nil || resp.StatusCode >= 500 {
			if err == nil {
				lastErr = b.Name + ": status " + strconv.Itoa(resp.StatusCode)
				resp.Body.Close()
			} else {
				lastErr = b.Name + ": " + err.Error()
			}
			b.Release()
			if r.Context().Err() != nil { // the client went away; don't try the others
				ticket.Done(0)
				return
			}
			s.log.Warn("backend failed", "backend", b.Name, "attempt", attempt+1, "err", lastErr)
			continue
		}
		tap := &usageTap{stream: req.Stream}
		ttfb, n, copyErr := s.stream(w, resp, b, attempt+1, start, tap)
		b.Release()
		used, prompt, completion := -1, 0, 0 // -1: usage unknown, keep the whole reservation
		if u := tap.Usage(); u != nil {
			used, prompt, completion = u.TotalTokens, u.PromptTokens, u.CompletionTokens
		}
		ticket.Done(used)
		s.cfg.Publisher.Publish(usage.Event{
			RequestID: reqID, Tenant: tenant, Backend: b.Name, Model: req.Model, Status: resp.StatusCode,
			Stream: req.Stream, PromptTokens: prompt, CompletionTokens: completion, UsageKnown: used >= 0,
			TTFBms: ms(ttfb), DurationMs: ms(time.Since(start)), Time: start.UTC(),
		})
		s.log.Info("request", "tenant", tenant, "backend", b.Name, "attempts", attempt+1, "status", resp.StatusCode,
			"stream", req.Stream, "ttfb_ms", ms(ttfb), "dur_ms", ms(time.Since(start)), "bytes", n,
			"prompt_tokens", prompt, "completion_tokens", completion, "client_gone", copyErr != nil)
		return
	}
	ticket.Done(0)
	s.cfg.Publisher.Publish(usage.Event{RequestID: reqID, Tenant: tenant, Model: req.Model, Status: http.StatusBadGateway,
		Stream: req.Stream, UsageKnown: true, DurationMs: ms(time.Since(start)), Time: start.UTC()})
	s.log.Error("all backends failed", "tried", len(order), "last", lastErr)
	writeError(w, http.StatusBadGateway, "no backend could serve the request: "+lastErr)
}

func (s *Server) forward(r *http.Request, b *router.Backend, body []byte) (*http.Response, error) {
	up, err := http.NewRequestWithContext(r.Context(), http.MethodPost, b.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	up.Header.Set("Content-Type", "application/json")
	// The client's own key stays here; backends get the gateway's key, if any.
	if s.cfg.BackendKey != "" {
		up.Header.Set("Authorization", "Bearer "+s.cfg.BackendKey)
	}
	return s.client.Do(up)
}

// stream copies the backend response to the client, flushing after every read so tokens are
// not held back. It returns the time from the request's arrival (start) to the first body byte,
// which includes the backend's queueing and prefill.
func (s *Server) stream(w http.ResponseWriter, resp *http.Response, b *router.Backend, attempts int, start time.Time, tap *usageTap) (time.Duration, int64, error) {
	defer resp.Body.Close()
	for _, h := range passHeaders {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.Header().Set("X-Gateway-Backend", b.Name)
	w.Header().Set("X-Gateway-Attempts", strconv.Itoa(attempts))
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)

	var ttfb time.Duration
	var total int64
	buf := make([]byte, 32<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if total == 0 {
				ttfb = time.Since(start)
			}
			total += int64(n)
			tap.Write(buf[:n])
			if _, werr := w.Write(buf[:n]); werr != nil {
				return ttfb, total, werr
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return ttfb, total, nil
			}
			return ttfb, total, rerr
		}
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": msg, "code": code}})
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
