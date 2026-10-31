// Package loadgen drives a gateway or backend with concurrent streaming chat requests and
// reports latency percentiles. Prompts share a configurable set of system prompts, so routing
// policies that exploit prefix caches have something to exploit.
package loadgen

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/byteB8/llm-gateway/internal/openai"
)

type Config struct {
	URL          string  `json:"url"`
	Concurrency  int     `json:"concurrency"`
	Requests     int     `json:"requests"`
	Prefixes     int     `json:"prefixes"`      // distinct system prompts
	PrefixTokens int     `json:"prefix_tokens"` // tokens per system prompt
	SuffixTokens int     `json:"suffix_tokens"` // tokens of the user message
	MaxTokens    int     `json:"max_tokens"`
	Zipf         float64 `json:"zipf"` // > 1 skews prefix popularity; 0 means uniform
	Seed         int64   `json:"seed"`
	Model        string  `json:"model"`
	Key          string  `json:"-"` // sent as a bearer token; kept out of the saved summary
}

type Result struct {
	Prefix   int           `json:"prefix"`
	Status   int           `json:"status"`
	Prompt   int           `json:"prompt_tokens"`
	Retries  int           `json:"retries"` // connections tried beyond the first
	TTFT     time.Duration `json:"ttft_ns"`
	Total    time.Duration `json:"total_ns"`
	Tokens   int           `json:"tokens"`
	Backend  string        `json:"backend"`
	CacheHit bool          `json:"cache_hit"`
	Err      string        `json:"err,omitempty"`
}

type Summary struct {
	Config       Config         `json:"config"`
	Requests     int            `json:"requests"`
	Errors       int            `json:"errors"`
	WallSec      float64        `json:"wall_sec"`
	ReqPerSec    float64        `json:"req_per_sec"`
	TokPerSec    float64        `json:"tokens_per_sec"`
	TTFTms       Percentiles    `json:"ttft_ms"`
	TotalMs      Percentiles    `json:"total_ms"`
	CacheHitRate float64        `json:"cache_hit_rate"`
	Retries      int            `json:"retries"`           // requests the HTTP client resent on a new connection
	PromptTokens int            `json:"prompt_tokens"`     // over successful requests
	OutputTokens int            `json:"completion_tokens"` // tokens streamed back
	StatusCodes  map[int]int    `json:"status_codes"`
	PerBackend   map[string]int `json:"per_backend"`
	FirstErrors  []string       `json:"first_errors,omitempty"`
}

type Percentiles struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	Mean float64 `json:"mean"`
}

// Run sends cfg.Requests requests from cfg.Concurrency workers and summarises them. It also
// returns every request's result, in the order the requests were issued.
func Run(ctx context.Context, cfg Config) (Summary, []Result) {
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: cfg.Concurrency + 8}}
	prompts := makePrefixes(cfg)
	jobs := make(chan int)
	results := make([]Result, cfg.Requests)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(w)))
			var zipf *rand.Zipf
			if cfg.Zipf > 1 && cfg.Prefixes > 1 {
				zipf = rand.NewZipf(rng, cfg.Zipf, 1, uint64(cfg.Prefixes-1))
			}
			for i := range jobs {
				p := rng.Intn(cfg.Prefixes)
				if zipf != nil {
					p = int(zipf.Uint64())
				}
				results[i] = one(ctx, client, cfg, prompts[p], randomWords(rng, cfg.SuffixTokens))
				results[i].Prefix = p
			}
		}(w)
	}
	for i := 0; i < cfg.Requests && ctx.Err() == nil; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return summarise(cfg, results, time.Since(start)), results
}

func one(ctx context.Context, client *http.Client, cfg Config, system, user string) Result {
	body, _ := json.Marshal(openai.ChatRequest{
		Model:     cfg.Model,
		Messages:  []openai.Message{{Role: "system", Content: system}, {Role: "user", Content: user}},
		Stream:    true,
		MaxTokens: cfg.MaxTokens,
	})
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Result{Err: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	// An Idempotency-Key makes the POST replayable: Go's client then resends it on a fresh
	// connection if a reused keep-alive connection was closed under it before the request went out.
	req.Header.Set("Idempotency-Key", newKey())
	conns := 0
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { conns++ }}))
	if cfg.Key != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Err: err.Error()}
	}
	defer resp.Body.Close()
	res := Result{Status: resp.StatusCode, Retries: max(conns-1, 0), Prompt: openai.CountTokens(system) + openai.CountTokens(user),
		Backend: resp.Header.Get("X-Gateway-Backend"), CacheHit: resp.Header.Get("X-Prefix-Cache") == "hit"}
	if res.Backend == "" {
		res.Backend = resp.Header.Get("X-Backend")
	}
	if resp.StatusCode != http.StatusOK {
		res.Err = fmt.Sprintf("status %d", resp.StatusCode)
		return res
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	done := false
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			done = true
			continue
		}
		var c openai.Chunk
		if json.Unmarshal([]byte(data), &c) != nil || len(c.Choices) == 0 || c.Choices[0].Delta.Content == "" {
			continue
		}
		if res.Tokens == 0 {
			res.TTFT = time.Since(start)
		}
		res.Tokens++
	}
	res.Total = time.Since(start)
	switch err := sc.Err(); {
	case err != nil:
		res.Err = err.Error()
	case res.Tokens == 0:
		res.Err = "no tokens"
	case !done:
		res.Err = "stream ended without [DONE]" // cut short behind a cleanly closed response
	}
	return res
}

func summarise(cfg Config, rs []Result, wall time.Duration) Summary {
	s := Summary{Config: cfg, Requests: len(rs), WallSec: wall.Seconds(), PerBackend: map[string]int{}, StatusCodes: map[int]int{}}
	var ttft, total []float64
	var tokens, hits int
	for _, r := range rs {
		s.StatusCodes[r.Status]++
		s.Retries += r.Retries
		if r.Err != "" {
			s.Errors++
			if len(s.FirstErrors) < 5 {
				s.FirstErrors = append(s.FirstErrors, r.Err)
			}
			continue
		}
		ttft = append(ttft, ms(r.TTFT))
		total = append(total, ms(r.Total))
		tokens += r.Tokens
		s.PromptTokens += r.Prompt
		if r.CacheHit {
			hits++
		}
		s.PerBackend[r.Backend]++
	}
	ok := len(ttft)
	if ok > 0 {
		s.CacheHitRate = float64(hits) / float64(ok)
	}
	s.OutputTokens = tokens
	s.ReqPerSec = float64(ok) / wall.Seconds()
	s.TokPerSec = float64(tokens) / wall.Seconds()
	s.TTFTms, s.TotalMs = percentiles(ttft), percentiles(total)
	return s
}

func percentiles(xs []float64) Percentiles {
	if len(xs) == 0 {
		return Percentiles{}
	}
	sort.Float64s(xs)
	at := func(q float64) float64 { return xs[int(q*float64(len(xs)-1)+0.5)] }
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return Percentiles{P50: at(0.50), P90: at(0.90), P99: at(0.99), Mean: sum / float64(len(xs))}
}

// makePrefixes builds the shared system prompts. Each starts with its own index so prompts of
// equal length still hash differently.
func makePrefixes(cfg Config) []string {
	rng := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed))
	out := make([]string, max(cfg.Prefixes, 1))
	for i := range out {
		out[i] = fmt.Sprintf("system-%d %s", i, randomWords(rng, max(cfg.PrefixTokens-1, 0)))
	}
	return out
}

var vocab = strings.Fields("data model token cache batch query answer filing revenue report latency stream agent tool plan step")

func randomWords(rng *rand.Rand, n int) string {
	w := make([]string, n)
	for i := range w {
		w[i] = vocab[rng.Intn(len(vocab))]
	}
	return strings.Join(w, " ")
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func newKey() string {
	b := make([]byte, 16)
	crand.Read(b)
	return hex.EncodeToString(b)
}
