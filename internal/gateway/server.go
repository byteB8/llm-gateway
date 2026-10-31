// Package gateway is the HTTP front end: it picks a backend with a routing policy, forwards the
// request and streams the response back as it arrives.
package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/byteB8/llm-gateway/internal/openai"
	"github.com/byteB8/llm-gateway/internal/router"
)

type Config struct {
	MaxAttempts           int           // backends tried per request; 0 means all of them
	ResponseHeaderTimeout time.Duration // a backend that sends no headers in this time is skipped
}

type Server struct {
	cfg      Config
	backends []*router.Backend
	policy   router.Policy
	client   *http.Client
	log      *slog.Logger
}

func New(cfg Config, backends []*router.Backend, policy router.Policy, log *slog.Logger) *Server {
	if cfg.MaxAttempts <= 0 || cfg.MaxAttempts > len(backends) {
		cfg.MaxAttempts = len(backends)
	}
	if cfg.ResponseHeaderTimeout <= 0 {
		cfg.ResponseHeaderTimeout = 2 * time.Minute
	}
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
	return &Server{cfg: cfg, backends: backends, policy: policy, client: &http.Client{Transport: transport}, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chat)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /backends", s.listBackends)
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
				return
			}
			s.log.Warn("backend failed", "backend", b.Name, "attempt", attempt+1, "err", lastErr)
			continue
		}
		ttfb, n, copyErr := s.stream(w, resp, b, attempt+1, start)
		b.Release()
		s.log.Info("request", "backend", b.Name, "attempts", attempt+1, "status", resp.StatusCode,
			"stream", req.Stream, "ttfb_ms", ms(ttfb), "dur_ms", ms(time.Since(start)), "bytes", n,
			"client_gone", copyErr != nil)
		return
	}
	s.log.Error("all backends failed", "tried", len(order), "last", lastErr)
	writeError(w, http.StatusBadGateway, "no backend could serve the request: "+lastErr)
}

func (s *Server) forward(r *http.Request, b *router.Backend, body []byte) (*http.Response, error) {
	up, err := http.NewRequestWithContext(r.Context(), http.MethodPost, b.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	up.Header.Set("Content-Type", "application/json")
	if a := r.Header.Get("Authorization"); a != "" {
		up.Header.Set("Authorization", a)
	}
	return s.client.Do(up)
}

// stream copies the backend response to the client, flushing after every read so tokens are
// not held back. It returns the time from the request's arrival (start) to the first body byte,
// which includes the backend's queueing and prefill.
func (s *Server) stream(w http.ResponseWriter, resp *http.Response, b *router.Backend, attempts int, start time.Time) (time.Duration, int64, error) {
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
