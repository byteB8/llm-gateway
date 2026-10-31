// Package gateway is the HTTP front end: it picks a backend with a routing policy, forwards the
// request and streams the response back as it arrives.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/byteB8/llm-gateway/internal/limit"
	"github.com/byteB8/llm-gateway/internal/metrics"
	"github.com/byteB8/llm-gateway/internal/openai"
	"github.com/byteB8/llm-gateway/internal/router"
	"github.com/byteB8/llm-gateway/internal/tracing"
	"github.com/byteB8/llm-gateway/internal/usage"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	MaxAttempts           int           // backends tried per request; 0 means all of them
	ResponseHeaderTimeout time.Duration // a backend that sends no headers in this time is skipped
	Tenants               *Tenants      // nil: no authentication, everyone is "anonymous"
	DefaultMaxTokens      int           // reserved for a request that sets no max_tokens
	BackendKey            string        // sent to backends as a bearer token (vLLM --api-key)
	Publisher             usage.Publisher
	Metrics               *metrics.Gateway // nil: metrics go to a private registry nobody scrapes
}

type Server struct {
	cfg      Config
	backends []*router.Backend
	policy   router.Policy
	client   *http.Client
	limiter  *limit.Limiter
	log      *slog.Logger
	draining atomic.Bool
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
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewGateway(prometheus.NewRegistry())
	}
	for _, b := range backends {
		b := b
		lbl := prometheus.Labels{"backend": b.Name}
		cfg.Metrics.GaugeFunc("llmgw_backend_inflight", "Requests in flight to the backend.", lbl, func() float64 { return float64(b.Inflight()) })
		cfg.Metrics.GaugeFunc("llmgw_backend_capacity", "Requests the backend serves at once (0 = unknown).", lbl, func() float64 { return float64(b.Capacity) })
		cfg.Metrics.GaugeFunc("llmgw_backend_consecutive_failures", "Attempts on the backend that failed in a row (0 once one succeeds).",
			lbl, func() float64 { return float64(b.ConsecutiveFailures()) })
		cfg.Metrics.GaugeFunc("llmgw_backend_ejected", "1 while the backend is ejected: tried only after the others.",
			lbl, func() float64 {
				if b.Ejected(time.Now()) {
					return 1
				}
				return 0
			})
		cfg.Metrics.CounterFunc("llmgw_backend_ejections_total", "Times the backend was ejected after failing in a row.",
			lbl, func() float64 { return float64(b.Ejections()) })
	}
	if k, ok := cfg.Publisher.(interface{ Stats() usage.Stats }); ok {
		for result, f := range map[string]func(usage.Stats) int64{
			"published": func(s usage.Stats) int64 { return s.Published },
			"failed":    func(s usage.Stats) int64 { return s.Failed },
			"dropped":   func(s usage.Stats) int64 { return s.Dropped },
		} {
			f := f
			cfg.Metrics.CounterFunc("llmgw_usage_events_total", "Usage events handed to Kafka, by outcome.",
				prometheus.Labels{"result": result}, func() float64 { return float64(f(k.Stats())) })
		}
	}
	// Series whose labels are known in advance start at 0, so dashboards and alert rules see "none
	// so far" rather than no data until the first failure.
	tenants := []string{"anonymous"}
	if cfg.Tenants != nil {
		tenants = tenants[:0]
		for _, t := range cfg.Tenants.list {
			tenants = append(tenants, t.Name)
		}
	}
	for _, t := range tenants {
		for _, reason := range []string{"rpm", "tpm", "concurrency", "too_large"} {
			cfg.Metrics.RateLimited.WithLabelValues(t, reason)
		}
		cfg.Metrics.Requests.WithLabelValues(t, "none", "502")
	}
	for _, b := range backends {
		for _, side := range []string{"client", "backend"} {
			cfg.Metrics.Interrupted.WithLabelValues(b.Name, side)
		}
		for _, reason := range []string{"connect", "status_5xx"} {
			cfg.Metrics.BackendFailures.WithLabelValues(b.Name, reason)
		}
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
	// otelhttp adds a client span per backend call and sends the W3C traceparent header with it.
	client := &http.Client{Transport: otelhttp.NewTransport(transport)}
	return &Server{cfg: cfg, backends: backends, policy: policy, client: client, limiter: limiter, log: log}
}

// StartDraining makes every later response carry "Connection: close". Clients then open their next
// request on a new connection, which the Service sends to another pod, so by the time the listener
// closes no client is about to reuse a connection to this one. Call it before shutting down.
func (s *Server) StartDraining() { s.draining.Store(true) }

func (s *Server) Handler() http.Handler {
	mux := s.routes()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			w.Header().Set("Connection", "close")
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) routes() *http.ServeMux {
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
	// Every served request gets its own id, the key billing deduplicates on: Kafka's replays of one
	// event are ignored, but two requests are two bills. The client's Idempotency-Key is only recorded.
	// (When it was the id, a client could reuse one key for many requests and pay once.)
	reqID := usage.NewRequestID()
	w.Header().Set("X-Request-Id", reqID)
	idemKey := r.Header.Get("Idempotency-Key")
	if !validKey(idemKey) {
		idemKey = ""
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		s.count("unknown", "none", http.StatusRequestEntityTooLarge)
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	var req openai.ChatRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		s.count("unknown", "none", http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "body must be a chat request with at least one message")
		return
	}
	tenant, ok := s.cfg.Tenants.identify(r)
	if !ok {
		s.count("unknown", "none", http.StatusUnauthorized)
		writeError(w, http.StatusUnauthorized, "missing or unknown API key")
		return
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = s.cfg.DefaultMaxTokens
	}
	reqSpan := trace.SpanFromContext(r.Context())
	reqSpan.SetAttributes(attribute.String("llmgw.tenant", tenant), attribute.String("gen_ai.request.model", req.Model),
		attribute.Bool("llmgw.stream", req.Stream), attribute.String("llmgw.policy", s.policy.Name()))
	_, admitSpan := tracer.Start(r.Context(), "admit")
	ticket, d := s.limiter.Admit(tenant, openai.PromptTokens(req.Messages)+maxTokens)
	admitSpan.SetAttributes(attribute.Bool("llmgw.allowed", d.Allowed), attribute.String("llmgw.limit_reason", d.Reason))
	admitSpan.End()
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
		s.cfg.Metrics.RateLimited.WithLabelValues(tenant, d.Reason).Inc()
		s.count(tenant, "none", http.StatusTooManyRequests)
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded: "+d.Reason)
		return
	}

	if req.Stream {
		// OpenAI-style servers (vLLM included) report usage in a stream only when asked; the
		// limiter's refund and the billing events need it.
		if body, err = withStreamUsage(body); err != nil {
			s.count(tenant, "none", http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "malformed request body")
			ticket.Done(0)
			return
		}
	}
	order := router.HealthyFirst(s.policy.Order(openai.PrefixKey(req.Messages)), time.Now())[:s.cfg.MaxAttempts]
	var lastErr string
	for attempt, b := range order {
		b.Acquire()
		actx, attemptSpan := tracer.Start(r.Context(), "backend "+b.Name, trace.WithAttributes(
			attribute.String("llmgw.backend", b.Name), attribute.Int("llmgw.attempt", attempt+1)))
		resp, err := s.forward(actx, r, b, body)
		if err != nil || resp.StatusCode >= 500 {
			reason := "connect"
			if err == nil {
				lastErr = b.Name + ": status " + strconv.Itoa(resp.StatusCode)
				resp.Body.Close()
				reason = "status_5xx"
			} else {
				lastErr = b.Name + ": " + err.Error()
			}
			b.Release()
			attemptSpan.SetStatus(codes.Error, lastErr)
			attemptSpan.End()
			if r.Context().Err() != nil { // the client went away; don't try the others
				s.count(tenant, b.Name, 499) // nginx's "client closed request"
				ticket.Done(0)
				return
			}
			s.cfg.Metrics.BackendFailures.WithLabelValues(b.Name, reason).Inc()
			s.log.Warn("backend failed", "backend", b.Name, "attempt", attempt+1, "err", lastErr)
			if d := b.Failed(time.Now()); d > 0 {
				s.log.Warn("backend ejected", "backend", b.Name, "for", d.String(), "failures_in_a_row", b.ConsecutiveFailures())
			}
			continue
		}
		b.Succeeded()
		tap := &usageTap{stream: req.Stream}
		ttfb, n, copyErr := s.stream(w, resp, b, attempt+1, start, tap)
		b.Release()
		if n > 0 {
			attemptSpan.AddEvent("first_byte", trace.WithTimestamp(start.Add(ttfb)))
		}
		if copyErr != nil {
			attemptSpan.SetStatus(codes.Error, copyErr.Error())
		}
		attemptSpan.End()
		used, prompt, completion := -1, 0, 0 // -1: usage unknown, keep the whole reservation
		if u := tap.Usage(); u != nil {
			used, prompt, completion = u.TotalTokens, u.PromptTokens, u.CompletionTokens
		}
		ticket.Done(used)
		traceID := tracing.SampledTraceID(r.Context())
		s.record(tenant, b.Name, resp.StatusCode, ttfb, n, time.Since(start), prompt, completion, used >= 0, copyErr, traceID)
		reqSpan.SetAttributes(attribute.String("llmgw.backend", b.Name), attribute.Int("llmgw.attempts", attempt+1),
			attribute.Float64("llmgw.ttft_ms", ms(ttfb)), attribute.Int("gen_ai.usage.input_tokens", prompt),
			attribute.Int("gen_ai.usage.output_tokens", completion))
		s.cfg.Publisher.Publish(usage.Event{
			Trace:     tracing.Inject(r.Context()),
			RequestID: reqID, IdempotencyKey: idemKey, Tenant: tenant, Backend: b.Name, Model: req.Model, Status: resp.StatusCode,
			Stream: req.Stream, PromptTokens: prompt, CompletionTokens: completion, UsageKnown: used >= 0,
			TTFBms: ms(ttfb), DurationMs: ms(time.Since(start)), Time: start.UTC(),
		})
		s.log.Info("request", "tenant", tenant, "backend", b.Name, "attempts", attempt+1, "status", resp.StatusCode,
			"stream", req.Stream, "ttfb_ms", ms(ttfb), "dur_ms", ms(time.Since(start)), "bytes", n,
			"prompt_tokens", prompt, "completion_tokens", completion, "client_gone", copyErr != nil)
		var se *streamError
		if errors.As(copyErr, &se) && se.side == "backend" {
			// The client already has a 200 and part of the answer. Ending the response normally would
			// make it look complete, so abort the connection, as httputil.ReverseProxy does: the client
			// gets a read error instead of a silently truncated answer.
			panic(http.ErrAbortHandler)
		}
		return
	}
	ticket.Done(0)
	s.count(tenant, "none", http.StatusBadGateway)
	s.cfg.Publisher.Publish(usage.Event{RequestID: reqID, IdempotencyKey: idemKey, Tenant: tenant, Model: req.Model, Status: http.StatusBadGateway,
		Stream: req.Stream, UsageKnown: true, DurationMs: ms(time.Since(start)), Time: start.UTC()})
	s.log.Error("all backends failed", "tried", len(order), "last", lastErr)
	writeError(w, http.StatusBadGateway, "no backend could serve the request: "+lastErr)
}

func (s *Server) forward(ctx context.Context, r *http.Request, b *router.Backend, body []byte) (*http.Response, error) {
	up, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL+"/v1/chat/completions", bytes.NewReader(body))
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
				return ttfb, total, &streamError{side: "client", err: werr}
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return ttfb, total, nil
			}
			if r := resp.Request; r != nil && r.Context().Err() != nil {
				return ttfb, total, &streamError{side: "client", err: rerr} // our read stopped because the client left
			}
			return ttfb, total, &streamError{side: "backend", err: rerr}
		}
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": msg, "code": code}})
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// validKey accepts 8-128 printable ASCII characters without spaces.
func validKey(k string) bool {
	if len(k) < 8 || len(k) > 128 {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] <= ' ' || k[i] > '~' {
			return false
		}
	}
	return true
}

// withStreamUsage sets stream_options.include_usage, keeping every other field of the request
// as the client sent it.
func withStreamUsage(body []byte) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	opts := map[string]json.RawMessage{}
	if raw, ok := m["stream_options"]; ok {
		if err := json.Unmarshal(raw, &opts); err != nil {
			return nil, err
		}
	}
	opts["include_usage"] = json.RawMessage("true")
	b, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	m["stream_options"] = b
	return json.Marshal(m)
}

// streamError says which side broke a response that had already started.
type streamError struct {
	side string // "client" or "backend"
	err  error
}

func (e *streamError) Error() string { return e.side + ": " + e.err.Error() }
func (e *streamError) Unwrap() error { return e.err }

func (s *Server) count(tenant, backend string, code int) {
	s.cfg.Metrics.Requests.WithLabelValues(tenant, backend, strconv.Itoa(code)).Inc()
}

// record updates the metrics of a request a backend answered.
func (s *Server) record(tenant, backend string, code int, ttfb time.Duration, bytes int64, dur time.Duration,
	prompt, completion int, usageKnown bool, streamErr error, traceID string) {
	m := s.cfg.Metrics
	s.count(tenant, backend, code)
	if bytes > 0 {
		observe(m.TTFT.WithLabelValues(backend), ttfb.Seconds(), traceID)
	}
	observe(m.Duration.WithLabelValues(backend), dur.Seconds(), traceID)
	if usageKnown {
		m.Tokens.WithLabelValues(tenant, "prompt").Add(float64(prompt))
		m.Tokens.WithLabelValues(tenant, "completion").Add(float64(completion))
	}
	var se *streamError
	if errors.As(streamErr, &se) {
		m.Interrupted.WithLabelValues(backend, se.side).Inc()
	}
}

var tracer = otel.Tracer("github.com/byteB8/llm-gateway/gateway")

// observe records a latency and, when the request is traced, the trace id as an exemplar, so a
// dashboard can jump from a slow histogram bucket to the trace of a request that landed in it.
func observe(o prometheus.Observer, v float64, traceID string) {
	if eo, ok := o.(prometheus.ExemplarObserver); ok && traceID != "" {
		eo.ObserveWithExemplar(v, prometheus.Labels{"trace_id": traceID})
		return
	}
	o.Observe(v)
}
