package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/byteB8/llm-gateway/internal/fakellm"
	"github.com/byteB8/llm-gateway/internal/limit"
	"github.com/byteB8/llm-gateway/internal/openai"
	"github.com/byteB8/llm-gateway/internal/router"
	"github.com/byteB8/llm-gateway/internal/usage"
)

// limitedGateway fronts one fake backend that writes 4 tokens unless the request asks for more.
func limitedGateway(t *testing.T, lim limit.Limits, backend http.Handler) *httptest.Server {
	t.Helper()
	if backend == nil {
		c := fakellm.DefaultConfig("f")
		c.PrefillPerToken, c.TokenInterval, c.DefaultMaxTokens = 0, time.Millisecond, 4
		backend = fakellm.New(c).Handler()
	}
	be := httptest.NewServer(backend)
	t.Cleanup(be.Close)
	tenants, err := NewTenants([]Tenant{{Name: "team-a", Keys: []string{"sk-a"}, Limits: lim}})
	if err != nil {
		t.Fatal(err)
	}
	bs := []*router.Backend{{Name: "f", URL: be.URL}}
	p, _ := router.New("round-robin", bs, 1.25)
	g := New(Config{Tenants: tenants, DefaultMaxTokens: 80, BackendKey: "backend-secret"}, bs, p,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	gw := httptest.NewServer(g.Handler())
	t.Cleanup(gw.Close)
	return gw
}

func call(t *testing.T, url, key string) *http.Response {
	t.Helper()
	b, _ := json.Marshal(openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: "hi there"}}, Stream: true})
	req, _ := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", bytes.NewReader(b))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func TestUnknownKeyIs401(t *testing.T) {
	gw := limitedGateway(t, limit.Limits{}, nil)
	for _, key := range []string{"", "sk-wrong"} {
		if resp := call(t, gw.URL, key); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("key %q: status %d", key, resp.StatusCode)
		}
	}
	if resp := call(t, gw.URL, "sk-a"); resp.StatusCode != http.StatusOK {
		t.Fatalf("valid key: status %d", resp.StatusCode)
	}
}

func TestRPMLimitReturns429WithRetryAfter(t *testing.T) {
	gw := limitedGateway(t, limit.Limits{RPM: 2}, nil)
	call(t, gw.URL, "sk-a")
	call(t, gw.URL, "sk-a")
	resp := call(t, gw.URL, "sk-a")
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "30" {
		t.Fatalf("status %d Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

func TestUnusedTokensAreRefunded(t *testing.T) {
	// Each request reserves 2 prompt + 80 default max tokens = 82 of 100. The model stops early, after
	// 4 tokens (the fake backend always writes its full cap, so this backend reports the usage itself).
	// Without the refund the second request would find only 18 tokens left.
	stopsEarly := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n" +
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":4,\"total_tokens\":6}}\n\n" +
			"data: [DONE]\n\n"))
	})
	gw := limitedGateway(t, limit.Limits{TPM: 100}, stopsEarly)
	if resp := call(t, gw.URL, "sk-a"); resp.Header.Get("X-RateLimit-Remaining-Tokens") != "18" {
		t.Fatalf("remaining after reservation: %q", resp.Header.Get("X-RateLimit-Remaining-Tokens"))
	}
	resp := call(t, gw.URL, "sk-a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second request: status %d (refund missing?)", resp.StatusCode)
	}
}

func TestClientKeyNotSentToBackend(t *testing.T) {
	var got string
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.Write([]byte(`{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`))
	})
	gw := limitedGateway(t, limit.Limits{}, backend)
	call(t, gw.URL, "sk-a")
	if got != "Bearer backend-secret" {
		t.Fatalf("backend saw Authorization %q", got)
	}
}

func TestUsageTapAcrossSplitWrites(t *testing.T) {
	tap := &usageTap{stream: true}
	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12}}\n\ndata: [DONE]\n\n"
	for i := 0; i < len(stream); i += 7 { // feed it in awkward pieces
		tap.Write([]byte(stream[i:min(i+7, len(stream))]))
	}
	if u := tap.Usage(); u == nil || u.TotalTokens != 12 || u.CompletionTokens != 7 {
		t.Fatalf("usage %+v", u)
	}
	plain := &usageTap{}
	plain.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	if u := plain.Usage(); u == nil || u.TotalTokens != 3 {
		t.Fatalf("non-stream usage %+v", u)
	}
}

func TestNoLimitHeadersWithoutLimits(t *testing.T) {
	gw := limitedGateway(t, limit.Limits{}, nil) // tenant exists but has no limits
	resp := call(t, gw.URL, "sk-a")
	if v := resp.Header.Values("X-RateLimit-Remaining-Requests"); len(v) != 0 {
		t.Fatalf("unexpected remaining-requests header %v", v)
	}
	if v := resp.Header.Values("X-RateLimit-Remaining-Tokens"); len(v) != 0 {
		t.Fatalf("unexpected remaining-tokens header %v", v)
	}
}

type capture struct {
	mu     sync.Mutex
	events []usage.Event
}

func (c *capture) Publish(e usage.Event) { c.mu.Lock(); c.events = append(c.events, e); c.mu.Unlock() }
func (c *capture) Close()                {}

func TestPublishesOneEventPerServedRequest(t *testing.T) {
	c := fakellm.DefaultConfig("f")
	c.PrefillPerToken, c.TokenInterval = 0, time.Millisecond
	be := httptest.NewServer(fakellm.New(c).Handler())
	defer be.Close()
	bs := []*router.Backend{{Name: "f", URL: be.URL}}
	p, _ := router.New("round-robin", bs, 1.25)
	events := &capture{}
	g := New(Config{Publisher: events}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	gw := httptest.NewServer(g.Handler())
	defer gw.Close()

	resp := chat(t, gw.URL, true) // 2 prompt tokens, 4 completion tokens
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if len(events.events) != 1 {
		t.Fatalf("%d events", len(events.events))
	}
	e := events.events[0]
	if e.RequestID != resp.Header.Get("X-Request-Id") || e.Tenant != "anonymous" || e.Backend != "f" ||
		e.PromptTokens != 2 || e.CompletionTokens != 4 || !e.UsageKnown || e.Status != 200 {
		t.Fatalf("event %+v (header id %q)", e, resp.Header.Get("X-Request-Id"))
	}
}

func TestIdempotencyKeyIsRecordedNotBilledOn(t *testing.T) {
	c := fakellm.DefaultConfig("f")
	c.PrefillPerToken, c.TokenInterval = 0, time.Millisecond
	be := httptest.NewServer(fakellm.New(c).Handler())
	defer be.Close()
	bs := []*router.Backend{{Name: "f", URL: be.URL}}
	p, _ := router.New("round-robin", bs, 1.25)
	events := &capture{}
	gw := httptest.NewServer(New(Config{Publisher: events}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer gw.Close()

	// One key reused for two different prompts must give two bills, and an invalid key is not recorded.
	send := func(key, content string) string {
		b, _ := json.Marshal(openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: content}}, MaxTokens: 2})
		req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/chat/completions", bytes.NewReader(b))
		req.Header.Set("Idempotency-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.Header.Get("X-Request-Id")
	}
	ids := []string{send("reused-key-0001", "first prompt"), send("reused-key-0001", "a different prompt"), send("short", "x")}
	if len(events.events) != 3 {
		t.Fatalf("%d events", len(events.events))
	}
	for i, want := range []string{"reused-key-0001", "reused-key-0001", ""} {
		e := events.events[i]
		if e.RequestID != ids[i] || e.IdempotencyKey != want {
			t.Fatalf("event %d: request id %q (header %q), key %q", i, e.RequestID, ids[i], e.IdempotencyKey)
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("two requests with the same Idempotency-Key share one billing id: the second would not be billed")
	}
}

func TestDefaultMaxTokensIsForwarded(t *testing.T) {
	var seen []json.RawMessage
	var mu sync.Mutex
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&m)
		mu.Lock()
		seen = append(seen, m["max_tokens"])
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	})
	gw := limitedGateway(t, limit.Limits{}, backend) // DefaultMaxTokens 80
	for _, body := range []string{
		`{"messages":[{"role":"user","content":"x"}]}`,                           // no cap: the gateway adds its reservation
		`{"messages":[{"role":"user","content":"x"}],"max_tokens":4}`,            // the client's cap is kept
		`{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":7}`, // the newer field counts as a cap
	} {
		req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer sk-a")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if len(seen) != 3 || string(seen[0]) != "80" || string(seen[1]) != "4" || seen[2] != nil {
		t.Fatalf("backend saw max_tokens %q", seen)
	}
}

func TestUnreportedUsageIsEstimated(t *testing.T) {
	// A stream that ends without the backend's usage (here the backend sends none; a client that leaves
	// mid-stream ends the same way) is billed as an estimate, not as zero tokens.
	noUsage := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n"))
		for _, tok := range []string{"one", " two", " three"} {
			w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"" + tok + "\"}}]}\n\n"))
		}
		w.Write([]byte("data: [DONE]\n\n"))
	})
	be := httptest.NewServer(noUsage)
	defer be.Close()
	bs := []*router.Backend{{Name: "f", URL: be.URL}}
	p, _ := router.New("round-robin", bs, 1.25)
	events := &capture{}
	gw := httptest.NewServer(New(Config{Publisher: events}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer gw.Close()

	resp := chat(t, gw.URL, true) // "hello there": 2 prompt tokens by the gateway's count
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if len(events.events) != 1 {
		t.Fatalf("%d events", len(events.events))
	}
	if e := events.events[0]; e.UsageKnown || e.PromptTokens != 2 || e.CompletionTokens != 3 {
		t.Fatalf("event %+v: want an estimate of 2 prompt + 3 completion tokens, marked unknown", e)
	}
}

func TestBackendErrorIsNotEstimated(t *testing.T) {
	// A backend's 4xx carries no usage because the backend did no work: it is billed zero, not an estimate.
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"prompt is too long","code":400}}`))
	}))
	defer be.Close()
	bs := []*router.Backend{{Name: "f", URL: be.URL}}
	p, _ := router.New("round-robin", bs, 1.25)
	events := &capture{}
	gw := httptest.NewServer(New(Config{Publisher: events}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer gw.Close()

	resp := chat(t, gw.URL, true)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || len(events.events) != 1 {
		t.Fatalf("status %d, %d events", resp.StatusCode, len(events.events))
	}
	if e := events.events[0]; e.Status != http.StatusBadRequest || e.PromptTokens != 0 || e.CompletionTokens != 0 {
		t.Fatalf("event %+v: want status 400 and 0 tokens", e)
	}
}
