package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	// Each request reserves 2 prompt + 80 default max tokens = 82 of 100, but uses 2 + 4 = 6.
	// Without the refund the second request would find only 18 tokens left.
	gw := limitedGateway(t, limit.Limits{TPM: 100}, nil)
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

func TestIdempotencyKeyBecomesRequestID(t *testing.T) {
	gw := limitedGateway(t, limit.Limits{}, nil)
	b, _ := json.Marshal(openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: "x"}}})
	for key, want := range map[string]bool{"retry-key-0001": true, "short": false, "has space inside": false} {
		req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/chat/completions", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer sk-a")
		req.Header.Set("Idempotency-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("X-Request-Id") == key; got != want {
			t.Fatalf("key %q: request id %q", key, resp.Header.Get("X-Request-Id"))
		}
	}
}
