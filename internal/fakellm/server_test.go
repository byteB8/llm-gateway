package fakellm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/byteB8/llm-gateway/internal/openai"
)

func fastConfig() Config {
	c := DefaultConfig("t")
	c.PrefillPerToken = 2 * time.Millisecond
	c.TokenInterval = time.Millisecond
	return c
}

func post(t *testing.T, url string, req openai.ChatRequest) *http.Response {
	t.Helper()
	b, _ := json.Marshal(req)
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestStreamSendsAllTokensThenDone(t *testing.T) {
	srv := httptest.NewServer(New(fastConfig()).Handler())
	defer srv.Close()
	resp := post(t, srv.URL, openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: "hi"}}, Stream: true, MaxTokens: 5})
	defer resp.Body.Close()

	var tokens int
	var done bool
	var usage *openai.Usage
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimPrefix(sc.Text(), "data: ")
		if line == "" || line == sc.Text() {
			continue
		}
		if line == "[DONE]" {
			done = true
			continue
		}
		var c openai.Chunk
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatal(err)
		}
		if c.Choices[0].Delta.Content != "" {
			tokens++
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if tokens != 5 || !done {
		t.Fatalf("got %d tokens, done=%v", tokens, done)
	}
	if usage == nil || usage.CompletionTokens != 5 || usage.PromptTokens != 1 {
		t.Fatalf("usage = %+v", usage)
	}
}

func TestPrefixCacheSkipsPrefill(t *testing.T) {
	s := New(fastConfig())
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	system := strings.Repeat("word ", 100) // 100 tokens = 200 ms of prefill when uncached
	req := openai.ChatRequest{Messages: []openai.Message{{Role: "system", Content: system}, {Role: "user", Content: "q"}}, MaxTokens: 1}

	timed := func() (time.Duration, string) {
		start := time.Now()
		resp := post(t, srv.URL, req)
		resp.Body.Close()
		return time.Since(start), resp.Header.Get("X-Prefix-Cache")
	}
	cold, h1 := timed()
	warm, h2 := timed()
	if h1 != "miss" || h2 != "hit" {
		t.Fatalf("cache headers %q then %q", h1, h2)
	}
	if warm > cold/2 {
		t.Fatalf("warm request %v not much faster than cold %v", warm, cold)
	}
	if s.Stats().CacheHits != 1 {
		t.Fatalf("stats %+v", s.Stats())
	}
}

func TestLRUEvictsOldest(t *testing.T) {
	c := newLRU(2)
	c.touch(1)
	c.touch(2)
	c.touch(3) // evicts 1
	if c.touch(1) {
		t.Fatal("1 should have been evicted")
	}
	if !c.touch(3) {
		t.Fatal("3 should still be cached")
	}
}

func TestInjectedFaults(t *testing.T) {
	s := New(fastConfig())
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	req := openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: "hi"}}, Stream: true, MaxTokens: 5}

	if err := s.SetFault(Fault{ErrorRate: 2}); err == nil {
		t.Fatal("rate above 1 must be rejected")
	}
	resp, err := http.Post(srv.URL+"/fault", "application/json", strings.NewReader(`{"error_rate": 1}`))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("POST /fault: %v %v", err, resp.StatusCode)
	}
	resp.Body.Close()
	if r := post(t, srv.URL, req); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("with error_rate 1: status %d", r.StatusCode)
	}

	s.SetFault(Fault{ExtraLatencyMs: 150})
	start := time.Now()
	r := post(t, srv.URL, req)
	r.Body.Close()
	if time.Since(start) < 150*time.Millisecond {
		t.Fatalf("extra latency not applied: %v", time.Since(start))
	}

	s.SetFault(Fault{CutRate: 1})
	r = post(t, srv.URL, req)
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if strings.Contains(string(body), "[DONE]") || !strings.Contains(string(body), "tok0") {
		t.Fatalf("cut stream should have the first token and no [DONE]: %q", body)
	}

	s.SetFault(Fault{})
	r = post(t, srv.URL, req)
	body, _ = io.ReadAll(r.Body)
	r.Body.Close()
	if !strings.Contains(string(body), "[DONE]") {
		t.Fatal("clearing the fault should restore normal streams")
	}
}
