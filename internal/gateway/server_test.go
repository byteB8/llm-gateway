package gateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/byteB8/llm-gateway/internal/fakellm"
	"github.com/byteB8/llm-gateway/internal/openai"
	"github.com/byteB8/llm-gateway/internal/router"
)

func fake(name string) (*fakellm.Server, *httptest.Server) {
	c := fakellm.DefaultConfig(name)
	c.PrefillPerToken = 0
	c.TokenInterval = time.Millisecond
	s := fakellm.New(c)
	return s, httptest.NewServer(s.Handler())
}

func gatewayFor(t *testing.T, policy string, urls ...string) (*httptest.Server, []*router.Backend) {
	t.Helper()
	var spec []string
	for i, u := range urls {
		spec = append(spec, string(rune('a'+i))+"="+u)
	}
	bs, err := router.ParseBackends(strings.Join(spec, ","))
	if err != nil {
		t.Fatal(err)
	}
	p, err := router.New(policy, bs, 1.25)
	if err != nil {
		t.Fatal(err)
	}
	g := New(Config{}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return httptest.NewServer(g.Handler()), bs
}

func chat(t *testing.T, url string, stream bool) *http.Response {
	t.Helper()
	b, _ := json.Marshal(openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: "hello there"}}, Stream: stream, MaxTokens: 4})
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestStreamsThroughGateway(t *testing.T) {
	_, be := fake("one")
	defer be.Close()
	gw, bs := gatewayFor(t, "round-robin", be.URL)
	defer gw.Close()

	resp := chat(t, gw.URL, true)
	defer resp.Body.Close()
	if resp.Header.Get("X-Gateway-Backend") != "a" || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("headers %v", resp.Header)
	}
	var events []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			events = append(events, sc.Text())
		}
	}
	if len(events) != 6 || events[5] != "data: [DONE]" { // 4 tokens + usage chunk + [DONE]
		t.Fatalf("got %d events: %v", len(events), events)
	}
	if bs[0].Inflight() != 0 {
		t.Fatalf("inflight %d after request", bs[0].Inflight())
	}
}

func TestFallsThroughDeadBackend(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close() // connection refused from now on
	_, live := fake("live")
	defer live.Close()
	gw, bs := gatewayFor(t, "round-robin", dead.URL, live.URL)
	defer gw.Close()

	resp := chat(t, gw.URL, false) // round-robin starts at the dead backend
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Gateway-Backend") != "b" || resp.Header.Get("X-Gateway-Attempts") != "2" {
		t.Fatalf("status %d headers %v", resp.StatusCode, resp.Header)
	}
	if bs[0].Inflight() != 0 || bs[1].Inflight() != 0 {
		t.Fatal("inflight counters leaked")
	}
}

func TestRetriesOn5xxThenReports502(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	defer failing.Close()
	gw, _ := gatewayFor(t, "round-robin", failing.URL, failing.URL)
	defer gw.Close()

	resp := chat(t, gw.URL, false)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestClientCancelStopsBackend(t *testing.T) {
	s, be := fake("slow")
	defer be.Close()
	gw, bs := gatewayFor(t, "round-robin", be.URL)
	defer gw.Close()

	b, _ := json.Marshal(openai.ChatRequest{Messages: []openai.Message{{Role: "user", Content: "x"}}, Stream: true, MaxTokens: 10000})
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	bufio.NewReader(resp.Body).ReadString('\n') // first token arrived
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for s.Stats().Active != 0 || bs[0].Inflight() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("backend still busy after client left: %+v inflight %d", s.Stats(), bs[0].Inflight())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRejectsBadBody(t *testing.T) {
	gw, _ := gatewayFor(t, "round-robin", "http://127.0.0.1:1")
	defer gw.Close()
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestDrainingClosesConnections(t *testing.T) {
	_, be := fake("one")
	defer be.Close()
	bs, _ := router.ParseBackends("a=" + be.URL)
	p, _ := router.New("round-robin", bs, 1.25)
	g := New(Config{}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	gw := httptest.NewServer(g.Handler())
	defer gw.Close()

	resp := chat(t, gw.URL, false)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.Close {
		t.Fatal("connection closed before draining")
	}
	g.StartDraining()
	resp = chat(t, gw.URL, false)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if !resp.Close || resp.StatusCode != 200 {
		t.Fatalf("draining response: status %d, Close=%v", resp.StatusCode, resp.Close)
	}
}
