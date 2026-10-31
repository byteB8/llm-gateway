package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/byteB8/llm-gateway/internal/fakellm"
	"github.com/byteB8/llm-gateway/internal/limit"
	"github.com/byteB8/llm-gateway/internal/metrics"
	"github.com/byteB8/llm-gateway/internal/router"
)

func metered(t *testing.T, cfg Config, backends ...http.Handler) (*httptest.Server, *metrics.Gateway, *prometheus.Registry) {
	t.Helper()
	var spec []string
	for i, h := range backends {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		spec = append(spec, string(rune('a'+i))+"="+srv.URL)
	}
	bs, err := router.ParseBackends(strings.Join(spec, ","))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := router.New("round-robin", bs, 1.25)
	reg := prometheus.NewRegistry()
	cfg.Metrics = metrics.NewGateway(reg)
	gw := httptest.NewServer(New(cfg, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(gw.Close)
	return gw, cfg.Metrics, reg
}

func fakeHandler() http.Handler {
	c := fakellm.DefaultConfig("f")
	c.PrefillPerToken, c.TokenInterval = 0, time.Millisecond
	return fakellm.New(c).Handler()
}

func drain(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func TestMetricsForServedStream(t *testing.T) {
	gw, m, reg := metered(t, Config{}, fakeHandler())
	drain(chat(t, gw.URL, true)) // 2 prompt tokens, 4 completion tokens

	if v := testutil.ToFloat64(m.Requests.WithLabelValues("anonymous", "a", "200")); v != 1 {
		t.Fatalf("requests{200} = %v", v)
	}
	if v := testutil.ToFloat64(m.Tokens.WithLabelValues("anonymous", "completion")); v != 4 {
		t.Fatalf("completion tokens = %v", v)
	}
	if v := testutil.ToFloat64(m.Tokens.WithLabelValues("anonymous", "prompt")); v != 2 {
		t.Fatalf("prompt tokens = %v", v)
	}
	if n := testutil.CollectAndCount(m.TTFT); n != 1 {
		t.Fatalf("ttft series = %d", n)
	}
	// in-flight is read from the router at scrape time and must be back to zero
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP llmgw_backend_inflight Requests in flight to the backend.
# TYPE llmgw_backend_inflight gauge
llmgw_backend_inflight{backend="a"} 0
`), "llmgw_backend_inflight"); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsForRateLimit(t *testing.T) {
	tenants, _ := NewTenants([]Tenant{{Name: "team-a", Keys: []string{"sk-a"}, Limits: limit.Limits{RPM: 1}}})
	gw, m, _ := metered(t, Config{Tenants: tenants}, fakeHandler())
	drain(call(t, gw.URL, "sk-a")) // allowed: the bucket starts with one request
	resp := call(t, gw.URL, "sk-a")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request: %d", resp.StatusCode)
	}
	if v := testutil.ToFloat64(m.RateLimited.WithLabelValues("team-a", "rpm")); v != 1 {
		t.Fatalf("rate_limited{rpm} = %v", v)
	}
	if v := testutil.ToFloat64(m.Requests.WithLabelValues("team-a", "none", "429")); v != 1 {
		t.Fatalf("requests{429} = %v", v)
	}
	if v := testutil.ToFloat64(m.Requests.WithLabelValues("team-a", "a", "200")); v != 1 {
		t.Fatalf("requests{200} = %v", v)
	}
}

func TestMetricsCountConnectFailures(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	live := httptest.NewServer(fakeHandler())
	defer live.Close()
	bs, _ := router.ParseBackends("dead=" + deadURL + ",live=" + live.URL)
	p, _ := router.New("round-robin", bs, 1.25)
	m := metrics.NewGateway(prometheus.NewRegistry())
	gw := httptest.NewServer(New(Config{Metrics: m}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer gw.Close()

	drain(chat(t, gw.URL, false))
	if v := testutil.ToFloat64(m.BackendFailures.WithLabelValues("dead", "connect")); v != 1 {
		t.Fatalf("backend_failures{dead,connect} = %v", v)
	}
	if v := testutil.ToFloat64(m.Requests.WithLabelValues("anonymous", "live", "200")); v != 1 {
		t.Fatalf("requests{live,200} = %v", v)
	}
}

func TestMetricsBackendDyingMidStream(t *testing.T) {
	dying := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"))
		w.(http.Flusher).Flush()
		conn, _, _ := w.(http.Hijacker).Hijack() // drop the connection mid-response
		conn.Close()
	})
	gw, m, _ := metered(t, Config{}, dying)
	resp := chat(t, gw.URL, true)
	_, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil { // a clean end would pass the truncated answer off as complete
		t.Fatal("client read the cut stream without an error")
	}
	if v := testutil.ToFloat64(m.Interrupted.WithLabelValues("a", "backend")); v != 1 {
		t.Fatalf("interrupted{backend} = %v", v)
	}
	if v := testutil.ToFloat64(m.Interrupted.WithLabelValues("a", "client")); v != 0 {
		t.Fatalf("interrupted{client} = %v", v)
	}
}

func TestKnownSeriesStartAtZero(t *testing.T) {
	_, m, _ := metered(t, Config{}, fakeHandler(), fakeHandler())
	if n := testutil.CollectAndCount(m.BackendFailures); n != 4 { // 2 backends x {connect, status_5xx}
		t.Fatalf("%d backend-failure series before any request, want 4", n)
	}
	if n := testutil.CollectAndCount(m.Interrupted); n != 4 { // 2 backends x {client, backend}
		t.Fatalf("%d interrupted series, want 4", n)
	}
	if n := testutil.CollectAndCount(m.RateLimited); n != 4 { // "anonymous" x 4 reasons
		t.Fatalf("%d rate-limited series, want 4", n)
	}
}
