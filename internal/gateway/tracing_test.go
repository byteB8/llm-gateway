package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/byteB8/llm-gateway/internal/metrics"
	"github.com/byteB8/llm-gateway/internal/router"
	"github.com/byteB8/llm-gateway/internal/tracing"
)

func TestTraceFollowsTheRequest(t *testing.T) {
	if _, err := tracing.Setup(context.Background(), "test", "", 1); err != nil { // W3C propagator
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	defer otel.SetTracerProvider(prev)

	var gotParent string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotParent = r.Header.Get("traceparent")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`))
	}))
	defer backend.Close()
	bs, _ := router.ParseBackends("a=" + backend.URL)
	p, _ := router.New("round-robin", bs, 1.25)
	reg := prometheus.NewRegistry()
	events := &capture{}
	g := New(Config{Metrics: metrics.NewGateway(reg), Publisher: events}, bs, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	gw := httptest.NewServer(otelhttp.NewHandler(g.Handler(), "gateway"))
	defer gw.Close()

	drain(chat(t, gw.URL, false))

	spans := rec.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}
	server, ok := byName["gateway"]
	if !ok {
		t.Fatalf("no server span among %d spans", len(spans))
	}
	traceID := server.SpanContext().TraceID().String()
	for _, name := range []string{"admit", "backend a"} {
		s, ok := byName[name]
		if !ok || s.SpanContext().TraceID().String() != traceID {
			t.Fatalf("span %q missing or in another trace", name)
		}
	}
	attempt := byName["backend a"]
	if len(attempt.Events()) == 0 || attempt.Events()[0].Name != "first_byte" {
		t.Fatalf("attempt span events %v", attempt.Events())
	}
	if !strings.Contains(gotParent, traceID) {
		t.Fatalf("backend got traceparent %q, want trace %s", gotParent, traceID)
	}
	if tp := events.events[0].Trace["traceparent"]; !strings.Contains(tp, traceID) {
		t.Fatalf("usage event trace context %q", tp)
	}

	// The TTFT observation carries the trace id as an exemplar.
	mfs, _ := reg.Gather()
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "llmgw_ttft_seconds" {
			continue
		}
		for _, b := range mf.GetMetric()[0].GetHistogram().GetBucket() {
			for _, l := range b.GetExemplar().GetLabel() {
				if l.GetName() == "trace_id" && l.GetValue() == traceID {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("no exemplar with the request's trace id on llmgw_ttft_seconds")
	}
}
