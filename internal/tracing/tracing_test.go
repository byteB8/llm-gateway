package tracing

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	if _, err := Setup(context.Background(), "test", "", 1); err != nil { // installs the propagator only
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("t").Start(context.Background(), "parent")
	defer span.End()

	headers := Inject(ctx)
	if headers["traceparent"] == "" {
		t.Fatalf("no traceparent in %v", headers)
	}
	got := Extract(context.Background(), headers)
	if SampledTraceID(got) != span.SpanContext().TraceID().String() {
		t.Fatalf("trace id lost: %q vs %q", SampledTraceID(got), span.SpanContext().TraceID())
	}
}

func TestSampledTraceIDEmptyWithoutSampledSpan(t *testing.T) {
	if id := SampledTraceID(context.Background()); id != "" {
		t.Fatalf("got %q", id)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	ctx, span := tp.Tracer("t").Start(context.Background(), "x")
	defer span.End()
	if id := SampledTraceID(ctx); id != "" {
		t.Fatalf("unsampled span gave %q", id)
	}
}
