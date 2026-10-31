// Package tracing sets up OpenTelemetry tracing: spans exported over OTLP/HTTP to a collector,
// W3C trace context propagation, and a parent-based ratio sampler. With no endpoint it exports
// nothing and every span is a no-op, so instrumented code costs almost nothing when tracing is off.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup installs the global tracer provider and propagator. endpoint is the collector's OTLP/HTTP
// base URL (e.g. http://otel-collector:4318); empty turns exporting off. ratio is the share of new
// traces sampled; a request that arrives with a sampled parent is always sampled.
func Setup(ctx context.Context, service, endpoint string, ratio float64) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service)), resource.WithFromEnv(),
		resource.WithTelemetrySDK(), resource.WithHost())
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// SampledTraceID returns the trace id of ctx's span if it is sampled (so the trace will exist in
// the backend), else "". Used to attach exemplars to latency observations.
func SampledTraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsSampled() {
		return ""
	}
	return sc.TraceID().String()
}

// Inject writes ctx's trace context into a plain map, e.g. to carry it in Kafka record headers.
func Inject(ctx context.Context) map[string]string {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c
}

// Extract returns ctx with the trace context found in m.
func Extract(ctx context.Context, m map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m))
}
