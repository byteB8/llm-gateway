// Command fakellm runs a fake OpenAI-compatible inference server for load tests.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/byteB8/llm-gateway/internal/fakellm"
	"github.com/byteB8/llm-gateway/internal/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	cfg := fakellm.DefaultConfig("fake")
	addr := flag.String("listen", ":9000", "listen address")
	flag.StringVar(&cfg.Name, "name", cfg.Name, "backend name returned in X-Backend")
	flag.IntVar(&cfg.Slots, "slots", cfg.Slots, "sequences decoded at once")
	flag.DurationVar(&cfg.PrefillPerToken, "prefill-per-token", cfg.PrefillPerToken, "prefill time per uncached prompt token")
	flag.DurationVar(&cfg.TokenInterval, "token-interval", cfg.TokenInterval, "time per output token with one active sequence")
	flag.Float64Var(&cfg.BatchSlowdown, "batch-slowdown", cfg.BatchSlowdown, "extra per-token time with every slot busy, as a fraction")
	flag.IntVar(&cfg.CacheEntries, "cache-entries", cfg.CacheEntries, "prefixes kept in the prefix cache")
	flag.IntVar(&cfg.DefaultMaxTokens, "max-tokens", cfg.DefaultMaxTokens, "output tokens when the request sets none")
	otlp := flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP/HTTP collector URL for traces (empty = tracing off)")
	flag.Parse()

	if _, err := tracing.Setup(context.Background(), "fakellm", *otlp, 1.0); err != nil {
		log.Fatalf("tracing: %v", err)
	}

	log.Printf("fakellm %s listening on %s (%+v)", cfg.Name, *addr, cfg)
	// Continue the gateway's trace (traceparent header) with a server span per chat request.
	h := otelhttp.NewHandler(fakellm.New(cfg).Handler(), "fakellm", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path == "/v1/chat/completions"
	}))
	log.Fatal(http.ListenAndServe(*addr, h))
}
