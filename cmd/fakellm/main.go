// Command fakellm runs a fake OpenAI-compatible inference server for load tests.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/byteB8/llm-gateway/internal/fakellm"
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
	flag.Parse()

	log.Printf("fakellm %s listening on %s (%+v)", cfg.Name, *addr, cfg)
	log.Fatal(http.ListenAndServe(*addr, fakellm.New(cfg).Handler()))
}
