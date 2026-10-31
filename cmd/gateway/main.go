// Command gateway runs the LLM gateway.
package main

import (
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/byteB8/llm-gateway/internal/gateway"
	"github.com/byteB8/llm-gateway/internal/router"
)

func main() {
	addr := flag.String("listen", ":8080", "listen address")
	spec := flag.String("backends", "", `backends as "name=url,name=url"`)
	policy := flag.String("policy", "prefix-affinity", "round-robin | least-loaded | prefix-affinity")
	loadFactor := flag.Float64("load-factor", 1.25, "prefix-affinity: skip a backend above this multiple of the average load")
	attempts := flag.Int("attempts", 0, "backends tried per request (0 = all)")
	capacity := flag.Int64("capacity", 0, "requests each backend serves at once, e.g. vLLM max_num_seqs (0 = unknown)")
	tenantsFile := flag.String("tenants", "", "JSON file of tenants, keys and limits (empty = no authentication)")
	defaultMax := flag.Int("default-max-tokens", 256, "tokens reserved for a request without max_tokens")
	flag.Parse()

	backends, err := router.ParseBackends(*spec)
	if err != nil {
		log.Fatal(err)
	}
	for _, b := range backends {
		b.Capacity = *capacity
	}
	p, err := router.New(*policy, backends, *loadFactor)
	if err != nil {
		log.Fatal(err)
	}
	var tenants *gateway.Tenants
	if *tenantsFile != "" {
		if tenants, err = gateway.LoadTenants(*tenantsFile); err != nil {
			log.Fatal(err)
		}
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	g := gateway.New(gateway.Config{
		MaxAttempts:      *attempts,
		Tenants:          tenants,
		DefaultMaxTokens: *defaultMax,
		BackendKey:       os.Getenv("BACKEND_API_KEY"), // an env var keeps it out of the process list
	}, backends, p, logger)
	logger.Info("gateway listening", "addr", *addr, "policy", p.Name(), "backends", len(backends))
	log.Fatal(http.ListenAndServe(*addr, g.Handler()))
}
