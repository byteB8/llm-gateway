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
	flag.Parse()

	backends, err := router.ParseBackends(*spec)
	if err != nil {
		log.Fatal(err)
	}
	p, err := router.New(*policy, backends, *loadFactor)
	if err != nil {
		log.Fatal(err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	g := gateway.New(gateway.Config{MaxAttempts: *attempts}, backends, p, logger)
	logger.Info("gateway listening", "addr", *addr, "policy", p.Name(), "backends", len(backends))
	log.Fatal(http.ListenAndServe(*addr, g.Handler()))
}
