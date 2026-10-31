// Command gateway runs the LLM gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/byteB8/llm-gateway/internal/gateway"
	"github.com/byteB8/llm-gateway/internal/router"
	"github.com/byteB8/llm-gateway/internal/usage"
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
	brokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers for usage events (empty = off)")
	topic := flag.String("usage-topic", "usage", "Kafka topic for usage events")
	partitions := flag.Int("usage-partitions", 4, "partitions if the topic has to be created")
	buffered := flag.Int("usage-buffer", 10000, "usage events held in memory before new ones are dropped")
	drain := flag.Duration("drain", 30*time.Second, "on SIGTERM, wait this long for open streams to finish")
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

	var pub usage.Publisher = usage.Nop{}
	if *brokers != "" {
		bl := strings.Split(*brokers, ",")
		k, err := usage.NewKafka(bl, *topic, *buffered) // connects lazily; never blocks startup
		if err != nil {
			log.Fatal(err)
		}
		pub = k
		// Metering must not decide whether we serve: create the topic in the background and keep
		// trying until Kafka is reachable. Events produced before that are counted as failed.
		go func() {
			for backoff := time.Second; ; backoff = min(backoff*2, 30*time.Second) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := usage.EnsureTopic(ctx, bl, *topic, int32(*partitions))
				cancel()
				if err == nil {
					logger.Info("usage topic ready", "topic", *topic)
					return
				}
				logger.Warn("usage topic not ready; retrying", "err", err, "backoff", backoff.String())
				time.Sleep(backoff)
			}
		}()
	}

	g := gateway.New(gateway.Config{
		MaxAttempts:      *attempts,
		Tenants:          tenants,
		DefaultMaxTokens: *defaultMax,
		BackendKey:       os.Getenv("BACKEND_API_KEY"), // an env var keeps it out of the process list
		Publisher:        pub,
	}, backends, p, logger)

	srv := &http.Server{Addr: *addr, Handler: g.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		logger.Info("gateway listening", "addr", *addr, "policy", p.Name(), "backends", len(backends), "kafka", *brokers != "")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Kubernetes sends SIGTERM before stopping a pod: stop accepting, let open streams finish,
	// then flush the usage events still in memory.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	<-stop
	logger.Info("draining", "timeout", drain.String())
	ctx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Warn("drain timed out; open streams were cut", "err", err)
	}
	pub.Close()
	logger.Info("stopped")
}
