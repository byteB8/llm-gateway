// Command meter consumes usage events from Kafka and writes them to Postgres. Offsets are committed
// only after the database transaction commits, so a crash replays at most one batch, which the
// store ignores (see internal/meter).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/byteB8/llm-gateway/internal/meter"
	"github.com/byteB8/llm-gateway/internal/metrics"
	"github.com/byteB8/llm-gateway/internal/tracing"
	"github.com/byteB8/llm-gateway/internal/usage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	brokers := flag.String("kafka-brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := flag.String("usage-topic", "usage", "topic to consume")
	group := flag.String("group", "meter", "consumer group")
	crashAt := flag.Int("crash-before-commit", 0, "testing: exit after the Nth batch's database commit, before its offset commit")
	metricsAddr := flag.String("metrics-listen", ":9090", "address for GET /metrics")
	otlp := flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP/HTTP collector URL for traces (empty = tracing off)")
	flag.Parse()
	dsn := os.Getenv("PG_DSN") // from the environment so the password stays out of the process list
	if dsn == "" {
		log.Fatal("PG_DSN is not set")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	shutdownTracing, err := tracing.Setup(context.Background(), "meter", *otlp, 1.0)
	if err != nil {
		log.Fatalf("tracing: %v", err)
	}
	defer shutdownTracing(context.Background())
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	mm := metrics.NewMeter(reg)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}))
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			log.Fatal(err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	var store *meter.Store
	for backoff := time.Second; ; backoff = min(backoff*2, 30*time.Second) {
		s, err := meter.Open(ctx, dsn)
		if err == nil {
			store = s
			break
		}
		if ctx.Err() != nil {
			return
		}
		logger.Warn("database not ready; retrying", "err", err, "backoff", backoff.String())
		time.Sleep(backoff)
	}
	defer store.Close()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	logger.Info("meter consuming", "topic", *topic, "group", *group)
	go watchLag(ctx, kadm.NewClient(cl), *group, mm.Lag, logger)

	var total, fresh, malformed, batches int
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			logger.Warn("fetch error", "topic", t, "partition", p, "err", err)
		})
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		events := make([]usage.Event, 0, len(records))
		// One span per batch, linked to the trace of every request it bills (a batch has many parents).
		var links []trace.Link
		for _, r := range records {
			h := make(map[string]string, len(r.Headers))
			for _, kv := range r.Headers {
				h[kv.Key] = string(kv.Value)
			}
			if sc := trace.SpanContextFromContext(tracing.Extract(context.Background(), h)); sc.IsValid() {
				links = append(links, trace.Link{SpanContext: sc})
			}
		}
		bctx, batchSpan := tracer.Start(ctx, "meter.batch", trace.WithLinks(links...),
			trace.WithAttributes(attribute.Int("meter.records", len(records))))
		for _, r := range records {
			var e usage.Event
			if err := json.Unmarshal(r.Value, &e); err != nil || e.RequestID == "" {
				malformed++ // skip it: retrying a record that can never parse would stall the partition
				mm.Records.WithLabelValues("malformed").Inc()
				logger.Error("malformed usage event", "partition", r.Partition, "offset", r.Offset)
				continue
			}
			events = append(events, e)
		}
		// Retry the same batch until the database takes it; never commit offsets past unwritten events.
		began := time.Now()
		for backoff := 200 * time.Millisecond; ; backoff = min(backoff*2, 10*time.Second) {
			actx, applySpan := tracer.Start(bctx, "postgres.apply")
			n, err := store.Apply(actx, events)
			if err != nil {
				applySpan.SetStatus(codes.Error, err.Error())
			}
			applySpan.End()
			if err == nil {
				batchSpan.SetAttributes(attribute.Int("meter.new", n), attribute.Int("meter.duplicate", len(events)-n))
				fresh += n
				mm.Records.WithLabelValues("new").Add(float64(n))
				mm.Records.WithLabelValues("duplicate").Add(float64(len(events) - n))
				break
			}
			if ctx.Err() != nil {
				batchSpan.End()
				return // shutting down; the uncommitted batch is replayed on restart
			}
			mm.DBErrors.Inc()
			logger.Warn("database write failed; retrying", "events", len(events), "err", err, "backoff", backoff.String())
			time.Sleep(backoff)
		}
		mm.BatchSeconds.Observe(time.Since(began).Seconds())
		if batches++; batches == *crashAt {
			logger.Warn("injected crash between database commit and offset commit", "batch", batches)
			os.Exit(3)
		}
		if err := cl.CommitRecords(ctx, records...); err != nil {
			logger.Warn("offset commit failed; the batch will be replayed and ignored", "err", err)
			mm.CommitErrors.Inc()
		}
		batchSpan.End()
		total += len(records)
		logger.Info("batch", "records", len(records), "new", fresh, "seen", total, "malformed", malformed)
	}
	logger.Info("stopped", "seen", total, "new", fresh, "malformed", malformed)
}

// watchLag sets the gauge to the group's total lag (log end minus committed offset) every 5 s.
// Lag is the meter's own health signal: it grows when Postgres is down or the meter falls behind.
func watchLag(ctx context.Context, adm *kadm.Client, group string, g prometheus.Gauge, logger *slog.Logger) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		lags, err := adm.Lag(ctx, group)
		if err == nil {
			if gl, ok := lags[group]; ok && gl.Error() == nil {
				g.Set(float64(gl.Lag.Total()))
			}
		} else if ctx.Err() == nil {
			logger.Warn("consumer lag unavailable", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

var tracer = otel.Tracer("github.com/byteB8/llm-gateway/meter")
