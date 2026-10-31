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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/byteB8/llm-gateway/internal/meter"
	"github.com/byteB8/llm-gateway/internal/usage"
)

func main() {
	brokers := flag.String("kafka-brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := flag.String("usage-topic", "usage", "topic to consume")
	group := flag.String("group", "meter", "consumer group")
	crashAt := flag.Int("crash-before-commit", 0, "testing: exit after the Nth batch's database commit, before its offset commit")
	flag.Parse()
	dsn := os.Getenv("PG_DSN") // from the environment so the password stays out of the process list
	if dsn == "" {
		log.Fatal("PG_DSN is not set")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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
		for _, r := range records {
			var e usage.Event
			if err := json.Unmarshal(r.Value, &e); err != nil || e.RequestID == "" {
				malformed++ // skip it: retrying a record that can never parse would stall the partition
				logger.Error("malformed usage event", "partition", r.Partition, "offset", r.Offset)
				continue
			}
			events = append(events, e)
		}
		// Retry the same batch until the database takes it; never commit offsets past unwritten events.
		for backoff := 200 * time.Millisecond; ; backoff = min(backoff*2, 10*time.Second) {
			n, err := store.Apply(ctx, events)
			if err == nil {
				fresh += n
				break
			}
			if ctx.Err() != nil {
				return // shutting down; the uncommitted batch is replayed on restart
			}
			logger.Warn("database write failed; retrying", "events", len(events), "err", err, "backoff", backoff.String())
			time.Sleep(backoff)
		}
		if batches++; batches == *crashAt {
			logger.Warn("injected crash between database commit and offset commit", "batch", batches)
			os.Exit(3)
		}
		if err := cl.CommitRecords(ctx, records...); err != nil {
			logger.Warn("offset commit failed; the batch will be replayed and ignored", "err", err)
		}
		total += len(records)
		logger.Info("batch", "records", len(records), "new", fresh, "seen", total, "malformed", malformed)
	}
	logger.Info("stopped", "seen", total, "new", fresh, "malformed", malformed)
}
