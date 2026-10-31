// Package usage carries one event per gateway request to Kafka. Publishing never blocks a
// request: when Kafka is slow or down and the client buffer is full, the event is dropped and
// counted, because serving traffic matters more than metering it.
package usage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Event struct {
	RequestID        string    `json:"request_id"`
	Tenant           string    `json:"tenant"`
	Backend          string    `json:"backend"`
	Model            string    `json:"model"`
	Status           int       `json:"status"`
	Stream           bool      `json:"stream"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	UsageKnown       bool      `json:"usage_known"` // false when the backend never reported usage
	TTFBms           float64   `json:"ttfb_ms"`
	DurationMs       float64   `json:"duration_ms"`
	Time             time.Time `json:"ts"`

	// Trace is the request's trace context (W3C traceparent), sent as Kafka record headers, not in the JSON.
	Trace map[string]string `json:"-"`
}

// NewRequestID returns 16 random bytes in hex; it is the idempotency key downstream.
func NewRequestID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type Publisher interface {
	Publish(Event)
	Close()
}

// Nop discards events; the gateway uses it when no Kafka brokers are configured.
type Nop struct{}

func (Nop) Publish(Event) {}
func (Nop) Close()        {}

type Stats struct {
	Published, Failed, Dropped int64
	LastError                  string
}

type Kafka struct {
	client *kgo.Client
	topic  string
	stats  struct{ published, failed, dropped atomic.Int64 }
	errMu  sync.Mutex
	err    string
}

// NewKafka connects an idempotent producer (franz-go's default, acks=all), so retries inside the
// client cannot write an event twice. Events are keyed by tenant, which keeps each tenant's events
// in order on one partition.
func NewKafka(brokers []string, topic string, maxBuffered int) (*Kafka, error) {
	c, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.MaxBufferedRecords(maxBuffered),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordDeliveryTimeout(30*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &Kafka{client: c, topic: topic}, nil
}

func (k *Kafka) Publish(e Event) {
	v, _ := json.Marshal(e)
	rec := &kgo.Record{Key: []byte(e.Tenant), Value: v}
	for k, val := range e.Trace {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(val)})
	}
	k.client.TryProduce(context.Background(), rec, func(_ *kgo.Record, err error) {
		switch {
		case err == nil:
			k.stats.published.Add(1)
		case err == kgo.ErrMaxBuffered:
			k.stats.dropped.Add(1)
		default:
			k.stats.failed.Add(1)
			k.errMu.Lock()
			k.err = err.Error()
			k.errMu.Unlock()
		}
	})
}

func (k *Kafka) Stats() Stats {
	k.errMu.Lock()
	defer k.errMu.Unlock()
	return Stats{k.stats.published.Load(), k.stats.failed.Load(), k.stats.dropped.Load(), k.err}
}

// Close waits up to 10 s for buffered events, then closes the client.
func (k *Kafka) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	k.client.Flush(ctx)
	k.client.Close()
}

// EnsureTopic creates the topic if it does not exist yet. Topics are created explicitly, with a
// chosen partition count, instead of relying on the broker's auto-creation defaults.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int32) error {
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer c.Close()
	// The single-topic CreateTopic returns the topic's own error as err, not only in the response.
	_, err = kadm.NewClient(c).CreateTopic(ctx, partitions, -1, nil, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	return nil
}
