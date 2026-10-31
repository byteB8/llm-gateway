package usage

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRequestIDsAreUniqueHex(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewRequestID()
		if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" || seen[id] {
			t.Fatalf("bad or repeated id %q", id)
		}
		seen[id] = true
	}
}

func TestEventJSONFieldNames(t *testing.T) {
	b, _ := json.Marshal(Event{RequestID: "r", Tenant: "t", PromptTokens: 3, Time: time.Unix(0, 0).UTC()})
	for _, f := range []string{`"request_id":"r"`, `"tenant":"t"`, `"prompt_tokens":3`, `"usage_known":false`, `"ts":"1970-01-01T00:00:00Z"`} {
		if !strings.Contains(string(b), f) {
			t.Fatalf("%s missing from %s", f, b)
		}
	}
}

// TestKafkaPublishes runs only with a broker: KAFKA_BROKERS=localhost:9092 go test ./internal/usage/
func TestKafkaPublishes(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := EnsureTopic(ctx, strings.Split(brokers, ","), "usage-test", 2); err != nil {
		t.Fatal(err)
	}
	if err := EnsureTopic(ctx, strings.Split(brokers, ","), "usage-test", 2); err != nil {
		t.Fatalf("second EnsureTopic must accept an existing topic: %v", err)
	}
	k, err := NewKafka(strings.Split(brokers, ","), "usage-test", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		k.Publish(Event{RequestID: NewRequestID(), Tenant: "t", Time: time.Now()})
	}
	k.Close()
	if s := k.Stats(); s.Published != 50 || s.Failed != 0 || s.Dropped != 0 {
		t.Fatalf("stats %+v", s)
	}
}
