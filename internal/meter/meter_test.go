package meter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/byteB8/llm-gateway/internal/usage"
)

// Runs only with a database: PG_DSN=postgres://meter:meter-dev@localhost:55432/meter go test ./internal/meter/
func store(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("PG_DSN not set")
	}
	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestReplayedBatchIsNotCountedTwice(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	tenant := "test-" + usage.NewRequestID()[:8] // fresh tenant: no rows from earlier runs
	batch := make([]usage.Event, 20)
	for i := range batch {
		batch[i] = usage.Event{RequestID: usage.NewRequestID(), Tenant: tenant, Backend: "b", Model: "m", Status: 200,
			PromptTokens: 10, CompletionTokens: 5, UsageKnown: true, Time: time.Now()}
	}
	if n, err := s.Apply(ctx, batch); err != nil || n != 20 {
		t.Fatalf("first apply: %d new, %v", n, err)
	}
	// Kafka redelivers the batch (crash before the offset commit), plus one new event.
	replay := append(batch, usage.Event{RequestID: usage.NewRequestID(), Tenant: tenant, Backend: "b", Model: "m",
		Status: 200, PromptTokens: 1, CompletionTokens: 1, UsageKnown: true, Time: time.Now()})
	if n, err := s.Apply(ctx, replay); err != nil || n != 1 {
		t.Fatalf("replay: %d new, %v", n, err)
	}
	tot, err := s.TenantTotals(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if tot != (Totals{Requests: 21, PromptTokens: 201, CompletionTokens: 101}) {
		t.Fatalf("totals %+v", tot)
	}
}
