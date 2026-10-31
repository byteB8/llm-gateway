// Package meter turns usage events into billing rows in Postgres. Kafka delivers at least once
// (a crash between the database commit and the offset commit replays a batch), so every write is
// idempotent: an event row is keyed by its request id, and the daily totals change only when that
// insert actually happened, in the same transaction.
package meter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/byteB8/llm-gateway/internal/usage"
)

const Schema = `
CREATE TABLE IF NOT EXISTS usage_events (
	request_id        TEXT PRIMARY KEY,
	idempotency_key   TEXT,
	tenant            TEXT        NOT NULL,
	backend           TEXT        NOT NULL,
	model             TEXT        NOT NULL,
	status            INT         NOT NULL,
	prompt_tokens     INT         NOT NULL,
	completion_tokens INT         NOT NULL,
	usage_known       BOOLEAN     NOT NULL,
	ts                TIMESTAMPTZ NOT NULL,
	inserted_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
CREATE TABLE IF NOT EXISTS usage_daily (
	tenant            TEXT   NOT NULL,
	day               DATE   NOT NULL,
	requests          BIGINT NOT NULL,
	prompt_tokens     BIGINT NOT NULL,
	completion_tokens BIGINT NOT NULL,
	PRIMARY KEY (tenant, day)
);`

// apply inserts one event and, only if it was new, adds it to the tenant's daily totals.
// Days are UTC dates. Rows affected is 1 for a new event and 0 for a replayed one.
const apply = `
WITH ins AS (
	INSERT INTO usage_events (request_id, tenant, backend, model, status, prompt_tokens, completion_tokens, usage_known, ts,
		idempotency_key)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''))
	ON CONFLICT (request_id) DO NOTHING
	RETURNING tenant, (ts AT TIME ZONE 'UTC')::date AS day, prompt_tokens, completion_tokens
)
INSERT INTO usage_daily (tenant, day, requests, prompt_tokens, completion_tokens)
SELECT tenant, day, 1, prompt_tokens, completion_tokens FROM ins
ON CONFLICT (tenant, day) DO UPDATE SET
	requests          = usage_daily.requests + 1,
	prompt_tokens     = usage_daily.prompt_tokens + EXCLUDED.prompt_tokens,
	completion_tokens = usage_daily.completion_tokens + EXCLUDED.completion_tokens`

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, Schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Apply writes a batch in one transaction and returns how many events were new.
func (s *Store) Apply(ctx context.Context, events []usage.Event) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	b := &pgx.Batch{}
	for _, e := range events {
		b.Queue(apply, e.RequestID, e.Tenant, e.Backend, e.Model, e.Status, e.PromptTokens, e.CompletionTokens, e.UsageKnown, e.Time,
			e.IdempotencyKey)
	}
	br := tx.SendBatch(ctx, b)
	inserted := 0
	for range events {
		tag, err := br.Exec()
		if err != nil {
			br.Close()
			return 0, err
		}
		inserted += int(tag.RowsAffected())
	}
	if err := br.Close(); err != nil {
		return 0, err
	}
	return inserted, tx.Commit(ctx)
}

type Totals struct {
	Requests, PromptTokens, CompletionTokens int64
}

// TenantTotals sums a tenant's daily rows.
func (s *Store) TenantTotals(ctx context.Context, tenant string) (Totals, error) {
	var t Totals
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(requests),0), COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0)
		FROM usage_daily WHERE tenant = $1`, tenant).Scan(&t.Requests, &t.PromptTokens, &t.CompletionTokens)
	return t, err
}
