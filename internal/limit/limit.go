// Package limit enforces per-tenant limits: requests per minute, tokens per minute and streams in
// flight. A request's token cost is unknown until it finishes, so admission reserves the prompt
// tokens plus max_tokens and Done refunds what the response did not use.
package limit

import (
	"math"
	"sync"
	"time"
)

type Limits struct {
	RPM           float64 `json:"rpm"`            // requests per minute; 0 = unlimited
	TPM           float64 `json:"tpm"`            // tokens per minute; 0 = unlimited
	MaxConcurrent int     `json:"max_concurrent"` // requests in flight; 0 = unlimited
}

// Decision explains a rejection. RetryAfter is how long until the request could be admitted if
// nothing else happened; it is zero for the concurrency limit, which frees up on its own.
type Decision struct {
	Allowed         bool
	Reason          string // "rpm", "tpm", "concurrency" or "too_large"
	RetryAfter      time.Duration
	RemainingTokens float64
	RemainingReqs   float64
}

// bucket is a token bucket that holds up to one minute of its rate.
type bucket struct {
	rate, level float64 // per second; current amount
	last        time.Time
}

func (b *bucket) refill(now time.Time) {
	b.level = math.Min(b.rate*60, b.level+b.rate*now.Sub(b.last).Seconds())
	b.last = now
}

func (b *bucket) wait(amount float64) time.Duration {
	if b.level >= amount {
		return 0
	}
	return time.Duration((amount - b.level) / b.rate * float64(time.Second))
}

type tenantState struct {
	limits   Limits
	reqs     *bucket
	tokens   *bucket
	inflight int
}

type Limiter struct {
	mu      sync.Mutex
	now     func() time.Time
	tenants map[string]*tenantState
}

func New(now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{now: now, tenants: map[string]*tenantState{}}
}

// Set installs or replaces a tenant's limits; its buckets start full.
func (l *Limiter) Set(tenant string, lim Limits) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	st := &tenantState{limits: lim}
	if lim.RPM > 0 {
		st.reqs = &bucket{rate: lim.RPM / 60, level: lim.RPM, last: now}
	}
	if lim.TPM > 0 {
		st.tokens = &bucket{rate: lim.TPM / 60, level: lim.TPM, last: now}
	}
	if old := l.tenants[tenant]; old != nil {
		st.inflight = old.inflight
	}
	l.tenants[tenant] = st
}

// Ticket is an admitted request. Call Done exactly once with the tokens it actually used.
type Ticket struct {
	l        *Limiter
	tenant   string
	tracked  bool // Admit counted it in flight
	reserved float64
	once     sync.Once
}

// Admit checks every limit and, only if all pass, takes one request and `cost` tokens.
// Unknown tenants are unlimited.
func (l *Limiter) Admit(tenant string, cost int) (*Ticket, Decision) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.tenants[tenant]
	if st == nil {
		return &Ticket{l: l, tenant: tenant}, Decision{Allowed: true}
	}
	now := l.now()
	d := Decision{Allowed: true, RemainingTokens: -1, RemainingReqs: -1}
	if st.limits.MaxConcurrent > 0 && st.inflight >= st.limits.MaxConcurrent {
		return nil, Decision{Reason: "concurrency"}
	}
	if st.reqs != nil {
		st.reqs.refill(now)
		if w := st.reqs.wait(1); w > 0 {
			return nil, Decision{Reason: "rpm", RetryAfter: w, RemainingReqs: st.reqs.level}
		}
	}
	if st.tokens != nil {
		st.tokens.refill(now)
		if float64(cost) > st.tokens.rate*60 {
			return nil, Decision{Reason: "too_large"} // more than a full minute of tokens: never fits
		}
		if w := st.tokens.wait(float64(cost)); w > 0 {
			return nil, Decision{Reason: "tpm", RetryAfter: w, RemainingTokens: st.tokens.level}
		}
		st.tokens.level -= float64(cost)
		d.RemainingTokens = st.tokens.level
	}
	if st.reqs != nil {
		st.reqs.level--
		d.RemainingReqs = st.reqs.level
	}
	st.inflight++
	return &Ticket{l: l, tenant: tenant, tracked: true, reserved: float64(cost)}, d
}

// Done releases the in-flight slot and refunds reserved tokens the request did not use. Pass a
// negative count when the usage is unknown; the reservation is then kept.
func (t *Ticket) Done(actual int) {
	t.once.Do(func() {
		t.l.mu.Lock()
		defer t.l.mu.Unlock()
		st := t.l.tenants[t.tenant]
		if !t.tracked || st == nil {
			return
		}
		st.inflight--
		if st.tokens != nil && actual >= 0 && float64(actual) < t.reserved {
			st.tokens.refill(t.l.now())
			st.tokens.level = math.Min(st.tokens.rate*60, st.tokens.level+t.reserved-float64(actual))
		}
	})
}
