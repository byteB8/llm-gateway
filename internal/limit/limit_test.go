package limit

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time           { return c.t }
func (c *clock) add(d time.Duration)      { c.t = c.t.Add(d) }
func newClock() *clock                    { return &clock{t: time.Unix(1_000_000, 0)} }
func admitted(_ *Ticket, d Decision) bool { return d.Allowed }

func TestRPMRefillsOverTime(t *testing.T) {
	c := newClock()
	l := New(c.now)
	l.Set("a", Limits{RPM: 60}) // 1 per second, burst 60
	for i := 0; i < 60; i++ {
		if !admitted(l.Admit("a", 0)) {
			t.Fatalf("request %d rejected within the burst", i)
		}
	}
	tk, d := l.Admit("a", 0)
	if tk != nil || d.Reason != "rpm" || d.RetryAfter != time.Second {
		t.Fatalf("61st request: %+v", d)
	}
	c.add(time.Second)
	if !admitted(l.Admit("a", 0)) {
		t.Fatal("not admitted after one second of refill")
	}
}

func TestTPMReservesAndRefunds(t *testing.T) {
	c := newClock()
	l := New(c.now)
	l.Set("a", Limits{TPM: 1000})
	tk, d := l.Admit("a", 800) // prompt + max_tokens reserved up front
	if !d.Allowed || d.RemainingTokens != 200 {
		t.Fatalf("first: %+v", d)
	}
	if _, d := l.Admit("a", 300); d.Reason != "tpm" {
		t.Fatalf("second should wait for tokens: %+v", d)
	}
	tk.Done(250) // used 250 of 800: refund 550
	if _, d := l.Admit("a", 300); !d.Allowed || d.RemainingTokens != 450 {
		t.Fatalf("after refund: %+v", d)
	}
}

func TestDoneWithUnknownUsageKeepsReservation(t *testing.T) {
	c := newClock()
	l := New(c.now)
	l.Set("a", Limits{TPM: 1000, MaxConcurrent: 1})
	tk, _ := l.Admit("a", 600)
	tk.Done(-1)
	tk.Done(10) // second call is ignored
	_, d := l.Admit("a", 500)
	if d.Reason != "tpm" {
		t.Fatalf("expected tpm rejection, got %+v", d)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	l := New(newClock().now)
	l.Set("a", Limits{MaxConcurrent: 2})
	t1, _ := l.Admit("a", 0)
	l.Admit("a", 0)
	if _, d := l.Admit("a", 0); d.Reason != "concurrency" {
		t.Fatalf("third concurrent request: %+v", d)
	}
	t1.Done(0)
	if !admitted(l.Admit("a", 0)) {
		t.Fatal("slot not freed by Done")
	}
}

func TestRejectionTakesNothing(t *testing.T) {
	c := newClock()
	l := New(c.now)
	l.Set("a", Limits{RPM: 60, TPM: 100})
	l.Admit("a", 100)                                // tokens now empty
	if _, d := l.Admit("a", 50); d.Reason != "tpm" { // rejected on tokens...
		t.Fatalf("%+v", d)
	}
	_, d := l.Admit("a", 0) // ...so it must not have used a request: 60 - 1 - this one
	if !d.Allowed || d.RemainingReqs != 58 {
		t.Fatalf("%+v", d)
	}
	c.add(30 * time.Second) // refill is capped at one minute's worth
	if _, d := l.Admit("a", 0); d.RemainingReqs != 59 {
		t.Fatalf("after refill: %+v", d)
	}
}

func TestTooLargeAndUnknownTenant(t *testing.T) {
	l := New(newClock().now)
	l.Set("a", Limits{TPM: 100})
	if _, d := l.Admit("a", 101); d.Reason != "too_large" {
		t.Fatalf("%+v", d)
	}
	tk, d := l.Admit("nobody", 1_000_000)
	if !d.Allowed {
		t.Fatal("unknown tenants are unlimited")
	}
	l.Set("nobody", Limits{MaxConcurrent: 1})
	tk.Done(0) // admitted before limits existed: must not free a slot it never took
	l.Admit("nobody", 0)
	if _, d := l.Admit("nobody", 0); d.Reason != "concurrency" {
		t.Fatalf("%+v", d)
	}
}
