package router

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func pool(n int) []*Backend {
	bs := make([]*Backend, n)
	for i := range bs {
		bs[i] = &Backend{Name: fmt.Sprintf("b%d", i), URL: fmt.Sprintf("http://b%d", i)}
	}
	return bs
}

func names(bs []*Backend) string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = b.Name
	}
	return strings.Join(out, " ")
}

func TestParseBackends(t *testing.T) {
	bs, err := ParseBackends("a=http://x:1/, http://y:2")
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 2 || bs[0].Name != "a" || bs[0].URL != "http://x:1" || bs[1].Name != "b1" {
		t.Fatalf("got %+v %+v", bs[0], bs[1])
	}
	if _, err := ParseBackends(" , "); err == nil {
		t.Fatal("empty spec should fail")
	}
}

func TestRoundRobinCycles(t *testing.T) {
	p := &RoundRobin{backends: pool(3)}
	var got string
	for i := 0; i < 6; i++ {
		got += p.Order("")[0].Name
	}
	if got != "b0b1b2b0b1b2" {
		t.Fatalf("order %s", got)
	}
}

func TestLeastLoadedPicksIdle(t *testing.T) {
	bs := pool(3)
	bs[0].Acquire()
	bs[1].Acquire()
	bs[1].Acquire()
	p := &LeastLoaded{backends: bs}
	for i := 0; i < 3; i++ {
		if o := p.Order(""); o[0] != bs[2] || o[2] != bs[1] {
			t.Fatalf("order %s %s %s", o[0].Name, o[1].Name, o[2].Name)
		}
	}
}

func TestPrefixAffinityIsSticky(t *testing.T) {
	p := NewPrefixAffinity(pool(4), 1.25)
	first := p.Order("system prompt A")[0]
	for i := 0; i < 10; i++ {
		if p.Order("system prompt A")[0] != first {
			t.Fatal("same key moved to another backend")
		}
	}
	if len(p.Order("x")) != 4 {
		t.Fatal("order must list every backend")
	}
}

func TestPrefixAffinitySpreadsKeys(t *testing.T) {
	p := NewPrefixAffinity(pool(4), 1.25)
	count := map[string]int{}
	for i := 0; i < 4000; i++ {
		count[p.Order(fmt.Sprintf("prefix-%d", i))[0].Name]++
	}
	for name, c := range count {
		if c < 700 || c > 1300 { // 1000 each when perfectly even
			t.Fatalf("%s got %d of 4000 keys: %v", name, c, count)
		}
	}
}

func TestPrefixAffinitySpillsWhenOverloaded(t *testing.T) {
	bs := pool(4)
	p := NewPrefixAffinity(bs, 1.25)
	home := p.Order("hot")[0]
	for i := 0; i < 5; i++ { // limit is ceil(1.25*(5+1)/4) = 2, so 5 in flight is over it
		home.Acquire()
	}
	if got := p.Order("hot")[0]; got == home {
		t.Fatal("overloaded home backend was still chosen first")
	}
	for i := 0; i < 5; i++ {
		home.Release()
	}
	if p.Order("hot")[0] != home {
		t.Fatal("key did not return home once load dropped")
	}
}

func TestPrefixAffinitySkipsFullBackend(t *testing.T) {
	bs := pool(4)
	for _, b := range bs {
		b.Capacity = 3
	}
	p := NewPrefixAffinity(bs, 1.25)
	order := p.Order("hot")
	home, spare := order[0], order[3]
	for _, b := range bs { // home 3 (full), others 3, 3, 2: total 11, load limit ceil(1.25*12/4) = 4
		n := 3
		if b == spare {
			n = 2
		}
		for i := 0; i < n; i++ {
			b.Acquire()
		}
	}
	if got := p.Order("hot")[0]; got != spare {
		t.Fatalf("chose %s, want the only backend with a free slot (%s)", got.Name, spare.Name)
	}
	spare.Acquire() // now every backend is full: queue at home, where the prefix is cached
	if got := p.Order("hot")[0]; got != home {
		t.Fatalf("all full: chose %s, want home %s", got.Name, home.Name)
	}
}

func TestEjectionAfterFailuresInARow(t *testing.T) {
	b := &Backend{Name: "a"}
	now := time.Unix(1000, 0)
	for i := 1; i < EjectAfter; i++ {
		if d := b.Failed(now); d != 0 || b.Ejected(now) {
			t.Fatalf("ejected after %d failures", i)
		}
	}
	if d := b.Failed(now); d != 5*time.Second || !b.Ejected(now.Add(4*time.Second)) || b.Ejected(now.Add(6*time.Second)) {
		t.Fatalf("first ejection %v", d)
	}
	if d := b.Failed(now.Add(time.Second)); d != 0 {
		t.Fatalf("a failure while ejected extended the ejection by %v", d)
	}
	// Still failing when it comes back: ejected again, for longer, up to the cap.
	at := now.Add(6 * time.Second)
	for _, want := range []time.Duration{10 * time.Second, 20 * time.Second, 30 * time.Second, 30 * time.Second} {
		if d := b.Failed(at); d != want {
			t.Fatalf("ejected for %v, want %v", d, want)
		}
		at = at.Add(want + time.Second)
	}
	b.Succeeded()
	if b.Ejected(at) || b.ConsecutiveFailures() != 0 || b.Ejections() != 5 {
		t.Fatalf("after a success: ejected=%v failures=%d ejections=%d", b.Ejected(at), b.ConsecutiveFailures(), b.Ejections())
	}
	if d := b.Failed(at); d != 0 {
		t.Fatal("one failure after a success ejected the backend")
	}
}

func TestHealthyFirst(t *testing.T) {
	a, b, c := &Backend{Name: "a"}, &Backend{Name: "b"}, &Backend{Name: "c"}
	now := time.Unix(1000, 0)
	for i := 0; i < EjectAfter; i++ {
		a.Failed(now)
	}
	if got := names(HealthyFirst([]*Backend{a, b, c}, now)); got != "b c a" {
		t.Fatalf("order %q", got)
	}
	for i := 0; i < EjectAfter; i++ {
		b.Failed(now)
		c.Failed(now)
	}
	if got := names(HealthyFirst([]*Backend{a, b, c}, now)); got != "a b c" { // all ejected: keep the order
		t.Fatalf("order %q", got)
	}
}
