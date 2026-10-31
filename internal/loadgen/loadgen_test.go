package loadgen

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/byteB8/llm-gateway/internal/fakellm"
)

func TestRunAgainstFakeBackend(t *testing.T) {
	c := fakellm.DefaultConfig("f")
	c.TokenInterval = time.Millisecond
	c.PrefillPerToken = 0
	srv := httptest.NewServer(fakellm.New(c).Handler())
	defer srv.Close()

	s, rs := Run(context.Background(), Config{URL: srv.URL, Concurrency: 4, Requests: 20, Prefixes: 2,
		PrefixTokens: 10, SuffixTokens: 5, MaxTokens: 3, Seed: 1})
	if s.Errors != 0 || s.Requests != 20 || s.PerBackend["f"] != 20 || len(rs) != 20 {
		t.Fatalf("summary %+v", s)
	}
	if s.TTFTms.P50 <= 0 || s.TotalMs.P99 < s.TTFTms.P99 {
		t.Fatalf("latencies %+v %+v", s.TTFTms, s.TotalMs)
	}
	if s.CacheHitRate < 0.5 { // 2 prefixes, 20 requests: only the first use of each misses
		t.Fatalf("cache hit rate %.2f", s.CacheHitRate)
	}
}

func TestPercentiles(t *testing.T) {
	p := percentiles([]float64{5, 1, 4, 2, 3})
	if p.P50 != 3 || p.P99 != 5 || p.Mean != 3 {
		t.Fatalf("%+v", p)
	}
}
