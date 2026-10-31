// Command loadgen sends concurrent streaming chat requests and prints a JSON latency summary.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/byteB8/llm-gateway/internal/loadgen"
)

func main() {
	var c loadgen.Config
	flag.StringVar(&c.URL, "url", "http://localhost:8080", "gateway or backend base URL")
	flag.IntVar(&c.Concurrency, "c", 32, "concurrent clients")
	flag.IntVar(&c.Requests, "n", 500, "total requests")
	flag.IntVar(&c.Prefixes, "prefixes", 16, "distinct system prompts")
	flag.IntVar(&c.PrefixTokens, "prefix-tokens", 1000, "tokens per system prompt")
	flag.IntVar(&c.SuffixTokens, "suffix-tokens", 50, "tokens per user message")
	flag.IntVar(&c.MaxTokens, "max-tokens", 64, "output tokens per request")
	flag.Float64Var(&c.Zipf, "zipf", 0, "prefix popularity skew (> 1), 0 for uniform")
	flag.Int64Var(&c.Seed, "seed", 1, "random seed")
	flag.StringVar(&c.Model, "model", "fake", "model name sent in the request")
	flag.StringVar(&c.Key, "key", "", "API key sent as a bearer token")
	out := flag.String("o", "", "also write the summary to this file")
	raw := flag.String("raw", "", "write one JSON line per request to this file")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s, results := loadgen.Run(ctx, c)
	b, _ := json.MarshalIndent(s, "", "  ")
	os.Stdout.Write(append(b, '\n'))
	if *out != "" {
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if *raw != "" {
		f, err := os.Create(*raw)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(f)
		for _, r := range results {
			enc.Encode(r)
		}
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
