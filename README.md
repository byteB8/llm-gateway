# llm-gateway

An OpenAI-compatible gateway in Go that sits in front of several LLM inference servers.
It routes streaming requests across backends, limits per-tenant load, retries failed backends
and publishes usage events to Kafka. It runs on Kubernetes.

Work in progress: routing, streaming and fallthrough work; Kafka metering and the Kubernetes
deployment are next.

## Layout

| Path | What it is |
|---|---|
| `cmd/gateway` | The gateway: `POST /v1/chat/completions`, `GET /backends`, `GET /health` |
| `cmd/fakellm` | A fake inference server for load tests (decode slots, prefill cost, prefix cache) |
| `cmd/loadgen` | Load generator: concurrent streaming requests, TTFT and latency percentiles |
| `internal/router` | Routing policies: round-robin, least-loaded, prefix-affinity |
| `internal/gateway` | Request forwarding, streaming pass-through, fallthrough on backend failure |
| `scripts/bench_local.sh` | Starts fake backends and the gateway, runs every policy, saves summaries |

## Routing policies

- **round-robin**: next backend in turn.
- **least-loaded**: the backend with the fewest requests in flight.
- **prefix-affinity**: requests with the same system prompt go to the same backend so its prefix cache
  is reused. Consistent hashing with bounded loads keeps a hot prompt from overloading one backend, and
  a backend at capacity (`-capacity`, e.g. vLLM's `max_num_seqs`) is skipped while another has room.

A request moves to the next backend only if the current one refuses the connection or returns a 5xx
before any byte has reached the client.

## Run locally

```sh
go test -race ./...
go build -o bin/ ./cmd/...
bin/fakellm -listen :9001 -name a &
bin/fakellm -listen :9002 -name b &
bin/gateway -backends a=http://localhost:9001,b=http://localhost:9002 -policy prefix-affinity -capacity 8 &
bin/loadgen -url http://localhost:8080 -c 16 -n 200
```

`scripts/bench_local.sh <tag> [loadgen flags]` runs all three policies against four fake backends and
writes `results/local/<tag>-<policy>.json`. The fake backend's costs are rough guesses, not
measurements of a real server.
