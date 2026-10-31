# llm-gateway

[![ci](https://github.com/byteB8/llm-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/byteB8/llm-gateway/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-kind-326CE5?logo=kubernetes&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![vLLM](https://img.shields.io/badge/backends-vLLM-6E56CF)

An OpenAI-compatible gateway in Go that sits in front of several LLM inference servers. It routes
streaming requests across backends with **prefix-cache-aware routing**, enforces **per-tenant limits**,
falls through to another backend when one fails, and publishes a usage event per request to **Kafka**;
a consumer turns those events into billing rows in **Postgres** without ever counting one twice. It
runs on **Kubernetes** and survives rolling restarts without dropping a request.

**Highlights** (every number is from a file in `results/`):
- **+27% throughput, −40% time-to-first-token** on two real vLLM servers under KV-cache pressure, by
  routing each system prompt to one GPU (prefix-cache hit rate 0.55 → 0.77).
- **0 of 3,000 requests failed** across two rolling restarts under load (108 without draining).
- **Billing exact to the token** through an injected consumer crash, and through Kafka and Postgres
  outages under load.

## Architecture

```mermaid
flowchart LR
    C["Clients<br/>(OpenAI SDK, curl)"]
    subgraph K8S["Kubernetes"]
        G["gateway × 2 (Go)<br/>auth · limits · routing<br/>streaming · drain"]
        KF[("Kafka<br/>topic: usage")]
        M["meter (Go)"]
        PG[("Postgres<br/>usage_events<br/>usage_daily")]
    end
    V1["vLLM · GPU 0"]
    V2["vLLM · GPU 1"]
    C -->|"API key · streaming"| G
    G -->|"SSE"| V1
    G -->|"SSE"| V2
    G -.->|"usage event<br/>(never blocks a request)"| KF
    KF --> M
    M -->|"one transaction per batch,<br/>then commit offsets"| PG
```

### One request, end to end

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as Gateway
    participant V as vLLM
    participant K as Kafka
    participant M as Meter
    participant P as Postgres
    C->>G: POST /v1/chat/completions (stream, API key, Idempotency-Key)
    G->>G: tenant from key, reserve prompt + max_tokens
    G->>G: route by system-prompt hash, skip full backends
    G->>V: forward (asks for usage in the stream)
    V-->>G: tokens (SSE)
    G-->>C: tokens, flushed as they arrive
    V-->>G: last chunk carries usage
    G->>G: refund the unused reservation
    G-)K: usage event, id = Idempotency-Key or a new id
    K->>M: batch of events
    M->>P: one transaction: insert events ON CONFLICT DO NOTHING, add only new ones to daily totals
    M->>K: commit offsets, only after the database commit
```

A crash between the database commit and the offset commit makes Kafka resend the batch; the
`ON CONFLICT DO NOTHING` insert ignores it, so totals never double. A client that resends a request
with the same `Idempotency-Key` is billed once for the same reason.

## Results

### Prefix-aware routing on real GPUs

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/routing-dark.svg">
  <img alt="Hit rate, time to first token and throughput for three routing policies on two vLLM servers" src="docs/figures/routing-light.svg">
</picture>

Two vLLM servers (Qwen3-1.7B, one per RTX A4000, prefix caching on, `max_num_seqs` 16), 16 clients,
1,500 streaming requests with 32 output tokens each, two runs per policy. 96 distinct system prompts of
~1,500 tokens add up to ~144k prompt tokens against ~86.5k tokens of KV cache per GPU. Round-robin makes
each GPU hold all of them, so its cache thrashes; prefix-affinity gives each GPU half, which fits.

| Policy | Prefix-cache hit rate | TTFT p50 | Latency p50 | Throughput |
|---|---|---|---|---|
| round-robin | 0.56 / 0.55 | 139 / 144 ms | 804 / 793 ms | 19.2 / 19.1 req/s |
| least-loaded | 0.55 / 0.56 | 258 / 245 ms | 780 / 779 ms | 19.7 / 19.8 req/s |
| **prefix-affinity** | **0.78 / 0.77** | **84 / 87 ms** | **610 / 618 ms** | **24.4 / 24.2 req/s** |

The hit rate is vLLM's own count (`prefix_cache_hits` / `prefix_cache_queries`, in tokens).
With only 16 prompts, which fit on both GPUs, the policies are close (round-robin had the lowest TTFT,
61 ms vs 74–76 ms): affinity pays off when the cache is under pressure, not otherwise.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/herding-dark.svg">
  <img alt="Mean number of requests sent in a row to the same GPU, per routing policy" src="docs/figures/herding-light.svg">
</picture>

Least-loaded has the worst time-to-first-token although it balances the request count: vLLM finishes
requests in waves, so one GPU's in-flight count drops all at once and the next burst of arrivals goes
there. 91% of its requests arrived in same-GPU runs of four or more, piling prefills onto one GPU at a time.

### Rolling restarts under load

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/rollout-dark.svg">
  <img alt="Failed requests during rolling restarts for three shutdown designs" src="docs/figures/rollout-light.svg">
</picture>

```mermaid
sequenceDiagram
    participant K as Kubernetes
    participant G as Old gateway pod
    participant C as Clients
    K->>G: pod marked terminating, removed from the Service
    K->>G: preStop hook: GET :8081/drain
    G-->>C: every response now says "Connection: close" (5 s)
    C->>K: next requests open new connections, which go to other pods
    K->>G: SIGTERM
    G->>G: stop accepting, let open streams finish (up to 25 s), flush usage events
```

Draining alone protects streams already in progress, but a client's idle keep-alive connection to the
old pod still gets closed under it: 9 requests were written into closed connections. Sending
`Connection: close` during the preStop window moves clients off first. The admin port is not part of
the Service, so outside clients cannot trigger a drain.

### Billing correctness

| Test | Clients received | Billed |
|---|---|---|
| Meter killed after a DB commit, before its offset commit | 626 requests · 137,720 + 10,016 tokens (1,374 rate-limited) | 626 requests · 137,720 + 10,016 tokens |
| Kafka down 30 s, then Postgres down 30 s, under load | 2,400 requests · 2,520,000 + 153,600 tokens, 0 errors | 2,400 requests · 2,520,000 + 153,600 tokens |

Tokens are prompt + completion. Rate-limited requests are not billed.

## Features

- **Routing**: round-robin, least-loaded, or prefix-affinity (consistent hashing with bounded loads,
  Mirrokni et al. 2018) with a per-backend capacity (`-capacity`, vLLM's `max_num_seqs`).
- **Streaming and fallthrough**: responses pass through as they arrive; a request moves to the next
  backend only if the current one refuses or fails before any byte reached the client.
- **Per-tenant limits**: requests/min, tokens/min and concurrent requests per API key; 401 for unknown
  keys, 429 with `Retry-After`; client keys are never forwarded to backends.
- **Usage events**: idempotent franz-go producer keyed by tenant; a bounded buffer that drops (and counts)
  rather than block serving when Kafka is down.
- **Kubernetes**: Kafka (KRaft) and Postgres StatefulSets, fake backends, gateway (×2, NodePort, probes,
  preStop drain), meter; one 34 MB distroless image for all four commands.
- **CI**: gofmt, vet and race-detector tests with real Kafka and Postgres service containers; the job
  fails if the integration tests are skipped.

## Layout

| Path | What it is |
|---|---|
| `cmd/gateway` | The gateway: `POST /v1/chat/completions`, `GET /backends`, `GET /stats`, `GET /health`; admin `GET /drain` on `:8081` |
| `cmd/meter` | Kafka → Postgres metering consumer |
| `cmd/fakellm` | Fake inference server for load tests (decode slots, prefill cost, prefix cache) |
| `cmd/loadgen` | Load generator: concurrent streaming requests, TTFT and latency percentiles |
| `internal/router` | Routing policies |
| `internal/gateway` | Forwarding, streaming, fallthrough, tenants, limits, usage capture, draining |
| `internal/limit` | Token buckets with reserve-and-refund |
| `internal/usage`, `internal/meter` | Usage events (Kafka) and idempotent billing (Postgres) |
| `deploy/` | Dockerfile, kind config, Kubernetes manifests, local compose file |
| `bench/sustain` | vLLM backends on a GPU server (`python run.py serve <name>`) and the routing experiment |
| `scripts/` | Experiments (`e03`–`e06`), local benchmark, figures (`plots.py`) |

## Run it

```sh
go test -race ./...                                   # integration tests need PG_DSN / KAFKA_BROKERS, else skip
docker compose -f deploy/compose.yaml -p llmgw up -d  # local Kafka + Postgres
go build -o bin/ ./cmd/...
bin/fakellm -listen :9001 -name a & bin/fakellm -listen :9002 -name b &
bin/gateway -backends a=http://localhost:9001,b=http://localhost:9002 -capacity 8 -kafka-brokers localhost:9092 &
PG_DSN=postgres://meter:meter-dev@localhost:55432/meter bin/meter &
bin/loadgen -url http://localhost:8080 -c 16 -n 200
```

On Kubernetes (kind):

```sh
kind create cluster --name llmgw --config deploy/kind.yaml
docker build -t llm-gateway:dev . && kind load docker-image llm-gateway:dev --name llmgw
kubectl apply -k deploy/k8s
curl -H "Authorization: Bearer sk-search-dev" -d '{"messages":[{"role":"user","content":"hi"}]}' localhost:8080/v1/chat/completions
```

## Known limits

- Rate limits are per gateway replica; with N replicas a tenant gets up to N× its limit. A shared store
  would fix that.
- Kafka outages longer than the producer's 30 s delivery timeout, or more than 10,000 buffered events,
  lose usage events. The loss is counted in `GET /stats`, not recovered.
- Least-loaded herds (above); power-of-two-choices or a random tie-break would likely fix it. Not tried.
- Kafka and Postgres run as single instances in the manifests; this is a laptop setup, not a production one.
