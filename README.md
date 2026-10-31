# llm-gateway

[![ci](https://github.com/byteB8/llm-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/byteB8/llm-gateway/actions/workflows/ci.yml)

An OpenAI-compatible gateway in Go in front of several LLM inference servers (vLLM). It routes
streaming requests across backends, enforces per-tenant limits, falls through to another backend when
one fails, and publishes a usage event per request to Kafka; a consumer turns those events into
billing rows in Postgres without ever counting one twice. It runs on Kubernetes.

```
clients ──► gateway (Go, ×2 on Kubernetes) ──► vLLM / fake backends
              │  routing · limits · streaming · drain
              └─► Kafka "usage" ──► meter (Go) ──► Postgres (usage_events, usage_daily)
```

## What it does

- **Routing**: round-robin, least-loaded, or **prefix-affinity**: requests that share a system prompt go
  to the same backend so its prefix (KV) cache is reused. Consistent hashing with bounded loads keeps a
  hot prompt from overloading one backend, and a backend at capacity (`-capacity`, vLLM's
  `max_num_seqs`) is skipped while another has room.
- **Streaming**: responses are passed through as they arrive. A request moves to the next backend only
  if the current one refuses or returns a 5xx before any byte reached the client.
- **Per-tenant limits**: API keys map to tenants with requests/min, tokens/min and concurrent-request
  limits. A request reserves prompt + `max_tokens` tokens; the unused part is refunded when the
  backend reports usage. 401 for unknown keys, 429 with `Retry-After` over a limit. Client keys are
  never forwarded to backends.
- **Usage events → billing**: one Kafka event per request (franz-go, idempotent producer, keyed by
  tenant). Publishing never blocks a request. The meter writes each batch in one Postgres transaction
  and commits offsets only afterwards; events are keyed by request id (the client's `Idempotency-Key`
  if sent), so a replayed batch or a resent request is billed once.
- **Kubernetes**: manifests for kind (Kafka KRaft, Postgres, fake backends, gateway, meter). Rolling
  updates drain through a preStop hook: 5 s of `Connection: close`, then up to 25 s for open streams.

## Results

All numbers come from the JSON files in `results/`; the scripts that produced them are in `scripts/`
and `bench/sustain/`.

**Rolling restarts under load** (kind, 32 clients, 3000 streaming requests, gateway restarted twice):

| Shutdown handling | Failed requests |
|---|---|
| none (no preStop, no drain) | 108 / 3000, of which 63 streams cut mid-response |
| preStop sleep + 25 s drain | 9 / 3000 (requests written into closed keep-alive connections) |
| preStop `/drain`: `Connection: close` for 5 s, then 25 s drain | **0 / 3000, in two runs** |

**Billing correctness**
- Meter crashed on purpose after a database commit and before its offset commit: Kafka redelivered
  the batch and billing still matched what clients received exactly (626 requests, 137,720 prompt and
  10,016 completion tokens; 1,374 rate-limited requests not billed).
- Kafka down for 30 s, then Postgres down for 30 s, under load: 2400 / 2400 requests served, and billing
  exact (2400 requests, 2,520,000 prompt and 153,600 completion tokens; 0 events lost).

**Routing on two real vLLM servers** (Qwen3-1.7B, one per RTX A4000, prefix caching on, `max_num_seqs`
16; 16 clients, 1500 streaming requests, 32 output tokens). 96 distinct system prompts of ~1,500 tokens:
about 144k prompt tokens against ~86.5k tokens of KV cache per GPU. Two runs per policy:

| Policy | Prefix-cache hit rate | TTFT p50 | Latency p50 | Throughput |
|---|---|---|---|---|
| round-robin | 0.56 / 0.55 | 139 / 144 ms | 804 / 793 ms | 19.2 / 19.1 req/s |
| least-loaded | 0.55 / 0.56 | 258 / 245 ms | 780 / 779 ms | 19.7 / 19.8 req/s |
| **prefix-affinity** | **0.78 / 0.77** | **84 / 87 ms** | **610 / 618 ms** | **24.4 / 24.2 req/s** |

Routing each system prompt to one GPU halves each GPU's working set, so it fits in the KV cache: the
hit rate goes from 0.55 to 0.77, median time-to-first-token drops ~40% and throughput rises ~27%.
The hit rate is vLLM's own token count (`prefix_cache_hits` / `prefix_cache_queries`).

Least-loaded is the worst on time-to-first-token. vLLM finishes requests in waves, so one GPU's in-flight
count drops all at once and the next burst of arrivals goes to it: it sent 5.95 requests in a row to the
same GPU on average (max 16), against 1.01 for round-robin, piling prefills onto one GPU at a time.

With only 16 prompts (all fit on both GPUs) the policies are close; round-robin had the lowest TTFT
(61 vs 74–76 ms for prefix-affinity). Fake-backend runs (`results/local/e01`, `e02`) showed the same
pattern: affinity pays off when there is cache pressure and spare capacity, not at saturation.

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
- Kafka and Postgres run as single instances in the manifests; this is a laptop setup, not a production one.
