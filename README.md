# llm-gateway

[![ci](https://github.com/byteB8/llm-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/byteB8/llm-gateway/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-kind-326CE5?logo=kubernetes&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![vLLM](https://img.shields.io/badge/backends-vLLM-6E56CF)
![Prometheus](https://img.shields.io/badge/metrics-Prometheus-E6522C?logo=prometheus&logoColor=white)
![OpenTelemetry](https://img.shields.io/badge/traces-OpenTelemetry-425CC7?logo=opentelemetry&logoColor=white)

An OpenAI-compatible gateway in Go that sits in front of several LLM inference servers. It routes
streaming requests across backends with **prefix-cache-aware routing**, enforces **per-tenant limits**,
falls through to another backend when one fails, and publishes a usage event per request to **Kafka**;
a consumer turns those events into billing rows in **Postgres** without ever counting one twice. It
runs on **Kubernetes** and survives rolling restarts without dropping a request. It is observable end to
end: **Prometheus** metrics with trace exemplars, **OpenTelemetry** traces that follow a request from the
client through the backend and Kafka into the billing transaction, and **SLOs with multi-window burn-rate
alerts**, all as code and tested by injecting faults.

**Highlights** (every number is from a file in `results/`):
- **+27% throughput, −40% time-to-first-token** on two real vLLM servers under KV-cache pressure, by
  routing each system prompt to one GPU (prefix-cache hit rate 0.55 → 0.77).
- **0 of 3,000 requests failed** across two rolling restarts under load (108 without draining).
- **Billing exact to the token** through an injected consumer crash, and through Kafka and Postgres
  outages under load.
- **SLO alerts tested by injecting 11 faults under load**: all 9 expected pages fired close to the
  predicted time, none was missed, nothing paged falsely, and the failures users never saw (a crashed
  backend, a 503-ing one, Postgres down, Kafka down) opened tickets instead.
- **Observability costs +7.5% CPU per request for metrics, +20.5% exporting every trace**, and +0.08 ms
  of median time to first token.

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
    G-)K: usage event, id = a new id per served request
    K->>M: batch of events
    M->>P: one transaction: insert events ON CONFLICT DO NOTHING, add only new ones to daily totals
    M->>K: commit offsets, only after the database commit
```

A crash between the database commit and the offset commit makes Kafka resend the batch; the
`ON CONFLICT DO NOTHING` insert ignores it, so totals never double. The id is the gateway's own, one per
served request: two requests are two bills. A client's `Idempotency-Key` is stored with the event for audit
but never used to skip billing (it used to be the id, which let a client reuse one key and pay once).

## Observability

```mermaid
flowchart LR
    subgraph APP["namespace llmgw"]
        G["gateway × 2"]
        F["inference backends"]
        M["meter"]
    end
    subgraph OBS["namespace observability"]
        OC["OTel Collector<br/>memory limit · batch"]
        T[("Tempo")]
        P[("Prometheus<br/>recording + alert rules")]
        AM["Alertmanager"]
        GR["Grafana<br/>dashboards as code"]
        S["alert-sink<br/>(webhook receiver)"]
    end
    G & F & M -- "OTLP traces" --> OC --> T
    P -- "scrape every 5 s<br/>(PodMonitors)" --> G & F & M
    P -- "burn-rate pages,<br/>cause tickets" --> AM -- "webhook" --> S
    GR --> P
    GR -- "exemplar → trace" --> T
```

**Traces.** The gateway continues the client's W3C `traceparent` (or starts a trace), records spans for
admission and for each backend attempt (with a `first_byte` event, so time to first token is visible),
and passes the context on to the backend and into the **Kafka record headers** of the usage event. The
meter's span for each billing batch **links** to the request spans it billed, so one request can be
followed from the client to the Postgres transaction. 100% of traces are sampled by default
(`-trace-sample`); the collector, not the apps, decides where traces go.

**Metrics.** Requests by tenant, backend and status; time to first token and duration histograms with the
trace id as an **exemplar** (a dot on a latency panel opens that request's trace); per-backend in-flight
requests, capacity, failures and ejections; streams cut mid-response, by side; rate limiting and tokens
per tenant; and the billing pipeline: usage events published, failed or dropped, meter records new or
duplicate, and consumer lag.

**SLOs and alerts** (generated by `scripts/slo_rules.py`):

| SLO | Objective | A request is good when |
|---|---|---|
| Availability | 99.5% | no 5xx and no stream cut by a backend (4xx are the client's own) |
| Latency | 99% | the first token arrives within 500 ms |

Each SLO pages when its error budget burns **14.4× too fast over 1 h and 5 min**, or **6× over 6 h and
30 min** (the multi-window, multi-burn-rate method of the Google SRE Workbook). For the experiments the
windows are compressed 12× (5 min/30 s and 30 min/150 s) so a trial takes minutes, not hours; a traffic
guard keeps one failed request out of two from paging. Causes open tickets, not pages: a backend failing
every attempt for 30 s, usage events lost (requests that will never be billed), the meter more than 500
records behind, or the meter not reporting.

**Dashboards** are generated too (`scripts/dashboards.py`): burn rates per window against the thresholds,
traffic and errors, latency with exemplars, backends, tenants, and the billing pipeline.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/grafana-e07-dark.png">
  <img alt="The Grafana dashboard over the 2 h 17 min fault experiment: burn rates per window, traffic, errors and latency" src="docs/figures/grafana-e07-light.png">
</picture>

The dashboard over the whole fault experiment below: three availability faults (left) and two latency
faults (right) rise through the page thresholds. At the far left the 30-minute window still holds an
earlier test's errors, but the short windows are clean, so nothing paged: that is what the short window
of each alert is for.

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

### Do the alerts work? Faults injected under load

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/detect-dark.svg">
  <img alt="Predicted against measured time from fault to page for every page in the fault experiment" src="docs/figures/detect-light.svg">
</picture>

`scripts/e07_alerts.py` injected eleven faults, one at a time, into the kind deployment while requests
arrived at 8 per second (an open loop: arrivals never wait for answers, so a slow server cannot slow the
load), and recorded when each alert reached Alertmanager's webhook. Trials of the same SLO started at
least 30 minutes apart, so each began with a clean error budget. A page's predicted time is when the
long window's error ratio crosses the alert's threshold, given the error ratio and the traffic that
Prometheus measured during the fault.

| Fault | Clients saw | Pages: predicted → measured | Tickets |
|---|---|---|---|
| None (normal traffic), 30 min | no errors | none | none |
| All backends cut 50% of streams, 4 min | 50% of streams cut | fast 42 s → **46 s** · slow 104 s → **116 s** | none |
| One backend 1 s slower to first token, 14 min | 21% of first tokens > 500 ms | fast 3.5 min → **3.7 min** · slow 8.7 min → **8.8 min** | none |
| One backend down, 3 min | 3% of first tokens > 500 ms | none | backend failing 40 s |
| One backend answers 503, 3 min | 4% of first tokens > 500 ms | none | backend failing 40 s |
| Postgres down, 3 min | no errors | none | billing lag 2.2 min |
| All backends cut 10% of streams, 12 min | 10% of streams cut | fast 3.8 min → **4.4 min** (flapped) · slow 9.5 min → **9.6 min** | none |
| New client: 2 req/s of 3,000-token prompts, 13 min | the new client: 99% of its first tokens > 500 ms; everyone else: 1% of first tokens > 500 ms | fast 3.5 min → **3.4 min** (flapped) · slow 8.0 min → **8.1 min** | none |
| Kafka down (past the 30 s delivery timeout), 2 min | no errors | none | usage events lost 35 s (796 events lost) |
| A tenant floods at 20 req/s (limit 60/min), 3 min | the flooding tenant got 3,141 × 429 of 3,619 | none | none |
| All backends cut 5% of streams, 24 min | 5% of streams cut | slow 18.9 min → **18.4 min** · no fast page: burn 10.0× < 14.4× | none |

**9 of 9 expected pages fired, none was missed, and nothing paged outside the SLO faults.**
Apart from one flapping page, every page fired between 34 s early and 12 s late: a scrape and a rule
evaluation (5 s each) on top of the prediction, and the sampling noise of the error ratio itself (5% of
streams cut for 18 minutes is 430 ± 21 cut streams, ±5% on the crossing time). Three trials show the
method's edges:
- **Near the threshold, the fast page is late and flaps.** At 19× burn (10% of streams cut), 1.3× above
  the 14.4× threshold, it fired 40 s late and three times. The compressed 30 s confirmation window holds
  only ~240 requests, so a 10% error ratio dips under 7.2% by chance; at full scale that window is
  5 minutes, about 2,400 requests.
- **Faults change traffic.** The client with 3,000-token prompts also added 22% more requests, which
  fills the long window faster: ignoring that, the prediction would have been 7–14% late.
- **Slow burns need the second alert.** With 5% of streams cut (10.0× burn) the fast alert, at
  14.4×, never fires; the slow one paged after 18.4 minutes (predicted 18.9).

Failures that users never saw opened tickets, not pages: a crashed backend, a backend answering 503,
Postgres down, Kafka down. A tenant flooding past its limit got 429s and fired nothing: 4xx are the
client's own errors, not the service's.

### What the fault experiments found

1. **Silently truncated answers.** When a backend died after the first token, the gateway ended the
   response normally, and the client got a clean 200 with half an answer. It now aborts the connection,
   as `httputil.ReverseProxy` does, so the client sees an error.
2. **A broken backend attracts traffic.** A backend that fails fast holds no requests, so it looks idle
   to load-aware routing: while one backend answered every request with 503, 48% of requests tried it
   first (its usual share was about 30%). Outlier ejection, below, fixes this as well.
3. **A dead backend stalls every request sent to it.** An attempt on it often ran out the gateway's whole
   2 s dial timeout, which covers the DNS lookup and the connection (the run did not record which one ran
   out); other lookups took ~1.2 s to say "no such host", because the name walks the pod's `ndots:5`
   search list out to the upstream DNS.
   **Outlier ejection** fixes it: after three failures in a row a backend is tried last, for 5 s,
   doubling up to 30 s while it keeps failing. Fully qualified backend names
   (`fakellm-3.fakellm.llmgw.svc.cluster.local.`), which CoreDNS answers by itself, were tried on top:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/trace-dead-backend-dark.png">
  <img alt="A trace: the first attempt spends 1.2 s failing to resolve the dead backend's name, then another backend serves the request" src="docs/figures/trace-dead-backend-light.png">
</picture>

One request from Tempo, during the outage: 1.2 s lost on "no such host" for the dead backend, then
fakellm-0 served it.

| Fault (3 min) | Build | Failed attempts | Requests stalled > 1 s | First tokens > 500 ms | Ticket after |
|---|---|---|---|---|---|
| Backend down | no ejection (e07) | 444 | 24 | 3.1% | 40 s |
| Backend down | outlier ejection | 18 | 3 | 1.7% | 38 s |
| Backend down | ejection + fully qualified names | 14 | 9 | 2.2% | 34 s |
| Backend answers 503 | no ejection (e07) | 692 | 12 | 3.5% | 40 s |
| Backend answers 503 | outlier ejection | 19 | 5 | 2.3% | 38 s |
| Backend answers 503 | ejection + fully qualified names | 14 | 8 | 2.7% | 34 s |

Ejection cut the attempts wasted on the broken backend by 96–97% and removed the 2-second stalls; the
ticket still fired, a little sooner. The fully qualified names made no measurable
difference on top: with the backend ejected, too few lookups fail for their cost to show above run-to-run
noise. What is left, 1.7–2.7% of first tokens over 500 ms, is the price of serving on three backends
instead of four, with the lost backend's prompts now cache misses elsewhere.

4. **The fix would have silenced an alert.** With ejection, a dead backend gets one probe every 5–30 s,
   so an alert on its failure rate stops firing. The backend alert now fires on state instead: every
   attempt failed, for 30 s.
5. **A PromQL precedence bug.** `a + b or vector(0)` means `(a + b) or vector(0)`, and `a + b` is empty
   while either side has no series yet: each kind of failure was invisible until the other kind had
   happened once. Caught in review before the first run, and now a promtool regression test.
6. **Kafka down for 2 minutes lost 796 usage events**, those older than the producer's 30 s delivery
   timeout. Serving did not notice, and the "usage events lost" ticket fired after 35 s, but the
   events are gone. A longer timeout would trade memory for durability.

### What observability costs

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/figures/overhead-dark.svg">
  <img alt="CPU time per request for the gateway before observability, with metrics only, and with 10% and 100% of traces exported" src="docs/figures/overhead-light.svg">
</picture>

The gateway on 2 cores of a server, in front of a fake backend that streams a token per millisecond, so
the gateway's own work is what is measured: 30,000 streaming requests per run, five runs per variant in a
new random order each round (`bench/sustain/e08.sh`). "Before" is the commit before metrics and tracing
were added. Exporting every trace costs **+20.5% CPU per request but only +0.08 ms of median time to first
token**: spans are exported in batches by a background goroutine, off the request path.

CPU profiles (`-pprof`, `bench/sustain/e08_profile.sh`) split the cost. With every trace exported, the
OpenTelemetry SDK takes **10.7%** of the gateway's CPU (6.1 points of it encoding and sending OTLP), the
otelhttp wrappers 3.6% and the Prometheus client 0.9%. With tracing off, the wrappers still take 2.7%: they
carry a caller's `traceparent` through to the backend, so a trace that starts upstream is not broken at
the gateway. The gateway's own largest costs are elsewhere: system calls (27–34%), allocation and garbage
collection (about 30%) and JSON (12–13%).

## Features

- **Routing**: round-robin, least-loaded, or prefix-affinity (consistent hashing with bounded loads,
  Mirrokni et al. 2018) with a per-backend capacity (`-capacity`, vLLM's `max_num_seqs`).
- **Streaming and fallthrough**: responses pass through as they arrive; a request moves to the next
  backend only if the current one cannot be reached or answers with a 5xx. Once a backend sends another
  status the gateway commits to it, so one that dies after its status line but before any body byte is
  aborted, not retried.
- **Per-tenant limits**: requests/min, tokens/min and concurrent requests per API key; 401 for unknown
  keys, 429 with `Retry-After`; client keys are never forwarded to backends.
- **Usage events**: idempotent franz-go producer keyed by tenant; a bounded buffer that drops (and counts)
  rather than block serving when Kafka is down.
- **Kubernetes**: Kafka (KRaft) and Postgres StatefulSets, fake backends, gateway (×2, NodePort, probes,
  preStop drain), meter; one distroless image (about 66 MB) for all five commands.
- **Outlier ejection**: a backend that fails three attempts in a row is tried last for 5 s, doubling up to
  30 s while it keeps failing; one answered request restores it.
- **Observability**: Prometheus metrics with trace exemplars (OpenMetrics), OpenTelemetry traces across
  the gateway, backends and billing consumer, SLO burn-rate alerts and Grafana dashboards generated from
  Python, kube-prometheus-stack + Tempo + OTel Collector on kind, an opt-in pprof endpoint.
- **CI**: gofmt, vet and race-detector tests with real Kafka and Postgres service containers; the job
  fails if the integration tests are skipped.

## Layout

| Path | What it is |
|---|---|
| `cmd/gateway` | The gateway: `POST /v1/chat/completions`, `GET /backends`, `GET /stats`, `GET /health`; admin `GET /metrics`, `GET /drain` (and `/debug/pprof/` with `-pprof`) on `:8081` |
| `cmd/meter` | Kafka → Postgres metering consumer |
| `cmd/fakellm` | Fake inference server for load tests (decode slots, prefill cost, prefix cache) |
| `cmd/loadgen` | Load generator: closed loop (N clients) or open loop (Poisson arrivals), TTFT and latency percentiles |
| `cmd/alertsink` | Alertmanager webhook receiver that records when each alert arrived (for the alert experiments) |
| `internal/router` | Routing policies |
| `internal/gateway` | Forwarding, streaming, fallthrough, tenants, limits, usage capture, draining |
| `internal/limit` | Token buckets with reserve-and-refund |
| `internal/usage`, `internal/meter` | Usage events (Kafka) and idempotent billing (Postgres) |
| `internal/metrics`, `internal/tracing` | Prometheus metrics; OpenTelemetry setup and trace context in Kafka headers |
| `deploy/` | Dockerfile, kind config, Kubernetes manifests, local compose file |
| `deploy/observability` | Helm values (kube-prometheus-stack, Tempo), OTel Collector, PodMonitors, SLO rules, dashboards |
| `bench/sustain` | vLLM backends on a GPU server (`python run.py serve <name>`), the routing and overhead experiments |
| `bench/otlpsink` | OTLP receiver that counts and drops spans (the overhead experiment) |
| `scripts/` | Experiments (`e03`–`e07`), SLO rules and dashboards generators, local benchmark, figures (`plots.py`) |

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

With the monitoring stack (kind and Helm; the whole cluster uses about 3.2 GB of memory):

```sh
scripts/obs_up.sh    # cluster, app, kube-prometheus-stack, Tempo, collector, rules, dashboards
# Grafana http://127.0.0.1:3000 (admin/admin, dashboard "LLM Gateway") · Prometheus :9090 · Alertmanager :9093
```

## Known limits

- Rate limits are per gateway replica; with N replicas a tenant gets up to N× its limit. A shared store
  would fix that.
- When Kafka is down, usage events older than the producer's 30 s delivery timeout are lost (796 in a
  2-minute outage), as are events beyond the 10,000-event buffer. The loss is counted and alerted on, not
  recovered.
- Outlier ejection reacts to failures, not to slowness: a backend that answered 1 s late still got 21%
  of the traffic.
- Least-loaded herds (above); power-of-two-choices or a random tie-break would likely fix it. Not tried.
- Usage a backend never reported (a client that left mid-stream) is billed as an estimate, marked
  `usage_known=false`: the gateway's word count of the prompt and one token per streamed chunk.
- Kafka and Postgres run as single instances in the manifests; this is a laptop setup, not a production one.
- The SLO windows are compressed 12× for the experiments; `COMPRESSION = 1` in `scripts/slo_rules.py` gives
  the real 1 h / 6 h windows. Outlier-ejection state and rate limits are per gateway replica.
- Grafana allows anonymous read-only access so scripts can screenshot dashboards; the port is bound to
  127.0.0.1 on the laptop.
