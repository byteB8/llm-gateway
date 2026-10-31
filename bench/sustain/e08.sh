#!/usr/bin/env bash
# e08: what does observability cost the gateway? CPU time per request and client latency for:
#   pre    the gateway before metrics and tracing were added (commit 8b54d20)
#   off    metrics on, tracing off (no OTLP endpoint)
#   on10   metrics and tracing, 10% of traces sampled and exported
#   on100  metrics and tracing, every trace exported
# A fast fake backend (1 ms per token, no prefill) makes the gateway's own work visible. Every round runs
# the four in a new random order. Throughput phase: closed loop, CPU per request from /proc. Latency
# phase: open loop at RATE req/s. CPUs (the 10-core budget on this machine): gateway 0-1 (GOMAXPROCS=2),
# fake backend 2-5, load generator 6-8, OTLP sink 9.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results/raw logs
REPEATS=${REPEATS:-5}
N=${N:-30000}
RATE=${RATE:-500}
LOAD=(-prefixes 16 -prefix-tokens 200 -suffix-tokens 20 -max-tokens 16)
GW=http://127.0.0.1:8200

taskset -c 9 bin/otlpsink -listen 127.0.0.1:4318 >logs/e08-otlpsink.log 2>&1 &
sink=$!
taskset -c 2-5 bin/fakellm -listen 127.0.0.1:9101 -name f -slots 1024 -token-interval 1ms -prefill-per-token 0 \
  -batch-slowdown 0 >logs/e08-fake.log 2>&1 &
fake=$!
trap 'kill $sink $fake 2>/dev/null || true' EXIT
ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }  # user + system CPU time, in clock ticks
spans() { curl -s http://127.0.0.1:4318/count | python3 -c 'import json,sys; print(json.load(sys.stdin)["spans"])'; }
up() { for _ in $(seq 50); do curl -sf "$1/health" >/dev/null && return 0; sleep 0.1; done; echo "no response from $1" >&2; exit 1; }
up http://127.0.0.1:9101

for rep in $(seq 1 "$REPEATS"); do
  for v in $(printf '%s\n' pre off on10 on100 | shuf); do
    bin=bin/gateway; flags=()
    case $v in
      pre) bin=bin/gateway-pre ;;
      on10) flags=(-otlp-endpoint http://127.0.0.1:4318 -trace-sample 0.1) ;;
      on100) flags=(-otlp-endpoint http://127.0.0.1:4318 -trace-sample 1) ;;
    esac
    tag="e08-$v-r$rep"
    GOMAXPROCS=2 taskset -c 0-1 "$bin" -listen 127.0.0.1:8200 -admin-listen 127.0.0.1:8201 \
      -backends f=http://127.0.0.1:9101 -policy round-robin "${flags[@]}" >"logs/$tag.log" 2>&1 &
    gw=$!
    up "$GW"
    s0=$(spans)
    taskset -c 6-8 bin/loadgen -url "$GW" -c 64 -n 3000 "${LOAD[@]}" >/dev/null  # warm-up
    c0=$(ticks "$gw")
    taskset -c 6-8 bin/loadgen -url "$GW" -c 64 -n "$N" "${LOAD[@]}" -o "results/raw/$tag-tput.json" >/dev/null
    c1=$(ticks "$gw")
    taskset -c 6-8 bin/loadgen -url "$GW" -rate "$RATE" -duration 20s -c 512 "${LOAD[@]}" -o "results/raw/$tag-lat.json" >/dev/null
    rss=$(awk '/VmRSS/ {print $2}' "/proc/$gw/status")
    kill "$gw"; wait "$gw" 2>/dev/null || true  # on exit the gateway flushes the spans it still holds
    s1=$(spans)
    python3 - "$tag" "$v" "$rep" "$((c1 - c0))" "$(getconf CLK_TCK)" "$rss" "$((s1 - s0))" <<'PY'
import json, sys
tag, v, rep, ticks, hz, rss, spans = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])
t = json.load(open(f"results/raw/{tag}-tput.json"))
l = json.load(open(f"results/raw/{tag}-lat.json"))
ok = t["requests"] - t["errors"]
lat_ok = l["requests"] - l["errors"]
out = {"variant": v, "rep": rep, "requests": t["requests"], "errors": t["errors"] + l["errors"],
       "throughput_rps": round(t["req_per_sec"], 1), "cpu_us_per_request": round(ticks / hz / ok * 1e6, 1),
       "latency_rate": l["config"]["rate"], "ttft_ms": l["ttft_ms"], "total_ms": l["total_ms"], "rss_mb": round(rss / 1024, 1),
       "spans_exported": spans, "spans_per_request": round(spans / (ok + lat_ok + 3000), 2)}
json.dump(out, open(f"results/{tag}.json", "w"), indent=2)
print(f'{tag:16} {out["throughput_rps"]:8.0f} req/s  {out["cpu_us_per_request"]:6.1f} us CPU/req  '
      f'ttft p50={l["ttft_ms"]["p50"]:.2f} p99={l["ttft_ms"]["p99"]:.2f} ms  rss={out["rss_mb"]} MB  '
      f'spans/req={out["spans_per_request"]}  errors={out["errors"]}', flush=True)
PY
  done
done
echo done
