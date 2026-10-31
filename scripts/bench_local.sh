#!/usr/bin/env bash
# Start N fake backends and the gateway on this machine, run the load generator once per routing
# policy and save each summary to results/local/<tag>-<policy>.json.
#   scripts/bench_local.sh [tag] [extra loadgen flags...]
# Backend and load settings come from the environment (defaults below).
set -euo pipefail
cd "$(dirname "$0")/.."
TAG=${1:-run}; shift || true
N=${N:-4}
POLICIES=${POLICIES:-"round-robin least-loaded prefix-affinity"}
FAKE_FLAGS=${FAKE_FLAGS:-""}
GATEWAY_FLAGS=${GATEWAY_FLAGS:-""}
OUT=results/local; RAW=results/raw/local
mkdir -p "$OUT" "$RAW" bin
go build -o bin/ ./cmd/...

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

wait_up() { for _ in $(seq 50); do curl -sf "$1/health" >/dev/null && return 0; sleep 0.1; done; echo "no response from $1" >&2; exit 1; }

for policy in $POLICIES; do
  pids=()
  spec=""
  for i in $(seq 1 "$N"); do
    port=$((9000 + i))
    # shellcheck disable=SC2086
    bin/fakellm -listen "127.0.0.1:$port" -name "fake$i" $FAKE_FLAGS >"$RAW/fake$i.log" 2>&1 &
    pids+=($!)
    spec+="fake$i=http://127.0.0.1:$port,"
  done
  # shellcheck disable=SC2086
  bin/gateway -listen 127.0.0.1:8080 -backends "${spec%,}" -policy "$policy" $GATEWAY_FLAGS >"$RAW/gateway-$TAG-$policy.log" 2>&1 &
  pids+=($!)
  for i in $(seq 1 "$N"); do wait_up "http://127.0.0.1:$((9000 + i))"; done
  wait_up http://127.0.0.1:8080
  bin/loadgen -url http://127.0.0.1:8080 -o "$OUT/$TAG-$policy.json" -raw "$RAW/$TAG-$policy.jsonl" "$@" >/dev/null
  python3 - "$OUT/$TAG-$policy.json" "$policy" <<'EOF'
import json, sys
s = json.load(open(sys.argv[1]))
t, e = s["ttft_ms"], s["total_ms"]
print(f'{sys.argv[2]:16} ok={s["requests"]-s["errors"]:4} err={s["errors"]} hit={s["cache_hit_rate"]:.2f} '
      f'ttft p50={t["p50"]:7.1f} p99={t["p99"]:7.1f} | total p50={e["p50"]:7.1f} p99={e["p99"]:7.1f} | '
      f'{s["req_per_sec"]:.1f} req/s | {s["per_backend"]}')
EOF
  cleanup
done
