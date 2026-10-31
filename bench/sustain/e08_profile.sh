#!/usr/bin/env bash
# e08, part 2: where does the observability cost go? CPU profiles of the gateway with tracing off and
# with every trace exported, under the same open-loop load (RATE req/s) and on the same CPUs as e08.sh.
# Profiles land in results/raw/e08-<variant>.pprof; read them with the matching gateway binary:
#   go tool pprof -top bin/linux/gateway results/sustain/raw/e08-off.pprof
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results/raw logs
RATE=${RATE:-2000}
LOAD=(-prefixes 16 -prefix-tokens 200 -suffix-tokens 20 -max-tokens 16)
GW=http://127.0.0.1:8200

taskset -c 9 bin/otlpsink -listen 127.0.0.1:4318 >logs/e08p-otlpsink.log 2>&1 &
sink=$!
taskset -c 2-5 bin/fakellm -listen 127.0.0.1:9101 -name f -slots 1024 -token-interval 1ms -prefill-per-token 0 \
  -batch-slowdown 0 >logs/e08p-fake.log 2>&1 &
fake=$!
trap 'kill $sink $fake 2>/dev/null || true' EXIT
up() { for _ in $(seq 50); do curl -sf "$1/health" >/dev/null && return 0; sleep 0.1; done; echo "no response from $1" >&2; exit 1; }
up http://127.0.0.1:9101

for v in off on100; do
  flags=()
  [ "$v" = on100 ] && flags=(-otlp-endpoint http://127.0.0.1:4318 -trace-sample 1)
  GOMAXPROCS=2 taskset -c 0-1 bin/gateway -listen 127.0.0.1:8200 -admin-listen 127.0.0.1:8201 -pprof \
    -backends f=http://127.0.0.1:9101 -policy round-robin "${flags[@]}" >"logs/e08p-$v.log" 2>&1 &
  gw=$!
  up "$GW"
  taskset -c 6-8 bin/loadgen -url "$GW" -rate "$RATE" -duration 35s -c 1024 "${LOAD[@]}" \
    -o "results/raw/e08p-$v-load.json" >/dev/null &
  load=$!
  sleep 5
  curl -sf "http://127.0.0.1:8201/debug/pprof/profile?seconds=25" -o "results/raw/e08-$v.pprof"
  wait "$load"
  kill "$gw"; wait "$gw" 2>/dev/null || true
  echo "$v: $(python3 -c "import json; s=json.load(open('results/raw/e08p-$v-load.json')); print(s['requests'], 'requests,', s['errors'], 'errors')")"
done
