#!/usr/bin/env bash
# e03: does billing in Postgres match what clients actually received, when the meter crashes after a
# database commit but before its Kafka offset commit (so Kafka replays that batch)?
# Needs deploy/compose.yaml running. Writes results/local/e03-<tenant>.json.
set -euo pipefail
cd "$(dirname "$0")/.."
TENANT=${TENANT:-e03-$(date +%H%M%S)}
KEY=sk-$TENANT
RAW=results/raw/e03; OUT=results/local
mkdir -p "$RAW" "$OUT"
export PG_DSN=${PG_DSN:-postgres://meter:meter-dev@localhost:55432/meter}
psql() { docker exec llmgw-postgres-1 psql -U meter -d meter -tAc "$1"; }
go build -o bin/ ./cmd/...
printf '{"tenants":[{"name":"%s","keys":["%s"],"limits":{"rpm":600}}]}\n' "$TENANT" "$KEY" >"$RAW/tenants-$TENANT.json"

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
spec=""
for i in 1 2 3 4; do
  bin/fakellm -listen "127.0.0.1:$((9200 + i))" -name "fake$i" -token-interval 5ms -prefill-per-token 20us >/dev/null 2>&1 &
  pids+=($!); spec+="fake$i=http://127.0.0.1:$((9200 + i)),"
done
bin/gateway -listen 127.0.0.1:8280 -backends "${spec%,}" -tenants "$RAW/tenants-$TENANT.json" \
  -kafka-brokers localhost:9092 -usage-topic usage-e03 >"$RAW/gateway-$TENANT.log" 2>&1 &
pids+=($!)
bin/meter -usage-topic usage-e03 -group "meter-$TENANT" -crash-before-commit 5 >"$RAW/meter-$TENANT-1.log" 2>&1 &
meter1=$!
sleep 2

bin/loadgen -url http://127.0.0.1:8280 -key "$KEY" -c 32 -n 2000 -prefixes 16 -prefix-tokens 200 -suffix-tokens 20 \
  -max-tokens 16 -o "$RAW/load-$TENANT.json" >/dev/null &
load=$!
wait "$meter1" || echo "meter exited with $? (injected crash expected: 3)"
bin/meter -usage-topic usage-e03 -group "meter-$TENANT" >"$RAW/meter-$TENANT-2.log" 2>&1 &
pids+=($!)
wait "$load"

ok=$(python3 -c "import json;print(json.load(open('$RAW/load-$TENANT.json'))['status_codes'].get('200',0))")
for _ in $(seq 60); do  # let the meter catch up
  [ "$(psql "SELECT COALESCE(SUM(requests),0) FROM usage_daily WHERE tenant='$TENANT'")" -ge "$ok" ] && break
  sleep 1
done
sleep 3  # anything counted twice would show up now
db=$(psql "SELECT COALESCE(SUM(requests),0)||' '||COALESCE(SUM(prompt_tokens),0)||' '||COALESCE(SUM(completion_tokens),0) FROM usage_daily WHERE tenant='$TENANT'")
events=$(psql "SELECT COUNT(*) FROM usage_events WHERE tenant='$TENANT'")
python3 - "$RAW/load-$TENANT.json" "$OUT/e03-$TENANT.json" "$TENANT" "$db" "$events" <<'EOF'
import json, sys
load = json.load(open(sys.argv[1])); req, pt, ct = map(int, sys.argv[4].split())
client = {"ok_requests": load["status_codes"].get("200", 0), "rate_limited": load["status_codes"].get("429", 0),
          "prompt_tokens": load["prompt_tokens"], "completion_tokens": load["completion_tokens"]}
db = {"requests": req, "prompt_tokens": pt, "completion_tokens": ct, "event_rows": int(sys.argv[5])}
match = (client["ok_requests"], client["prompt_tokens"], client["completion_tokens"]) == (req, pt, ct) and req == db["event_rows"]
out = {"tenant": sys.argv[3], "client": client, "billing": db, "match": match}
json.dump(out, open(sys.argv[2], "w"), indent=2)
print(json.dumps(out))
EOF
grep -h '"batch"' "$RAW"/meter-"$TENANT"-*.log | tail -1 | cut -c1-160
grep -h "injected crash" "$RAW/meter-$TENANT-1.log" | cut -c1-140 || true
