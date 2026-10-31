#!/usr/bin/env bash
# e06: take Kafka down, then Postgres, while clients stream through the gateway in the kind cluster.
# Serving should not notice either. Billing should end up exact if the outages are shorter than the
# producer's delivery timeout (Kafka) and the meter retries its writes (Postgres).
set -euo pipefail
cd "$(dirname "$0")/.."
K="kubectl -n llmgw"
OUT=results/local; mkdir -p "$OUT"
TAG=${TAG:-e06}
sql() { $K exec postgres-0 -- psql -U meter -d meter -tAc "$1"; }
totals() { sql "SELECT COALESCE(SUM(requests),0)||' '||COALESCE(SUM(prompt_tokens),0)||' '||COALESCE(SUM(completion_tokens),0) FROM usage_daily WHERE tenant='team-search'"; }
gwstats() {  # sum of usage_events counters over gateway pods, via port-forward
  local p=0 f=0 d=0 port=18090
  for pod in $($K get pods -l app=gateway -o name); do
    $K port-forward "$pod" $port:8080 >/dev/null 2>&1 & pf=$!; sleep 1.5
    read -r a b c < <(curl -s localhost:$port/stats | python3 -c 'import json,sys; u=json.load(sys.stdin).get("usage_events",{}); print(u.get("Published",0), u.get("Failed",0), u.get("Dropped",0))')
    kill $pf; wait $pf 2>/dev/null || true
    p=$((p + a)); f=$((f + b)); d=$((d + c)); port=$((port + 1))
  done
  echo "$p $f $d"
}
before=$(totals); s0=$(gwstats)
$K delete job loadgen --ignore-not-found >/dev/null
sed -e 's/-c=32, -n=3000/-c=16, -n=2400/' deploy/k8s/jobs/loadgen.yaml | $K apply -f - >/dev/null
t0=$(date +%s); at() { while [ $(( $(date +%s) - t0 )) -lt "$1" ]; do sleep 1; done; echo "t=$1s $2"; }
at 20 "kafka down";      $K scale statefulset kafka --replicas=0 >/dev/null
at 50 "kafka up";        $K scale statefulset kafka --replicas=1 >/dev/null
at 110 "postgres down";  $K scale statefulset postgres --replicas=0 >/dev/null
at 140 "postgres up";    $K scale statefulset postgres --replicas=1 >/dev/null
$K wait --for=condition=complete job/loadgen --timeout=900s >/dev/null
$K logs job/loadgen >"$OUT/$TAG-load.json"
$K wait --for=condition=Ready pod/postgres-0 --timeout=120s >/dev/null
ok=$(python3 -c "import json;print(json.load(open('$OUT/$TAG-load.json'))['status_codes'].get('200',0))")
read -r r0 _ _ <<<"$before"
for _ in $(seq 90); do read -r r _ _ <<<"$(totals)"; [ $((r - r0)) -ge "$ok" ] && break; sleep 2; done
sleep 5
after=$(totals); s1=$(gwstats)
python3 - "$OUT/$TAG-load.json" "$OUT/$TAG.json" "$before" "$after" "$s0" "$s1" <<'EOF'
import json, sys
load = json.load(open(sys.argv[1]))
b, a = list(map(int, sys.argv[3].split())), list(map(int, sys.argv[4].split()))
s0, s1 = list(map(int, sys.argv[5].split())), list(map(int, sys.argv[6].split()))
client = {"requests": load["requests"], "errors": load["errors"], "status": load["status_codes"],
          "prompt_tokens": load["prompt_tokens"], "completion_tokens": load["completion_tokens"]}
billed = {"requests": a[0] - b[0], "prompt_tokens": a[1] - b[1], "completion_tokens": a[2] - b[2]}
events = {"published": s1[0] - s0[0], "failed": s1[1] - s0[1], "dropped": s1[2] - s0[2]}
out = {"client": client, "billed": billed, "producer": events,
       "billing_exact": (billed["requests"], billed["prompt_tokens"], billed["completion_tokens"]) ==
                        (client["status"].get("200", 0), client["prompt_tokens"], client["completion_tokens"])}
json.dump(out, open(sys.argv[2], "w"), indent=2)
print(json.dumps(out))
EOF
