#!/usr/bin/env bash
# e04: restart the gateway Deployment twice while 32 clients stream through it. Does any request fail?
#   scripts/e04_rollout.sh safe    # manifests as they are: preStop pause + 25 s drain
#   scripts/e04_rollout.sh naive   # no preStop pause, no drain: what most first deployments look like
set -euo pipefail
cd "$(dirname "$0")/.."
VARIANT=${1:-safe}
K="kubectl -n llmgw"
OUT=results/local; mkdir -p "$OUT"
restore() { $K apply -f deploy/k8s/gateway.yaml >/dev/null; }
if [ "$VARIANT" = naive ]; then
  trap 'restore; $K rollout status deploy/gateway --timeout=180s >/dev/null' EXIT
  $K patch deploy gateway --type=json -p '[
    {"op":"remove","path":"/spec/template/spec/containers/0/lifecycle"},
    {"op":"replace","path":"/spec/template/spec/containers/0/args/6","value":"-drain=1ms"}]' >/dev/null
fi
$K rollout status deploy/gateway --timeout=180s >/dev/null
$K delete job loadgen --ignore-not-found >/dev/null
$K apply -f deploy/k8s/jobs/loadgen.yaml >/dev/null
sleep 25
for i in 1 2; do
  echo "rollout $i at $(date +%T)"
  $K rollout restart deploy/gateway >/dev/null
  $K rollout status deploy/gateway --timeout=180s >/dev/null
  sleep 15
done
$K wait --for=condition=complete job/loadgen --timeout=600s >/dev/null
$K logs job/loadgen >"$OUT/e04-$VARIANT.json"
python3 - "$OUT/e04-$VARIANT.json" "$VARIANT" <<'EOF'
import json, sys
s = json.load(open(sys.argv[1]))
print(f'{sys.argv[2]}: requests={s["requests"]} errors={s["errors"]} status={s["status_codes"]} '
      f'ttft_p99={s["ttft_ms"]["p99"]:.0f}ms first_errors={s.get("first_errors", [])[:3]}')
EOF
