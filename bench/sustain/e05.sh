#!/usr/bin/env bash
# e05: the three routing policies in front of two real vLLM servers (Qwen3-1.7B, one per A4000, prefix
# caching on). "fit": 16 system prompts, which both GPUs can hold whatever the routing. "pressure": 96,
# about 144k prompt tokens against ~86.5k tokens of KV cache per GPU. Prefix caches are reset before
# every run; the hit rate is vLLM's own token count (prefix_cache_hits / prefix_cache_queries).
# Gateway and loadgen run on CPUs 8-9, the servers on 0-9 (the 10-core budget on this machine).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results/raw logs
export BACKEND_API_KEY=$(cat .backend_key)
REPEATS=${REPEATS:-2}
B="gpu0=http://127.0.0.1:8100,gpu1=http://127.0.0.1:8101"
counter() {  # counter <name>: sum over both servers
  for port in 8100 8101; do curl -s "http://127.0.0.1:$port/metrics"; done |
    awk -v m="vllm:$1" 'index($1, m"{") == 1 {s += $2} END {printf "%.0f", s}'
}
for workload in "fit 16" "pressure 96"; do
  read -r name prefixes <<<"$workload"
  for rep in $(seq 1 "$REPEATS"); do
    for policy in round-robin least-loaded prefix-affinity; do
      tag="e05-$name-$policy-r$rep"
      for port in 8100 8101; do
        curl -sf -X POST -H "Authorization: Bearer $BACKEND_API_KEY" "http://127.0.0.1:$port/reset_prefix_cache" >/dev/null
      done
      q0=$(counter prefix_cache_queries_total); h0=$(counter prefix_cache_hits_total)
      taskset -c 8-9 bin/gateway -listen 127.0.0.1:8200 -backends "$B" -policy "$policy" -capacity 16 \
        >"logs/gw-$tag.log" 2>&1 &
      gw=$!
      sleep 1
      taskset -c 8-9 bin/loadgen -url http://127.0.0.1:8200 -model qwen3-1.7b -c 16 -n 1500 -prefixes "$prefixes" \
        -prefix-tokens 1500 -suffix-tokens 20 -max-tokens 32 -seed "$rep" -o "results/raw/$tag.load.json" >/dev/null
      kill "$gw"; wait "$gw" 2>/dev/null || true
      q1=$(counter prefix_cache_queries_total); h1=$(counter prefix_cache_hits_total)
      python3 - "$tag" "$((q1 - q0))" "$((h1 - h0))" <<'PY'
import json, sys
tag, q, h = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
s = json.load(open(f"results/raw/{tag}.load.json"))
s["vllm_prefix_cache"] = {"query_tokens": q, "hit_tokens": h, "hit_rate": round(h / q, 4) if q else None}
json.dump(s, open(f"results/{tag}.json", "w"), indent=2)
t, e = s["ttft_ms"], s["total_ms"]
print(f'{tag:34} ok={s["requests"]-s["errors"]} hit={s["vllm_prefix_cache"]["hit_rate"]} ttft p50={t["p50"]:.0f} '
      f'p90={t["p90"]:.0f} p99={t["p99"]:.0f} total p50={e["p50"]:.0f} {s["req_per_sec"]:.1f} req/s {s["per_backend"]}', flush=True)
PY
    done
  done
done
echo done
