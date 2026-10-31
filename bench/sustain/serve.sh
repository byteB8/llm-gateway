#!/usr/bin/env bash
# Start/stop the vLLM servers of servers.toml, one at a time (two engines starting together on shared
# GPUs miscount each other's memory). Everything listens on 127.0.0.1 and runs on CPUs 0-9.
#   ./serve.sh up | down | status
set -uo pipefail
cd "$(dirname "$0")"
mkdir -p logs
CPUS=${CPUS:-0-9}
export HF_HOME=${HF_HOME:-$HOME/work/exp/hf} HF_HUB_OFFLINE=1 HF_HUB_DISABLE_XET=1
export VLLM_USE_FLASHINFER_SAMPLER=0 VLLM_SERVER_DEV_MODE=1   # dev mode enables POST /reset_prefix_cache
[ -f .backend_key ] || head -c 24 /dev/urandom | base64 | tr -d '/+=' > .backend_key
export VLLM_API_KEY=$(cat .backend_key)
PY=${PY:-$HOME/work/exp/venv/bin/python}
servers() { $PY -c 'import tomllib; c=tomllib.load(open("servers.toml","rb"))
for n,s in c["servers"].items(): print(n, s["gpu"], s["port"])'; }
alive() { [ -f "logs/$1.pid" ] && kill -0 "$(cat "logs/$1.pid")" 2>/dev/null; }
case "${1:-status}" in
  up)
    while read -r name gpu port; do
      if curl -sf "http://127.0.0.1:$port/health" >/dev/null; then echo "$name already up"; continue; fi
      setsid nohup taskset -c "$CPUS" "$PY" run.py serve "$name" > "logs/srv_$name.log" 2>&1 < /dev/null &
      echo $! > "logs/srv_$name.pid"
      for _ in $(seq 120); do
        curl -sf "http://127.0.0.1:$port/health" >/dev/null && break
        alive "srv_$name" || { echo "FAILED: $name exited"; tail -20 "logs/srv_$name.log"; exit 1; }
        sleep 5
      done
      echo "$name up on GPU $gpu, port $port"
    done < <(servers) ;;
  down)
    for f in logs/srv_*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null; rm -f "$f"; done
    echo "stopped" ;;
  status)
    while read -r name gpu port; do
      printf '%s (GPU %s, port %s): ' "$name" "$gpu" "$port"
      curl -sf "http://127.0.0.1:$port/health" >/dev/null && echo up || echo down
    done < <(servers) ;;
esac
