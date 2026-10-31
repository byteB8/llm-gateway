#!/usr/bin/env bash
# Laptop side. push: build static Linux binaries and copy them with bench/sustain to the GPU server.
# pull: copy results back to results/sustain.
#   scripts/sync_sustain.sh push | pull
set -euo pipefail
cd "$(dirname "$0")/.."
REMOTE=${REMOTE:-sustain}; DIR=${DIR:-work/llmgw}
case "${1:-push}" in
  push)
    mkdir -p bin/linux
    GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/linux/ ./cmd/gateway ./cmd/loadgen
    ssh "$REMOTE" "mkdir -p $DIR/bin"
    rsync -az bench/sustain/ "$REMOTE:$DIR/"
    rsync -az bin/linux/ "$REMOTE:$DIR/bin/" ;;
  pull)
    mkdir -p results/sustain
    rsync -az --exclude raw "$REMOTE:$DIR/results/" results/sustain/ ;;
  *) echo "usage: $0 push|pull"; exit 1 ;;
esac
