#!/usr/bin/env bash
# Bring up everything on a local kind cluster: the app (Kafka, Postgres, fake backends, gateway, meter)
# and its monitoring (kube-prometheus-stack, Tempo, OTel Collector, dashboards, PodMonitors).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH=$HOME/opt/bin:$PATH
kind get clusters 2>/dev/null | grep -qx llmgw || kind create cluster --name llmgw --config deploy/kind.yaml
before=$(docker image inspect -f '{{.Id}}' llm-gateway:dev 2>/dev/null || true)
docker build -q -t llm-gateway:dev . >/dev/null
kind load docker-image llm-gateway:dev --name llmgw >/dev/null
helm upgrade --install kps prometheus-community/kube-prometheus-stack --version 92.16.1 -n observability \
  --create-namespace -f deploy/observability/kube-prometheus-stack.values.yaml --wait --timeout 15m >/dev/null
helm upgrade --install tempo grafana/tempo --version 1.24.4 -n observability \
  -f deploy/observability/tempo.values.yaml --wait --timeout 10m >/dev/null
kubectl apply -k deploy/k8s >/dev/null
kubectl apply -k deploy/observability >/dev/null
# Running pods keep the image they started with: restart ours when the build produced a new one.
if [ "$before" != "$(docker image inspect -f '{{.Id}}' llm-gateway:dev)" ]; then
  kubectl -n llmgw rollout restart deployment/gateway deployment/meter statefulset/fakellm >/dev/null
  kubectl -n observability rollout restart deployment/alert-sink >/dev/null
  kubectl -n llmgw rollout status deployment/gateway --timeout=300s >/dev/null
  kubectl -n llmgw rollout status statefulset/fakellm --timeout=300s >/dev/null
fi
kubectl -n llmgw wait --for=condition=Ready pod --all --timeout=600s >/dev/null
kubectl -n observability wait --for=condition=Ready pod --all --timeout=600s >/dev/null
echo "Grafana      http://127.0.0.1:3000  (admin / admin, dashboard \"LLM Gateway\")"
echo "Prometheus   http://127.0.0.1:9090"
echo "Alertmanager http://127.0.0.1:9093"
echo "Gateway      http://127.0.0.1:8080"
