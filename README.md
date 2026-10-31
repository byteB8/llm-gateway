# llm-gateway

An OpenAI-compatible gateway in Go that sits in front of several LLM inference servers.
It routes streaming requests across backends, limits per-tenant load, retries failed backends
and publishes usage events to Kafka. It runs on Kubernetes.

Work in progress.
