"""Grafana dashboards as code: writes deploy/observability/dashboards/*.json from the panel lists below.

    ~/ba/.venv/bin/python scripts/dashboards.py

The JSON is committed and loaded by Grafana's sidecar (a ConfigMap labelled grafana_dashboard=1).
"""
import json
from pathlib import Path

from slo_rules import SLOS

OUT = Path(__file__).resolve().parent.parent / "deploy" / "observability" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}


class Layout:
    """Places panels left to right on Grafana's 24-column grid, wrapping to the next row."""

    def __init__(self):
        self.panels, self.x, self.y, self.row_h, self.next_id = [], 0, 0, 0, 1

    def _add(self, panel, w, h):
        if self.x + w > 24:
            self.x, self.y = 0, self.y + self.row_h
            self.row_h = 0
        panel.update(id=self.next_id, gridPos={"x": self.x, "y": self.y, "w": w, "h": h})
        self.next_id += 1
        self.x += w
        self.row_h = max(self.row_h, h)
        self.panels.append(panel)

    def row(self, title):
        self.x, self.y = 0, self.y + self.row_h
        self.row_h = 0
        self._add({"type": "row", "title": title, "collapsed": False, "panels": []}, 24, 1)
        self.x, self.y, self.row_h = 0, self.y + 1, 0

    def series(self, title, targets, unit="short", w=8, h=8, desc="", exemplars=False, min0=True):
        self._add({
            "type": "timeseries", "title": title, "description": desc, "datasource": PROM,
            "targets": [{"refId": chr(65 + i), "datasource": PROM, "expr": e, "legendFormat": leg,
                         "exemplar": exemplars, "range": True} for i, (e, leg) in enumerate(targets)],
            "fieldConfig": {"defaults": {"unit": unit, "min": 0 if min0 else None,
                                         "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never"}},
                            "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
        }, w, h)


def rate(metric, by="", window="1m"):
    grouping = f" by ({by})" if by else ""
    return f"sum{grouping} (rate({metric}[{window}]))"


def quantile(q, metric, by=""):
    extra = f", {by}" if by else ""
    return f"histogram_quantile({q}, sum by (le{extra}) (rate({metric}_bucket[1m])))"


def gateway_dashboard():
    L = Layout()
    L.row("SLOs (windows compressed 12x for experiments)")
    for slo, budget in (("availability", 0.005), ("latency", 0.01)):
        L.series(f"{slo.capitalize()} burn rate",
                 [(f"slo:llmgw_{slo}:error_ratio_rate{w} / {budget}", f"{w} window") for w in ("30s", "150s", "5m", "30m")]
                 + [("vector(14.4)", "fast-burn page threshold"), ("vector(6)", "slow-burn page threshold")],
                 "short", w=12,
                 desc=f"How many times faster than allowed the {slo} error budget is being spent. "
                      "A page fires when both the long and the short window cross the same threshold.")

    L.row("Traffic and errors")
    L.series("Requests / s by status", [(rate("llmgw_requests_total", "code"), "{{code}}")], "reqps",
             desc="Every chat request, by the HTTP status the client got. 429 = rate limited, 502 = no backend, "
                  "499 = client left before an answer.")
    availability = next(s for s in SLOS if s["name"] == "availability")
    L.series("Error ratio (availability SLI)", [(availability["error"]("1m"), "bad share")], "percentunit",
             desc="The availability SLO's own expression over 1 minute: 5xx answers plus streams cut by a backend, "
                  "over requests that were not the client's fault (4xx).")
    L.series("Streams cut after they started", [(rate("llmgw_stream_interrupted_total", "side"), "{{side}}")], "reqps",
             desc="Responses broken mid-stream: the client left, or the backend failed. Not visible in status codes.")

    L.row("Latency")
    L.series("Time to first token", [(quantile(q, "llmgw_ttft_seconds"), f"p{int(q * 100)}") for q in (0.5, 0.9, 0.99)],
             "s", w=12, exemplars=True,
             desc="From request arrival to the first byte from the backend. Exemplar dots link to the trace in Tempo.")
    L.series("Time to first token p99 by backend", [(quantile(0.99, "llmgw_ttft_seconds", "backend"), "{{backend}}")],
             "s", w=12, exemplars=True)
    L.series("Request duration", [(quantile(q, "llmgw_request_duration_seconds"), f"p{int(q * 100)}") for q in (0.5, 0.99)],
             "s", w=12)
    L.series("Gateway CPU and memory", [("sum(rate(process_cpu_seconds_total{job=~\".*gateway.*\"}[1m]))", "CPU cores"),
                                        ("sum(process_resident_memory_bytes{job=~\".*gateway.*\"}) / 1e9", "RSS GB")],
             "short", w=12)

    L.row("Backends")
    L.series("In flight vs capacity", [("sum by (backend) (llmgw_backend_inflight)", "{{backend}} in flight"),
                                       ("max by (backend) (llmgw_backend_capacity)", "{{backend}} capacity")], "short",
             desc="Summed over gateway replicas. Capacity is vLLM's max_num_seqs; above it, requests queue in the backend.")
    L.series("Backend failures / s", [(rate("llmgw_backend_failures_total", "backend, reason"), "{{backend}} {{reason}}")],
             "reqps", desc="Attempts that failed before any byte; the gateway moved those requests to another backend.")
    L.series("Backend queue (fake backends)", [("sum by (pod) (fakellm_requests_waiting)", "{{pod}} waiting"),
                                               ("sum by (pod) (fakellm_requests_running)", "{{pod}} running")], "short")
    L.series("Prefix-cache hit rate (fake backends)",
             [(f'{rate("fakellm_prefix_cache_hits_total")} / clamp_min({rate("fakellm_prefix_cache_queries_total")}, 1e-9)',
               "hit rate")], "percentunit", w=12)
    L.series("Injected faults / s", [(rate("fakellm_injected_faults_total", "pod, kind"), "{{pod}} {{kind}}")], "reqps",
             w=12, desc="Faults switched on through POST /fault during experiments.")

    L.row("Tenants and limits")
    L.series("Rate-limited / s", [(rate("llmgw_rate_limited_total", "tenant, reason"), "{{tenant}} {{reason}}")],
             "reqps", w=12)
    L.series("Tokens / s", [(rate("llmgw_tokens_total", "tenant, kind"), "{{tenant}} {{kind}}")], "short", w=12)

    L.row("Billing pipeline")
    L.series("Usage events / s", [(rate("llmgw_usage_events_total", "result"), "{{result}}")], "short",
             desc="Events handed to Kafka. Failed or dropped events are revenue the meter never sees.")
    L.series("Meter records / s", [(rate("llmgw_meter_records_total", "result"), "{{result}}")], "short",
             desc="new = billed; duplicate = a replayed batch, ignored by the idempotent insert.")
    L.series("Consumer lag", [("max(llmgw_meter_consumer_lag)", "records behind")], "short",
             desc="Usage records in Kafka the meter has not committed. Grows when Postgres is down.")

    return {"uid": "llm-gateway", "title": "LLM Gateway", "tags": ["llm-gateway"], "timezone": "browser",
            "schemaVersion": 39, "version": 1, "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
            "panels": L.panels, "templating": {"list": []}, "annotations": {"list": []}}


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    d = gateway_dashboard()
    (OUT / "llm-gateway.json").write_text(json.dumps(d, indent=1) + "\n")
    print(f"wrote {OUT / 'llm-gateway.json'}: {sum(p['type'] != 'row' for p in d['panels'])} panels")
