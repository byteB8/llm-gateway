"""SLOs as code: writes deploy/observability/slo-rules.yaml (a PrometheusRule) from the SLO list below.

    ~/ba/.venv/bin/python scripts/slo_rules.py

Alerts follow the multi-window, multi-burn-rate method of the Google SRE Workbook (chapter 5): page when
the error budget burns 14.4x too fast over both a long and a short window, or 6x over longer ones. The
windows are compressed 12x (1 h -> 5 m, 6 h -> 30 m, ...) so that experiments fit in minutes; for a real
service use COMPRESSION = 1.
"""
from pathlib import Path

import yaml

OUT = Path(__file__).resolve().parent.parent / "deploy" / "observability" / "slo-rules.yaml"
COMPRESSION = 12

# (name, objective, bad-or-good expression builder, description)
SLOS = [
    dict(name="availability", objective=0.995,
         desc="99.5% of chat requests succeed: no 5xx and no stream cut by the backend (4xx are the client's).",
         # Each side falls back to 0 on its own: a series that has never been incremented does not
         # exist, and "a + b" with an empty side is empty, which would hide the other side's errors.
         error=lambda w: (f'((sum(rate(llmgw_requests_total{{code=~"5.."}}[{w}])) or vector(0))'
                          f' + (sum(rate(llmgw_stream_interrupted_total{{side="backend"}}[{w}])) or vector(0)))'
                          f' / sum(rate(llmgw_requests_total{{code!~"4.."}}[{w}]))')),
    dict(name="latency", objective=0.99,
         desc="99% of chat requests get their first token within 500 ms.",
         error=lambda w: (f'1 - sum(rate(llmgw_ttft_seconds_bucket{{le="0.5"}}[{w}]))'
                          f' / sum(rate(llmgw_ttft_seconds_count[{w}]))')),
]

# (severity, burn factor, long window, short window) at real scale, from the Workbook.
ALERTS = [("page", 14.4, 3600, 300), ("page", 6, 21600, 1800)]


def window(seconds):
    s = max(int(seconds / COMPRESSION), 30)  # at least 6 scrapes at 5 s
    return f"{s // 60}m" if s % 60 == 0 else f"{s}s"


def build():
    windows = sorted({window(w) for _, _, lw, sw in ALERTS for w in (lw, sw)}, key=lambda w: (w[-1] == "m", int(w[:-1])))
    recording, alerts = [], []
    for slo in SLOS:
        budget = 1 - slo["objective"]
        for w in windows:
            recording.append({"record": f"slo:llmgw_{slo['name']}:error_ratio_rate{w}", "expr": slo["error"](w)})
        for severity, factor, lw, sw in ALERTS:
            long_w, short_w = window(lw), window(sw)
            speed = "Fast" if factor > 10 else "Slow"
            threshold = round(factor * budget, 6)
            alerts.append({
                "alert": f"LLMGateway{slo['name'].capitalize()}{speed}Burn",
                "expr": (f"slo:llmgw_{slo['name']}:error_ratio_rate{long_w} > {threshold}"
                         f" and slo:llmgw_{slo['name']}:error_ratio_rate{short_w} > {threshold}"
                         # too little traffic for a ratio to mean anything
                         f" and on() sum(rate(llmgw_requests_total[{short_w}])) > 0.5"),
                "labels": {"severity": severity, "slo": slo["name"]},
                "annotations": {
                    "summary": f"{slo['name']} error budget burning {factor}x too fast ({long_w} and {short_w})",
                    "description": slo["desc"] + f" Threshold: error ratio > {threshold} over both windows "
                                                 f"(windows compressed {COMPRESSION}x for experiments)."},
            })
    causes = [
        # On state, not on the failure rate: once the gateway ejects a dead backend it stops sending it
        # requests, so its failure rate falls to a probe now and then while it is still down.
        {"alert": "LLMGatewayBackendFailing",
         "expr": ("max by (backend) (llmgw_backend_consecutive_failures) >= 3"
                  " and on() sum(rate(llmgw_requests_total[1m])) > 0.5"), "for": "30s",
         "labels": {"severity": "ticket"},
         "annotations": {"summary": "Backend {{ $labels.backend }} has failed every attempt for 30 s; "
                                    "the gateway is routing around it"}},
        {"alert": "LLMGatewayUsageEventsLost",
         "expr": 'sum(increase(llmgw_usage_events_total{result=~"failed|dropped"}[5m])) > 0',
         "labels": {"severity": "ticket"},
         "annotations": {"summary": "Usage events failed or were dropped: those requests will never be billed"}},
        {"alert": "LLMGatewayBillingLag",
         "expr": "max(llmgw_meter_consumer_lag) > 500", "for": "1m",
         "labels": {"severity": "ticket"},
         "annotations": {"summary": "The meter is {{ $value }} usage records behind (Postgres down or meter slow)"}},
        {"alert": "LLMGatewayMeterNotReporting",
         "expr": "absent_over_time(llmgw_meter_consumer_lag[1m])", "for": "1m",
         "labels": {"severity": "ticket"},
         "annotations": {"summary": "No metrics from the meter for a minute: is it running?"}},
    ]
    return {
        "apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
        "metadata": {"name": "llm-gateway-slos", "namespace": "observability"},
        "spec": {"groups": [
            {"name": "llm-gateway-slo-recording", "interval": "5s", "rules": recording},
            {"name": "llm-gateway-slo-alerts", "interval": "5s", "rules": alerts},
            {"name": "llm-gateway-cause-alerts", "interval": "5s", "rules": causes},
        ]},
    }


if __name__ == "__main__":
    rules = build()
    header = "# Generated by scripts/slo_rules.py - edit that, not this file.\n"
    OUT.write_text(header + yaml.safe_dump(rules, sort_keys=False, width=200))
    groups = rules["spec"]["groups"]
    print(f"wrote {OUT}: {sum(len(g['rules']) for g in groups)} rules")
    for r in groups[1]["rules"]:
        print(" ", r["alert"], "|", r["expr"][:110])
