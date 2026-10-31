"""e07: do the alerts catch real faults, how fast, and do they stay quiet otherwise?

Runs a fixed schedule of faults against the kind deployment while bin/loadgen sends steady open-loop
traffic from the host, then joins three views of the run: what clients saw (loadgen's per-request
results), what Prometheus measured over each fault (the SLIs), and which alerts reached Alertmanager's
webhook receiver (alert-sink) and when.

    scripts/obs_up.sh && go build -o bin/ ./cmd/...
    ~/ba/.venv/bin/python scripts/e07_alerts.py                       # the full schedule, about 2 h 20 min
    ~/ba/.venv/bin/python scripts/e07_alerts.py --schedule ejection [--label fqdn]  # C1 and C5 again, 15 min
    ~/ba/.venv/bin/python scripts/e07_alerts.py --schedule smoke      # a 9-minute check of the machinery
    ~/ba/.venv/bin/python scripts/e07_alerts.py --analyse [--schedule ...]  # redo the analysis of a run

SLO trials of the same SLO start at least 30 minutes apart (the longest compressed window), so each
one starts from a clean error budget and its time to detect can be compared with the prediction.
Writes results/local/e07.json (e07-ejection.json for that schedule); per-request results and the run
state go to results/raw/ (not committed).
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from slo_rules import ALERTS, COMPRESSION, SLOS, window  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "local" / "e07.json"
RAW = ROOT / "results" / "raw"
PROM = "http://127.0.0.1:9090"
GATEWAY = "http://127.0.0.1:8080"
KUBECTL = str(Path.home() / "opt" / "bin" / "kubectl")
LOADGEN = str(ROOT / "bin" / "loadgen")
FAKES = [f"fakellm-{i}" for i in range(4)]
BASE = dict(rate=8, prefixes=16, prefix_tokens=1000, suffix_tokens=50, max_tokens=64)
TTFT_GOOD = 0.5  # seconds; the latency SLO's threshold


def now():
    return time.time()


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds")


def parse_time(s):
    """RFC 3339 time to Unix seconds. Go writes up to 9 fractional digits; Python reads at most 6."""
    head, frac, tz = re.fullmatch(r"(.*?)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", s).groups()
    return datetime.fromisoformat(head + (f".{frac[:6]}" if frac else "") + ("+00:00" if tz == "Z" else tz)).timestamp()


def log(msg):
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


def kubectl(*args, check=True):
    return subprocess.run([KUBECTL, *args], capture_output=True, text=True, check=check).stdout


def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw) if raw.strip() else None


def prom(query, at=None):
    params = {"query": query}
    if at is not None:
        params["time"] = f"{at:.3f}"
    r = http("GET", f"{PROM}/api/v1/query?" + urllib.parse.urlencode(params))
    return [(x["metric"], float(x["value"][1])) for x in r["data"]["result"]]


def prom_scalar(query, at=None):
    r = prom(query, at)
    return r[0][1] if r else None


class Forward:
    """A kubectl port-forward, restarted when it dies (it does when its pod is replaced)."""

    def __init__(self, target, port, remote, namespace):
        self.target, self.port, self.remote, self.ns, self.proc = target, port, remote, namespace, None

    def url(self):
        if self.proc is None or self.proc.poll() is not None:
            self.restart()
        return f"http://127.0.0.1:{self.port}"

    def restart(self):
        self.stop()
        self.proc = subprocess.Popen([KUBECTL, "-n", self.ns, "port-forward", self.target, f"{self.port}:{self.remote}"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError(f"port-forward to {self.target} did not come up")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait()
        self.proc = None


FWD = {name: Forward(f"pod/{name}", 19000 + i, 9000, "llmgw") for i, name in enumerate(FAKES)}
SINK = Forward("svc/alert-sink", 18080, 8080, "observability")


def call(fwd, method, path, body=None):
    for attempt in range(5):
        try:
            return http(method, fwd.url() + path, body)
        except OSError:
            time.sleep(1 + attempt)
            fwd.restart()
    raise RuntimeError(f"{method} {path} on {fwd.target} kept failing")


def set_fault(pods, **fault):
    for p in pods:
        call(FWD[p], "POST", "/fault", fault)


def scale(what, n, ready_pod=None):
    kubectl("-n", "llmgw", "scale", what, f"--replicas={n}")
    if ready_pod:
        for _ in range(60):  # the pod object may not exist yet right after scaling up
            if kubectl("-n", "llmgw", "get", "pod", ready_pod, "--ignore-not-found", "-o", "name"):
                break
            time.sleep(1)
        kubectl("-n", "llmgw", "wait", "--for=condition=Ready", f"pod/{ready_pod}", "--timeout=300s")


def run_tag(name, label=""):
    return ("e07" if name == "full" else f"e07-{name}") + (f"-{label}" if label else "")


TAG = run_tag("full")  # set from --schedule; names the raw files of a run


def state_file():
    return RAW / f"{TAG}-run.json"


def loadgen(tag, minutes, key="sk-search-dev", seed=1, **opts):
    """Starts an open-loop loadgen on the host; returns (process, raw results path)."""
    o = {**BASE, **opts}
    raw = RAW / f"{TAG}-{tag}.jsonl"
    args = [LOADGEN, "-url", GATEWAY, "-key", key, "-rate", str(o["rate"]), "-duration", f"{minutes * 60:.0f}s",
            "-c", "128", "-prefixes", str(o["prefixes"]), "-prefix-tokens", str(o["prefix_tokens"]),
            "-suffix-tokens", str(o["suffix_tokens"]), "-max-tokens", str(o["max_tokens"]), "-seed", str(seed),
            "-raw", str(raw), "-o", str(RAW / f"{TAG}-{tag}-summary.json")]
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(RAW / f"{TAG}-{tag}.log", "w")), raw


EXTRA = []  # loadgens started by scenarios, waited for at the end


def extra_load(tag, minutes, **kw):
    def start():
        proc, raw = loadgen(tag, minutes, **kw)
        EXTRA.append((tag, proc, raw))
    return start


# (id, start minute, minutes, what happens, inject, clear, kind)
# kind: "quiet" (nothing may fire), "slo" (a page should fire), "cause" (a ticket may fire, no page).
def schedule(name):
    restore_fakes = lambda: set_fault(FAKES)  # noqa: E731
    backend_down = (lambda: scale("statefulset/fakellm", 3), lambda: scale("statefulset/fakellm", 4, "fakellm-3"))
    if name == "smoke":
        return [
            ("F0", 0, 1, "no fault", None, None, "quiet"),
            ("A1", 1, 3, "all backends cut 50% of streams after the first token",
             lambda: set_fault(FAKES, cut_rate=0.5), restore_fakes, "slo"),
            ("C1", 5, 2, "backend fakellm-3 down (StatefulSet scaled to 3)", *backend_down, "cause"),
        ]
    if name == "ejection":  # the two backend failures again, on a build with outlier ejection
        return [
            ("F0", 0, 4, "no fault", None, None, "quiet"),
            ("C1", 4, 3, "backend fakellm-3 down (StatefulSet scaled to 3)", *backend_down, "cause"),
            ("C5", 8, 3, "backend fakellm-2 answers every request with 503",
             lambda: set_fault(["fakellm-2"], error_rate=1.0), restore_fakes, "cause"),
        ]
    return [
        ("F0", 0, 30, "no fault: normal traffic only", None, None, "quiet"),
        ("A1", 30, 4, "all backends cut 50% of streams after the first token",
         lambda: set_fault(FAKES, cut_rate=0.5), restore_fakes, "slo"),
        ("L1", 35, 14, "backend fakellm-1 adds 1 s before the first token",
         lambda: set_fault(["fakellm-1"], extra_latency_ms=1000), restore_fakes, "slo"),
        ("C1", 50, 3, "backend fakellm-3 down (StatefulSet scaled to 3)", *backend_down, "cause"),
        ("C5", 54, 3, "backend fakellm-2 answers every request with 503",
         lambda: set_fault(["fakellm-2"], error_rate=1.0), restore_fakes, "cause"),
        ("C3", 58, 3, "Postgres down (billing writes fail)",
         lambda: scale("statefulset/postgres", 0), lambda: scale("statefulset/postgres", 1, "postgres-0"), "cause"),
        ("A2", 64, 12, "all backends cut 10% of streams",
         lambda: set_fault(FAKES, cut_rate=0.1), restore_fakes, "slo"),
        ("L2", 80, 13, "a new client sends 2 req/s of 3,000-token prompts that are never reused",
         extra_load("L2-longprompts", 13, rate=2, prefixes=1000, prefix_tokens=3000, max_tokens=32, seed=7), None, "slo"),
        ("C2", 94, 2, "Kafka down for 2 minutes (longer than the producer's 30 s delivery timeout)",
         lambda: scale("statefulset/kafka", 0), lambda: scale("statefulset/kafka", 1, "kafka-0"), "cause"),
        ("C4", 101, 3, "tenant team-batch floods at 20 req/s (its limit is 60 per minute)",
         extra_load("C4-noisy", 3, key="sk-batch-dev", rate=20, prefixes=4, prefix_tokens=200, max_tokens=32, seed=3),
         None, "cause"),
        ("A3", 107, 24, "all backends cut 5% of streams",
         lambda: set_fault(FAKES, cut_rate=0.05), restore_fakes, "slo"),
    ]


def prepare():
    set_fault(FAKES)
    for what, n, pod in (("statefulset/fakellm", 4, "fakellm-3"), ("statefulset/kafka", 1, "kafka-0"),
                         ("statefulset/postgres", 1, "postgres-0")):
        scale(what, n, pod)
    firing = [m["alertname"] for m, _ in prom('ALERTS{alertname=~"LLMGateway.+"}')]
    if firing:
        raise SystemExit(f"alerts already pending or firing, wait for them to clear: {firing}")
    call(SINK, "DELETE", "/alerts")


def run(name):
    RAW.mkdir(parents=True, exist_ok=True)
    plan = schedule(name)
    total = max(s[1] + s[2] for s in plan) + (6 if name == "full" else 3)  # quiet tail for the resolve notifications
    prepare()
    base, base_raw = loadgen("base", total)
    t0 = now()
    state = {"schedule": name, "label": TAG.split(f"e07-{name}-", 1)[1] if TAG.startswith(f"e07-{name}-") else "", "t0": t0, "minutes": total, "base_raw": str(base_raw), "scenarios": [], "extra": []}
    log(f"baseline load started ({BASE['rate']} req/s for {total} min); {len(plan)} scenarios")
    try:
        for sid, start, minutes, what, inject, clear, kind in plan:
            wait_until(t0 + start * 60)
            if inject:
                inject()
            t_inj = now()
            log(f"{sid} start: {what}")
            wait_until(t_inj + minutes * 60)
            if clear:
                clear()
            t_clr = now()
            log(f"{sid} end")
            state["scenarios"].append({"id": sid, "what": what, "kind": kind, "inject": t_inj, "clear": t_clr})
            state["alerts"] = call(SINK, "GET", "/alerts")
            state_file().write_text(json.dumps(state, indent=1))
        wait_until(t0 + total * 60)
    finally:
        set_fault(FAKES)
        for what, n in (("statefulset/fakellm", 4), ("statefulset/kafka", 1), ("statefulset/postgres", 1)):
            kubectl("-n", "llmgw", "scale", what, f"--replicas={n}", check=False)
    base.wait()
    for tag, proc, raw in EXTRA:
        proc.wait()
        state["extra"].append({"tag": tag, "raw": str(raw)})
    state["alerts"] = call(SINK, "GET", "/alerts")
    state_file().write_text(json.dumps(state, indent=1))
    log("run finished")


def wait_until(t):
    while (d := t - now()) > 0:
        time.sleep(min(d, 5))


# ---------------------------------------------------------------- analysis


def burn_alerts():
    """The page alerts as generated by slo_rules.py: name, factor, long and short window in seconds."""
    out = []
    for slo in SLOS:
        for _, factor, long_s, short_s in ALERTS:
            speed = "Fast" if factor > 10 else "Slow"
            out.append({"alert": f"LLMGateway{slo['name'].capitalize()}{speed}Burn", "slo": slo["name"],
                        "factor": factor, "long": window(long_s), "long_s": seconds(window(long_s)),
                        "budget": round(1 - slo["objective"], 6)})
    return out


def seconds(w):
    return int(w[:-1]) * (60 if w.endswith("m") else 1)


def client_view(rows, t_from, t_to):
    """What the clients sent in [t_from, t_to) and got back, per load generator."""
    out = {}
    for src in sorted({r["src"] for r in rows}):
        sent = [r for r in rows if r["src"] == src and t_from <= r["t"] < t_to]
        if not sent:
            continue
        ok = [r for r in sent if not r.get("err")]
        errors = {}
        for r in sent:
            if r.get("err"):
                key = str(r["status"]) if r["status"] not in (0, 200) else (
                    "cut" if "DONE" in r["err"] or "EOF" in r["err"] else r["err"][:40])
                errors[key] = errors.get(key, 0) + 1
        slow = sum(r["ttft_ns"] / 1e9 > TTFT_GOOD for r in ok)
        stalled = sum(r["ttft_ns"] / 1e9 > 1 for r in ok)  # a dial timeout or a slow DNS failure, not queueing
        out[src] = {"requests": len(sent), "ok": len(ok), "errors": errors,
                    "ttft_over_500ms": round(slow / len(ok), 4) if ok else None, "ttft_over_1s": stalled}
    return out


def load_rows(path, key_tag):
    rows = []
    if not Path(path).exists():
        return rows
    for line in open(path):
        r = json.loads(line)
        r["t"] = parse_time(r["start"])
        r["src"] = key_tag
        rows.append(r)
    return rows


def sli_over(t_from, t_to, t_after):
    """The two SLIs over [t_from, t_to], measured by Prometheus from the gateway's counters, plus failed
    backend attempts in the fault and usage events lost by t_after (an event fails up to 30 s later)."""
    d = f"{max(int(t_to - t_from), 10)}s"
    after = f"{max(int(t_after - t_from), 10)}s"
    avail = next(s for s in SLOS if s["name"] == "availability")["error"]
    lat = next(s for s in SLOS if s["name"] == "latency")["error"]
    inc = lambda e: e.replace("rate(", "increase(")  # noqa: E731  same expressions, counts instead of rates
    return {"availability_bad": prom_scalar(inc(avail(d)), t_to), "latency_bad": prom_scalar(inc(lat(d)), t_to),
            "requests_per_s": prom_scalar(f"sum(rate(llmgw_requests_total[{d}]))", t_to),
            "failed_backend_attempts": prom_scalar(f"sum(increase(llmgw_backend_failures_total[{d}]))", t_to),
            "usage_events_lost": prom_scalar(
                f'sum(increase(llmgw_usage_events_total{{result=~"failed|dropped"}}[{after}]))', t_after)}


DENOMINATORS = {"availability": 'sum(rate(llmgw_requests_total{{code!~"4.."}}[{w}]))',
                "latency": "sum(rate(llmgw_ttft_seconds_count[{w}]))"}


def denominator_rates(slo, long_w, t_inject, t_clear):
    """The SLI's denominator (events per second) over the long window before the fault, and during it."""
    during = f"{max(int(t_clear - t_inject), 10)}s"
    return (prom_scalar(DENOMINATORS[slo].format(w=long_w), t_inject),
            prom_scalar(DENOMINATORS[slo].format(w=during), t_clear))


def crossing(window_s, threshold, bad, r0, r1):
    """Seconds until the long window's error ratio passes the threshold when the fault (bad ratio `bad`)
    also changes the traffic from r0 to r1 events/s: bad*r1*t / (r0*(W - t) + r1*t) = threshold."""
    if not (r0 and r1) or bad <= threshold:
        return None
    return round(window_s * threshold * r0 / (r1 * (bad - threshold) + threshold * r0), 1)


def analyse():
    state = json.loads(state_file().read_text())
    name = state.get("schedule", "smoke" if state.get("smoke") else "full")
    rows = load_rows(state["base_raw"], "base")
    for e in state.get("extra", []):
        rows += load_rows(e["raw"], e["tag"])
    notes = [a for a in state["alerts"] if a["alertname"].startswith("LLMGateway")]
    for a in notes:
        a["t"], a["start"] = parse_time(a["received_at"]), parse_time(a["starts_at"])
    pages = burn_alerts()
    scen = state["scenarios"]
    out = {"experiment": TAG, "schedule": name, "label": state.get("label", ""), "started": iso(state["t0"]), "minutes": state["minutes"],
           "load": BASE, "compression": COMPRESSION, "scenarios": []}
    for i, s in enumerate(scen):
        until = scen[i + 1]["inject"] if i + 1 < len(scen) else state["t0"] + state["minutes"] * 60
        fired, resolves = {}, {}
        for a in sorted(notes, key=lambda a: a["t"]):
            if not (s["inject"] - 5 <= a["start"] < until):
                continue
            k = a["alertname"] + (f"/{a['labels']['backend']}" if "backend" in a["labels"] else "")
            f = fired.setdefault(k, {"alert": k, "severity": a["severity"], "starts": []})
            if a["start"] not in f["starts"]:  # one startsAt per episode: a second one means it flapped
                f["starts"].append(a["start"])
            if a["status"] == "firing" and "ttd_s" not in f:
                f["ttd_s"] = round(a["t"] - s["inject"], 1)  # to the webhook
                f["fired_in_prometheus_s"] = round(a["start"] - s["inject"], 1)  # its startsAt
            if a["status"] == "resolved":  # Alertmanager repeats a resolved alert with its group: keep the first
                resolves.setdefault((k, a["start"]), a["t"])
        for k, f in fired.items():
            starts = f.pop("starts")
            f["episodes"] = len(starts)
            if (k, starts[-1]) in resolves:  # when the last episode ended
                f["resolved_after_clear_s"] = round(resolves[(k, starts[-1])] - s["clear"], 1)
        sli = sli_over(s["inject"], s["clear"], until)
        predicted = {}
        for p in pages:
            bad = sli.get(f"{p['slo']}_bad")
            if bad is None:
                continue
            burn = bad / p["budget"]
            r0, r1 = denominator_rates(p["slo"], p["long"], s["inject"], s["clear"])
            predicted[p["alert"]] = {"burn_rate": round(burn, 2),
                                     "ttd_s": round(p["long_s"] * p["factor"] / burn, 1) if burn > p["factor"] else None,
                                     "ttd_traffic_s": crossing(p["long_s"], p["factor"] * p["budget"], bad, r0, r1)}
        entry = {"id": s["id"], "what": s["what"], "kind": s["kind"], "inject": iso(s["inject"]),
                 "minutes": round((s["clear"] - s["inject"]) / 60, 2), "client": client_view(rows, s["inject"], s["clear"]),
                 "sli": {k: (round(v, 5) if v is not None else None) for k, v in sli.items()},
                 "predicted_pages": predicted, "alerts": list(fired.values())}
        n_pages = sum(f["severity"] == "page" and "ttd_s" in f for f in fired.values())
        entry["pages"] = n_pages
        # Each page alert: predicted to fire (burn above its factor) against fired or not.
        entry["page_outcomes"] = {}
        for p in pages:
            expected = s["kind"] == "slo" and predicted.get(p["alert"], {}).get("ttd_s") is not None
            got = p["alert"] in fired and "ttd_s" in fired[p["alert"]]
            if expected or got:
                entry["page_outcomes"][p["alert"]] = {(True, True): "hit", (True, False): "miss",
                                                      (False, True): "false page"}[(expected, got)]
        out["scenarios"].append(entry)
    out["false_pages"] = sum(e["pages"] for e in out["scenarios"] if e["kind"] != "slo")
    outcomes = [o for e in out["scenarios"] for o in e["page_outcomes"].values()]
    out["page_scorecard"] = {k: outcomes.count(k) for k in ("hit", "miss", "false page")}
    out["quiet_alerts"] = sum(len(e["alerts"]) for e in out["scenarios"] if e["kind"] == "quiet")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    target = RAW / "e07-smoke.json" if name == "smoke" else OUT.with_name(f"{TAG}.json")
    target.write_text(json.dumps(out, indent=1) + "\n")
    report(out)
    print(f"wrote {target}")


def report(out):
    for e in out["scenarios"]:
        print(f"\n{e['id']} [{e['kind']}] {e['what']} ({e['minutes']} min)")
        for src, c in e["client"].items():
            print(f"  clients ({src}): {c['requests']} sent, {c['ok']} ok, errors {c['errors']}, "
                  f"TTFT>500ms {c['ttft_over_500ms']}, TTFT>1s {c['ttft_over_1s']} requests")
        print(f"  SLIs: {e['sli']}")
        for name, p in e["predicted_pages"].items():
            if p["ttd_s"] is not None:
                print(f"  predicted {name}: burn {p['burn_rate']}x -> fires after {p['ttd_s']} s "
                      f"({p['ttd_traffic_s']} s with the traffic change)")
        for f in e["alerts"]:
            print(f"  fired {f['alert']} ({f['severity']}): {f.get('ttd_s')} s after the fault "
                  f"(firing in Prometheus at {f.get('fired_in_prometheus_s')} s), {f['episodes']} episode(s), "
                  f"last resolved {f.get('resolved_after_clear_s')} s after it ended")
    print(f"\npages outside SLO faults: {out['false_pages']}; alerts during quiet periods: {out['quiet_alerts']}; "
          f"page scorecard: {out['page_scorecard']}")


SHORT = {"F0": "None (normal traffic)", "A1": "All backends cut 50% of streams", "L1": "One backend 1 s slower to first token",
         "C1": "One backend down", "C5": "One backend answers 503", "C3": "Postgres down", "A2": "All backends cut 10% of streams",
         "L2": "New client: 2 req/s of 3,000-token prompts", "C2": "Kafka down (past the 30 s delivery timeout)",
         "C4": "A tenant floods at 20 req/s (limit 60/min)", "A3": "All backends cut 5% of streams"}
TICKETS = {"LLMGatewayBackendFailing": "backend failing", "LLMGatewayBillingLag": "billing lag",
           "LLMGatewayUsageEventsLost": "usage events lost", "LLMGatewayMeterNotReporting": "meter silent"}


def secs(x):
    return f"{x:.0f} s" if x < 120 else f"{x / 60:.1f} min"


def markdown(out):
    """The README table: one row per scenario, every number from the analysis."""
    rows = ["| Fault | Clients saw | Pages: predicted → measured | Tickets |", "|---|---|---|---|"]
    for e in out["scenarios"]:
        extra = []
        for src, v in e["client"].items():
            if src == "base":
                continue
            if v["errors"].get("429"):
                extra.append(f"the flooding tenant got {v['errors']['429']:,} × 429 of {v['requests']:,}")
            elif (v["ttft_over_500ms"] or 0) >= 0.005:
                extra.append(f"the new client: {v['ttft_over_500ms']:.0%} of its first tokens > 500 ms")
        c = e["client"].get("base", {})
        seen = []
        if c:
            cut = c["errors"].get("cut", 0)
            other = sum(v for k, v in c["errors"].items() if k != "cut")
            seen.append(f"{cut / c['requests']:.0%} of streams cut" if cut else "")
            seen.append(f"{other} errors" if other else "")
            slow = c["ttft_over_500ms"] or 0
            seen.append(f"{slow:.0%} of first tokens > 500 ms" if slow >= 0.005 else "")
        seen = "; ".join(x for x in seen if x)
        if extra:
            seen = "; ".join(extra + ([f"everyone else: {seen}"] if seen else []))
        seen = seen or "no errors"
        pages = []
        for f in e["alerts"]:
            if f["severity"] != "page" or "ttd_s" not in f:
                continue
            pred = e["predicted_pages"].get(f["alert"], {}).get("ttd_traffic_s")
            kind = "fast" if "Fast" in f["alert"] else "slow"
            pages.append(f"{kind} {secs(pred)} → **{secs(f['ttd_s'])}**" + (" (flapped)" if f["episodes"] > 1 else "")
                         if pred else f"{kind} **{secs(f['ttd_s'])}**")
        if e["kind"] == "slo":
            for name, p in e["predicted_pages"].items():
                fired = any(f["alert"] == name for f in e["alerts"])
                if p["burn_rate"] > 1 and p["ttd_s"] is None and not fired:
                    factor = next(a["factor"] for a in burn_alerts() if a["alert"] == name)
                    pages.append(f"no fast page: burn {p['burn_rate']:.1f}× < {factor}×" if "Fast" in name else
                                 f"no slow page: burn {p['burn_rate']:.1f}× < {factor}×")
        tickets = [f"{TICKETS.get(f['alert'].split('/')[0], f['alert'])} {secs(f['ttd_s'])}" for f in e["alerts"]
                   if f["severity"] == "ticket" and "ttd_s" in f]
        lost = e["sli"].get("usage_events_lost") or 0
        if lost >= 1:
            tickets.append(f"({lost:.0f} events lost)")
        rows.append(f"| {SHORT.get(e['id'], e['what'])}, {e['minutes']:.0f} min | {seen} | "
                    f"{' · '.join(pages) or 'none'} | {' '.join(tickets) or 'none'} |")
    print("\n".join(rows))


def compare():
    """README table: the two backend failures before and after outlier ejection (and FQDN backend names)."""
    builds = [("e07", "no ejection (e07)"), ("e07-ejection", "outlier ejection"),
              ("e07-ejection-fqdn", "ejection + fully qualified names")]
    rows = ["| Fault (3 min) | Build | Failed attempts | Requests stalled > 1 s | First tokens > 500 ms | Ticket after |",
            "|---|---|---|---|---|---|"]
    for sid, fault in (("C1", "Backend down"), ("C5", "Backend answers 503")):
        for tag, build in builds:
            f = OUT.with_name(f"{tag}.json")
            if not f.exists():
                continue
            e = next(e for e in json.loads(f.read_text())["scenarios"] if e["id"] == sid)
            c = e["client"]["base"]
            ticket = next((a["ttd_s"] for a in e["alerts"] if a["alert"].startswith("LLMGatewayBackendFailing")), None)
            rows.append(f"| {fault} | {build} | {e['sli']['failed_backend_attempts']:,.0f} | {c['ttft_over_1s']} | "
                        f"{c['ttft_over_500ms']:.1%} | {secs(ticket) if ticket else 'none'} |")
    print("\n".join(rows))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--schedule", choices=["full", "ejection", "smoke"], default="full")
    ap.add_argument("--label", default="", help="names a variant of a schedule, e.g. a different build")
    ap.add_argument("--analyse", action="store_true", help="only analyse the last run of the schedule")
    ap.add_argument("--markdown", action="store_true", help="print the README table from the saved analysis")
    ap.add_argument("--compare", action="store_true", help="print the before/after table for outlier ejection")
    a = ap.parse_args()
    TAG = run_tag(a.schedule, a.label)
    os.environ["PATH"] = f"{Path.home() / 'opt' / 'bin'}:{os.environ['PATH']}"
    if a.compare:
        compare()
        sys.exit()
    if a.markdown:
        markdown(json.loads((RAW / "e07-smoke.json" if a.schedule == "smoke" else OUT.with_name(f"{TAG}.json")).read_text()))
        sys.exit()
    try:
        if not a.analyse:
            run(a.schedule)
        analyse()
    finally:
        for f in [*FWD.values(), SINK]:
            f.stop()
