"""e08 summary: what observability costs the gateway, from the runs and profiles in results/sustain/.

    ~/ba/.venv/bin/python scripts/e08_summary.py

Medians (and ranges) over the repeated runs of bench/sustain/e08.sh, and the share of CPU samples per
component in the two profiles of bench/sustain/e08_profile.sh (read with `go tool pprof`). Writes
results/sustain/e08.json.
"""
import json
import os
import re
import statistics
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results" / "sustain"
VARIANTS = {"pre": "before metrics and tracing", "off": "metrics, tracing off", "on10": "metrics, 10% of traces",
            "on100": "metrics, every trace"}
# (name, focus regex, ignore regex): CPU samples with a matching frame anywhere on the stack.
COMPONENTS = [
    ("OTel SDK: spans and batch export", r"go\.opentelemetry\.io/otel/sdk", ""),
    ("OTLP encoding and sending (part of the SDK row)", r"otlptrace|google\.golang\.org/protobuf", ""),
    ("otelhttp wrappers, without the work they wrap", "otelhttp",
     r"gateway\.\(\*Server\)|net/http\.\(\*Transport\)\.roundTrip|persistConn"),
    ("Prometheus client", "prometheus/client_golang", ""),
    ("garbage collection", "gcBgMarkWorker|gcAssistAlloc|bgsweep", ""),
    ("memory allocation", r"runtime\.mallocgc", ""),
    ("system calls", r"syscall\.Syscall|syscall\.RawSyscall|internal/runtime/syscall|runtime\.netpoll", ""),
    ("JSON", "encoding/json", ""),
    ("usage tap (reads token counts from the stream)", "usageTap", ""),
]


def runs():
    by = {v: [] for v in VARIANTS}
    for f in sorted(RES.glob("e08-*-r*.json")):
        r = json.loads(f.read_text())
        by[r["variant"]].append(r)
    return by


def spread(xs):
    return {"median": round(statistics.median(xs), 2), "min": round(min(xs), 2), "max": round(max(xs), 2)}


def share(profile, focus, ignore):
    args = ["go", "tool", "pprof", "-top", "-nodecount=100000", "-nodefraction=0", "-edgefraction=0", f"-focus={focus}"]
    if ignore:
        args.append(f"-ignore={ignore}")
    out = subprocess.run([*args, str(profile)], capture_output=True, text=True,
                         env={**os.environ, "GOTOOLCHAIN": "local"}).stdout
    m = re.search(r"Showing nodes accounting for \S+ (\S+)%", out)
    return float(m.group(1)) if m else 0.0


def main():
    by = runs()
    out = {"experiment": "e08", "machine": "sustain, gateway on 2 cores (GOMAXPROCS=2)", "variants": {}}
    pre = statistics.median(r["cpu_us_per_request"] for r in by["pre"])
    for v, label in VARIANTS.items():
        rs = by[v]
        cpu = [r["cpu_us_per_request"] for r in rs]
        out["variants"][v] = {
            "label": label, "runs": len(rs), "errors": sum(r["errors"] for r in rs),
            "cpu_us_per_request": spread(cpu),
            "cpu_vs_pre_percent": round((statistics.median(cpu) / pre - 1) * 100, 1),
            "throughput_rps": spread([r["throughput_rps"] for r in rs]),
            "ttft_p50_ms": spread([r["ttft_ms"]["p50"] for r in rs]),
            "ttft_p99_ms": spread([r["ttft_ms"]["p99"] for r in rs]),
            "rss_mb": spread([r["rss_mb"] for r in rs]),
            "spans_per_request": spread([r["spans_per_request"] for r in rs]),
            "latency_phase_rate": rs[0]["latency_rate"]}
    profiles = {v: RES / "raw" / f"e08-{v}.pprof" for v in ("off", "on100")}
    if all(p.exists() for p in profiles.values()):
        out["profile_cpu_share_percent"] = {
            name: {v: share(p, focus, ignore) for v, p in profiles.items()} for name, focus, ignore in COMPONENTS}
    (RES / "e08.json").write_text(json.dumps(out, indent=1) + "\n")
    for v, s in out["variants"].items():
        print(f"{v:6} {s['label']:28} CPU {s['cpu_us_per_request']['median']:6.1f} us/req ({s['cpu_vs_pre_percent']:+5.1f}%) "
              f"[{s['cpu_us_per_request']['min']}-{s['cpu_us_per_request']['max']}]  TTFT p50 {s['ttft_p50_ms']['median']:.2f} "
              f"p99 {s['ttft_p99_ms']['median']:.2f} ms  RSS {s['rss_mb']['median']} MB  spans/req {s['spans_per_request']['median']}  "
              f"runs {s['runs']} errors {s['errors']}")
    for name, sh in out.get("profile_cpu_share_percent", {}).items():
        print(f"  {name:52} off {sh['off']:5.2f}%  on100 {sh['on100']:5.2f}%")
    print(f"wrote {RES / 'e08.json'}")


if __name__ == "__main__":
    main()
