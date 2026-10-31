"""README figures from the saved results, each in a light and a dark version (docs/figures/*-{light,dark}.svg).

    ~/ba/.venv/bin/python scripts/plots.py

Every number comes from results/; nothing is typed in by hand. Sizes are in SVG points (1 pt = 4/3 px).
"""
import json
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch, Rectangle  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "figures"
POLICIES = ["round-robin", "least-loaded", "prefix-affinity"]

# Validated categorical slots 1-3 (dataviz reference palette), chart chrome and ink per mode.
THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781", grid="#e1e0d9",
                  base="#c3c2b7", series=["#2a78d6", "#eb6834", "#1baf7a"]),
    "dark": dict(surface="#1a1a19", ink="#f0efec", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a",
                 base="#383835", series=["#3987e5", "#d95926", "#199e70"]),
}
BAR = 18      # pt  (24 px)
ROUND = 3     # pt  (4 px rounded data end)
GAP = 1.5     # pt  (2 px surface gap)
FONT = "system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

plt.rcParams.update({"svg.fonttype": "none", "svg.hashsalt": "llm-gateway",  # fixed ids: stable diffs
                     "font.family": "DejaVu Sans", "font.size": 10})


def load(path):
    return json.loads((ROOT / path).read_text())


def style(ax, t, ygrid=True):
    ax.set_facecolor(t["surface"])
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(t["base"])
    ax.tick_params(colors=t["muted"], length=0, labelsize=9)
    if ygrid:
        ax.yaxis.grid(True, color=t["grid"], linewidth=0.75)
    ax.set_axisbelow(True)


def px_to_data(ax, fig):
    """Data units per point along x and y, after limits are final."""
    fig.canvas.draw()
    bb = ax.get_window_extent()
    (x0, x1), (y0, y1) = ax.get_xlim(), ax.get_ylim()
    scale = 72 / fig.dpi  # window extent is in pixels at fig.dpi; SVG output is in points
    return (x1 - x0) / (bb.width * scale), (y1 - y0) / (bb.height * scale)


def column(ax, fig, x, h, color, t):
    """A column with a rounded top and a square base, BAR points wide."""
    if h <= 0:
        return
    ux, uy = px_to_data(ax, fig)
    w, r = BAR * ux, ROUND * ux
    ax.add_patch(FancyBboxPatch((x - w / 2, 0), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
                                mutation_aspect=uy / ux, linewidth=0, facecolor=color, zorder=3))
    ax.add_patch(Rectangle((x - w / 2, 0), w, max(h - ROUND * uy, 0), linewidth=0, facecolor=color, zorder=3))


def hbar(ax, fig, y, segments, colors, t):
    """A horizontal stacked bar: square at the baseline, rounded at the data end, 2 px gaps between parts."""
    ux, uy = px_to_data(ax, fig)
    hgt, gap = BAR * uy, GAP * ux
    parts = [(v, c) for v, c in zip(segments, colors) if v > 0]
    left = 0.0
    for i, (v, c) in enumerate(parts):
        last = i == len(parts) - 1
        width = v - (0 if last else gap)
        if last:
            r = ROUND * ux
            ax.add_patch(FancyBboxPatch((left, y - hgt / 2), width, hgt, boxstyle=f"round,pad=0,rounding_size={r}",
                                        mutation_aspect=uy / ux, linewidth=0, facecolor=c, zorder=3))
            ax.add_patch(Rectangle((left, y - hgt / 2), max(width - r, 0), hgt, linewidth=0, facecolor=c, zorder=3))
        else:
            ax.add_patch(Rectangle((left, y - hgt / 2), width, hgt, linewidth=0, facecolor=c, zorder=3))
        left += v


def legend(fig, t, labels, colors, y, x=0.04):
    """Swatch + label per series, laid out from the measured text widths."""
    renderer = fig.canvas.get_renderer()
    h = 9 / (fig.bbox.height * 72 / fig.dpi)  # a 9 pt square swatch
    w = h * fig.bbox.height / fig.bbox.width
    for lab, c in zip(labels, colors):
        fig.patches.append(FancyBboxPatch((x, y - h / 2), w, h, boxstyle="round,pad=0,rounding_size=0.002",
                                          transform=fig.transFigure, facecolor=c, linewidth=0))
        txt = fig.text(x + w * 1.6, y, lab, color=t["ink2"], va="center", fontsize=9.5)
        x += w * 1.6 + txt.get_window_extent(renderer=renderer).width / fig.bbox.width + 0.035


def save(fig, name, mode):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{name}-{mode}.svg"
    fig.savefig(path, format="svg", facecolor=fig.get_facecolor())
    plt.close(fig)
    svg = path.read_text()
    svg = re.sub(r"font-family:[^;\"]*", "font-family:" + FONT, svg)  # system font stack in the browser
    svg = re.sub(r"<metadata>.*?</metadata>", "", svg, flags=re.S)       # no build date: stable diffs
    path.write_text(svg)


def routing(mode):
    t = THEMES[mode]
    runs = {p: [load(f"results/sustain/e05-pressure-{p}-r{r}.json") for r in (1, 2)] for p in POLICIES}
    panels = [
        ("Prefix-cache hit rate", "higher is better", lambda s: s["vllm_prefix_cache"]["hit_rate"], "{:.2f}", 1.0),
        ("Time to first token, median", "ms · lower is better", lambda s: s["ttft_ms"]["p50"], "{:.0f}", None),
        ("Throughput", "requests/s · higher is better", lambda s: s["req_per_sec"], "{:.1f}", None),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(9.6, 4.1), dpi=72, facecolor=t["surface"])
    fig.subplots_adjust(left=0.06, right=0.98, top=0.63, bottom=0.12, wspace=0.35)
    fig.text(0.04, 0.975, "Routing in front of two vLLM servers under KV-cache pressure", color=t["ink"],
             fontsize=13, fontweight="semibold", va="top")
    fig.text(0.04, 0.905, "Qwen3-1.7B on 2× RTX A4000 · 96 system prompts (~144k tokens) vs ~86.5k tokens of KV cache "
             "per GPU · 16 clients · bars = mean of 2 runs, dots = each run", color=t["ink2"], fontsize=9, va="top")
    legend(fig, t, POLICIES, t["series"], y=0.815)
    for ax, (title, sub, metric, fmt, ymax) in zip(axes, panels):
        vals = {p: [metric(s) for s in runs[p]] for p in POLICIES}
        top = ymax or max(max(v) for v in vals.values()) * 1.25
        ax.set_xlim(-0.6, 2.6)
        ax.set_ylim(0, top)
        style(ax, t)
        ax.set_xticks(range(3), [p.replace("-", "-\n", 1) if len(p) > 12 else p for p in POLICIES], fontsize=8.5)
        ax.set_title(title, loc="left", color=t["ink"], fontsize=10.5, pad=16)
        ax.text(0, 1.02, sub, transform=ax.transAxes, color=t["muted"], fontsize=8.5)
        for i, p in enumerate(POLICIES):
            mean = sum(vals[p]) / len(vals[p])
            column(ax, fig, i, mean, t["series"][i], t)
            ax.scatter([i] * len(vals[p]), vals[p], s=26, color=t["ink2"], edgecolors=t["surface"], linewidths=1.5,
                       zorder=4)
            ax.text(i, max(vals[p]) + top * 0.035, fmt.format(mean), ha="center", va="bottom", color=t["ink"],
                    fontsize=9.5, fontweight="semibold", zorder=5)
    save(fig, "routing", mode)


def rollout(mode):
    t = THEMES[mode]
    rows = [
        ("No preStop hook, no drain", "results/local/e04-naive.json"),
        ("preStop sleep 5 s + 25 s drain", "results/local/e04-safe-noretry.json"),
        ("preStop /drain: Connection: close, then drain · run 1", "results/local/e04-safe-drain-run1.json"),
        ("preStop /drain: Connection: close, then drain · run 2", "results/local/e04-safe-drain-run2.json"),
    ]
    data = []
    for label, f in rows:
        s = load(f)
        before = s["status_codes"].get("0", 0)  # never got a response: connection refused, reset, broken pipe
        data.append((label, before, s["errors"] - before, s["requests"]))
    fig, ax = plt.subplots(figsize=(9.6, 3.3), dpi=72, facecolor=t["surface"])
    fig.subplots_adjust(left=0.36, right=0.93, top=0.66, bottom=0.14)
    fig.text(0.04, 0.97, "Failed requests during two rolling restarts of the gateway", color=t["ink"], fontsize=13,
             fontweight="semibold", va="top")
    fig.text(0.04, 0.875, "kind cluster · 32 clients · 3,000 streaming requests per run · gateway Deployment restarted "
             "twice mid-run", color=t["ink2"], fontsize=9, va="top")
    cats = ["connection failed before a response", "stream cut mid-response"]
    legend(fig, t, cats, t["series"][:2], y=0.765, x=0.04)
    top = max(b + c for _, b, c, _ in data) * 1.55
    ax.set_xlim(0, top)
    ax.set_ylim(len(data) - 0.5, -0.6)
    style(ax, t, ygrid=False)
    ax.xaxis.grid(True, color=t["grid"], linewidth=0.75)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["left"].set_color(t["base"])
    ax.set_yticks(range(len(data)), [d[0] for d in data], color=t["ink2"], fontsize=9)
    ax.set_xlabel("failed requests (of 3,000)", color=t["muted"], fontsize=8.5)
    for y, (_, before, cut, total) in enumerate(data):
        hbar(ax, fig, y, [before, cut], t["series"][:2], t)
        n = before + cut
        label = f"{n}  ({n / total:.1%})" + (f" · {cut} streams cut" if cut else "") if n else "0 failed"
        ax.text(n + top * 0.012, y, label, va="center", color=t["ink"], fontsize=9.5, fontweight="semibold")
    save(fig, "rollout", mode)


def herding(mode):
    t = THEMES[mode]
    s = load("results/sustain/e05-dispatch-streaks.json")["runs"]
    vals = {p: [s[f"{p}-r{r}"]["mean_streak"] for r in (1, 2)] for p in POLICIES}
    share = {p: sum(s[f"{p}-r{r}"]["requests_in_streaks_ge4"] for r in (1, 2)) /
             sum(s[f"{p}-r{r}"]["dispatches"] for r in (1, 2)) for p in POLICIES}
    peak = {p: max(s[f"{p}-r{r}"]["max_streak"] for r in (1, 2)) for p in POLICIES}
    fig, ax = plt.subplots(figsize=(9.6, 3.2), dpi=72, facecolor=t["surface"])
    fig.subplots_adjust(left=0.16, right=0.97, top=0.62, bottom=0.15)
    fig.text(0.04, 0.97, "Why least-loaded has the slowest first token: it herds", color=t["ink"], fontsize=13,
             fontweight="semibold", va="top")
    fig.text(0.04, 0.88, "Requests the gateway sent in a row to the same GPU (mean of 2 runs; dots = each run).\n"
             "vLLM finishes a batch at once, so one GPU's in-flight count drops together and the next burst goes there.",
             color=t["ink2"], fontsize=9, va="top", linespacing=1.5)
    legend(fig, t, POLICIES, t["series"], y=0.715)
    top = max(max(v) for v in vals.values()) * 2.3
    ax.set_xlim(0, top)
    ax.set_ylim(2.5, -0.5)
    style(ax, t, ygrid=False)
    ax.xaxis.grid(True, color=t["grid"], linewidth=0.75)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["left"].set_color(t["base"])
    ax.set_yticks(range(3), POLICIES, color=t["ink2"], fontsize=9.5)
    ax.set_xlabel("requests in a row to the same GPU", color=t["muted"], fontsize=8.5)
    for i, p in enumerate(POLICIES):
        mean = sum(vals[p]) / 2
        hbar(ax, fig, i, [mean], [t["series"][i]], t)
        ax.scatter(vals[p], [i, i], s=26, color=t["ink2"], edgecolors=t["surface"], linewidths=1.5, zorder=4)
        ax.text(max(vals[p]) + top * 0.015, i, f"{mean:.2f}  ·  longest {peak[p]}  ·  {share[p]:.0%} of requests in runs of 4+",
                va="center", color=t["ink"], fontsize=9.5, fontweight="semibold", zorder=5)
    save(fig, "herding", mode)


def overhead(mode):
    t = THEMES[mode]
    summary = load("results/sustain/e08.json")["variants"]
    runs = {v: [] for v in summary}
    for f in sorted((ROOT / "results" / "sustain").glob("e08-*-r*.json")):
        r = json.loads(f.read_text())
        runs[r["variant"]].append(r["cpu_us_per_request"])
    rows = [("pre", "before metrics and tracing"), ("off", "metrics, tracing off"),
            ("on10", "metrics + 10% of traces exported"), ("on100", "metrics + every trace exported")]
    fig, ax = plt.subplots(figsize=(9.6, 3.3), dpi=72, facecolor=t["surface"])
    fig.subplots_adjust(left=0.30, right=0.97, top=0.70, bottom=0.14)
    fig.text(0.04, 0.97, "What observability costs the gateway", color=t["ink"], fontsize=13, fontweight="semibold",
             va="top")
    fig.text(0.04, 0.875, "CPU time per streaming request, gateway on 2 cores · 30,000 requests per run, 5 runs per "
             "variant in random order · bar = median, dots = each run.\nTime to first token measured at 500 requests/s "
             "through a 1 ms-per-token fake backend.", color=t["ink2"], fontsize=9, va="top", linespacing=1.5)
    top = max(max(v) for v in runs.values()) * 1.75
    ax.set_xlim(0, top)
    ax.set_ylim(len(rows) - 0.5, -0.6)
    style(ax, t, ygrid=False)
    ax.xaxis.grid(True, color=t["grid"], linewidth=0.75)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["left"].set_color(t["base"])
    ax.set_yticks(range(len(rows)), [label for _, label in rows], color=t["ink2"], fontsize=9.5)
    ax.set_xlabel("microseconds of CPU per request", color=t["muted"], fontsize=8.5)
    for y, (v, _) in enumerate(rows):
        s = summary[v]
        med = s["cpu_us_per_request"]["median"]
        hbar(ax, fig, y, [med], [t["muted"] if v == "pre" else t["series"][0]], t)
        ax.scatter(runs[v], [y] * len(runs[v]), s=26, color=t["ink2"], edgecolors=t["surface"], linewidths=1.5, zorder=4)
        change = "baseline" if v == "pre" else f"{s['cpu_vs_pre_percent']:+.1f}%"
        ax.text(max(runs[v]) + top * 0.015, y, f"{med:.0f} µs  ·  {change}  ·  first token {s['ttft_p50_ms']['median']:.2f} ms",
                va="center", color=t["ink"], fontsize=9.5, fontweight="semibold", zorder=5)
    save(fig, "overhead", mode)


def place_labels(ax, fig, items, t):
    """Labels next to points without overlaps: each takes the first free spot (left, right, then further
    above or below), with a leader line when it had to move off the point's own row."""
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    taken = [ax.transData.transform((x, y)) for x, y, _ in items]  # the markers themselves
    taken = [(px - 7, py - 7, px + 7, py + 7) for px, py in taken]
    spots = [(-9, 0), (9, 0), (-9, 13), (-9, -13), (9, 13), (9, -13), (-9, 26), (-9, -26), (9, 26), (9, -26)]
    for x, y, text in sorted(items, key=lambda i: -i[1]):
        for dx, dy in spots:
            a = ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", ha="right" if dx < 0 else "left",
                            va="center", color=t["ink"], fontsize=9, fontweight="semibold", zorder=5)
            b = a.get_window_extent(renderer)
            box = (b.x0 - 2, b.y0 - 1, b.x1 + 2, b.y1 + 1)
            if not any(box[0] < o[2] and o[0] < box[2] and box[1] < o[3] and o[1] < box[3] for o in taken):
                taken.append(box)
                if dy:
                    a.arrow_patch = None
                    a.remove()
                    ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", ha="right" if dx < 0 else "left",
                                va="center", color=t["ink"], fontsize=9, fontweight="semibold", zorder=5,
                                arrowprops=dict(arrowstyle="-", color=t["muted"], linewidth=0.75, shrinkA=0, shrinkB=5))
                break
            a.remove()


def detect(mode, path="results/local/e07.json"):
    """Each page from the fault experiment: the time the window maths predicted against the time it fired."""
    t = THEMES[mode]
    e07 = load(path)
    points = []  # (predicted s, measured s, fast?, trial, episodes)
    for sc in e07["scenarios"]:
        for a in sc["alerts"]:
            pred = sc["predicted_pages"].get(a["alert"], {})
            if a["severity"] == "page" and "ttd_s" in a and pred.get("ttd_traffic_s"):
                points.append((pred["ttd_traffic_s"], a["ttd_s"], "Fast" in a["alert"], sc["id"], a.get("episodes", 1)))
    fig, ax = plt.subplots(figsize=(9.6, 5.0), dpi=72, facecolor=t["surface"])
    fig.subplots_adjust(left=0.11, right=0.97, top=0.73, bottom=0.12)
    fig.text(0.04, 0.975, "When the SLO alerts paged: predicted and measured", color=t["ink"], fontsize=13,
             fontweight="semibold", va="top")
    fig.text(0.04, 0.905, "Faults injected one at a time under steady load (kind, 8 requests/s). Predicted: when the long window's "
             "error ratio crosses the alert's threshold,\nfrom the error ratio and traffic Prometheus measured during the fault "
             f"(windows compressed {e07['compression']}x). Measured: arrival at Alertmanager's webhook.", color=t["ink2"],
             fontsize=9, va="top", linespacing=1.5)
    legend(fig, t, ["fast burn: 14.4x over 5 min and 30 s", "slow burn: 6x over 30 min and 150 s"], t["series"][:2], y=0.79)
    lo, hi = 25, 2000
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    style(ax, t)
    ax.xaxis.grid(True, color=t["grid"], linewidth=0.75)
    ticks = [30, 60, 120, 300, 600, 1200]
    names = ["30 s", "1 min", "2 min", "5 min", "10 min", "20 min"]
    ax.set_xticks(ticks, names)
    ax.set_yticks(ticks, names)
    ax.minorticks_off()
    ax.set_xlabel("predicted time from fault to page", color=t["muted"], fontsize=8.5)
    ax.set_ylabel("measured", color=t["muted"], fontsize=8.5)
    ax.plot([lo, hi], [lo, hi], color=t["muted"], linewidth=1, zorder=2)
    ax.text(0.985, 0.04, "on the diagonal: fired exactly when predicted", transform=ax.transAxes, color=t["muted"],
            fontsize=8.5, ha="right", va="bottom")
    for x, y, fast, _, _ in points:
        ax.scatter([x], [y], s=64, color=t["series"][0 if fast else 1], edgecolors=t["surface"], linewidths=2, zorder=4)
    place_labels(ax, fig, [(x, y, trial + (f" · flapped, {n} pages" if n > 1 else "")) for x, y, _, trial, n in points], t)
    save(fig, "detect", mode)


if __name__ == "__main__":
    for mode in THEMES:
        routing(mode)
        rollout(mode)
        herding(mode)
        overhead(mode)
        if (ROOT / "results/local/e07.json").exists():
            detect(mode)
    print("wrote", sorted(p.name for p in OUT.glob("*.svg")))
