#!/usr/bin/env python3
"""Compare the rpc_perf JSON reports of several clients as a Markdown summary.

Reads every ``<client>-<label>-result.json`` under ``--results-dir`` and prints an
overview for each of p50, p90, p95 and max, with the latency of each client and the
status of Erigon per test label and QPS step, then, per test label and QPS step, the
p50, p95 and max latency of each client.
p50, p90 and p95 are the median over the repetitions, max is the worst one.
HTTP is the share of requests answered with HTTP 200, RPC the share of requests
answered with a result and no JSON-RPC error; both are the worst over the repetitions.
"""
import argparse
import glob
import json
import os
import statistics

CLIENT_ORDER = ["erigon", "geth", "nethermind", "reth"]
STATUS_RATIO = 1.3
RPC_TOLERANCE = 0.01


def load_results(root):
    results = {}
    for path in sorted(glob.glob(os.path.join(root, "**", "*-result.json"), recursive=True)):
        with open(path, encoding="utf-8") as fh:
            report = json.load(fh)
        client = report["configuration"]["testingDaemon"]
        label = os.path.basename(path)[len(client) + 1:-len("-result.json")]
        for step in report["results"]:
            reps = [r["vegetaReport"] for r in step["testRepetitions"]]
            stats = results.setdefault(label, {}).setdefault(client, {})[step["qps"]] = {
                "p50": statistics.median(r["latencies"]["50th"] for r in reps) / 1e6,
                "p95": statistics.median(r["latencies"]["95th"] for r in reps) / 1e6,
                "max": max(r["latencies"]["max"] for r in reps) / 1e6,
                "success": min(r["success"] for r in reps),
                "rpc_success": min(r["rpcSuccessRatio"] for r in step["testRepetitions"]),
            }
            if all("90th" in r["latencies"] for r in reps):
                stats["p90"] = statistics.median(r["latencies"]["90th"] for r in reps) / 1e6
    return results


def client_rank(client):
    return CLIENT_ORDER.index(client) if client in CLIENT_ORDER else len(CLIENT_ORDER)


def all_succeeded(step, best_rpc):
    """Some requests of a pattern can fail on every client, so RPC errors count only above those of the best client."""
    return step["success"] >= 1 and step["rpc_success"] >= best_rpc - RPC_TOLERANCE


def erigon_status(erigon, best, next_best, best_rpc, percentile):
    if erigon is None:
        return "⚪"
    if not all_succeeded(erigon, best_rpc) or best is None:
        return "🔴"
    if erigon[percentile] == best:
        return "🟢🟢" if next_best is not None and next_best >= best * STATUS_RATIO else "🟢"
    return "🟡" if erigon[percentile] < best * STATUS_RATIO else "🔴"


def latency_cell(step, best, best_rpc, percentile):
    if step is None:
        return "- "
    if not all_succeeded(step, best_rpc):
        return "err "
    return f"{step[percentile]:.2f}" + ("*" if step[percentile] == best else " ")


def render_overview(results, percentile):
    """Return a plain-text code block, readable in the GitHub summary and pasteable into Discord.

    Only the Erigon status at the end of each row is not ASCII: emoji and symbols have no fixed
    width in Discord, so anywhere else they would break the alignment of the columns.
    """
    clients = sorted({c for label in results for c in results[label]}, key=lambda c: (client_rank(c), c))
    rows = []
    for label in sorted(results):
        for qps in sorted({q for c in results[label] for q in results[label][c]}, key=float):
            steps = {c: results[label][c].get(qps) for c in clients if c in results[label]}
            best_rpc = max(s["rpc_success"] for s in steps.values() if s)
            ok = {c: s[percentile] for c, s in steps.items() if s and all_succeeded(s, best_rpc)}
            best = min(ok.values()) if ok else None
            next_best = min((v for c, v in ok.items() if c != "erigon"), default=None)
            cells = [latency_cell(steps.get(c), best, best_rpc, percentile) for c in clients]
            status = erigon_status(steps.get("erigon"), best, next_best, best_rpc, percentile)
            rows.append((status, [label, qps] + cells))
    header = ["Test", "QPS"] + [c + " " for c in clients]
    widths = [max(len(r[i]) for r in [header] + [cells for _, cells in rows]) for i in range(len(header))]

    def line(cells):
        return " ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(cells, widths)))

    lines = ["```",
             f"{percentile} latency in ms, lower is better. * fastest, - no result,",
             "err HTTP errors or RPC errors > 1% above the best client.",
             f"Erigon status on {percentile}, at the end of the row: 🟢🟢 fastest by >= 1.3x, 🟢 fastest,",
             "🟡 < 1.3x the fastest, 🔴 >= 1.3x or errors, ⚪ no Erigon result.",
             "",
             line(header)]
    lines += [f"{line(cells)} {status}" for status, cells in rows]
    lines += ["```", ""]
    return "\n".join(lines)


def render(results):
    lines = []
    for label in sorted(results):
        clients = sorted(results[label], key=lambda c: (client_rank(c), c))
        steps = sorted({q for c in clients for q in results[label][c]}, key=float)
        lines += [f"<details><summary>{label}</summary>", "",
                  "| QPS | Client | p50 (ms) | p95 (ms) | max (ms) | HTTP | RPC |",
                  "|---:|---|---:|---:|---:|---:|---:|"]
        for qps in steps:
            for client in clients:
                s = results[label][client].get(qps)
                if s:
                    lines.append(f"| {qps} | {client} | {s['p50']:.2f} | {s['p95']:.2f} | {s['max']:.2f} "
                                 f"| {s['success']:.2%} | {s['rpc_success']:.2%} |")
        lines += ["", "</details>", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    args = parser.parse_args()
    results = load_results(args.results_dir)
    print("## RPC Performance Comparison (latest block)\n")
    steps = [s for clients in results.values() for qps in clients.values() for s in qps.values()]
    overviews = [render_overview(results, percentile) for percentile in ("p50", "p90", "p95", "max")
                 if all(percentile in s for s in steps)]
    print("\n".join(overviews + [render(results)]) if results else "No results found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
