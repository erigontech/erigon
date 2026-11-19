#!/usr/bin/env python3
"""Compare the rpc_perf JSON reports of several clients as a Markdown summary.

Reads every ``<client>-<label>-result.json`` under ``--results-dir`` and prints an
overview for each of p50, p90, p95 and max, with the latency of each client and the
status of Erigon per test label and QPS step, then, per test label and QPS step, the
p50, p95 and max latency of each client, followed by its rows by method (mixed, run with
--verbose) or by subscription measure (eth_subscribe), when the reports have them.
The fan-out measures of eth_subscribe (how long the node takes to send one notification
to every connection) and its vs newHeads measures (how long after the head a log or receipt
of the block reaches the same connection) also get overview rows, one per client class when
the pattern has them, since the latency of the test itself is mostly the block import time,
the same for all clients.
p50, p90 and p95 are the median over the repetitions, max is the worst one.
HTTP is the share of requests answered with HTTP 200, RPC the share of requests
answered with a result and no JSON-RPC error; both are the worst over the repetitions.
A QPS step that rpc_perf recorded with an error (the client died) is dead; every QPS step
of a pattern that a client did not run because it was left out at a barrier is excluded;
every QPS step of a client of ``--clients`` with no report at all (its job ended before
uploading them) is lost.
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
    excluded = []
    for path in sorted(glob.glob(os.path.join(root, "**", "*-result.json"), recursive=True)):
        with open(path, encoding="utf-8") as fh:
            report = json.load(fh)
        client = report["configuration"]["testingDaemon"]
        label = os.path.basename(path)[len(client) + 1:-len("-result.json")]
        if report.get("excluded"):
            excluded.append((label, client))
            continue
        for step in report["results"]:
            if step.get("error"):
                results.setdefault(label, {}).setdefault(client, {})[step["qps"]] = {"failed": "dead", "error": step["error"]}
                continue
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
            apis = load_apis(step["testRepetitions"])
            if apis:
                stats["apis"] = apis
    for label, client in excluded:
        steps = {q for c in results.get(label, {}).values() for q in c}
        results.setdefault(label, {})[client] = {q: {"failed": "excluded"} for q in steps}
    return results


def load_apis(repetitions):
    """Merge the rows by method or by subscription measure of the repetitions, as the step stats."""
    rows = {}
    for r in repetitions:
        for name, row in r.get("apis", {}).items():
            rows.setdefault(name, []).append(row)
    apis = {}
    for name, reps in rows.items():
        apis[name] = {
            "p50": statistics.median(r["latencies"]["50th"] for r in reps) / 1e6,
            "p95": statistics.median(r["latencies"]["95th"] for r in reps) / 1e6,
            "max": max(r["latencies"]["max"] for r in reps) / 1e6,
            "requests": sum(r["requests"] for r in reps),
            "errors": sum(r["errors"] for r in reps),
        }
        if all("90th" in r["latencies"] for r in reps):
            apis[name]["p90"] = statistics.median(r["latencies"]["90th"] for r in reps) / 1e6
    return apis


def overview_results(results):
    """Add to the results a test per fan-out or vs newHeads measure, as "<label> <measure>", for the overview only."""
    out = dict(results)
    for label, clients in results.items():
        names = {n for steps in clients.values() for s in steps.values() for n in s.get("apis", {})
                 if "fan-out" in n or "vs newHeads" in n}
        for name in names:
            if any(other.startswith(name + " ") for other in names):
                continue
            out[f"{label} {name}"] = {c: {qps: measure_step(s, name) for qps, s in steps.items()}
                                      for c, steps in clients.items()}
    return out


def measure_step(step, name):
    if is_failed(step):
        return step
    api = step.get("apis", {}).get(name)
    if api is None:
        return None
    measure = {k: api[k] for k in ("p50", "p90", "p95", "max") if k in api}
    measure["success"] = 1
    measure["rpc_success"] = 1 - api["errors"] / max(api["requests"], 1)
    return measure


def add_lost(results, clients):
    """Mark every qps step of the clients with no report at all as lost, and return them."""
    lost = [c for c in clients if not any(c in label for label in results.values())]
    for label in results:
        steps = {q for c in results[label].values() for q in c}
        for client in lost:
            results[label][client] = {q: {"failed": "lost"} for q in steps}
    return lost


def is_failed(step):
    return step is not None and "failed" in step


def client_rank(client):
    return CLIENT_ORDER.index(client) if client in CLIENT_ORDER else len(CLIENT_ORDER)


def all_succeeded(step, best_rpc):
    """Some requests of a pattern can fail on every client, so RPC errors count only above those of the best client."""
    return step["success"] >= 1 and step["rpc_success"] >= best_rpc - RPC_TOLERANCE


def erigon_status(erigon, best, next_best, best_rpc, percentile):
    if erigon is None or is_failed(erigon) or not all_succeeded(erigon, best_rpc) or best is None:
        return "🔴"
    if erigon[percentile] == best:
        return "🟢🟢" if next_best is not None and next_best >= best * STATUS_RATIO else "🟢"
    return "🟡" if erigon[percentile] < best * STATUS_RATIO else "🔴"


def latency_cell(step, best, best_rpc, percentile):
    if step is None:
        return "- "
    if is_failed(step):
        return step["failed"] + " "
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
            alive = {c: s for c, s in steps.items() if s and not is_failed(s)}
            best_rpc = max((s["rpc_success"] for s in alive.values()), default=1)
            ok = {c: s[percentile] for c, s in alive.items() if all_succeeded(s, best_rpc)}
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
             "err HTTP errors or RPC errors > 1% above the best client, dead the client died,",
             "excluded left out at a barrier, lost no results from the client's job.",
             f"Erigon status on {percentile}, at the end of the row: 🟢🟢 fastest by >= 1.3x, 🟢 fastest,",
             "🟡 < 1.3x the fastest, 🔴 >= 1.3x, errors or no Erigon result.",
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
                if is_failed(s):
                    detail = f"{s['failed']}: {s['error'].splitlines()[0]}" if "error" in s else s["failed"]
                    lines.append(f"| {qps} | {client} | {detail} | | | | |")
                elif s:
                    lines.append(f"| {qps} | {client} | {s['p50']:.2f} | {s['p95']:.2f} | {s['max']:.2f} "
                                 f"| {s['success']:.2%} | {s['rpc_success']:.2%} |")
        lines += render_apis(results[label], clients, steps)
        lines += ["", "</details>", ""]
    return "\n".join(lines)


def render_apis(clients_steps, clients, steps):
    rows = []
    for qps in steps:
        apis = {c: clients_steps[c].get(qps, {}).get("apis", {}) for c in clients}
        for name in sorted({n for a in apis.values() for n in a}):
            rows += [f"| {qps} | {name} | {c} | {a['p50']:.2f} | {a['p95']:.2f} | {a['max']:.2f} "
                     f"| {a['requests']} | {a['errors']} |" for c in clients if (a := apis[c].get(name))]
    if not rows:
        return []
    return ["", "| QPS | API | Client | p50 (ms) | p95 (ms) | max (ms) | Requests | Errors |",
            "|---:|---|---|---:|---:|---:|---:|---:|"] + rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--clients", default="", help="comma-separated clients of the run")
    args = parser.parse_args()
    clients = [c for c in args.clients.split(",") if c]
    results = load_results(args.results_dir)
    print("## RPC Performance Comparison (latest block)\n")
    for client in add_lost(results, clients):
        print(f"⚠️ {client}: no results, its job ended before uploading them (e.g. the runner was lost).\n")
    overview = overview_results(results)
    steps = [s for clients in overview.values() for qps in clients.values() for s in qps.values() if s]
    overviews = [render_overview(overview, percentile) for percentile in ("p50", "p90", "p95", "max")
                 if all(percentile in s for s in steps if not is_failed(s))]
    print("\n".join(overviews + [render(results)]) if results else "No results found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
