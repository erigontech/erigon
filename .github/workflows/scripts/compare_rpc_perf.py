#!/usr/bin/env python3
"""Compare the rpc_perf JSON reports of several clients as a Markdown summary.

Reads every ``<client>-<label>-result.json`` under ``--results-dir`` and prints,
per test label and QPS step, the p50, p95 and max latency of each client.
p50 and p95 are the median over the repetitions, max is the worst one.
"""
import argparse
import glob
import json
import os
import statistics

CLIENT_ORDER = ["erigon", "geth", "nethermind"]


def load_results(root):
    results = {}
    for path in sorted(glob.glob(os.path.join(root, "**", "*-result.json"), recursive=True)):
        with open(path, encoding="utf-8") as fh:
            report = json.load(fh)
        client = report["configuration"]["testingDaemon"]
        label = os.path.basename(path)[len(client) + 1:-len("-result.json")]
        for step in report["results"]:
            reps = [r["vegetaReport"] for r in step["testRepetitions"]]
            results.setdefault(label, {}).setdefault(client, {})[step["qps"]] = {
                "p50": statistics.median(r["latencies"]["50th"] for r in reps) / 1e6,
                "p95": statistics.median(r["latencies"]["95th"] for r in reps) / 1e6,
                "max": max(r["latencies"]["max"] for r in reps) / 1e6,
                "success": min(r["success"] for r in reps),
            }
    return results


def client_rank(client):
    return CLIENT_ORDER.index(client) if client in CLIENT_ORDER else len(CLIENT_ORDER)


def render(results):
    lines = []
    for label in sorted(results):
        clients = sorted(results[label], key=lambda c: (client_rank(c), c))
        steps = sorted({q for c in clients for q in results[label][c]}, key=float)
        lines += [f"### {label}", "",
                  "| QPS | Client | p50 (ms) | p95 (ms) | max (ms) | Success |",
                  "|---:|---|---:|---:|---:|---:|"]
        for qps in steps:
            for client in clients:
                s = results[label][client].get(qps)
                if s:
                    lines.append(f"| {qps} | {client} | {s['p50']:.2f} | {s['p95']:.2f} | {s['max']:.2f} "
                                 f"| {s['success']:.2%} |")
        lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    args = parser.parse_args()
    results = load_results(args.results_dir)
    print("## RPC Performance Comparison (latest block)\n")
    print(render(results) if results else "No results found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
