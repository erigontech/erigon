#!/usr/bin/env python3
"""
bench_eth_getlogs.py — measure eth_getLogs latency across filter types × block ranges.

For each (filter, block_range) pair the script records:
  cold — the very first call (cache empty, loading from snapshot/DB)
  warm — subsequent calls (receipt LRU populated)

Usage:
    python3 -u bench_eth_getlogs.py [OPTIONS]

Typical workflow:
    # 1. restart node (fresh state, cold caches)
    python3 -u bench_eth_getlogs.py --label before --out before.csv --pre-warmup 0
    # 2. restart node with new binary
    python3 -u bench_eth_getlogs.py --label after  --out after.csv  --pre-warmup 0
    # 3. compare
    python3 -u bench_eth_getlogs.py --compare before.csv after.csv

Options:
    --url        RPC endpoint  (default: http://localhost:8546)
    --tip        tip block to anchor the range (default: auto)
    --repeats    warm requests per scenario (default: 5)
    --pre-warmup extra full passes before measuring (default: 0 — keep 0 for cold data)
    --ranges     comma-separated block ranges (default: 1,10,100,1000)
    --out        output CSV file (default: getlogs_bench_<timestamp>.csv)
    --label      label for this run (default: "run")
    --compare    compare two CSV files instead of running
"""

import argparse
import csv
import json
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime

# ── well-known mainnet constants ──────────────────────────────────────────────

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# Rough logs-per-block estimates at ~block 24.6M, used only for warnings.
FILTER_CONFIGS = [
    {
        "filter":         "usdc_only",
        "filter_type":    "addr",
        "desc":           "USDC address only",
        "addresses":      [USDC],
        "topics":         None,
        "logs_per_block": 90,
    },
    {
        "filter":         "usdc_transfer",
        "filter_type":    "addr+topic",
        "desc":           "USDC + Transfer topic",
        "addresses":      [USDC],
        "topics":         [[TRANSFER_TOPIC]],
        "logs_per_block": 80,
    },
    {
        "filter":         "transfer_all",
        "filter_type":    "topic",
        "desc":           "Transfer topic, all contracts",
        "addresses":      None,
        "topics":         [[TRANSFER_TOPIC]],
        "logs_per_block": 500,
    },
    {
        "filter":         "multi",
        "filter_type":    "addr+topic",
        "desc":           "Transfer+Approval, USDC+USDT+WETH",
        "addresses":      [USDC, USDT, WETH],
        "topics":         [[TRANSFER_TOPIC, APPROVAL_TOPIC]],
        "logs_per_block": 260,
    },
]

MAX_LOGS_WARN = 20_000


def rpc(url: str, method: str, params: list, timeout: int = 120) -> dict:
    payload = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": 1}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def get_block_number(url: str) -> int:
    return int(rpc(url, "eth_blockNumber", [])["result"], 16)


def call_getlogs(url, from_block, to_block, addresses=None, topics=None):
    """Returns (latency_ms, status, log_count)."""
    filter_obj = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
    if addresses:
        filter_obj["address"] = addresses if len(addresses) > 1 else addresses[0]
    if topics:
        filter_obj["topics"] = topics
    payload = json.dumps({"jsonrpc": "2.0", "method": "eth_getLogs",
                          "params": [filter_obj], "id": 1}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        ms = (time.perf_counter() - t0) * 1000
        if "error" in data:
            return ms, f"err:{data['error'].get('message', '?')[:60]}", 0
        return ms, "ok", len(data.get("result", []))
    except Exception as e:
        ms = (time.perf_counter() - t0) * 1000
        return ms, f"exc:{str(e)[:60]}", 0


def build_scenarios(tip, ranges):
    end = tip - 10  # avoid very tip (may be reorging)
    scenarios = []
    for cfg in FILTER_CONFIGS:
        for n in ranges:
            est = cfg["logs_per_block"] * n
            warn = f"  ⚠ est.~{est:,} logs" if est > MAX_LOGS_WARN else ""
            scenarios.append({
                "name":      f"{cfg['filter']}_{n}",
                "filter":    cfg["filter"],
                "n_blocks":  n,
                "desc":      f"{cfg['desc']}, {n} block{'s' if n > 1 else ''}{warn}",
                "from":      end - n + 1,
                "to":        end,
                "addresses": cfg["addresses"],
                "topics":    cfg["topics"],
            })
    return scenarios


def run_bench(args):
    url     = args.url
    label   = args.label
    repeats = args.repeats
    ranges  = [int(x) for x in args.ranges.split(",")]

    print(f"Connecting to {url} ...", flush=True)
    try:
        tip = args.tip if args.tip else get_block_number(url)
    except Exception as e:
        print(f"ERROR: cannot reach {url}: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Tip block: {tip}  |  ranges: {ranges}", flush=True)

    scenarios = build_scenarios(tip, ranges)
    out = args.out or f"getlogs_bench_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Optional pre-warmup: populates the receipt LRU across all scenarios before
    # measuring. Set --pre-warmup 0 (default) when cold latency is important.
    if args.pre_warmup > 0:
        print(f"\n── PRE-WARMUP ({args.pre_warmup} pass) ──────────────────────────────", flush=True)
        for pw in range(args.pre_warmup):
            print(f"   pass {pw+1}/{args.pre_warmup} ...", flush=True)
            for sc in scenarios:
                ms, st, n = call_getlogs(url, sc["from"], sc["to"], sc["addresses"], sc["topics"])
                print(f"   [{sc['name']:24}] {ms:7.0f}ms  logs={n:6}  {st}", flush=True)

    with open(out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["label", "scenario", "filter", "n_blocks", "run_type",
                         "repeat", "from_block", "to_block", "log_count", "latency_ms", "status"])

        for sc in scenarios:
            print(f"\n── {sc['name']} ──────────────────────────────", flush=True)
            print(f"   {sc['desc']}", flush=True)
            print(f"   range [{sc['from']}..{sc['to']}]", flush=True)

            # First call = cold (cache empty for this scenario)
            ms, st, n = call_getlogs(url, sc["from"], sc["to"], sc["addresses"], sc["topics"])
            print(f"   cold:         {ms:8.1f}ms  logs={n:<6}  {st}", flush=True)
            writer.writerow([label, sc["name"], sc["filter"], sc["n_blocks"],
                             "cold", 0, sc["from"], sc["to"], n, f"{ms:.1f}", st])
            f.flush()

            # Subsequent calls = warm
            warm_latencies = []
            for r in range(repeats):
                ms, st, n = call_getlogs(url, sc["from"], sc["to"], sc["addresses"], sc["topics"])
                writer.writerow([label, sc["name"], sc["filter"], sc["n_blocks"],
                                 "warm", r + 1, sc["from"], sc["to"], n, f"{ms:.1f}", st])
                f.flush()
                marker = "✓" if st == "ok" else "✗"
                print(f"   [{r+1}/{repeats}] warm  {ms:8.1f}ms  logs={n:<6} {marker} {st}", flush=True)
                if st == "ok":
                    warm_latencies.append(ms)

            if warm_latencies:
                avg = sum(warm_latencies) / len(warm_latencies)
                print(f"   → warm avg={avg:.1f}ms  min={min(warm_latencies):.1f}ms  max={max(warm_latencies):.1f}ms", flush=True)

    print(f"\nDone. Results saved to: {out}", flush=True)


def _load_csv(path):
    """Returns dict: (filter, n_blocks, run_type) → avg_ms, plus metadata."""
    result = defaultdict(list)
    label = None
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["status"] != "ok":
                continue
            label = row["label"]
            key = (row["filter"], int(row["n_blocks"]), row["run_type"])
            result[key].append(float(row["latency_ms"]))
    avgs = {k: sum(v) / len(v) for k, v in result.items()}
    return label, avgs


def _fmt(ms):
    return f"{ms:.0f}ms" if ms is not None else "N/A"


def _filter_type(flt_name):
    for cfg in FILTER_CONFIGS:
        if cfg["filter"] == flt_name:
            return cfg.get("filter_type", "")
    return ""


def run_single_table(path):
    """Print a cold/warm table for a single benchmark file."""
    label, data = _load_csv(path)

    filters = [cfg["filter"] for cfg in FILTER_CONFIGS]
    ranges  = sorted({k[1] for k in data})

    nw = 16  # name column width
    tw = 10  # filter_type column width
    cw = 8   # cell width for a single ms value

    print(f"\n  {label}  —  cold (1st call) / warm (avg subsequent)")
    hdr = f"  {'':>{nw}}  {'':>{tw}}" + "".join(f"{'%d blk' % n:>{cw*2+3}}" for n in ranges)
    print(hdr)
    subhdr = f"  {'filter':<{nw}}  {'type':<{tw}}" + "".join(f"{'cold':>{cw}}  {'warm':<{cw}} " for n in ranges)
    print(subhdr)
    sep = "  " + "─" * nw + "  " + "─" * tw + ("  " + "─" * (cw * 2 + 1)) * len(ranges)
    print(sep)

    for flt in filters:
        ft = _filter_type(flt)
        cells = "".join(f"{_fmt(data.get((flt,n,'cold'))):>{cw}}  {_fmt(data.get((flt,n,'warm'))):<{cw}} " for n in ranges)
        print(f"  {flt:<{nw}}  {ft:<{tw}}{cells}")
    print()


def run_compare(files):
    """
    Show a 2D comparison table:
      rows = filter, column groups = block range.
    Each cell: after_ms (Δ% vs before).
    """
    label_a, data_a = _load_csv(files[0])
    label_b, data_b = _load_csv(files[1])

    filters = [cfg["filter"] for cfg in FILTER_CONFIGS]
    ranges  = sorted({k[1] for k in set(data_a) | set(data_b)})

    nw = 16  # name column
    tw = 10  # type column
    cw = 13  # cell width

    def cell(a_ms, b_ms):
        if a_ms is None or b_ms is None:
            return f"{'N/A':^{cw}}"
        delta = (b_ms - a_ms) / a_ms * 100
        sign  = "+" if delta > 0 else ""
        return f"{b_ms:.0f}ms({sign}{delta:.0f}%)".center(cw)

    print(f"\n  {label_a} → {label_b}")

    # Two sub-tables: cold and warm
    for run_type in ("cold", "warm"):
        label_rt = "first call, cache empty" if run_type == "cold" else "subsequent calls, cache warm"
        print(f"\n  [{run_type.upper()} — {label_rt}]")
        hdrs = [f"{'filter':<{nw}}", f"{'type':<{tw}}"]
        for n in ranges:
            hdrs.append(f"{'%d blk' % n:^{cw}}")
        print("  " + "  ".join(hdrs))
        sep = "  " + "─" * nw + "  " + "─" * tw + ("  " + "─" * cw) * len(ranges)
        print(sep)

        totals_a, totals_b = defaultdict(list), defaultdict(list)
        for flt in filters:
            ft = _filter_type(flt)
            row = [f"{flt:<{nw}}", f"{ft:<{tw}}"]
            for n in ranges:
                k = (flt, n, run_type)
                a_ms = data_a.get(k)
                b_ms = data_b.get(k)
                row.append(cell(a_ms, b_ms))
                if a_ms and b_ms:
                    totals_a[n].append(a_ms)
                    totals_b[n].append(b_ms)
            print("  " + "  ".join(row))

        print(sep)
        ovr = [f"{'OVERALL AVG':<{nw}}", f"{'':>{tw}}"]
        for n in ranges:
            if totals_a[n]:
                a_avg = sum(totals_a[n]) / len(totals_a[n])
                b_avg = sum(totals_b[n]) / len(totals_b[n])
                ovr.append(cell(a_avg, b_avg))
            else:
                ovr.append(f"{'N/A':^{cw}}")
        print("  " + "  ".join(ovr))
    print()


def main():
    parser = argparse.ArgumentParser(description="Benchmark eth_getLogs latency")
    parser.add_argument("--url",        default="http://localhost:8546")
    parser.add_argument("--tip",        type=int, default=None)
    parser.add_argument("--repeats",    type=int, default=5)
    parser.add_argument("--pre-warmup", type=int, default=0, dest="pre_warmup",
                        help="Extra full passes before measuring. Keep 0 to capture cold latency. "
                             "Use 3+ only when comparing warm-to-warm performance.")
    parser.add_argument("--ranges",     default="1,10,100,1000")
    parser.add_argument("--out",        default=None)
    parser.add_argument("--label",      default="run")
    parser.add_argument("--compare",    nargs=2, metavar="FILE")
    parser.add_argument("--show",       metavar="FILE",
                        help="Print cold/warm table for a single file")
    args = parser.parse_args()

    if args.compare:
        run_compare(args.compare)
    elif args.show:
        run_single_table(args.show)
    else:
        run_bench(args)


if __name__ == "__main__":
    main()
