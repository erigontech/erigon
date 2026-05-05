#!/usr/bin/env python3
"""
analyze_logs.py — analyse stage_exec / integration logs.

Usage: ./analyze_logs.py log1 [log2 ...] [--out outdir]

For each log file, extracts per-block timing and emits:
  - per-log summary table (commit/serial/gas-per-second percentiles, memory)
  - comparison table (delta vs first log)
  - per-log CSV with one row per block (commit_ms, serial_ms, mgas_per_s)
  - ASCII histograms of commitment time and mgas/s
  - PNG plots if matplotlib is installed (otherwise skipped silently)

Designed to be re-run as more logs arrive.
"""

import argparse
import csv
import math
import os
import re
import sys
from collections import defaultdict
from typing import Optional


# ---------- duration parsing ----------

DUR_RE = re.compile(
    r'(?:(?P<h>[0-9.]+)h)?'
    r'(?:(?P<m>[0-9.]+)m(?!s))?'
    r'(?:(?P<s>[0-9.]+)s)?'
    r'(?:(?P<ms>[0-9.]+)ms)?'
    r'(?:(?P<us>[0-9.]+)(?:µs|us))?'
    r'(?:(?P<ns>[0-9.]+)ns)?'
    r'$'
)

def parse_duration_ms(s: str) -> Optional[float]:
    """Parse a Go-style duration like '14m8.31s', '6.474ms', '850µs', '14.2s' to milliseconds."""
    if not s:
        return None
    m = DUR_RE.match(s)
    if not m or not any(m.groups()):
        return None
    h  = float(m.group('h')  or 0)
    mi = float(m.group('m')  or 0)
    se = float(m.group('s')  or 0)
    ms = float(m.group('ms') or 0)
    us = float(m.group('us') or 0)
    ns = float(m.group('ns') or 0)
    return h * 3600_000 + mi * 60_000 + se * 1000 + ms + us / 1000 + ns / 1_000_000

def fmt_ms(ms: Optional[float]) -> str:
    if ms is None:
        return "—"
    if ms >= 60_000:
        m = int(ms // 60_000); s = (ms - m * 60_000) / 1000
        return f"{m}m{s:05.2f}s"
    if ms >= 1000:
        return f"{ms/1000:.2f}s"
    if ms >= 1:
        return f"{ms:.3f}ms"
    return f"{ms*1000:.1f}µs"


# ---------- gas/s parsing ----------

# log shows things like "gas/s=309.93M" or "gas/s=1.50G" or "gas/s=12345"
def parse_gas_per_s(s: str) -> Optional[float]:
    """Returns Mgas/s (millions of gas per second)."""
    if not s:
        return None
    m = re.match(r'([0-9.]+)([KMG]?)$', s)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2)
    if unit == 'K':
        val *= 1e3
    elif unit == 'M':
        val *= 1e6
    elif unit == 'G':
        val *= 1e9
    return val / 1e6  # → Mgas/s


# ---------- byte-count parsing ----------

def parse_bytes(s: str) -> Optional[float]:
    """Parse '3.9GB', '512.0MB' etc. to bytes."""
    if not s:
        return None
    m = re.match(r'([0-9.]+)([KMGT]?)B?$', s)
    if not m:
        return None
    val = float(m.group(1))
    mult = {'K': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12}.get(m.group(2), 1)
    return val * mult


# ---------- log line patterns ----------

COMMIT_RE = re.compile(
    r'\[6/8 Execution\] commitment\s+block=(\d+)\s+took=(\S+)\s+avg-per-block=(\S+)\s+blocks=(\d+)'
)

SERIAL_RE = re.compile(
    r'\[6/8 Execution\] serial done\s+in=(\S+).*?'
    r'blk=(\d+).*?'
    r'gas/s=(\S+)\s+'
    r'.*?'
    r'alloc=(\S+)\s+sys=(\S+)'
)

TOTAL_RE = re.compile(r'\] total\s+took=(\S+)')

BUILD_RE = re.compile(r'BuildFilesInBackground|Build state history snapshots')
MERGE_RE = re.compile(r'\[snapshots\] (?:merged|state merge done)')


# ---------- per-log analysis ----------

class LogStats:
    def __init__(self, path: str):
        self.path = path
        self.commits: list[tuple[int, float]] = []   # (block, ms)
        self.serials: list[tuple[int, float]] = []   # (block, ms)
        self.gas_mps: list[tuple[int, float]] = []   # (block, Mgas/s)
        self.alloc_bytes: list[tuple[int, float]] = []
        self.sys_bytes:   list[tuple[int, float]] = []
        self.total_ms: Optional[float] = None
        self.merge_events = 0
        self.build_events = 0

    def parse(self):
        with open(self.path, 'r', errors='replace') as fh:
            for line in fh:
                m = COMMIT_RE.search(line)
                if m:
                    blk = int(m.group(1)); took = parse_duration_ms(m.group(2))
                    if took is not None:
                        self.commits.append((blk, took))
                    continue
                m = SERIAL_RE.search(line)
                if m:
                    in_ms = parse_duration_ms(m.group(1))
                    blk = int(m.group(2))
                    mgas = parse_gas_per_s(m.group(3))
                    alloc = parse_bytes(m.group(4))
                    sys_b = parse_bytes(m.group(5))
                    if in_ms is not None: self.serials.append((blk, in_ms))
                    if mgas  is not None: self.gas_mps.append((blk, mgas))
                    if alloc is not None: self.alloc_bytes.append((blk, alloc))
                    if sys_b is not None: self.sys_bytes.append((blk, sys_b))
                    continue
                m = TOTAL_RE.search(line)
                if m:
                    t = parse_duration_ms(m.group(1))
                    if t is not None:
                        self.total_ms = t
                if BUILD_RE.search(line):
                    self.build_events += 1
                if MERGE_RE.search(line):
                    self.merge_events += 1


def percentiles(values: list[float]) -> dict:
    if not values:
        return {'n': 0}
    s = sorted(values)
    n = len(s)
    pick = lambda p: s[min(n - 1, max(0, int(p * n)))]
    return {
        'n':    n,
        'sum':  sum(s),
        'mean': sum(s) / n,
        'p50':  pick(0.50),
        'p90':  pick(0.90),
        'p95':  pick(0.95),
        'p99':  pick(0.99),
        'max':  s[-1],
        'min':  s[0],
    }


# ---------- formatting ----------

def fmt_bytes(b: Optional[float]) -> str:
    if b is None: return "—"
    for unit, scale in [('TB', 1e12), ('GB', 1e9), ('MB', 1e6), ('KB', 1e3)]:
        if b >= scale: return f"{b/scale:.2f}{unit}"
    return f"{int(b)}B"

def print_per_log_summary(stats_list: list[LogStats]) -> None:
    for st in stats_list:
        print(f"\n--- {st.path} ---")
        print(f"  total wall-clock : {fmt_ms(st.total_ms)}")
        if st.serials:
            blks = [b for b, _ in st.serials]
            print(f"  blocks           : {min(blks)} → {max(blks)}  (n={len(blks)})")
        c = percentiles([t for _, t in st.commits])
        if c['n']:
            print(f"  commit calls     : {c['n']}  cum={fmt_ms(c['sum'])}  mean={c['mean']:.3f}ms")
            print(f"  commit p50/p90/p95/p99/max ms : "
                  f"{c['p50']:.3f} / {c['p90']:.3f} / {c['p95']:.3f} / {c['p99']:.3f} / {c['max']:.3f}")
        s = percentiles([t for _, t in st.serials])
        if s['n']:
            print(f"  serial-done mean : {s['mean']:.3f}ms (p95={s['p95']:.2f}, p99={s['p99']:.2f}, max={s['max']:.2f})")
        g = percentiles([m for _, m in st.gas_mps])
        if g['n']:
            print(f"  Mgas/s mean      : {g['mean']:.2f}  (p50={g['p50']:.2f}, p95={g['p95']:.2f}, max={g['max']:.2f})")
        if st.alloc_bytes:
            a0 = st.alloc_bytes[0][1]; aN = st.alloc_bytes[-1][1]
            sN = st.sys_bytes[-1][1] if st.sys_bytes else None
            print(f"  alloc first→last : {fmt_bytes(a0)} → {fmt_bytes(aN)}   final sys: {fmt_bytes(sN)}")
        print(f"  merge events     : {st.merge_events}")
        print(f"  build events     : {st.build_events}")


def print_compare(stats_list: list[LogStats]) -> None:
    if len(stats_list) < 2:
        return

    base = stats_list[0]
    print(f"\n== comparison vs {base.path} ==")

    rows = []
    bc = percentiles([t for _, t in base.commits])
    bs = percentiles([t for _, t in base.serials])
    bg = percentiles([m for _, m in base.gas_mps])
    rows.append(('total ms',          base.total_ms,
                 [s.total_ms for s in stats_list[1:]]))
    rows.append(('commit cum ms',     bc.get('sum'),
                 [percentiles([t for _, t in s.commits]).get('sum') for s in stats_list[1:]]))
    rows.append(('commit mean ms',    bc.get('mean'),
                 [percentiles([t for _, t in s.commits]).get('mean') for s in stats_list[1:]]))
    rows.append(('commit p50 ms',     bc.get('p50'),
                 [percentiles([t for _, t in s.commits]).get('p50') for s in stats_list[1:]]))
    rows.append(('commit p95 ms',     bc.get('p95'),
                 [percentiles([t for _, t in s.commits]).get('p95') for s in stats_list[1:]]))
    rows.append(('commit p99 ms',     bc.get('p99'),
                 [percentiles([t for _, t in s.commits]).get('p99') for s in stats_list[1:]]))
    rows.append(('commit max ms',     bc.get('max'),
                 [percentiles([t for _, t in s.commits]).get('max') for s in stats_list[1:]]))
    rows.append(('serial mean ms',    bs.get('mean'),
                 [percentiles([t for _, t in s.serials]).get('mean') for s in stats_list[1:]]))
    rows.append(('Mgas/s mean',       bg.get('mean'),
                 [percentiles([m for _, m in s.gas_mps]).get('mean') for s in stats_list[1:]]))
    rows.append(('Mgas/s p50',        bg.get('p50'),
                 [percentiles([m for _, m in s.gas_mps]).get('p50') for s in stats_list[1:]]))
    rows.append(('Mgas/s max',        bg.get('max'),
                 [percentiles([m for _, m in s.gas_mps]).get('max') for s in stats_list[1:]]))

    # header
    name = lambda s: os.path.basename(s.path)
    hdr = f"{'metric':<22}{name(base):>14}"
    for s in stats_list[1:]:
        hdr += f" {name(s):>14}{'Δ%':>9}"
    print(hdr)

    for label, b, vs in rows:
        line = f"{label:<22}{(f'{b:.3f}' if isinstance(b,float) else '—' if b is None else b):>14}"
        for v in vs:
            if v is None or b is None or b == 0:
                line += f" {'—':>14}{'—':>9}"
            else:
                pct = (v - b) / b * 100
                line += f" {v:>14.3f}{pct:>+8.2f}%"
        print(line)


def print_histogram(stats: LogStats, key: str = 'commit', bins: int = 30, width: int = 50) -> None:
    if key == 'commit':
        vals = [t for _, t in stats.commits]; label = 'commit ms'
    elif key == 'gas':
        vals = [m for _, m in stats.gas_mps]; label = 'Mgas/s'
    elif key == 'serial':
        vals = [t for _, t in stats.serials]; label = 'serial ms'
    else:
        return
    if not vals:
        return
    lo, hi = min(vals), max(vals)
    if lo == hi:
        return
    step = (hi - lo) / bins
    counts = [0] * bins
    for v in vals:
        idx = min(bins - 1, int((v - lo) / step))
        counts[idx] += 1
    cmax = max(counts)
    print(f"\n  histogram: {label}  (n={len(vals)}, range {lo:.2f}–{hi:.2f})")
    for i, c in enumerate(counts):
        bar_lo = lo + i * step
        bar = '#' * int(c / cmax * width)
        print(f"    {bar_lo:>8.2f} | {bar} {c}")


# ---------- CSV / plot output ----------

def write_csv(stats: LogStats, outdir: str) -> str:
    os.makedirs(outdir, exist_ok=True)
    csv_path = os.path.join(outdir, os.path.basename(stats.path) + '.csv')

    by_block = defaultdict(dict)
    for blk, t in stats.commits: by_block[blk]['commit_ms'] = t
    for blk, t in stats.serials: by_block[blk]['serial_ms'] = t
    for blk, m in stats.gas_mps: by_block[blk]['mgas_per_s'] = m
    for blk, b in stats.alloc_bytes: by_block[blk]['alloc_mb'] = b / 1e6

    with open(csv_path, 'w', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['block', 'commit_ms', 'serial_ms', 'mgas_per_s', 'alloc_mb'])
        for blk in sorted(by_block):
            row = by_block[blk]
            w.writerow([blk,
                        row.get('commit_ms', ''),
                        row.get('serial_ms', ''),
                        row.get('mgas_per_s', ''),
                        row.get('alloc_mb', '')])
    return csv_path


def make_plots(stats_list: list[LogStats], outdir: str) -> Optional[list[str]]:
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    os.makedirs(outdir, exist_ok=True)
    paths = []

    # 1) commit time over blocks (rolling mean)
    fig, ax = plt.subplots(figsize=(10, 4))
    for s in stats_list:
        if not s.commits: continue
        xs = [b for b, _ in s.commits]
        ys = [t for _, t in s.commits]
        # rolling 50-block mean for readability
        win = 50
        if len(ys) >= win:
            ry = [sum(ys[max(0,i-win):i+1]) / min(i+1, win) for i in range(len(ys))]
        else:
            ry = ys
        ax.plot(xs, ry, label=os.path.basename(s.path), linewidth=1)
    ax.set_xlabel('block'); ax.set_ylabel('commit ms (50-block rolling mean)')
    ax.set_title('Commitment time per block'); ax.legend(); ax.grid(alpha=0.3)
    p = os.path.join(outdir, 'commit_over_blocks.png')
    fig.tight_layout(); fig.savefig(p, dpi=120); plt.close(fig); paths.append(p)

    # 2) commit time CDF
    fig, ax = plt.subplots(figsize=(8, 4))
    for s in stats_list:
        ys = sorted([t for _, t in s.commits])
        if not ys: continue
        cdf = [(i + 1) / len(ys) for i in range(len(ys))]
        ax.plot(ys, cdf, label=os.path.basename(s.path))
    ax.set_xlabel('commit ms'); ax.set_ylabel('CDF')
    ax.set_title('Commitment time CDF'); ax.legend(); ax.grid(alpha=0.3)
    ax.set_xscale('log')
    p = os.path.join(outdir, 'commit_cdf.png')
    fig.tight_layout(); fig.savefig(p, dpi=120); plt.close(fig); paths.append(p)

    # 3) Mgas/s over blocks (rolling)
    fig, ax = plt.subplots(figsize=(10, 4))
    for s in stats_list:
        if not s.gas_mps: continue
        xs = [b for b, _ in s.gas_mps]
        ys = [m for _, m in s.gas_mps]
        win = 50
        if len(ys) >= win:
            ry = [sum(ys[max(0,i-win):i+1]) / min(i+1, win) for i in range(len(ys))]
        else:
            ry = ys
        ax.plot(xs, ry, label=os.path.basename(s.path), linewidth=1)
    ax.set_xlabel('block'); ax.set_ylabel('Mgas/s (50-block rolling mean)')
    ax.set_title('Throughput per block'); ax.legend(); ax.grid(alpha=0.3)
    p = os.path.join(outdir, 'mgas_over_blocks.png')
    fig.tight_layout(); fig.savefig(p, dpi=120); plt.close(fig); paths.append(p)

    # 4) memory over blocks
    fig, ax = plt.subplots(figsize=(10, 4))
    for s in stats_list:
        if not s.alloc_bytes: continue
        xs = [b for b, _ in s.alloc_bytes]
        ys = [a / 1e9 for _, a in s.alloc_bytes]
        ax.plot(xs, ys, label=os.path.basename(s.path), linewidth=1)
    ax.set_xlabel('block'); ax.set_ylabel('alloc (GB)')
    ax.set_title('Heap alloc per block'); ax.legend(); ax.grid(alpha=0.3)
    p = os.path.join(outdir, 'alloc_over_blocks.png')
    fig.tight_layout(); fig.savefig(p, dpi=120); plt.close(fig); paths.append(p)

    return paths


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('logs', nargs='+', help='log files to analyse')
    ap.add_argument('--out', default='analysis_out', help='output dir for CSV/PNG')
    ap.add_argument('--no-hist', action='store_true', help='skip ASCII histograms')
    args = ap.parse_args()

    stats_list = []
    for p in args.logs:
        if not os.path.isfile(p):
            print(f"skip (not a file): {p}", file=sys.stderr); continue
        s = LogStats(p)
        print(f"parsing {p} ...", file=sys.stderr)
        s.parse()
        stats_list.append(s)

    if not stats_list:
        print("no logs", file=sys.stderr); sys.exit(1)

    print("\n== per-log summary ==")
    print_per_log_summary(stats_list)

    if not args.no_hist:
        for s in stats_list:
            print(f"\n--- {s.path} ASCII histograms ---")
            print_histogram(s, 'commit')
            print_histogram(s, 'gas')

    print_compare(stats_list)

    print(f"\n== writing CSVs to {args.out}/ ==")
    for s in stats_list:
        p = write_csv(s, args.out)
        print(f"  {p}")

    plot_paths = make_plots(stats_list, args.out)
    if plot_paths:
        print("\n== plots written ==")
        for p in plot_paths: print(f"  {p}")
    else:
        print("\n(matplotlib not installed — skipping PNGs. `pip install matplotlib` to enable plots.)")


if __name__ == '__main__':
    main()
