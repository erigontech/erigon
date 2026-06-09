#!/usr/bin/env python3
"""Scrape kvei/btnav/kvval/kv_get split summaries from erigon's prometheus endpoint.

Usage: scrape_split.py [PORT] [DOMAINS]
  PORT     metrics port (default 6070)
  DOMAINS  comma list (default storage,account,code,commitment)

Prints, per (domain, level): mean microseconds and call count for each phase.
mean = <metric>_sum / <metric>_count.
"""
import sys, re, urllib.request

port = sys.argv[1] if len(sys.argv) > 1 else "6070"
domains = (sys.argv[2] if len(sys.argv) > 2 else "storage,account,code,commitment").split(",")
url = f"http://127.0.0.1:{port}/debug/metrics/prometheus"

text = urllib.request.urlopen(url, timeout=10).read().decode()

# metric{labels} value   -> capture _sum and _count for our four metrics
metrics = ["kvei_get", "btnav_get", "kvval_get", "kv_get"]
levels = ["L0", "L1", "L2", "L3", "L4", "recent"]
# data[(metric, domain, level)] = {"sum":x, "count":y}
data = {}
line_re = re.compile(r'^(\w+)\{([^}]*)\}\s+([0-9eE.+-]+)')
for ln in text.splitlines():
    m = line_re.match(ln)
    if not m:
        continue
    name, labels, val = m.group(1), m.group(2), float(m.group(3))
    base = None
    if name.endswith("_sum"):
        base, kind = name[:-4], "sum"
    elif name.endswith("_count"):
        base, kind = name[:-6], "count"
    else:
        continue
    if base not in metrics:
        continue
    lv = re.search(r'level="([^"]*)"', labels)
    dm = re.search(r'domain="([^"]*)"', labels)
    if not lv or not dm:
        continue
    key = (base, dm.group(1), lv.group(1))
    data.setdefault(key, {})[kind] = val

def mean_us(metric, dom, lv):
    d = data.get((metric, dom, lv))
    if not d or d.get("count", 0) == 0:
        return None, 0
    return d["sum"] / d["count"] * 1e6, int(d["count"])

def fc_line(dom):
    d = data.get(("filecache_get", dom, "__"))
    if not d or d.get("count", 0) == 0:
        return "filecache: (none)"
    return f"filecache: {d['sum']/d['count']*1e6:.2f}us/{int(d['count'])} hits"

# filecache has no level label; capture it under a synthetic level "__"
for ln in text.splitlines():
    m = line_re.match(ln)
    if not m:
        continue
    name, labels, val = m.group(1), m.group(2), float(m.group(3))
    if name in ("filecache_get_sum", "filecache_get_count"):
        base = name[:-4] if name.endswith("_sum") else name[:-6]
        kind = "sum" if name.endswith("_sum") else "count"
        dm = re.search(r'domain="([^"]*)"', labels)
        if dm:
            data.setdefault((base, dm.group(1), "__"), {})[kind] = val

for dom in domains:
    print(f"\n=== domain={dom} (mean microseconds | count) ===")
    print(f"  {fc_line(dom)}")
    print(f"{'level':>7} | {'kvei':>14} | {'btnav':>14} | {'kvval':>14} | {'kv_get(total)':>16}")
    for lv in levels:
        cells = []
        for metric in metrics:
            us, cnt = mean_us(metric, dom, lv)
            if us is None:
                cells.append(f"{'-':>14}")
            else:
                cells.append(f"{us:8.2f}us/{cnt:>6}" if metric != "kv_get" else f"{us:9.2f}us/{cnt:>6}")
        print(f"{lv:>7} | {cells[0]} | {cells[1]} | {cells[2]} | {cells[3]}")
