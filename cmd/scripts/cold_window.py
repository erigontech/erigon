#!/usr/bin/env python3
"""Measure cold btnav by windowed delta around a page-cache eviction.

Usage: cold_window.py PORT DATADIR WINDOW_SECONDS [DOMAINS]

Snapshots btnav_get_sum/count, evicts the 0-8192 .kv+.bt for the given domains
(vmtouch -e), waits WINDOW_SECONDS while the node keeps executing (reading those
files cold), snapshots again, and prints the windowed mean btnav per (domain,level).
mean_cold = (sum1-sum0)/(count1-count0).
"""
import sys, re, time, subprocess, urllib.request, glob, os

port = sys.argv[1]
datadir = sys.argv[2]
window = int(sys.argv[3])
domains = (sys.argv[4] if len(sys.argv) > 4 else "storage,account,code").split(",")
url = f"http://127.0.0.1:{port}/debug/metrics/prometheus"

line_re = re.compile(r'^(btnav_get)_(sum|count)\{([^}]*)\}\s+([0-9eE.+-]+)')

def snap():
    text = urllib.request.urlopen(url, timeout=10).read().decode()
    d = {}
    for ln in text.splitlines():
        m = line_re.match(ln)
        if not m:
            continue
        kind, labels, val = m.group(2), m.group(3), float(m.group(4))
        lv = re.search(r'level="([^"]*)"', labels)
        dm = re.search(r'domain="([^"]*)"', labels)
        if lv and dm:
            d[(dm.group(1), lv.group(1), kind)] = val
    return d

# domain label -> file basename token
filename = {"storage": "storage", "account": "accounts", "code": "code"}

s0 = snap()
evicted = []
for dom in domains:
    tok = filename.get(dom, dom)
    for f in glob.glob(os.path.join(datadir, "snapshots/domain", f"*-{tok}.0-8192.kv")) + \
             glob.glob(os.path.join(datadir, "snapshots/domain", f"*-{tok}.0-8192.bt")):
        subprocess.run(["vmtouch", "-e", f], capture_output=True)
        evicted.append(os.path.basename(f))
print(f"evicted: {evicted}")
print(f"window: {window}s of live execution reading cold...")
time.sleep(window)
s1 = snap()

print(f"\n{'domain':>8} {'level':>7} | {'cold btnav (windowed)':>22} | {'reads in window':>16}")
for dom in domains:
    for lv in ["L0", "L1", "L2", "recent"]:
        c0 = s0.get((dom, lv, "count"), 0); c1 = s1.get((dom, lv, "count"), 0)
        u0 = s0.get((dom, lv, "sum"), 0);  u1 = s1.get((dom, lv, "sum"), 0)
        dc = c1 - c0; du = u1 - u0
        if dc <= 0:
            print(f"{dom:>8} {lv:>7} | {'(no reads)':>22} | {int(dc):>16}")
        else:
            print(f"{dom:>8} {lv:>7} | {du/dc*1e6:18.2f} us | {int(dc):>16}")
