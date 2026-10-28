#!/usr/bin/env python3
"""Fixture tests for compare_rpc_perf.py.

Run: python3 .github/workflows/scripts/compare_rpc_perf.test.py
"""
import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("compare_rpc_perf", os.path.join(HERE, "compare_rpc_perf.py"))
assert spec is not None and spec.loader is not None
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

passed = 0
failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"PASS {name}")
    else:
        failed += 1
        print(f"FAIL {name} {detail}")


def rep(p50_ms, p95_ms, max_ms, success=1.0):
    return {"vegetaReport": {
        "latencies": {"50th": int(p50_ms * 1e6), "95th": int(p95_ms * 1e6), "max": int(max_ms * 1e6)},
        "success": success,
    }}


def write_result(root, client, label, results):
    path = os.path.join(root, "mainnet", f"{client}-{label}-result.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"configuration": {"testingDaemon": client}, "results": results}, fh)


def run_main(argv):
    out = io.StringIO()
    old = sys.argv
    sys.argv = ["compare_rpc_perf.py"] + argv
    try:
        with contextlib.redirect_stdout(out):
            code = mod.main()
    except SystemExit as e:
        code = e.code
    finally:
        sys.argv = old
    return code, out.getvalue()


# --- load_results: medians of p50/p95 over repetitions, max of max -----------

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "mixed", [
        {"qps": "100", "testRepetitions": [rep(1, 10, 50), rep(3, 30, 90), rep(2, 20, 70)]},
    ])
    res = mod.load_results(tmp)
    s = res["mixed"]["erigon"]["100"]
    check("p50 is the median over repetitions", s["p50"] == 2.0, s)
    check("p95 is the median over repetitions", s["p95"] == 20.0, s)
    check("max is the max over repetitions", s["max"] == 90.0, s)
    check("success is the worst repetition", s["success"] == 1.0, s)

# --- label with an underscore and a client-name prefix -----------------------

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "nethermind", "eth_call_gen", [{"qps": "1", "testRepetitions": [rep(1, 1, 1, 0.5)]}])
    res = mod.load_results(tmp)
    check("label is the file name without client and suffix", list(res) == ["eth_call_gen"], res)
    check("client comes from the report", list(res["eth_call_gen"]) == ["nethermind"], res)
    check("worst success kept", res["eth_call_gen"]["nethermind"]["1"]["success"] == 0.5, res)

# --- render: one row per qps and client, clients in fixed order --------------

with tempfile.TemporaryDirectory() as tmp:
    for client in ("nethermind", "geth", "erigon"):
        write_result(tmp, client, "eth_call", [
            {"qps": "1", "testRepetitions": [rep(1, 2, 3)]},
            {"qps": "1000", "testRepetitions": [rep(4, 5, 6)]},
        ])
    write_result(tmp, "erigon", "mixed", [{"qps": "100", "testRepetitions": [rep(1, 2, 3)]}])
    md = mod.render(mod.load_results(tmp))
    rows = [line for line in md.splitlines() if line.startswith("| ") and "---" not in line and "QPS" not in line]
    check("one row per test, qps and client", len(rows) == 7, md)
    eth_rows = rows[:6]
    check("qps sorted numerically", [r.split("|")[1].strip() for r in eth_rows] == ["1"] * 3 + ["1000"] * 3, md)
    check("clients in fixed order", [r.split("|")[2].strip() for r in eth_rows[:3]] == ["erigon", "geth", "nethermind"], md)
    check("latencies rendered in ms", "| 1000 | erigon | 4.00 | 5.00 | 6.00 | 100.00% |" in md, md)
    check("one section per test", md.count("### eth_call") == 1 and md.count("### mixed") == 1, md)

# --- main ---------------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "geth", "mixed", [{"qps": "1", "testRepetitions": [rep(1, 2, 3)]}])
    code, out = run_main(["--results-dir", tmp])
    check("main exits 0", code in (0, None), code)
    check("main prints the comparison", "| 1 | geth |" in out, out)

with tempfile.TemporaryDirectory() as tmp:
    code, out = run_main(["--results-dir", tmp])
    check("main without results exits 0", code in (0, None), code)
    check("main without results says so", "No results" in out, out)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
