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


def rep(p50_ms, p95_ms, max_ms, success=1.0, rpc_success=1.0, p90_ms=None):
    p90_ms = p95_ms if p90_ms is None else p90_ms
    return {"vegetaReport": {
        "latencies": {"50th": int(p50_ms * 1e6), "90th": int(p90_ms * 1e6), "95th": int(p95_ms * 1e6),
                      "max": int(max_ms * 1e6)},
        "success": success,
    }, "rpcSuccessRatio": rpc_success}


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
        {"qps": "100", "testRepetitions": [rep(1, 10, 50, p90_ms=5), rep(3, 30, 90, p90_ms=15), rep(2, 20, 70, p90_ms=10)]},
    ])
    res = mod.load_results(tmp)
    s = res["mixed"]["erigon"]["100"]
    check("p50 is the median over repetitions", s["p50"] == 2.0, s)
    check("p90 is the median over repetitions", s.get("p90") == 10.0, s)
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
    check("one collapsed section per test",
          md.count("<details><summary>eth_call</summary>") == 1 and md.count("<details><summary>mixed</summary>") == 1
          and md.count("</details>") == 2, md)

# --- RPC success: the worst repetition, rendered apart from the HTTP one ---------

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "reth", "trace_block_struct_log", [
        {"qps": "5", "testRepetitions": [rep(1, 2, 3, 1.0, 0.25), rep(1, 2, 3, 1.0, 0.06), rep(1, 2, 3, 1.0, 0.5)]},
    ])
    res = mod.load_results(tmp)
    check("rpc success is the worst repetition", res["trace_block_struct_log"]["reth"]["5"]["rpc_success"] == 0.06, res)
    md = mod.render(res)
    check("rpc success rendered next to http success", "| 100.00% | 6.00% |" in md, md)

# --- overview: one row per test and qps, p95 per client, Erigon status --------


def overview_row(text, label, qps):
    return next((line.split() for line in text.splitlines() if line.split()[:2] == [label, qps]), [])


with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_call", [
        {"qps": "1", "testRepetitions": [rep(1, 2.0, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 12.0, 3)]},
        {"qps": "3", "testRepetitions": [rep(1, 13.0, 3)]},
        {"qps": "4", "testRepetitions": [rep(1, 1.05, 3)]},
        {"qps": "5", "testRepetitions": [rep(1, 2.0, 3, 0.9)]},
    ])
    write_result(tmp, "reth", "eth_call", [
        {"qps": "1", "testRepetitions": [rep(1, 4.0, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 10.0, 3)]},
        {"qps": "3", "testRepetitions": [rep(1, 10.0, 3)]},
        {"qps": "4", "testRepetitions": [rep(1, 1.0, 3)]},
        {"qps": "5", "testRepetitions": [rep(1, 1.0, 3)]},
    ])
    write_result(tmp, "geth", "eth_call", [
        {"qps": "1", "testRepetitions": [rep(1, 1.0, 3, 0.99)]},
    ])
    write_result(tmp, "reth", "mixed", [{"qps": "1", "testRepetitions": [rep(1, 1.0, 3)]}])
    md = mod.render_overview(mod.load_results(tmp), "p95")
    check("overview is a code block, for the summary and for Discord",
          md.startswith("```\n") and md.rstrip().endswith("```"), md)
    check("overview header has the clients in fixed order",
          any(line.split() == ["Test", "QPS", "erigon", "geth", "reth"] for line in md.splitlines()), md)
    header = next(line for line in md.splitlines() if line.split()[:2] == ["Test", "QPS"])
    rows = [line.rsplit(" ", 1)[0] for line in md.splitlines() if line[-1:] in ("🟢", "🟡", "🔴", "⚪")]
    check("overview columns are ASCII and aligned, the status closes each row",
          len(rows) == 6 and all(r.isascii() and len(r) == len(header) for r in rows), md)
    row = overview_row(md, "eth_call", "1")
    check("erigon fastest by 1.3x or more is double green, fastest marked with *",
          row == ["eth_call", "1", "2.00*", "err", "4.00", "🟢🟢"], row)
    check("a client below 100% HTTP is err and not the fastest", "1.00*" not in row, row)
    row = overview_row(md, "eth_call", "2")
    check("erigon below 1.3x the fastest is yellow", row[-1:] == ["🟡"] and "10.00*" in row, row)
    row = overview_row(md, "eth_call", "3")
    check("erigon at 1.3x the fastest is red", row[-1:] == ["🔴"], row)
    row = overview_row(md, "eth_call", "4")
    check("erigon not the fastest is never green", row[-1:] == ["🟡"], row)
    row = overview_row(md, "eth_call", "5")
    check("erigon below 100% HTTP is red", row == ["eth_call", "5", "err", "-", "1.00*", "🔴"], row)
    row = overview_row(md, "mixed", "1")
    check("no erigon result, no status", row == ["mixed", "1", "-", "-", "1.00*", "⚪"], row)
    check("legend explains the white status", "⚪ no Erigon result" in md, md)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "mixed", [
        {"qps": "1", "testRepetitions": [rep(1, 20.0, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 1.0, 3, 1.0, 0.9)]},
    ])
    write_result(tmp, "geth", "mixed", [
        {"qps": "1", "testRepetitions": [rep(1, 1.0, 3, 1.0, 0.5)]},
        {"qps": "2", "testRepetitions": [rep(1, 2.0, 3)]},
    ])
    md = mod.render_overview(mod.load_results(tmp), "p95")
    row = overview_row(md, "mixed", "1")
    check("a client below 100% RPC is err and not the fastest", row == ["mixed", "1", "20.00*", "err", "🟢"], row)
    row = overview_row(md, "mixed", "2")
    check("erigon below 100% RPC is red", row == ["mixed", "2", "err", "2.00*", "🔴"], row)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_call", [{"qps": "1", "testRepetitions": [rep(1, 2.0, 3, 1.0, 0.964)]}])
    write_result(tmp, "geth", "eth_call", [{"qps": "1", "testRepetitions": [rep(1, 1.0, 3, 1.0, 0.97)]}])
    write_result(tmp, "reth", "eth_call", [{"qps": "1", "testRepetitions": [rep(1, 0.5, 3, 1.0, 0.95)]}])
    row = overview_row(mod.render_overview(mod.load_results(tmp), "p95"), "eth_call", "1")
    check("RPC errors of every client do not count, more than 1 point below the best does",
          row == ["eth_call", "1", "2.00", "1.00*", "err", "🔴"], row)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_call", [{"qps": "1", "testRepetitions": [rep(1, 3.0, 9)]}])
    write_result(tmp, "reth", "eth_call", [{"qps": "1", "testRepetitions": [rep(2, 2.0, 9)]}])
    text = mod.render_overview(mod.load_results(tmp), "p50")
    row = overview_row(text, "eth_call", "1")
    check("p50 overview uses p50 latency and status", row == ["eth_call", "1", "1.00*", "2.00", "🟢🟢"], row)
    check("p50 overview legend names p50", "status on p50" in text and "p50 latency" in text, text)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_call", [
        {"qps": "1", "testRepetitions": [rep(1, 1.0, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 1.0, 3)]},
        {"qps": "3", "testRepetitions": [rep(1, 1.0, 3)]},
    ])
    write_result(tmp, "geth", "eth_call", [
        {"qps": "1", "testRepetitions": [rep(1, 1.3, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 1.2, 3)]},
        {"qps": "3", "testRepetitions": [rep(1, 1.0, 3)]},
    ])
    write_result(tmp, "reth", "eth_call", [
        {"qps": "1", "testRepetitions": [rep(1, 5.0, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 5.0, 3)]},
        {"qps": "3", "testRepetitions": [rep(1, 5.0, 3)]},
    ])
    text = mod.render_overview(mod.load_results(tmp), "p95")
    row = overview_row(text, "eth_call", "1")
    check("erigon fastest by exactly 1.3x the next client is double green", row[-1:] == ["🟢🟢"], row)
    row = overview_row(text, "eth_call", "2")
    check("erigon fastest by less than 1.3x the next client is green", row[-1:] == ["🟢"], row)
    row = overview_row(text, "eth_call", "3")
    check("erigon tied with another client is green", row[-1:] == ["🟢"], row)
    check("legend explains double green", "🟢🟢 fastest by >= 1.3x" in text, text)

# --- main ---------------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "geth", "mixed", [{"qps": "1", "testRepetitions": [rep(1, 2, 3)]}])
    code, out = run_main(["--results-dir", tmp])
    check("main exits 0", code in (0, None), code)
    check("main prints the comparison", "| 1 | geth |" in out, out)
    check("main prints the p50, p90, p95 and max overviews in this order",
          0 <= out.find("status on p50") < out.find("status on p90") < out.find("status on p95")
          < out.find("status on max"), out)

with tempfile.TemporaryDirectory() as tmp:
    without_p90 = rep(1, 2, 3)
    del without_p90["vegetaReport"]["latencies"]["90th"]
    write_result(tmp, "geth", "mixed", [{"qps": "1", "testRepetitions": [without_p90]}])
    code, out = run_main(["--results-dir", tmp])
    check("main without p90 exits 0", code in (0, None), code)
    check("main without p90 prints no p90 overview",
          "status on p90" not in out and "status on p50" in out and "status on p95" in out, out)

with tempfile.TemporaryDirectory() as tmp:
    code, out = run_main(["--results-dir", tmp])
    check("main without results exits 0", code in (0, None), code)
    check("main without results says so", "No results" in out, out)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
