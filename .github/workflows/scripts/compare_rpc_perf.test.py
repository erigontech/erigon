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


def write_result(root, client, label, results, pattern=None):
    path = os.path.join(root, "mainnet", f"{client}-{label}-result.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    configuration = {"testingDaemon": client}
    if pattern:
        configuration["pattern"] = pattern
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"configuration": configuration, "results": results}, fh)


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
    rows = [line.rsplit(" ", 1)[0] for line in md.splitlines() if line[-1:] in ("🟢", "🟡", "🔴")]
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
    check("no erigon result where another client has one is red", row == ["mixed", "1", "-", "-", "1.00*", "🔴"], row)
    check("legend says a missing Erigon result is red", "🔴 >= 1.3x, errors or no Erigon result" in md, md)
    check("legend has no white status", "⚪" not in md, md)

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

# --- dead: a level that rpc_perf recorded with an error -------------------------

DEAD = "server died during test"

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "reth", "trace_block_struct_log", [
        {"qps": "3", "testRepetitions": [rep(1, 2, 3)]},
        {"qps": "5", "testRepetitions": [], "error": DEAD},
    ])
    write_result(tmp, "geth", "trace_block_struct_log", [
        {"qps": "3", "testRepetitions": [rep(1, 2, 3)]},
        {"qps": "5", "testRepetitions": [rep(1, 2, 3)], "error": DEAD},
    ])
    res = mod.load_results(tmp)
    check("a level with an error and no repetition is dead", res["trace_block_struct_log"]["reth"]["5"] == {"failed": "dead", "error": DEAD}, res)
    check("a level with an error after some repetitions is dead", res["trace_block_struct_log"]["geth"]["5"] == {"failed": "dead", "error": DEAD}, res)
    md = mod.render(res)
    check("the detail table shows the error of a dead level", f"| 5 | reth | dead: {DEAD} |" in md, md)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "trace_block_struct_log", [
        {"qps": "3", "testRepetitions": [rep(1, 2.0, 3)]},
        {"qps": "5", "testRepetitions": [rep(1, 2.0, 3)]},
    ])
    write_result(tmp, "reth", "trace_block_struct_log", [
        {"qps": "3", "testRepetitions": [rep(1, 1.0, 3)]},
        {"qps": "5", "testRepetitions": [], "error": DEAD},
    ])
    write_result(tmp, "erigon", "mixed", [{"qps": "1", "testRepetitions": [], "error": DEAD}])
    write_result(tmp, "geth", "mixed", [{"qps": "1", "testRepetitions": [rep(1, 1.0, 3)]}])
    md = mod.render_overview(mod.load_results(tmp), "p95")
    row = overview_row(md, "trace_block_struct_log", "5")
    check("a dead client is dead in the overview and erigon is the fastest",
          row == ["trace_block_struct_log", "5", "2.00*", "-", "dead", "🟢"], row)
    row = overview_row(md, "mixed", "1")
    check("a dead erigon is red", row == ["mixed", "1", "dead", "1.00*", "-", "🔴"], row)
    check("legend explains dead", "dead the client died" in md, md)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "reth", "mixed", [
        {"qps": "1", "testRepetitions": [rep(1, 2, 3)]},
        {"qps": "2", "testRepetitions": [], "error": DEAD},
    ])
    code, out = run_main(["--results-dir", tmp])
    check("main with a dead level exits 0", code in (0, None), code)
    check("main with a dead level prints all the overviews",
          all(f"status on {p}" in out for p in ("p50", "p90", "p95", "max")), out)

# --- excluded: a client left out at a barrier, so its later patterns did not run ---


def write_excluded(root, client, label):
    path = os.path.join(root, f"{client}-{label}-result.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"configuration": {"testingDaemon": client}, "excluded": True, "results": []}, fh)


with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_call_gen", [
        {"qps": "100", "testRepetitions": [rep(1, 2.0, 3)]},
        {"qps": "1000", "testRepetitions": [rep(1, 2.0, 3)]},
    ])
    write_result(tmp, "reth", "eth_call_gen", [
        {"qps": "100", "testRepetitions": [rep(1, 1.0, 3)]},
        {"qps": "1000", "testRepetitions": [rep(1, 1.0, 3)]},
    ])
    write_excluded(tmp, "geth", "eth_call_gen")
    write_excluded(tmp, "erigon", "mixed")
    write_result(tmp, "reth", "mixed", [{"qps": "1", "testRepetitions": [rep(1, 1.0, 3)]}])
    res = mod.load_results(tmp)
    check("an excluded client has every qps step of the pattern, excluded",
          res["eth_call_gen"]["geth"] == {"100": {"failed": "excluded"}, "1000": {"failed": "excluded"}}, res)
    md = mod.render_overview(res, "p95")
    row = overview_row(md, "eth_call_gen", "1000")
    check("an excluded client is excluded in the overview",
          row == ["eth_call_gen", "1000", "2.00", "excluded", "1.00*", "🔴"], row)
    row = overview_row(md, "mixed", "1")
    check("an excluded erigon is red", row == ["mixed", "1", "excluded", "-", "1.00*", "🔴"], row)
    check("legend explains excluded", "excluded left out at a barrier" in md, md)
    md = mod.render(res)
    check("the detail table shows an excluded client", "| 100 | geth | excluded | | | | |" in md, md)
    code, out = run_main(["--results-dir", tmp])
    check("main with an excluded client prints all the overviews",
          code in (0, None) and all(f"status on {p}" in out for p in ("p50", "p90", "p95", "max")), out)

# --- lost: a client of the run whose job uploaded no report -------------------

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "reth", "mixed", [
        {"qps": "1", "testRepetitions": [rep(1, 1.0, 3)]},
        {"qps": "2", "testRepetitions": [rep(1, 1.0, 3)]},
    ])
    write_excluded(tmp, "geth", "mixed")
    res = mod.load_results(tmp)
    check("add_lost returns the clients with no report", mod.add_lost(res, ["erigon", "geth", "reth"]) == ["erigon"], res)
    check("a client with no report is lost on every qps step",
          res["mixed"]["erigon"] == {"1": {"failed": "lost"}, "2": {"failed": "lost"}}, res)
    check("a client with an excluded report is not lost", res["mixed"]["geth"]["1"] == {"failed": "excluded"}, res)
    md = mod.render_overview(res, "p50")
    row = overview_row(md, "mixed", "1")
    check("a lost erigon is lost and red in the overview", row == ["mixed", "1", "lost", "excluded", "1.00*", "🔴"], row)
    check("legend explains lost", "lost no results from the client's job" in md, md)
    md = mod.render(res)
    check("the detail table shows a lost client", "| 1 | erigon | lost | | | | |" in md, md)
    code, out = run_main(["--results-dir", tmp, "--clients", "erigon,geth,reth"])
    check("main warns about a lost client", "⚠️ erigon: no results" in out, out)
    check("main does not warn about a client with a report", "⚠️ geth" not in out and "⚠️ reth" not in out, out)
    code, out = run_main(["--results-dir", tmp])
    check("main without --clients has no lost client", "⚠️" not in out and "| erigon |" not in out, out)

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

# --- apis: the rows by method (mixed with --verbose) or by subscription measure ---


def api(p50_ms, p95_ms, max_ms, requests, errors=0, p90_ms=None):
    latencies = {"50th": int(p50_ms * 1e6), "95th": int(p95_ms * 1e6), "max": int(max_ms * 1e6)}
    if p90_ms is not None:
        latencies["90th"] = int(p90_ms * 1e6)
    return {"requests": requests, "errors": errors, "empty": 0, "latencies": latencies}


def rep_apis(apis):
    r = rep(1, 2, 3)
    r["apis"] = apis
    return r


with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "mixed", [{"qps": "100", "testRepetitions": [
        rep_apis({"eth_call": api(1, 10, 50, 100, 1), "eth_getLogs": api(4, 40, 400, 10)}),
        rep_apis({"eth_call": api(3, 30, 90, 100, 2)}),
        rep_apis({"eth_call": api(2, 20, 70, 100)}),
    ]}])
    res = mod.load_results(tmp)
    apis = res["mixed"]["erigon"]["100"]["apis"]
    check("api p50 and p95 are the median over repetitions",
          apis["eth_call"]["p50"] == 2.0 and apis["eth_call"]["p95"] == 20.0, apis)
    check("api max is the max over repetitions", apis["eth_call"]["max"] == 90.0, apis)
    check("api requests and errors are summed over repetitions",
          apis["eth_call"]["requests"] == 300 and apis["eth_call"]["errors"] == 3, apis)
    check("an api of some repetitions only is over those", apis["eth_getLogs"]["requests"] == 10, apis)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_call", [{"qps": "1", "testRepetitions": [rep(1, 2, 3)]}])
    res = mod.load_results(tmp)
    check("a step without apis has no apis", "apis" not in res["eth_call"]["erigon"]["1"], res)
    md = mod.render(res)
    check("the detail without apis has no api table", "| API |" not in md, md)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_subscribe", [{"qps": "10", "testRepetitions": [
        rep_apis({"newHeads latency": api(1, 2, 3, 30), "logs vs newHeads": api(4, 5, 6, 90, 1)})]}])
    write_result(tmp, "geth", "eth_subscribe", [{"qps": "10", "testRepetitions": [
        rep_apis({"newHeads latency": api(7, 8, 9, 30)})]}])
    md = mod.render(mod.load_results(tmp))
    check("the api table has a row per api and client",
          "| 10 | logs vs newHeads | erigon | 4.00 | 5.00 | 6.00 | 90 | 1 |" in md
          and "| 10 | newHeads latency | erigon | 1.00 | 2.00 | 3.00 | 30 | 0 |" in md
          and "| 10 | newHeads latency | geth | 7.00 | 8.00 | 9.00 | 30 | 0 |" in md, md)
    check("the api rows are by api, then by client",
          md.find("logs vs newHeads | erigon") < md.find("newHeads latency | erigon")
          < md.find("newHeads latency | geth"), md)
    check("the api table follows the step table in the same details",
          md.find("| QPS | Client |") < md.find("| QPS | API | Client |") < md.find("</details>"), md)

# --- eth_subscribe: the fan-out rows get an overview row of their own ------------


def fanout_row(text, name, qps):
    """The cells of the overview row of a fan-out measure, whose name holds spaces."""
    line = next((line for line in text.splitlines() if line.startswith(name + " ")), "")
    cells = line[len(name):].split()
    return cells if cells[:1] == [qps] else []


with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "newHeads fan-out": api(11, 39, 66, 1000, p90_ms=30),
        "newHeads fan-out wallet": api(11, 39, 66, 900, p90_ms=30),
        "newHeads latency wallet": api(1600, 3300, 3500, 900, p90_ms=3200),
        "transactionReceipts fan-out indexer": api(5, 17, 26, 10, p90_ms=12)})]}])
    write_result(tmp, "geth", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "newHeads fan-out": api(5, 20, 85, 1000, p90_ms=15),
        "newHeads fan-out wallet": api(5, 20, 85, 900, p90_ms=15),
        "newHeads latency wallet": api(1700, 3400, 3700, 900, p90_ms=3300),
        "transactionReceipts fan-out indexer": api(4.5, 16, 43, 10, 1, p90_ms=11)})]}])
    write_result(tmp, "reth", "eth_subscribe_classes", [
        {"qps": "500", "testRepetitions": [], "error": "dial: bad handshake\ndial: bad handshake"}])
    res = mod.load_results(tmp)
    md = mod.render_overview(mod.overview_results(res), "p50")
    row = fanout_row(md, "eth_subscribe_classes newHeads fan-out wallet", "500")
    check("a fan-out measure has its own overview row with the erigon status",
          row == ["500", "11.00", "5.00*", "dead", "🔴"], md)
    row = fanout_row(md, "eth_subscribe_classes transactionReceipts fan-out indexer", "500")
    check("errors in a fan-out measure make the client err", row == ["500", "5.00*", "err", "dead", "🟢"], md)
    check("a measure with copies per class has no overview row of its own",
          "eth_subscribe_classes newHeads fan-out 500" not in " ".join(md.split()), md)
    check("the latency copies per class have no overview row", "newHeads latency" not in md, md)
    md90 = mod.render_overview(mod.overview_results(res), "p90")
    check("the p90 overview has the fan-out rows",
          fanout_row(md90, "eth_subscribe_classes newHeads fan-out wallet", "500")[:3] == ["500", "30.00", "15.00*"], md90)
    details = mod.render(res)
    check("the fan-out measures have no details section of their own", details.count("<details>") == 1, details)
    check("a dead client shows only the first line of its error",
          "| 500 | reth | dead: dial: bad handshake | | | | |" in details, details)

# --- eth_subscribe: newHeads latency gets one overview row, for all the classes ---

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "newHeads latency": api(2087, 3384, 3500, 1000, p90_ms=3300),
        "newHeads latency wallet": api(2087, 3384, 3500, 900, p90_ms=3300),
        "logs latency dapp": api(3086, 3485, 3500, 60, p90_ms=3400)})]}])
    write_result(tmp, "geth", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "newHeads latency": api(1971, 3295, 3450, 1000, p90_ms=3200),
        "newHeads latency wallet": api(1971, 3295, 3450, 900, p90_ms=3200),
        "logs latency dapp": api(2982, 3432, 3450, 60, p90_ms=3400)})]}])
    md = mod.render_overview(mod.overview_results(mod.load_results(tmp)), "p50")
    check("newHeads latency has one overview row",
          fanout_row(md, "eth_subscribe_classes newHeads latency", "500") == ["500", "2087.00", "1971.00*", "🟡"], md)
    check("the newHeads latency copies per class have no overview row", "newHeads latency wallet" not in md, md)
    check("the logs and receipts latency measures have no overview row", "logs latency" not in md, md)

# --- eth_subscribe: the vs first newHeads rows get an overview row, the vs newHeads rows do not ---

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "logs vs newHeads dapp": api(2.4, 12, 20, 60, p90_ms=8),
        "logs vs first newHeads": api(3, 12, 20, 100, p90_ms=8),
        "logs vs first newHeads dapp": api(3, 12, 20, 60, p90_ms=8),
        "transactionReceipts vs first newHeads indexer": api(20, 40, 60, 10, p90_ms=30)})]}])
    write_result(tmp, "geth", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "logs vs newHeads dapp": api(0.2, 13, 20, 60, p90_ms=9),
        "logs vs first newHeads": api(3.3, 13, 20, 100, p90_ms=9),
        "logs vs first newHeads dapp": api(3.3, 13, 20, 60, p90_ms=9),
        "transactionReceipts vs first newHeads indexer": api(25, 45, 70, 10, p90_ms=35)})]}])
    md = mod.render_overview(mod.overview_results(mod.load_results(tmp)), "p50")
    check("a vs first newHeads measure has its own overview row",
          fanout_row(md, "eth_subscribe_classes logs vs first newHeads dapp", "500") == ["500", "3.00*", "3.30", "🟢"], md)
    check("the vs first newHeads rows of every class are in the overview",
          fanout_row(md, "eth_subscribe_classes transactionReceipts vs first newHeads indexer", "500")
          == ["500", "20.00*", "25.00", "🟢"], md)
    check("a vs first newHeads measure with copies per class has no overview row of its own",
          "eth_subscribe_classes logs vs first newHeads 500" not in " ".join(md.split()), md)
    check("the vs newHeads measures are not in the overview", "logs vs newHeads" not in md, md)

# --- eth_subscribe client classes: legend and the three measures per class -------

CLASSES = {"description": "eth_subscribe by client class: wallet share 90: 1 newHeads; dapp share 9, pick 1: "
                          "1 newHeads, 50 logs filter=address; indexer share 1: 1 newHeads, 1 logs filter=all, "
                          "1 transactionReceipts; logs filters from the 3000 most recent blocks",
           "clients": [{"name": "wallet", "share": 90}, {"name": "dapp", "share": 9, "pick": 1},
                       {"name": "indexer", "share": 1}]}

# --- eth_subscribe: a client that sends one notification per receipt is marked with ( )! and not compared ---

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "newHeads fan-out indexer": api(1, 2, 3, 10),
        "transactionReceipts fan-out indexer": api(0.63, 2, 3, 10)})]}])
    write_result(tmp, "nethermind", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "newHeads fan-out indexer": api(0.3, 2, 3, 10),
        "transactionReceipts fan-out indexer": api(0.62, 2, 3, 10),
        "transactionReceipts per receipt indexer": api(5, 9, 12, 10)})]}])
    md = mod.render_overview(mod.overview_results(mod.load_results(tmp)), "p50")
    check("the receipts rows show in ( )! the client that sends one notification per receipt, out of the status",
          fanout_row(md, "eth_subscribe_classes transactionReceipts fan-out indexer", "500")
          == ["500", "0.63*", "(0.62)!", "🟢"], md)
    check("the rows of the other types have no !",
          fanout_row(md, "eth_subscribe_classes newHeads fan-out indexer", "500") == ["500", "1.00", "0.30*", "🔴"], md)
    check("the overview legend explains the ( )!",
          "( )! not compared: the client sends the receipts of a block one notification each" in md, md)
    md = mod.render_classes(mod.load_results(tmp), {"eth_subscribe_classes": CLASSES})
    check("the class table shows the receipts measures of that client in ( )!, out of the fastest",
          "| indexer | transactionReceipts fan-out | 0.63* / 2.00 | (0.62)! / (2.00) |" in md
          and "| indexer | newHeads fan-out | 1.00 / 2.00 | 0.30* / 2.00 |" in md, md)

with tempfile.TemporaryDirectory() as tmp:
    write_result(tmp, "erigon", "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
        "transactionReceipts fan-out indexer": api(0.63, 2, 3, 10)})]}])
    md = mod.render_overview(mod.overview_results(mod.load_results(tmp)), "p50")
    check("without a client that sends one notification per receipt the legend has no ( )!", "( )!" not in md, md)

with tempfile.TemporaryDirectory() as tmp:
    for client, f in (("erigon", 1), ("geth", 2)):
        write_result(tmp, client, "eth_subscribe_classes", [{"qps": "500", "testRepetitions": [rep_apis({
            "newHeads latency wallet": api(1900 * f, 3300, 3500, 900),
            "newHeads fan-out wallet": api(3 * f, 16, 20, 900),
            "newHeads fan-out": api(3 * f, 16, 20, 990),
            "logs vs newHeads dapp": api(0.4 * f, 12, 20, 60),
            "logs vs first newHeads dapp": api(3 * f, 12, 20, 60)})]}], CLASSES)
    write_result(tmp, "erigon", "eth_call", [{"qps": "1", "testRepetitions": [rep(1, 2, 3)]}])
    patterns = mod.load_patterns(tmp)
    check("the pattern of a test with client classes is loaded", list(patterns) == ["eth_subscribe_classes"], patterns)
    md = mod.render_classes(mod.load_results(tmp), patterns)
    check("the legend says that the classes split the connections and what each one subscribes to",
          "The connections are split into 3 classes of clients, by share of the total (90% + 9% + 1% = 100%):" in md
          and "- **wallet**, 90% of the connections. Each one subscribes to `newHeads`: one notification per new block, "
              "with its header." in md
          and "- **dapp**, 9% of the connections. Each one subscribes to `newHeads` and to `logs` filtered by one contract "
              "address, a different one per connection, taken in turn from 50 addresses active in recent blocks." in md
          and "- **indexer**, 1% of the connections. Each one subscribes to `newHeads`, to `logs` with no filter (every log "
              "of every block) and to `transactionReceipts` (all the receipts of a block in one notification)." in md, md)
    check("each table says how many connections each class has", "500 connections: 450 wallet, 45 dapp, 5 indexer" in md, md)
    check("the split of the connections is by largest remainder with at least one per class",
          mod.split_connections(10, [90, 9, 1]) == [8, 1, 1] and mod.split_connections(500, [90, 9, 1]) == [450, 45, 5],
          mod.split_connections(10, [90, 9, 1]))
    check("the legend explains the four measures",
          "**latency**" in md and "**fan-out**" in md and "**vs first newHeads**" in md and "**vs newHeads**" in md, md)
    check("the table has the measures of each class, p50 / p95 per client, the fastest p50 marked",
          "| wallet | newHeads latency | 1900.00* / 3300.00 | 3800.00 / 3300.00 |" in md
          and "| wallet | newHeads fan-out | 3.00* / 16.00 | 6.00 / 16.00 |" in md
          and "| dapp | logs vs newHeads | 0.40* / 12.00 | 0.80 / 12.00 |" in md
          and "| dapp | logs vs first newHeads | 3.00* / 12.00 | 6.00 / 12.00 |" in md, md)
    check("the classes follow the order of the pattern", md.find("| wallet |") < md.find("| dapp |"), md)
    check("in a class the measures are by type, newHeads first, then latency, fan-out, vs first newHeads, vs newHeads",
          md.find("| wallet | newHeads latency") < md.find("| wallet | newHeads fan-out")
          and md.find("| dapp | logs vs first newHeads") < md.find("| dapp | logs vs newHeads"), md)
    rows = [line.split("|")[1].strip() for line in md.splitlines() if line.startswith("| ") and "Class" not in line]
    check("every row of the class table is of one class", set(rows) == {"wallet", "dapp"}, rows)
    code, out = run_main(["--results-dir", tmp])
    check("main prints the class section after the overviews and before the details",
          out.find("p50 latency") < out.find("client classes") < out.find("<details>"), out)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
