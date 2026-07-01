#!/usr/bin/env python3
"""Fixture tests for render_rpc_summary.py.

Pins the fragile output.log / test_report.json parsing so rpc-tests
console-format drift is caught here rather than in a misleading CI summary.
Run: python3 .github/workflows/scripts/render_rpc_summary.test.py
"""
import importlib.util
import json
import os
import sys
import tempfile
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("render_rpc_summary", os.path.join(HERE, "render_rpc_summary.py"))
assert spec is not None and spec.loader is not None
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

passed = 0
failed = 0


def render(tmp, result="failure", **kw):
    args = SimpleNamespace(result_dir=tmp, workflow="QA - RPC Integration Tests", chain="mainnet",
                           result=result, title_suffix="", max_failures=200, log_tail_lines=50)
    for k, v in kw.items():
        setattr(args, k, v)
    return mod.render(args)


def write(tmp, rel, content):
    p = os.path.join(tmp, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(content)


def check(name, cond):
    global passed, failed
    if cond:
        print(f"ok   - {name}")
        passed += 1
    else:
        print(f"FAIL - {name}")
        failed += 1


def stats_block(av, ex, ne, su, fa):
    return (f"Available tests:              {av}\n"
            f"Available tested api:         112\n"
            f"Number of loop:               1\n"
            f"Number of executed tests:     {ex}\n"
            f"Number of NOT executed tests: {ne}\n"
            f"Number of success tests:      {su}\n"
            f"Number of failed tests:       {fa}\n")


def line(n, tr, name, status):
    return f"{n:04d}. {tr.ljust(15)}::{name.ljust(60)}   {status}\n"


def case_success_log_only():
    with tempfile.TemporaryDirectory() as tmp:
        log = "".join(line(i, "http", f"eth_call/test_{i}.json", "OK") for i in range(1, 6))
        log += stats_block(1435, 1390, 45, 1390, 0)
        write(tmp, "output.log", log)
        out = render(tmp, result="success")
        check("success: all-passed message", "All executed tests passed" in out)
        check("success: stats from log", "| Executed | 1390 |" in out)
        check("success: no failure section", "Failed tests" not in out)


def case_failing_with_report():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "output.log", stats_block(1435, 1390, 45, 1388, 2))
        report = {"summary": {"available_tests": 1435, "executed_tests": 1390, "not_executed_tests": 45,
                              "success_tests": 1388, "failed_tests": 2, "available_tested_api": 112,
                              "number_of_loops": 1, "time_elapsed": "0:03:10"},
                  "test_results": [
                      {"transport_type": "http", "test_name": "eth_getLogs/test_03.json", "result": "FAILED",
                       "error_message": "json diff"},
                      {"transport_type": "websocket", "test_name": "eth_getLogs/test_03.json", "result": "FAILED",
                       "error_message": "timeout"}]}
        write(tmp, "results/test_report.json", json.dumps(report))
        out = render(tmp, result="failure")
        check("report: failed-tests header", "## ❌ Failed tests (2)" in out)
        check("report: lists test", "eth_getLogs/test_03.json" in out)
        check("report: not labeled log-parsed", "Parsed from `output.log`" not in out)


def case_failing_no_report_logparsed():
    with tempfile.TemporaryDirectory() as tmp:
        log = line(11, "http", "eth_call/test_12.json", "ASSERT diff")
        log += "".join(line(i, "http", f"eth_x/test_{i}.json", "OK") for i in range(12, 40))
        log += line(44, "http", "trace_block/test_02.json", "500 error")
        log += line(66, "websocket", "eth_getLogs/test_09.json", "mismatch")
        log += stats_block(1435, 1390, 45, 1387, 3)
        write(tmp, "output.log", log)
        out = render(tmp, result="failure")
        check("logparse: failed-tests header (3)", "## ❌ Failed tests (3)" in out)
        check("logparse: labeled log-parsed", "Parsed from `output.log`" in out)
        check("logparse: lists parsed failures", "trace_block/test_02.json" in out and "eth_call/test_12.json" in out)
        rows = mod.parse_failed_from_log(log)
        check("logparse: classifier ignores OK lines",
              len(rows) == 3 and all("eth_x" not in r["test_name"] for r in rows))


def case_format_drift_suppressed():
    # rpc-tests changes the pass token OK -> GREEN: every line now looks like a
    # failure. Stats still say 2 failed, so the wildly-larger parsed list is dropped.
    with tempfile.TemporaryDirectory() as tmp:
        log = "".join(line(i, "http", f"eth_x/test_{i}.json", "GREEN") for i in range(1, 800))
        log += stats_block(1435, 1390, 45, 1388, 2)
        write(tmp, "output.log", log)
        out = render(tmp, result="failure")
        check("drift: count shown", "2 test(s) failed" in out)
        check("drift: format-change warning", "console format may have changed" in out)
        check("drift: no fabricated giant table", "Failed tests (799)" not in out and "Failed tests (2)" not in out)


def case_setup_failure():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "rpcdaemon.log", "boom\n")
        out = render(tmp, result="failure")
        check("setup: no-results notice", "No results produced" in out)
        check("setup: no false all-passed", "All executed tests passed" not in out)


def case_contradiction_success_with_failures():
    with tempfile.TemporaryDirectory() as tmp:
        report = {"summary": {"failed_tests": 2, "executed_tests": 1390, "success_tests": 1388},
                  "test_results": [{"transport_type": "http", "test_name": "a/t.json", "result": "FAILED",
                                    "error_message": "x"},
                                   {"transport_type": "http", "test_name": "b/t.json", "result": "FAILED",
                                    "error_message": "y"}]}
        write(tmp, "results/test_report.json", json.dumps(report))
        out = render(tmp, result="success")
        check("contradiction: mismatch warning", "marked success but 2 failing" in out)


def case_incoherent_stats():
    # executed(1390) != success(1390) + failed(5): the stats block is untrusted.
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "output.log", stats_block(1435, 1390, 45, 1390, 5))
        out = render(tmp, result="failure")
        check("incoherent: arithmetic warning", "arithmetic check" in out)
        check("incoherent: stats table suppressed", "| Executed | 1390 |" not in out)


def main():
    for fn in (case_success_log_only, case_failing_with_report, case_failing_no_report_logparsed,
               case_format_drift_suppressed, case_setup_failure, case_contradiction_success_with_failures,
               case_incoherent_stats):
        fn()
    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
