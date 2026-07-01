#!/usr/bin/env python3
"""Fixture tests for render_rpc_summary.py.

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
                           result=result, title_suffix="")
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
    print(f"ok   - {name}" if cond else f"FAIL - {name}")
    if cond:
        passed += 1
    else:
        failed += 1


def report(failed_tests, rows):
    return json.dumps({
        "summary": {"available_tests": 1435, "executed_tests": 1390, "not_executed_tests": 45,
                    "success_tests": 1390 - failed_tests, "failed_tests": failed_tests,
                    "available_tested_api": 112, "number_of_loops": 1, "time_elapsed": "0:03:10"},
        "test_results": rows,
    })


def case_report_all_passed():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "results/test_report.json", report(0, [{"transport_type": "http", "test_name": "eth_call/test_1.json",
                                                           "result": "OK", "error_message": ""}]))
        out = render(tmp, result="success")
        check("report/pass: all-passed message", "All executed tests passed" in out)
        check("report/pass: stats table", "| Executed | 1390 |" in out and "| Failed | 0 |" in out)
        check("report/pass: no failure section", "Failed tests" not in out)


def case_report_failures():
    with tempfile.TemporaryDirectory() as tmp:
        rows = [{"transport_type": "http", "test_name": "eth_getLogs/test_03.json", "result": "FAILED",
                 "error_message": "json diff"},
                {"transport_type": "websocket", "test_name": "eth_getLogs/test_03.json", "result": "FAILED",
                 "error_message": "timeout"}]
        write(tmp, "results/test_report.json", report(2, rows))
        out = render(tmp, result="failure")
        check("report/fail: header (2)", "## ❌ Failed tests (2)" in out)
        check("report/fail: lists test + transport", "eth_getLogs/test_03.json" in out and "websocket" in out)
        check("report/fail: stats table", "| Failed | 2 |" in out)


def case_no_report_with_log():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "output.log", "Number of failed tests:       3\n")
        out = render(tmp, result="failure")
        check("no-report: notice", "No structured report" in out)
        check("no-report: opens raw log", "<details open>" in out and "Number of failed tests:" in out)
        check("no-report: no false all-passed", "All executed tests passed" not in out)


def case_setup_failure():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "rpcdaemon.log", "boom\n")
        out = render(tmp, result="failure")
        check("setup: no-results notice", "No results produced" in out)
        check("setup: no false all-passed", "All executed tests passed" not in out)


def case_malformed_json_shape():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "results/test_report.json", "[1, 2, 3]")  # valid JSON, wrong shape (not an object)
        write(tmp, "output.log", "Number of failed tests:       1\n")
        out = render(tmp, result="failure")
        check("malformed: keeps verdict badge", "❌ failure" in out)
        check("malformed: notes ignored report", "not a JSON object" in out)
        check("malformed: falls back to log", "output.log" in out)


def case_empty_log():
    with tempfile.TemporaryDirectory() as tmp:
        write(tmp, "output.log", "")  # present but empty
        out = render(tmp, result="failure")
        check("empty-log: no-results notice", "No results produced" in out)
        check("empty-log: no empty details block", "<details" not in out)


def case_missing_dir():
    out = mod.render(SimpleNamespace(result_dir=os.path.join(HERE, "nope"), workflow="W", chain="",
                                     result="failure", title_suffix=""))
    check("missing-dir: still shows badge", "❌ failure" in out)


def main():
    for fn in (case_report_all_passed, case_report_failures, case_no_report_with_log,
               case_setup_failure, case_malformed_json_shape, case_empty_log, case_missing_dir):
        fn()
    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
