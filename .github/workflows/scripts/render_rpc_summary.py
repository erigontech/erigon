#!/usr/bin/env python3
"""Render a GitHub Actions job summary for the QA RPC integration tests.

Reads the structured report the rpc-tests runner writes
(``results/test_report.json``) into Markdown: a result badge, a stats table, and
a table of failed tests. With no report, falls back to the badge plus a
size-capped ``output.log`` tail. No console-text parsing, so a runner
output-format change cannot make it show a wrong result. The verdict always
comes from ``--result`` (the test step's exit code); the script never fails the
job and always prints something.
"""
import argparse
import json
import os
import sys

STEP_SUMMARY_CAP = 900 * 1024
LOG_TAIL_MAX_BYTES = 100 * 1024
ERROR_MSG_MAXLEN = 240
MAX_FAILURES = 200
LOG_TAIL_LINES = 200


def badge(result):
    r = (result or "").lower()
    if r == "success":
        return "✅ success"
    if r == "failure":
        return "❌ failure"
    return f"❓ {result or 'unknown'}"


def find_file(result_dir, *relatives):
    for rel in relatives:
        p = os.path.join(result_dir, rel)
        if os.path.isfile(p):
            return p
    return None


def one_line(text, maxlen):
    """Collapse to a single, table-safe line and truncate."""
    s = " ".join(str(text).split())
    if len(s) > maxlen:
        s = s[: maxlen - 1].rstrip() + "…"
    return s.replace("\\", "\\\\").replace("|", "\\|")


def read_log_tail(path, max_lines, max_bytes):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > max_bytes:
                fh.seek(-max_bytes, os.SEEK_END)
            data = fh.read()
    except OSError:
        return None, False
    text = data.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.splitlines()
    clipped = size > max_bytes or len(lines) > max_lines
    return "\n".join(lines[-max_lines:]), clipped


def render(args):
    out = [f"# {args.workflow}" + (f" — {args.title_suffix}" if args.title_suffix else ""), ""]
    meta = ([f"**Chain:** {args.chain}"] if args.chain else []) + [f"**Result:** {badge(args.result)}"]
    out += ["  |  ".join(meta), ""]

    report_path = find_file(args.result_dir, "results/test_report.json", "test_report.json")
    log_path = find_file(args.result_dir, "output.log", "results/output.log")

    report = None
    if report_path:
        try:
            with open(report_path, encoding="utf-8") as fh:
                report = json.load(fh)
        except (OSError, ValueError) as exc:
            out += [f"> ⚠️ Could not parse `{os.path.basename(report_path)}`: {exc}", ""]

    if report:
        summary = report.get("summary", {}) or {}
        out += ["## Overall", "", "| Metric | Value |", "| --- | ---: |"]
        for name, key in (
            ("Available tests", "available_tests"), ("Executed", "executed_tests"),
            ("Passed", "success_tests"), ("Failed", "failed_tests"),
            ("Not executed", "not_executed_tests"), ("Tested APIs", "available_tested_api"),
            ("Loops", "number_of_loops"), ("Time elapsed", "time_elapsed"),
        ):
            if summary.get(key) is not None:
                out.append(f"| {name} | {summary[key]} |")
        out.append("")

        failed = [r for r in report.get("test_results", []) or [] if str(r.get("result", "")).upper() == "FAILED"]
        if failed:
            by_transport = {}
            for f in failed:
                t = f.get("transport_type") or "?"
                by_transport[t] = by_transport.get(t, 0) + 1
            out += [f"## ❌ Failed tests ({len(failed)})", ""]
            out += ["By transport: " + ", ".join(f"`{t}` {n}" for t, n in sorted(by_transport.items())), ""]
            out += ["| # | Test | Transport | Error |", "| ---: | --- | --- | --- |"]
            for i, f in enumerate(failed[:MAX_FAILURES], 1):
                name = one_line(f.get("test_name", ""), 120)
                transport = one_line(f.get("transport_type", ""), 20)
                err = one_line(f.get("error_message", ""), ERROR_MSG_MAXLEN) or "—"
                out.append(f"| {i} | {name} | {transport} | {err} |")
            out.append("")
            if len(failed) > MAX_FAILURES:
                out += [f"> …and {len(failed) - MAX_FAILURES} more — see the `test-results` artifact.", ""]
        elif (args.result or "").lower() != "failure":
            out += ["✅ All executed tests passed.", ""]
    else:
        out += ["## No structured report", "",
                "> No `results/test_report.json` for this run. The pass/fail result is the badge above; "
                "per-test detail is in the `output.log` below and the `test-results` artifact.", ""]

    if log_path:
        tail, clipped = read_log_tail(log_path, LOG_TAIL_LINES, LOG_TAIL_MAX_BYTES)
        if tail:
            open_attr = " open" if ((args.result or "").lower() == "failure" and not report) else ""
            out += [f"<details{open_attr}><summary>output.log{' (tail)' if clipped else ''}</summary>", "",
                    "```", tail, "```", "", "</details>", ""]
    elif not report:
        out += ["## No results produced", "",
                "> ⚠️ No `output.log` or `results/test_report.json` — the run produced no results "
                "(it likely failed during setup before any test ran). See the step logs and artifact.", ""]

    text = "\n".join(out).rstrip() + "\n"
    if len(text.encode("utf-8")) > STEP_SUMMARY_CAP:
        text = text.encode("utf-8")[:STEP_SUMMARY_CAP].decode("utf-8", errors="ignore")
        text += "\n\n> …summary truncated to stay under the step-summary size limit.\n"
    return text


def main():
    parser = argparse.ArgumentParser(description="Render RPC integration test results as a GitHub job summary.")
    parser.add_argument("--result-dir", required=True, help="Directory holding output.log and results/test_report.json")
    parser.add_argument("--workflow", default="RPC Integration Tests")
    parser.add_argument("--chain", default="")
    parser.add_argument("--result", default="unknown", help="success | failure | unknown")
    parser.add_argument("--title-suffix", default="", help="Extra title context, e.g. the client name")
    args = parser.parse_args()

    try:
        if not args.result_dir or not os.path.isdir(args.result_dir):
            sys.stdout.write(
                f"# {args.workflow}\n\n**Result:** {badge(args.result)}\n\n"
                f"> ⚠️ Result directory `{args.result_dir}` not found — no artifacts to summarize.\n"
            )
            return 0
        sys.stdout.write(render(args))
    except Exception as exc:  # never fail the job because of the reporter
        sys.stdout.write(f"# {args.workflow}\n\n> ⚠️ Failed to render summary: {exc}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
