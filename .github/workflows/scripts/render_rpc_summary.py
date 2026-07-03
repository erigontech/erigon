#!/usr/bin/env python3
"""Render a GitHub Actions job summary for the QA RPC integration tests.

Reads the structured report the rpc-tests runner writes
(``results/test_report.json``) into Markdown: a result badge, a stats table, and
a table of failed tests. With no report, falls back to the badge plus a
size-capped ``output.log`` tail. The pass/fail verdict always comes from
``--result`` (the job/test-step exit code) and the structured report — never
from parsing console text — so a runner output-format change cannot make it show
a wrong result. The one log-derived section, ``Attempts``, is informational
retry history and never changes the verdict. The script never fails the job and
always prints something, including when the run died during setup before any
test ran.
"""
import argparse
import json
import os
import re
import sys

STEP_SUMMARY_CAP = 900 * 1024
LOG_TAIL_MAX_BYTES = 100 * 1024
ERROR_MSG_MAXLEN = 240
MAX_FAILURES = 200
LOG_TAIL_LINES = 200
ATTEMPTS_SCAN_MAX_BYTES = 8 * 1024 * 1024
ATTEMPT_RE = re.compile(r"^Attempt\s+(\d+)\s*$")


def badge(result):
    r = (result or "").lower()
    if r == "success":
        return "✅ success"
    if r == "failure":
        return "❌ failure"
    return f"❓ {result or 'unknown'}"


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


def summarize_attempts(log_path):
    """Classify each ``Attempt N`` retry block the test runner writes to
    output.log. The suite retries up to a limit, rerunning every test; only the
    last attempt's counts reach ``test_report.json``, so earlier-attempt
    failures live only in the log — and the log tail keeps the *end*, dropping
    them. This scans the whole log so the retry history survives. Informational
    only: the pass/fail verdict never comes from here.
    """
    try:
        with open(log_path, "rb") as fh:
            data = fh.read(ATTEMPTS_SCAN_MAX_BYTES)
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    marks = []
    for i, line in enumerate(lines):
        m = ATTEMPT_RE.match(line)
        if m:
            marks.append((i, m.group(1)))
    attempts = []
    for idx, (start, n) in enumerate(marks):
        end = marks[idx + 1][0] if idx + 1 < len(marks) else len(lines)
        block = "\n".join(lines[start + 1:end]).lower()
        fails = block.count("failed:")
        if "not synced" in block or "sync on latest block number failed" in block:
            status = "❌ sync failed"
        elif fails:
            status = f"❌ {fails} failing test{'s' if fails != 1 else ''}"
        else:
            status = "✅ passed"
        attempts.append((n, status))
    return attempts


def render(args):
    out = [f"# {args.workflow}" + (f" — {args.title_suffix}" if args.title_suffix else ""), ""]
    meta = ([f"**Chain:** {args.chain}"] if args.chain else []) + [f"**Result:** {badge(args.result)}"]
    out += ["  |  ".join(meta), ""]

    report_path = os.path.join(args.result_dir, "results", "test_report.json")
    log_path = os.path.join(args.result_dir, "output.log")

    attempts = summarize_attempts(log_path)
    if len(attempts) > 1:
        if (args.result or "").lower() != "success" and attempts[-1][1] == "✅ passed":
            attempts[-1] = (attempts[-1][0], "❌ failed")
        out += ["## Attempts", "",
                "> Each attempt reruns the full suite; the result above is the final attempt.", "",
                "| Attempt | Outcome |", "| ---: | --- |"]
        out += [f"| {n} | {s} |" for n, s in attempts]
        out.append("")

    report = None
    if os.path.isfile(report_path):
        try:
            with open(report_path, encoding="utf-8") as fh:
                loaded = json.load(fh)
            if isinstance(loaded, dict):
                report = loaded
            else:
                out += ["> ⚠️ `results/test_report.json` is not a JSON object; ignoring it.", ""]
        except (OSError, ValueError) as exc:
            out += [f"> ⚠️ Could not parse `test_report.json`: {exc}", ""]

    if report:
        summary = report.get("summary")
        if not isinstance(summary, dict):
            summary = {}
        stat_rows = [(name, summary[key]) for name, key in (
            ("Available tests", "available_tests"), ("Executed", "executed_tests"),
            ("Passed", "success_tests"), ("Failed", "failed_tests"),
            ("Not executed", "not_executed_tests"), ("Tested APIs", "available_tested_api"),
            ("Loops", "number_of_loops"), ("Time elapsed", "time_elapsed"),
        ) if summary.get(key) is not None]
        if stat_rows:
            out += ["## Overall", "", "| Metric | Value |", "| --- | ---: |"]
            out += [f"| {name} | {val} |" for name, val in stat_rows]
            out.append("")

        rows = report.get("test_results")
        failed = [r for r in (rows if isinstance(rows, list) else [])
                  if isinstance(r, dict) and str(r.get("result", "")).upper() == "FAILED"]
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
        elif (args.result or "").lower() == "success" and not summary.get("failed_tests"):
            out += ["✅ All executed tests passed.", ""]
    else:
        out += ["## No structured report", "",
                "> No `results/test_report.json` for this run. The pass/fail result is the badge above; "
                "per-test detail is in the `output.log` below and the `test-results` artifact.", ""]

    tail, clipped = None, False
    if os.path.isfile(log_path):
        tail, clipped = read_log_tail(log_path, LOG_TAIL_LINES, LOG_TAIL_MAX_BYTES)
    if tail:
        open_attr = " open" if ((args.result or "").lower() == "failure" and not report) else ""
        out += [f"<details{open_attr}><summary>output.log{' (tail)' if clipped else ''}</summary>", "",
                "```", tail, "```", "", "</details>", ""]
    elif not report:
        out += ["## No results produced", "",
                "> ⚠️ No test results were produced — the run likely failed during setup before any "
                "test ran. See the step logs and the `test-results` artifact.", ""]

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
    try:
        args = parser.parse_args()
    except SystemExit:
        sys.stdout.write("# RPC Integration Tests\n\n> ⚠️ Summary renderer called with invalid arguments.\n")
        return 0

    try:
        if not args.result_dir or not os.path.isdir(args.result_dir):
            if (args.result or "").lower() == "failure":
                note = ("> ⚠️ The run failed before any test produced results — it likely died during "
                        "setup (build, migrations, or sync). See the step logs and the `test-results` artifact.")
            else:
                note = f"> ⚠️ Result directory `{args.result_dir}` not found — no artifacts to summarize."
            chain = f"**Chain:** {args.chain}  |  " if args.chain else ""
            sys.stdout.write(f"# {args.workflow}\n\n{chain}**Result:** {badge(args.result)}\n\n{note}\n")
            return 0
        sys.stdout.write(render(args))
    except Exception as exc:  # never fail the job because of the reporter
        sys.stdout.write(f"# {args.workflow}\n\n> ⚠️ Failed to render summary: {exc}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
