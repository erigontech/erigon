#!/usr/bin/env python3
"""Render a GitHub Actions job summary for the QA RPC integration tests.

Turns what the rpc-tests runner leaves in the result dir into Markdown on the
run summary page: an overall result badge, a stats table, and — when available
— a table of the tests that failed.

Two data sources, in order of reliability:

1. ``output.log`` — always written when the test phase runs. Its trailing
   statistics block ("Available tests: N", "Number of failed tests: N", …) is
   printed unconditionally by rpc-tests ``run_tests.py`` and is stable across
   versions, so it is the primary source for the overall counts. The runner
   writes no literal "FAILED" token per line (a failed line just carries the
   error text where "OK" would be), so the log is NOT parsed for the per-test
   list — only for the summary block.

2. ``results/test_report.json`` — a structured report the runner writes only
   with ``--verbose 1`` and only on recent rpc-tests versions (older pinned
   versions omit it). Used, when present, for the per-test failed list with
   transport and error message. Treated as optional: absence is normal.

The script is a *reporter*: it never fails the job. It always exits 0 and always
prints something, so the run page shows an outcome even when the run died during
setup before producing any results. Pass/fail gating stays in the test step.

Usage:
  render_rpc_summary.py --result-dir DIR [--workflow NAME] [--chain CHAIN]
                        [--result success|failure|unknown] [--title-suffix STR]
                        [--max-failures N] [--log-tail-lines N]
"""

import argparse
import json
import os
import re
import sys

# Keep the summary comfortably under GitHub's 1 MiB per-step limit.
MAX_FAILURES_DEFAULT = 200
LOG_TAIL_LINES_DEFAULT = 200
LOG_TAIL_MAX_BYTES = 100 * 1024
ERROR_MSG_MAXLEN = 240
OUTPUT_HARD_CAP_BYTES = 900 * 1024

# Labels printed by rpc-tests run_tests.py at the end of a run. Values are read
# by substring (not line-anchored) so \r-delimited logs parse fine. "executed"
# is disambiguated from "NOT executed" by the distinct label text.
STAT_PATTERNS = [
    ("available_tests", r"Available tests:\s+(\d+)"),
    ("tested_apis", r"Available tested api:\s+(\d+)"),
    ("loops", r"Number of loop:\s+(\d+)"),
    ("executed", r"Number of executed tests:\s+(\d+)"),
    ("not_executed", r"Number of NOT executed tests:\s+(\d+)"),
    ("success", r"Number of success tests:\s+(\d+)"),
    ("failed", r"Number of failed tests:\s+(\d+)"),
    ("elapsed", r"Test time-elapsed:\s+([0-9:.]+)"),
]


def find_file(result_dir, *relatives):
    """Return the first existing path among preferred locations, else a recursive search."""
    for rel in relatives:
        cand = os.path.join(result_dir, rel)
        if os.path.isfile(cand):
            return cand
    target = os.path.basename(relatives[-1])
    for root, _, files in os.walk(result_dir):
        if target in files:
            return os.path.join(root, target)
    return None


def read_text(path):
    try:
        with open(path, "rb") as fh:
            return fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def parse_stats(log_text):
    """Extract the final statistics block from output.log; None if not present.

    The log may hold several blocks (the runner retries on failure); take the
    last occurrence of each label so the final attempt wins.
    """
    if not log_text:
        return None
    stats = {}
    for key, pat in STAT_PATTERNS:
        matches = re.findall(pat, log_text)
        if matches:
            stats[key] = matches[-1]
    # Require the failed-count line as the marker that a real stats block exists.
    return stats if "failed" in stats else None


def one_line(text, maxlen):
    """Collapse to a single, table-safe line and truncate."""
    s = " ".join(str(text).split())
    if len(s) > maxlen:
        s = s[: maxlen - 1].rstrip() + "…"
    return s.replace("\\", "\\\\").replace("|", "\\|")


def badge(result):
    r = (result or "").lower()
    if r == "success":
        return "✅ success"
    if r == "failure":
        return "❌ failure"
    return f"❓ {result or 'unknown'}"


def read_log_tail(text, max_lines, max_bytes):
    if text is None:
        return None, False
    raw = text.encode("utf-8", errors="replace")
    clipped_bytes = len(raw) > max_bytes
    if clipped_bytes:
        text = raw[-max_bytes:].decode("utf-8", errors="replace")
    # Normalize carriage returns the runner uses for in-place status updates.
    lines = text.replace("\r\n", "\n").replace("\r", "\n").splitlines()
    clipped = clipped_bytes or len(lines) > max_lines
    return "\n".join(lines[-max_lines:]), clipped


def int_or_none(stats, key):
    try:
        return int(stats[key])
    except (KeyError, ValueError, TypeError):
        return None


def render(args):
    out = []
    title = args.workflow or "RPC Integration Tests"
    if args.title_suffix:
        title += f" — {args.title_suffix}"
    out.append(f"# {title}")
    out.append("")

    meta = []
    if args.chain:
        meta.append(f"**Chain:** {args.chain}")
    meta.append(f"**Result:** {badge(args.result)}")
    out.append("  |  ".join(meta))
    out.append("")

    log_path = find_file(args.result_dir, "output.log", "results/output.log")
    report_path = find_file(args.result_dir, "results/test_report.json", "test_report.json")

    log_text = read_text(log_path) if log_path else None
    stats = parse_stats(log_text)

    report = None
    if report_path:
        try:
            with open(report_path, encoding="utf-8") as fh:
                report = json.load(fh)
        except (OSError, ValueError) as exc:
            out.append(f"> ⚠️ Found `{os.path.relpath(report_path, args.result_dir)}` but could not parse it: {exc}")
            out.append("")

    report_summary = (report or {}).get("summary", {}) or {}
    failed_rows = [r for r in (report or {}).get("test_results", []) or [] if str(r.get("result", "")).upper() == "FAILED"]

    # Overall counts: prefer the log's stats block, fall back to the report summary.
    failed_count = int_or_none(stats or {}, "failed")
    if failed_count is None and "failed_tests" in report_summary:
        try:
            failed_count = int(report_summary["failed_tests"])
        except (ValueError, TypeError):
            pass

    # --- Overall stats table -------------------------------------------------
    if stats:
        table_rows = [
            ("Available tests", stats.get("available_tests")),
            ("Executed", stats.get("executed")),
            ("Passed", stats.get("success")),
            ("Failed", stats.get("failed")),
            ("Not executed", stats.get("not_executed")),
            ("Tested APIs", stats.get("tested_apis")),
            ("Loops", stats.get("loops")),
            ("Time elapsed", stats.get("elapsed")),
        ]
        out.append("## Overall")
        out.append("")
        out.append("| Metric | Value |")
        out.append("| --- | ---: |")
        for name, val in table_rows:
            if val is not None:
                out.append(f"| {name} | {val} |")
        out.append("")
    elif report_summary:
        # No log stats block, but a structured report exists — use it.
        out.append("## Overall")
        out.append("")
        out.append("| Metric | Value |")
        out.append("| --- | ---: |")
        for name, key in [
            ("Available tests", "available_tests"), ("Executed", "executed_tests"),
            ("Passed", "success_tests"), ("Failed", "failed_tests"),
            ("Not executed", "not_executed_tests"), ("Tested APIs", "available_tested_api"),
            ("Loops", "number_of_loops"), ("Time elapsed", "time_elapsed"),
        ]:
            if report_summary.get(key) is not None:
                out.append(f"| {name} | {report_summary[key]} |")
        out.append("")

    # --- Failed tests --------------------------------------------------------
    if failed_rows:
        by_transport, by_api = {}, {}
        for f in failed_rows:
            t = f.get("transport_type") or "?"
            by_transport[t] = by_transport.get(t, 0) + 1
            api = (f.get("test_name") or "?").split("/")[0]
            by_api[api] = by_api.get(api, 0) + 1

        out.append(f"## ❌ Failed tests ({len(failed_rows)})")
        out.append("")
        out.append("By transport: " + ", ".join(f"`{t}` {n}" for t, n in sorted(by_transport.items())))
        out.append("")
        top_apis = sorted(by_api.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        out.append("Top failing APIs: " + ", ".join(f"`{a}` ({n})" for a, n in top_apis))
        out.append("")

        shown = failed_rows[: args.max_failures]
        out.append("| # | Test | Transport | Error |")
        out.append("| ---: | --- | --- | --- |")
        for i, f in enumerate(shown, 1):
            name = one_line(f.get("test_name", ""), 120)
            transport = one_line(f.get("transport_type", ""), 20)
            err = one_line(f.get("error_message", ""), ERROR_MSG_MAXLEN) or "—"
            out.append(f"| {i} | {name} | {transport} | {err} |")
        out.append("")
        if len(failed_rows) > len(shown):
            out.append(f"> …and {len(failed_rows) - len(shown)} more — see the `test-results` artifact.")
            out.append("")
    elif failed_count and failed_count > 0:
        # We know the count but have no per-test detail (no/old structured report).
        out.append(f"## ❌ {failed_count} test(s) failed")
        out.append("")
        out.append(
            "> Per-test details aren't in a structured report for this run "
            "(`results/test_report.json` absent or without failures listed). "
            "The failing tests are in the `output.log` below and in the `test-results` artifact."
        )
        out.append("")
    elif failed_count == 0 and (args.result or "").lower() != "failure":
        out.append("✅ All executed tests passed.")
        out.append("")
    elif (args.result or "").lower() == "failure" and (stats or report is not None or log_text):
        # Marked failed, some results exist, but no failed test-count was found.
        out.append("## ⚠️ Marked failed with no failing tests recorded")
        out.append("")
        out.append(
            "> The job is marked failed but no per-test failure was recorded — the run likely "
            "failed outside the test phase (build, datadir, rpcdaemon startup, environment, or "
            "teardown). See the log below and the `test-results` artifact."
        )
        out.append("")

    if not stats and not report and not log_text:
        out.append("## No results produced")
        out.append("")
        out.append(
            "> ⚠️ No `output.log` or `results/test_report.json` was found — the run produced no "
            "test results (it likely failed during setup before any test executed). "
            "See the step logs and the `test-results` artifact."
        )
        out.append("")

    # --- output.log tail (collapsible; open when there are failures) ----------
    if log_text is not None:
        tail, clipped = read_log_tail(log_text, args.log_tail_lines, LOG_TAIL_MAX_BYTES)
        if tail:
            note = " (tail)" if clipped else ""
            open_attr = " open" if (failed_count and failed_count > 0 and not failed_rows) else ""
            out.append(f"<details{open_attr}><summary>output.log{note}</summary>")
            out.append("")
            out.append("```")
            out.append(tail)
            out.append("```")
            out.append("")
            out.append("</details>")
            out.append("")

    text = "\n".join(out).rstrip() + "\n"
    if len(text.encode("utf-8")) > OUTPUT_HARD_CAP_BYTES:
        text = text.encode("utf-8")[:OUTPUT_HARD_CAP_BYTES].decode("utf-8", errors="ignore")
        text += "\n\n> …summary truncated to stay under the step-summary size limit.\n"
    return text


def main():
    parser = argparse.ArgumentParser(description="Render RPC integration test results as a GitHub job summary.")
    parser.add_argument("--result-dir", required=True, help="Directory holding output.log and results/test_report.json")
    parser.add_argument("--workflow", default="RPC Integration Tests")
    parser.add_argument("--chain", default="")
    parser.add_argument("--result", default="unknown", help="success | failure | unknown")
    parser.add_argument("--title-suffix", default="", help="Extra title context, e.g. the client name")
    parser.add_argument("--max-failures", type=int, default=MAX_FAILURES_DEFAULT)
    parser.add_argument("--log-tail-lines", type=int, default=LOG_TAIL_LINES_DEFAULT)
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
