#!/usr/bin/env python3
"""Render a GitHub Actions job summary for the QA RPC integration tests.

Reads the structured report the rpc-tests runner leaves in the result dir
(``results/test_report.json``, produced by rpc-tests ``run_tests.py`` with
``--verbose 1``) and emits Markdown to stdout: an overall result badge, a
stats table, and a table of the tests that failed (with transport and a
truncated error message). Falls back to a clear notice plus the tail of
``output.log`` when no structured report was produced (e.g. the run died
during setup before any test executed).

The script is a *reporter*: it never fails the job. It always exits 0 and
always prints something, so the workflow run page shows an outcome even when
the test step errored early. The pass/fail gating stays in the test step.

Usage:
  render_rpc_summary.py --result-dir DIR [--workflow NAME] [--chain CHAIN]
                        [--result success|failure|unknown] [--title-suffix STR]
                        [--max-failures N] [--log-tail-lines N]

Output goes to stdout; the caller appends it to $GITHUB_STEP_SUMMARY.
"""

import argparse
import json
import os
import sys

# Keep the summary comfortably under GitHub's 1 MiB per-step limit.
MAX_FAILURES_DEFAULT = 200
LOG_TAIL_LINES_DEFAULT = 200
LOG_TAIL_MAX_BYTES = 100 * 1024
ERROR_MSG_MAXLEN = 240


def find_file(result_dir, *relatives):
    """Return the first existing path among preferred locations, else a recursive search."""
    for rel in relatives:
        cand = os.path.join(result_dir, rel)
        if os.path.isfile(cand):
            return cand
    # Fall back to a recursive search for the basename of the last candidate.
    target = os.path.basename(relatives[-1])
    for root, _dirs, files in os.walk(result_dir):
        if target in files:
            return os.path.join(root, target)
    return None


def one_line(text, maxlen):
    """Collapse to a single, table-safe line and truncate."""
    s = " ".join(str(text).split())
    if len(s) > maxlen:
        s = s[: maxlen - 1].rstrip() + "…"
    # Escape characters that would break a Markdown table cell.
    return s.replace("\\", "\\\\").replace("|", "\\|")


def badge(result):
    r = (result or "").lower()
    if r == "success":
        return "✅ success"
    if r == "failure":
        return "❌ failure"
    return f"❓ {result or 'unknown'}"


def read_log_tail(path, max_lines, max_bytes):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > max_bytes:
                fh.seek(-max_bytes, os.SEEK_END)
            data = fh.read()
        text = data.decode("utf-8", errors="replace")
        lines = text.splitlines()
        clipped = size > max_bytes or len(lines) > max_lines
        tail = lines[-max_lines:]
        return "\n".join(tail), clipped
    except OSError:
        return None, False


def render(args):
    out = []
    title = f"{args.workflow or 'RPC Integration Tests'}"
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

    report_path = find_file(args.result_dir, "results/test_report.json", "test_report.json")
    log_path = find_file(args.result_dir, "output.log", "results/output.log")

    report = None
    if report_path:
        try:
            with open(report_path, encoding="utf-8") as fh:
                report = json.load(fh)
        except (OSError, ValueError) as exc:
            out.append(f"> ⚠️ Found `{os.path.relpath(report_path, args.result_dir)}` but could not parse it: {exc}")
            out.append("")

    if report:
        summary = report.get("summary", {}) or {}
        results = report.get("test_results", []) or []
        failures = [r for r in results if str(r.get("result", "")).upper() == "FAILED"]

        # Overall stats.
        out.append("## Overall")
        out.append("")
        out.append("| Metric | Value |")
        out.append("| --- | ---: |")
        rows = [
            ("Available tests", summary.get("available_tests")),
            ("Executed", summary.get("executed_tests")),
            ("Passed", summary.get("success_tests")),
            ("Failed", summary.get("failed_tests")),
            ("Not executed", summary.get("not_executed_tests")),
            ("Tested APIs", summary.get("available_tested_api")),
            ("Loops", summary.get("number_of_loops")),
            ("Time elapsed", summary.get("time_elapsed")),
        ]
        for name, val in rows:
            if val is not None:
                out.append(f"| {name} | {val} |")
        out.append("")

        if failures:
            # Per-transport breakdown.
            by_transport = {}
            by_api = {}
            for f in failures:
                by_transport[f.get("transport_type") or "?"] = by_transport.get(f.get("transport_type") or "?", 0) + 1
                api = (f.get("test_name") or "?").split("/")[0]
                by_api[api] = by_api.get(api, 0) + 1

            out.append(f"## ❌ Failed tests ({len(failures)})")
            out.append("")
            out.append("By transport: " + ", ".join(f"`{t}` {n}" for t, n in sorted(by_transport.items())))
            out.append("")
            top_apis = sorted(by_api.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
            out.append("Top failing APIs: " + ", ".join(f"`{a}` ({n})" for a, n in top_apis))
            out.append("")

            shown = failures[: args.max_failures]
            out.append("| # | Test | Transport | Error |")
            out.append("| ---: | --- | --- | --- |")
            for i, f in enumerate(shown, 1):
                name = one_line(f.get("test_name", ""), 120)
                transport = one_line(f.get("transport_type", ""), 20)
                err = one_line(f.get("error_message", ""), ERROR_MSG_MAXLEN) or "—"
                out.append(f"| {i} | {name} | {transport} | {err} |")
            out.append("")
            if len(failures) > len(shown):
                out.append(f"> …and {len(failures) - len(shown)} more failures — see the `test-results` artifact.")
                out.append("")
        else:
            out.append("✅ All executed tests passed.")
            out.append("")
    else:
        out.append("## No structured test report")
        out.append("")
        out.append(
            "> ⚠️ No `results/test_report.json` was produced — the run likely failed during "
            "setup (build, datadir, rpcdaemon startup, or environment) before any test executed. "
            "See the step logs below and the `test-results` artifact."
        )
        out.append("")

    # Always include a collapsible tail of output.log when present, for context.
    if log_path:
        tail, clipped = read_log_tail(log_path, args.log_tail_lines, LOG_TAIL_MAX_BYTES)
        if tail is not None:
            note = " (tail)" if clipped else ""
            out.append(f"<details><summary>output.log{note}</summary>")
            out.append("")
            out.append("```")
            out.append(tail)
            out.append("```")
            out.append("")
            out.append("</details>")
            out.append("")

    return "\n".join(out).rstrip() + "\n"


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
            # Still emit a header so the run page is not blank.
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
