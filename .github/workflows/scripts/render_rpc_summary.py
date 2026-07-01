#!/usr/bin/env python3
"""Render a GitHub Actions job summary for the QA RPC integration tests.

Turns what the rpc-tests runner leaves in the result dir into Markdown on the
run summary page: an overall result badge, a stats table, and — when available
— a table of the tests that failed.

Robustness layering, most stable first — parsing can only add detail, never
change the verdict, so console-format drift degrades safely:

1. Verdict — ``--result`` (the test step's exit code). Format-independent,
   always the headline badge; never derived from parsing.

2. Counts and per-test failures — ``results/test_report.json`` (a stable JSON
   schema). Preferred when present. The runner writes it with ``--verbose 1``,
   but only on rpc-tests versions that support it; older pinned versions omit
   it, so absence is normal, not an error.

3. Fallback when no report — ``output.log`` console text: the trailing stats
   block ("Number of failed tests: N", …) for counts, and the per-test lines
   ("NNNN. <transport>::<name>   <status>", non-OK/Skipped = failure) for the
   failed list. Fragile to format changes and clearly labeled as log-parsed;
   if it breaks, the verdict and the raw log tail still stand.

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


ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def _norm(text):
    return ANSI_RE.sub("", text.replace("\r\n", "\n").replace("\r", "\n"))


def parse_stats(log_text):
    """Extract the final statistics block from output.log; None if not present.

    Scoped to the last block (the runner retries and emits one block per attempt)
    so counts are not mixed across attempts.
    """
    if not log_text:
        return None
    text = _norm(log_text)
    idx = text.rfind("Available tests:")
    region = text[idx:] if idx != -1 else text
    stats = {}
    for key, pat in STAT_PATTERNS:
        m = re.search(pat, region)
        if m:
            stats[key] = m.group(1)
    # Require the failed-count line as the marker that a real stats block exists.
    return stats if "failed" in stats else None


def stats_coherent(stats):
    """True when the parsed counts satisfy the runner's arithmetic invariants.

    A drifted/partial parse that violates executed == success + failed or
    available == executed + not_executed is not trustworthy as a count source.
    """
    def g(k):
        try:
            return int(stats[k])
        except (KeyError, ValueError, TypeError):
            return None
    ex, su, fa, av, ne = g("executed"), g("success"), g("failed"), g("available_tests"), g("not_executed")
    if ex is not None and su is not None and fa is not None and ex != su + fa:
        return False
    if av is not None and ex is not None and ne is not None and av != ex + ne:
        return False
    return True


# A per-test result line: "NNNN. <transport>::<test_name>   <status-or-error>".
# rpc-tests prints this for every executed test (OK lines are not suppressed by
# --display-only-fail), so failures sit interspersed among the OK lines rather
# than at the tail — parse them out explicitly.
TEST_LINE_RE = re.compile(r"^\s*(\d{3,5})\.\s+(\S+)\s*::\s*(\S+)\s+(.*?)\s*$")
PASS_TOKENS = ("ok", "pass", "passed", "success", "skip", "skipped")


def parse_failed_from_log(log_text):
    """Best-effort per-test failed list from output.log when no structured report.

    A test-result line whose status field does not begin with a known pass/skip
    token is treated as a failure, with the trailing text as the error message.
    Fragile to console-format drift; the caller cross-checks the count and drops
    the list if it disagrees wildly with the reported failure count.
    """
    if not log_text:
        return []
    text = _norm(log_text)
    failed = []
    for line in text.split("\n"):
        m = TEST_LINE_RE.match(line)
        if not m:
            continue
        status = m.group(4).strip()
        low = status.lower()
        if status == "" or any(low.startswith(t) for t in PASS_TOKENS):
            continue
        failed.append({
            "test_number": int(m.group(1)),
            "transport_type": m.group(2),
            "test_name": m.group(3),
            "result": "FAILED",
            "error_message": status,
        })
    return failed


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

    report_valid = report is not None
    report_summary = (report or {}).get("summary", {}) or {}
    result_lc = (args.result or "").lower()
    stats_ok = stats is not None and stats_coherent(stats)

    # Counts: structured report first (stable schema); fall back to the log's stats
    # block only when it is coherent. The verdict comes from --result, not here.
    failed_count = None
    if "failed_tests" in report_summary:
        try:
            failed_count = int(report_summary["failed_tests"])
        except (ValueError, TypeError):
            failed_count = None
    if failed_count is None and stats_ok:
        failed_count = int_or_none(stats, "failed")

    # Failed list: the report is authoritative when present; fall back to fragile
    # log parsing only when there is no report. Drop the parsed list if its size
    # disagrees wildly with the reported count (console-format drift).
    failed_rows, failed_source, drift_note = [], None, None
    if report_valid:
        failed_rows = [r for r in report.get("test_results", []) or [] if str(r.get("result", "")).upper() == "FAILED"]
        failed_source = "report"
    elif failed_count and failed_count > 0:
        parsed = parse_failed_from_log(log_text)
        if parsed and len(parsed) <= max(failed_count * 3, failed_count + 5):
            failed_rows, failed_source = parsed, "log"
        elif parsed:
            drift_note = (f"`output.log` parsing found {len(parsed)} candidate failures vs "
                          f"{failed_count} reported — the console format may have changed; showing the count only.")

    warn = []
    if result_lc == "success" and failed_count and failed_count > 0:
        warn.append(f"Job marked success but {failed_count} failing test(s) were recorded — investigate the mismatch.")
    if stats is not None and not stats_ok:
        warn.append("The `output.log` stats block failed its arithmetic check (format may have changed); see the raw log.")
    for w in warn:
        out.append(f"> ⚠️ {w}")
        out.append("")

    # --- Overall stats table: structured report first, coherent console stats next.
    if report_summary:
        rows = [(name, report_summary.get(key)) for name, key in (
            ("Available tests", "available_tests"), ("Executed", "executed_tests"),
            ("Passed", "success_tests"), ("Failed", "failed_tests"),
            ("Not executed", "not_executed_tests"), ("Tested APIs", "available_tested_api"),
            ("Loops", "number_of_loops"), ("Time elapsed", "time_elapsed"),
        )]
    elif stats is not None and stats_ok:
        rows = [
            ("Available tests", stats.get("available_tests")),
            ("Executed", stats.get("executed")),
            ("Passed", stats.get("success")),
            ("Failed", stats.get("failed")),
            ("Not executed", stats.get("not_executed")),
            ("Tested APIs", stats.get("tested_apis")),
            ("Loops", stats.get("loops")),
            ("Time elapsed", stats.get("elapsed")),
        ]
    else:
        rows = []
    if rows:
        out.append("## Overall")
        out.append("")
        out.append("| Metric | Value |")
        out.append("| --- | ---: |")
        for name, val in rows:
            if val is not None:
                out.append(f"| {name} | {val} |")
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
        if failed_source == "log":
            note = "Parsed from `output.log` (no structured `test_report.json` for this run); errors are the first log line only."
            if failed_count is not None and failed_count != len(failed_rows):
                note += f" Stats report {failed_count} failures vs {len(failed_rows)} parsed — see `output.log`."
            out.append(f"> {note}")
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
        out.append(f"## ❌ {failed_count} test(s) failed")
        out.append("")
        msg = ("Per-test details aren't available in a structured report for this run. "
               "The failing tests are in the `output.log` below and in the `test-results` artifact.")
        if drift_note:
            msg = drift_note + " " + msg
        out.append(f"> {msg}")
        out.append("")
    elif failed_count == 0 and result_lc != "failure":
        out.append("✅ All executed tests passed.")
        out.append("")
    elif result_lc == "failure" and (stats is not None or report_valid or log_text):
        # Marked failed, some results exist, but no failed test-count was found.
        out.append("## ⚠️ Marked failed with no failing tests recorded")
        out.append("")
        out.append(
            "> The job is marked failed but no per-test failure was recorded — the run likely "
            "failed outside the test phase (build, datadir, rpcdaemon startup, environment, or "
            "teardown). See the log below and the `test-results` artifact."
        )
        out.append("")

    if not stats and not report_valid and not log_text:
        out.append("## No results produced")
        out.append("")
        out.append(
            "> ⚠️ No `output.log` or `results/test_report.json` was found — the run produced no "
            "test results (it likely failed during setup before any test executed). "
            "See the step logs and the `test-results` artifact."
        )
        out.append("")

    # --- output.log tail: opened on failure, log-parsed failures, or any warning.
    if log_text is not None:
        tail, clipped = read_log_tail(log_text, args.log_tail_lines, LOG_TAIL_MAX_BYTES)
        if tail:
            note = " (tail)" if clipped else ""
            open_it = result_lc == "failure" or failed_source == "log" or bool(drift_note) or bool(warn)
            open_attr = " open" if open_it else ""
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
