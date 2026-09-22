#!/usr/bin/env python3
"""Check what the RPC performance benchmark actually got back from each client.

Vegeta counts a request as successful on any 2xx status, and a JSON-RPC node
answers HTTP 200 also for ``"result": null`` and for ``"error"`` objects. A node
that lacks the requested data (pruned state, missing tx index, pruned history)
therefore looks healthy, and usually faster, in the latency report.

``classify`` reads the vegeta result files of one client and method through
``vegeta encode --to json`` and counts the responses per category:

  ok, empty ([], {}, "", "0x"), null, error, unknown  -- JSON-RPC answers
  http_error, no_response                             -- transport/load failures

Only a prefix of each body is decoded: the category is decided by the first
top-level ``result``/``error`` key, which precedes any nested one.

``compare`` loads the classify outputs of all clients and fails when, for the
same method, the share of a JSON-RPC category differs from Erigon's by more
than ``--max-delta`` percentage points. HTTP failures are excluded from the
shares because they measure load, not correctness.
"""
import argparse
import base64
import collections
import json
import os
import re
import subprocess
import sys

REFERENCE_CLIENT = "erigon"

JSONRPC_CATEGORIES = ("ok", "empty", "null", "error", "unknown")
TRANSPORT_CATEGORIES = ("http_error", "no_response")
CATEGORIES = JSONRPC_CATEGORIES + TRANSPORT_CATEGORIES

BODY_PREFIX_B64_CHARS = 1024
MAX_ERROR_MESSAGE_LEN = 120
TOP_ERRORS = 10

KEY_RE = re.compile(rb'"(result|error)"\s*:\s*')
EMPTY_RE = re.compile(rb'(\[\s*\]|\{\s*\}|"(?:0x)?")')
MESSAGE_RE = re.compile(rb'"message"\s*:\s*"((?:[^"\\]|\\.)*)')
HEX_RE = re.compile(r"0x[0-9a-fA-F]+")
BARE_HEX_RE = re.compile(r"\b[0-9a-fA-F]{16,}\b")
NUMBER_RE = re.compile(r"\b\d+\b")


def normalize_error(message):
    message = HEX_RE.sub("0x…", message)
    message = BARE_HEX_RE.sub("…", message)
    message = NUMBER_RE.sub("N", message)
    return message[:MAX_ERROR_MESSAGE_LEN]


def classify_body(body):
    m = KEY_RE.search(body)
    if not m:
        return "unknown", None
    if m.group(1) == b"error":
        msg = MESSAGE_RE.search(body, m.end())
        return "error", msg.group(1).decode("utf-8", "replace") if msg else ""
    rest = body[m.end():]
    if rest.startswith(b"null"):
        return "null", None
    if EMPTY_RE.match(rest):
        return "empty", None
    return "ok", None


def classify_record(rec):
    code = rec.get("code") or 0
    if code == 0:
        return "no_response", None
    if not 200 <= code < 300:
        return "http_error", None
    body_b64 = rec.get("body") or ""
    prefix = body_b64[:BODY_PREFIX_B64_CHARS]
    prefix = prefix[:len(prefix) - len(prefix) % 4]
    return classify_body(base64.b64decode(prefix))


class Tally:
    def __init__(self):
        self.counts = {c: 0 for c in CATEGORIES}
        self.errors = collections.Counter()

    def add(self, rec):
        category, message = classify_record(rec)
        self.counts[category] += 1
        if category == "error":
            self.errors[normalize_error(message)] += 1

    def top_errors(self, n):
        return self.errors.most_common(n)


def tally(lines, into=None):
    t = into or Tally()
    for line in lines:
        line = line.strip()
        if line:
            t.add(json.loads(line))
    return t


def bin_files(bin_dir, client, method, repetition=None):
    rep = str(repetition) if repetition else r"\d+"
    name_re = re.compile(rf"_{re.escape(client)}_{re.escape(method)}_\d+_\d+_{rep}\.bin$")
    return sorted(os.path.join(bin_dir, f) for f in os.listdir(bin_dir) if name_re.search(f))


class Row:
    def __init__(self, method, client, counts, top_errors):
        self.method = method
        self.client = client
        self.responses = sum(counts.get(c, 0) for c in JSONRPC_CATEGORIES)
        self.transport_failures = sum(counts.get(c, 0) for c in TRANSPORT_CATEGORIES)
        self.shares = {c: 100.0 * counts.get(c, 0) / self.responses if self.responses else 0.0
                       for c in JSONRPC_CATEGORIES}
        self.top_error = top_errors[0][0] if top_errors else ""


class Divergence:
    def __init__(self, method, client, category, reference_share, share):
        self.method = method
        self.client = client
        self.category = category
        self.reference_share = reference_share
        self.share = share

    def describe(self):
        if self.category == "responses":
            return f"{self.method}: {self.client} returned no JSON-RPC response"
        return (f"{self.method}: {self.category} is {self.share:.2f}% on {self.client} "
                f"vs {self.reference_share:.2f}% on {REFERENCE_CLIENT}")

    def __repr__(self):
        return f"Divergence({self.describe()})"


def compare(reports, max_delta):
    by_method = collections.defaultdict(dict)
    for rep in reports:
        by_method[rep["method"]][rep["client"]] = rep

    rows, flagged, uncompared = [], [], []
    for method in sorted(by_method):
        clients = by_method[method]
        method_rows = {client: Row(method, client, rep["counts"], rep.get("top_errors", []))
                       for client, rep in sorted(clients.items(), key=lambda kv: kv[0] != REFERENCE_CLIENT)}
        rows.extend(method_rows.values())
        ref = method_rows.get(REFERENCE_CLIENT)
        others = [r for c, r in method_rows.items() if c != REFERENCE_CLIENT]
        if ref is None or not others:
            uncompared.append(method)
            continue
        for row in [ref] + others:
            if row.responses == 0:
                flagged.append(Divergence(method, row.client, "responses", 0.0, 0.0))
        if ref.responses == 0:
            continue
        for row in others:
            if row.responses == 0:
                continue
            for category in JSONRPC_CATEGORIES:
                if abs(row.shares[category] - ref.shares[category]) > max_delta:
                    flagged.append(Divergence(method, row.client, category,
                                              ref.shares[category], row.shares[category]))
    return rows, flagged, uncompared


def render_summary(rows, flagged, uncompared, max_delta):
    badge = "❌" if flagged else "✅"
    lines = [f"## {badge} RPC performance: response check", "",
             "Share of JSON-RPC responses per category. Vegeta counts every HTTP 2xx as a success, "
             f"so a client answering `null` or `error` must not be compared on latency. "
             f"A difference above {max_delta} percentage points from {REFERENCE_CLIENT} fails the check. "
             "HTTP failures are counted apart: they measure load, not correctness.", "",
             "| method | client | responses | " + " | ".join(JSONRPC_CATEGORIES) + " | HTTP failures | top error |",
             "|---|---|---:|" + "---:|" * len(JSONRPC_CATEGORIES) + "---:|---|"]
    for r in rows:
        shares = " | ".join(f"{r.shares[c]:.2f}%" for c in JSONRPC_CATEGORIES)
        top_error = f"`{r.top_error}`" if r.top_error else ""
        lines.append(f"| {r.method} | {r.client} | {r.responses} | {shares} | {r.transport_failures} | {top_error} |")
    lines.append("")
    if flagged:
        lines += ["### Diverging responses", ""] + [f"- {d.describe()}" for d in flagged] + [""]
    if uncompared:
        lines += [f"Not compared (results from one client only): {', '.join(uncompared)}", ""]
    return "\n".join(lines)


def annotate(level, message):
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}")
    else:
        print(f"{level.upper()}: {message}")


def run_classify(args):
    files = bin_files(args.bin_dir, args.client, args.method, args.repetition)
    if not files:
        print(f"no vegeta result file for {args.client} {args.method} in {args.bin_dir}")
        return 1
    t = Tally()
    for path in files:
        with subprocess.Popen([args.vegeta, "encode", "--to", "json", path],
                              stdout=subprocess.PIPE, text=True) as proc:
            tally(proc.stdout, into=t)
        if proc.returncode != 0:
            print(f"vegeta encode failed on {path} with exit code {proc.returncode}")
            return 1
    result = {"client": args.client, "method": args.method,
              "files": [os.path.basename(f) for f in files],
              "counts": t.counts, "top_errors": t.top_errors(TOP_ERRORS)}
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2)
    print(f"{args.client} {args.method}: " + ", ".join(f"{c}={n}" for c, n in t.counts.items()))
    for message, count in t.top_errors(TOP_ERRORS):
        print(f"  {count:>9}  {message}")
    return 0


def load_reports(input_dir):
    reports = []
    for root, _, files in os.walk(input_dir):
        for name in sorted(files):
            if name.endswith("-responses.json"):
                with open(os.path.join(root, name), encoding="utf-8") as fh:
                    reports.append(json.load(fh))
    return reports


def run_compare(args):
    rows, flagged, uncompared = compare(load_reports(args.input_dir), args.max_delta)
    summary = render_summary(rows, flagged, uncompared, args.max_delta)
    print(summary)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as fh:
            fh.write(summary + "\n")
    if not rows:
        annotate("warning", "no response classification found, nothing was compared")
    if uncompared:
        annotate("warning", f"results from one client only, not compared: {', '.join(uncompared)}")
    for d in flagged:
        annotate("error", d.describe())
    return 1 if flagged else 0


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    classify = sub.add_parser("classify", help="classify the responses of one client and method")
    classify.add_argument("--bin-dir", required=True, dest="bin_dir",
                          help="directory with the vegeta .bin result files")
    classify.add_argument("--client", required=True, help="client name as in the .bin file names")
    classify.add_argument("--method", required=True, help="test type as in the .bin file names")
    classify.add_argument("--repetition", type=int, default=None,
                          help="classify only this repetition; repetitions run the same requests (default: all)")
    classify.add_argument("--vegeta", default="vegeta", help="vegeta CLI (default: %(default)s)")
    classify.add_argument("--output", required=True, help="write the classification JSON here")

    comp = sub.add_parser("compare", help="compare the classifications of all clients")
    comp.add_argument("--input-dir", required=True, dest="input_dir",
                      help="directory searched recursively for *-responses.json")
    comp.add_argument("--max-delta", type=float, default=2.0, dest="max_delta",
                      help="max difference in percentage points per category (default: %(default)s)")
    comp.add_argument("--summary-file", default=None, dest="summary_file",
                      help="append a Markdown summary here, e.g. $GITHUB_STEP_SUMMARY")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "classify":
        return run_classify(args)
    return run_compare(args)


if __name__ == "__main__":
    sys.exit(main())
