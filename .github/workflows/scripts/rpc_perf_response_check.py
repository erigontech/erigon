#!/usr/bin/env python3
"""Check that the clients of an RPC performance benchmark answered the same requests with data.

Vegeta counts a request as successful on any 2xx status, and a JSON-RPC node
answers HTTP 200 also when it lacks the requested data (pruned state or history,
missing tx index). Such a node looks healthy, and usually faster, in the latency
report, so its numbers must not be compared with a node that did the work.

Clients do not return identical payloads, so only the presence of data is
compared. ``classify`` reads the vegeta result files of one client and method
through ``vegeta encode --to json`` and puts every response in one state:

  data     -- a result carrying data, or an EVM execution failure (the EVM ran)
  no_data  -- ``null``; an empty value ([], {}, "", "0x", "0x0"); an object whose
              data field is empty (see EMPTY_FIELDS); any other error, including
              missing state/history and rejections before execution
  transport -- HTTP failure or no response: load, not correctness

``compare`` pairs the responses of Erigon and of each other client by JSON-RPC
id (unique per pattern file) and fails when the same request got data from one
client and no data from the other. No data on both sides is accepted.
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

EMPTY_VALUES = ("", "0x", "0x0")

EMPTY_FIELDS = {
    "debug_storageRangeAt": "storage",
    "debug_traceTransaction": "structLogs",
    "eth_createAccessList": "accessList",
    "eth_getBlockByHash": "transactions",
    "eth_getBlockByNumber": "transactions",
}

EXECUTION_ERROR_RE = re.compile(
    r"^(execution reverted|out of gas|contract creation code storage out of gas|max call depth exceeded"
    r"|insufficient balance for transfer|contract address collision|max code size exceeded"
    r"|max initcode size exceeded|invalid jump destination|write protection|return data out of bounds"
    r"|gas uint64 overflow|nonce uint64 overflow|invalid code|stack underflow|stack limit reached|invalid opcode)")

FULL_PARSE_MAX_BYTES = 64 * 1024
HEAD_BYTES = 768
TAIL_BYTES = 768
MAX_REASON_LEN = 120
TOP_REASONS = 10
MAX_IDS_SHOWN = 10

KEY_RE = re.compile(rb'"(result|error)"\s*:\s*')
ID_RE = re.compile(rb'"id"\s*:\s*(\d+)')
MESSAGE_RE = re.compile(rb'"message"\s*:\s*"((?:[^"\\]|\\.)*)')
TOP_LEVEL_ERROR_RE = re.compile(rb'"error"\s*:\s*\{\s*"code"')
HEX_RE = re.compile(r"0x[0-9a-fA-F]+")
BARE_HEX_RE = re.compile(r"\b[0-9a-fA-F]{16,}\b")
NUMBER_RE = re.compile(r"\b\d+\b")


def normalize(message):
    message = HEX_RE.sub("0x…", message)
    message = BARE_HEX_RE.sub("…", message)
    message = NUMBER_RE.sub("N", message)
    return message[:MAX_REASON_LEN]


def is_empty(value):
    return value is None or value == [] or value == {} or (isinstance(value, str) and value in EMPTY_VALUES)


def classify_error(message):
    if EXECUTION_ERROR_RE.match(message):
        return "data", None
    return "no_data", f"error: {normalize(message)}"


def classify_result(method, result):
    if result is None:
        return "no_data", "result: null"
    if is_empty(result):
        return "no_data", "result: empty value"
    field = EMPTY_FIELDS.get(method)
    if field and isinstance(result, dict):
        if method == "eth_createAccessList" and result.get("error"):
            return "data", None
        if is_empty(result.get(field)):
            return "no_data", f"result: {field} empty"
    return "data", None


def classify_object(method, obj):
    rid = obj.get("id")
    if "error" in obj:
        message = str((obj["error"] or {}).get("message", ""))
        if "result" in obj:
            return "no_data", f"partial result with error: {normalize(message)}", rid
        return (*classify_error(message), rid)
    if "result" not in obj:
        return "no_data", "not a JSON-RPC response", rid
    return (*classify_result(method, obj["result"]), rid)


def classify_head_tail(head, tail):
    """Classify a body too large, or truncated, to parse: no empty answer is that large."""
    m_id = ID_RE.search(head)
    rid = int(m_id.group(1)) if m_id else None
    m = KEY_RE.search(head)
    if not m:
        return "no_data", "not a JSON-RPC response", rid
    if m.group(1) == b"error":
        msg = MESSAGE_RE.search(head, m.end())
        return (*classify_error(msg.group(1).decode("utf-8", "replace") if msg else ""), rid)
    if head[m.end():].startswith(b"null"):
        return "no_data", "result: null", rid
    if TOP_LEVEL_ERROR_RE.search(tail):
        msg = MESSAGE_RE.search(tail)
        message = msg.group(1).decode("utf-8", "replace") if msg else ""
        return "no_data", f"partial result with error: {normalize(message)}", rid
    return "data", None, rid


def classify_response(method, body):
    if len(body) <= FULL_PARSE_MAX_BYTES:
        try:
            obj = json.loads(body)
        except ValueError:
            obj = None
        if isinstance(obj, dict):
            return classify_object(method, obj)
    return classify_head_tail(body[:HEAD_BYTES], body[-TAIL_BYTES:])


def classify_record(method, rec):
    code = rec.get("code") or 0
    if code == 0:
        return "transport", "no response", None
    if not 200 <= code < 300:
        return "transport", f"HTTP {code}", None
    body_b64 = rec.get("body") or ""
    if len(body_b64) * 3 // 4 <= FULL_PARSE_MAX_BYTES:
        return classify_response(method, base64.b64decode(body_b64))
    head_b64 = body_b64[:HEAD_BYTES * 4 // 3]
    tail_b64 = body_b64[-(TAIL_BYTES * 4 // 3):] if len(body_b64) % 4 == 0 else ""
    return classify_head_tail(base64.b64decode(head_b64), base64.b64decode(tail_b64))


class Tally:
    def __init__(self, method):
        self.method = method
        self.counts = {"data": 0, "no_data": 0, "transport": 0}
        self.data_ids = set()
        self.no_data_ids = collections.defaultdict(set)
        self.transport_reasons = collections.Counter()

    def add(self, rec):
        state, reason, rid = classify_record(self.method, rec)
        self.counts[state] += 1
        if state == "transport":
            self.transport_reasons[reason] += 1
        elif rid is not None:
            if state == "data":
                self.data_ids.add(rid)
            else:
                self.no_data_ids[reason].add(rid)


def tally(method, lines, into=None):
    t = into or Tally(method)
    for line in lines:
        line = line.strip()
        if line:
            t.add(json.loads(line))
    return t


def bin_files(bin_dir, client, method, repetition=None):
    rep = str(repetition) if repetition else r"\d+"
    name_re = re.compile(rf"_{re.escape(client)}_{re.escape(method)}_\d+_\d+_{rep}\.bin$")
    return sorted(os.path.join(bin_dir, f) for f in os.listdir(bin_dir) if name_re.search(f))


def sorted_ids(ids):
    return sorted(ids, key=lambda i: (isinstance(i, str), i))


class Mismatch:
    def __init__(self, method, data_client, empty_client, reason, ids):
        self.method = method
        self.data_client = data_client
        self.empty_client = empty_client
        self.reason = reason
        self.ids = ids

    def describe(self):
        shown = ", ".join(str(i) for i in self.ids[:MAX_IDS_SHOWN])
        more = f" and {len(self.ids) - MAX_IDS_SHOWN} more" if len(self.ids) > MAX_IDS_SHOWN else ""
        return (f"{self.method}: {len(self.ids)} requests got data from {self.data_client} "
                f"but no data from {self.empty_client} ({self.reason}); ids: {shown}{more}")

    def __repr__(self):
        return f"Mismatch({self.describe()})"


class Pairing:
    def __init__(self, method, reports):
        self.method = method
        self.reports = reports
        self.paired = self.both_data = self.both_no_data = 0
        self.data_only_on = collections.Counter()


def all_ids(rep):
    return set(rep["data_ids"]).union(*[set(ids) for ids in rep["no_data_ids"].values()])


def compare(reports):
    by_method = collections.defaultdict(dict)
    for rep in reports:
        by_method[rep["method"]][rep["client"]] = rep

    pairings, mismatches, uncompared = [], [], []
    for method in sorted(by_method):
        clients = by_method[method]
        pairing = Pairing(method, [clients[c] for c in sorted(clients, key=lambda c: (c != REFERENCE_CLIENT, c))])
        pairings.append(pairing)
        ref = clients.get(REFERENCE_CLIENT)
        others = [rep for c, rep in clients.items() if c != REFERENCE_CLIENT]
        if ref is None or not others:
            uncompared.append(method)
            continue
        for other in others:
            common = all_ids(ref) & all_ids(other)
            ref_data, other_data = set(ref["data_ids"]) & common, set(other["data_ids"]) & common
            pairing.paired += len(common)
            pairing.both_data += len(ref_data & other_data)
            pairing.both_no_data += len(common - ref_data - other_data)
            for data_rep, empty_rep in ((ref, other), (other, ref)):
                data_ids = set(data_rep["data_ids"])
                for reason, ids in sorted(empty_rep["no_data_ids"].items()):
                    hit = data_ids & set(ids)
                    if hit:
                        pairing.data_only_on[data_rep["client"]] += len(hit)
                        mismatches.append(Mismatch(method, data_rep["client"], empty_rep["client"],
                                                   reason, sorted_ids(hit)))
    return pairings, mismatches, uncompared


def top_no_data_reason(rep):
    reasons = sorted(rep["no_data_ids"].items(), key=lambda kv: -len(kv[1]))
    return f"`{reasons[0][0]}` ({len(reasons[0][1])})" if reasons else ""


def render_summary(pairings, mismatches, uncompared):
    badge = "❌" if mismatches else "✅"
    lines = [f"## {badge} RPC performance: response check", "",
             "Vegeta counts every HTTP 2xx as a success, so latencies are comparable only if the clients "
             "returned data for the same requests. Responses are paired by JSON-RPC id: data from one client "
             "and no data (null, empty value, empty data field, error other than an EVM failure) from the other "
             "fails the check; no data on both sides is accepted.", "",
             "| method | paired requests | both data | both no data | data on one side only |",
             "|---|---:|---:|---:|---|"]
    for p in pairings:
        one_side = ", ".join(f"{c}: {n}" for c, n in sorted(p.data_only_on.items())) or "0"
        lines.append(f"| {p.method} | {p.paired} | {p.both_data} | {p.both_no_data} | {one_side} |")
    lines += ["", "| method | client | data | no data | transport failures | top no-data reason |",
              "|---|---|---:|---:|---:|---|"]
    for p in pairings:
        for rep in p.reports:
            c = rep["counts"]
            lines.append(f"| {p.method} | {rep['client']} | {c['data']} | {c['no_data']} | {c['transport']} | "
                         f"{top_no_data_reason(rep)} |")
    lines.append("")
    if mismatches:
        lines += ["### Data on one side only", ""] + [f"- {m.describe()}" for m in mismatches] + [""]
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
    t = Tally(args.method)
    for path in files:
        with subprocess.Popen([args.vegeta, "encode", "--to", "json", path],
                              stdout=subprocess.PIPE, text=True) as proc:
            tally(args.method, proc.stdout, into=t)
        if proc.returncode != 0:
            print(f"vegeta encode failed on {path} with exit code {proc.returncode}")
            return 1
    result = {"client": args.client, "method": args.method,
              "files": [os.path.basename(f) for f in files],
              "counts": t.counts,
              "data_ids": sorted_ids(t.data_ids),
              "no_data_ids": {r: sorted_ids(ids) for r, ids in sorted(t.no_data_ids.items())},
              "transport_reasons": t.transport_reasons.most_common()}
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(result, fh)
    print(f"{args.client} {args.method}: " + ", ".join(f"{s}={n}" for s, n in t.counts.items()))
    for reason, ids in sorted(t.no_data_ids.items(), key=lambda kv: -len(kv[1]))[:TOP_REASONS]:
        print(f"  no data for {len(ids):>7} requests: {reason}")
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
    pairings, mismatches, uncompared = compare(load_reports(args.input_dir))
    summary = render_summary(pairings, mismatches, uncompared)
    print(summary)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as fh:
            fh.write(summary + "\n")
    if not pairings:
        annotate("warning", "no response classification found, nothing was compared")
    if uncompared:
        annotate("warning", f"results from one client only, not compared: {', '.join(uncompared)}")
    for m in mismatches:
        annotate("error", m.describe())
    return 1 if mismatches else 0


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

    comp = sub.add_parser("compare", help="pair the classifications of all clients by request id")
    comp.add_argument("--input-dir", required=True, dest="input_dir",
                      help="directory searched recursively for *-responses.json")
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
