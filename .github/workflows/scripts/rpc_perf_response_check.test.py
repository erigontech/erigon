#!/usr/bin/env python3
"""Fixture tests for rpc_perf_response_check.py.

Run: python3 .github/workflows/scripts/rpc_perf_response_check.test.py
"""
import base64
import contextlib
import importlib.util
import io
import json
import os
import stat
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "rpc_perf_response_check", os.path.join(HERE, "rpc_perf_response_check.py"))
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


def record(body, code=200):
    return {"code": code, "body": base64.b64encode(body).decode()}


def run_main(argv):
    out = io.StringIO()
    old = sys.argv
    sys.argv = ["rpc_perf_response_check.py"] + argv
    try:
        with contextlib.redirect_stdout(out):
            code = mod.main()
    except SystemExit as e:
        code = e.code
    finally:
        sys.argv = old
    return code, out.getvalue()


def report(client, method, **counts):
    full = {c: 0 for c in mod.CATEGORIES}
    full.update(counts)
    return {"client": client, "method": method, "counts": full, "top_errors": []}


# --- classify_body -----------------------------------------------------------

check("body: scalar result is ok",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"result":"0x01"}') == ("ok", None))
check("body: null result",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"result":null}') == ("null", None))
check("body: null result with whitespace",
      mod.classify_body(b'{"jsonrpc": "2.0", "id": 1, "result" : null }') == ("null", None))
check("body: empty array result",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"result":[]}') == ("empty", None))
check("body: empty hex result",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"result":"0x"}') == ("empty", None))
check("body: error carries its message",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}')
      == ("error", "header not found"))
check("body: nested error inside a result is still ok",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"result":{"structLogs":[{"error":"out of gas"}]}}')[0] == "ok")
check("body: a truncated body is classified from its prefix",
      mod.classify_body(b'{"jsonrpc":"2.0","id":1,"result":{"blockHash":"0xab')[0] == "ok")
check("body: not JSON-RPC is unknown",
      mod.classify_body(b"<html>bad gateway</html>")[0] == "unknown")

# --- classify_record ---------------------------------------------------------

check("record: HTTP 503 is an HTTP error",
      mod.classify_record(record(b"busy", code=503))[0] == "http_error")
check("record: code 0 means no response",
      mod.classify_record({"code": 0, "body": "", "error": "timeout"})[0] == "no_response")
check("record: HTTP 200 is classified by its body",
      mod.classify_record(record(b'{"jsonrpc":"2.0","id":1,"result":null}'))[0] == "null")

# --- tally -------------------------------------------------------------------

lines = [json.dumps(r) for r in [
    record(b'{"jsonrpc":"2.0","id":1,"result":"0x1"}'),
    record(b'{"jsonrpc":"2.0","id":2,"result":null}'),
    record(b'{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"missing trie node 0xabc1 (path )"}}'),
    record(b'{"jsonrpc":"2.0","id":4,"error":{"code":-32000,"message":"missing trie node 0xdef2 (path )"}}'),
    record(b"busy", code=503),
]]
t = mod.tally(lines)
check("tally: counts per category",
      t.counts["ok"] == 1 and t.counts["null"] == 1 and t.counts["error"] == 2 and t.counts["http_error"] == 1,
      str(t.counts))
check("tally: error messages differing only in hashes are grouped",
      t.top_errors(5) == [("missing trie node 0x… (path )", 2)], str(t.top_errors(5)))

# --- bin_files ---------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    for name in ["20260922104741_mainnet_geth_eth_call_1_1_1.bin",
                 "20260922104749_mainnet_geth_eth_call_100_30_2.bin",
                 "20260922104741_mainnet_geth_eth_callMany_1_1_1.bin",
                 "20260922104741_mainnet_erigon_eth_call_1_1_1.bin",
                 "notes.txt"]:
        open(os.path.join(tmp, name), "w").close()
    found = [os.path.basename(p) for p in mod.bin_files(tmp, "geth", "eth_call")]
    check("bin_files: only this client and exactly this method",
          found == ["20260922104741_mainnet_geth_eth_call_1_1_1.bin",
                    "20260922104749_mainnet_geth_eth_call_100_30_2.bin"], str(found))
    found = [os.path.basename(p) for p in mod.bin_files(tmp, "geth", "eth_call", repetition=2)]
    check("bin_files: filters by repetition",
          found == ["20260922104749_mainnet_geth_eth_call_100_30_2.bin"], str(found))

# --- compare -----------------------------------------------------------------

rows, flagged, uncompared = mod.compare(
    [report("erigon", "eth_call", ok=900, error=100), report("geth", "eth_call", ok=895, error=105)], 2.0)
check("compare: shares within the threshold pass", flagged == [] and uncompared == [], str(flagged))

rows, flagged, uncompared = mod.compare(
    [report("erigon", "eth_getBalance", ok=1000), report("geth", "eth_getBalance", null=1000)], 2.0)
check("compare: geth answering null is flagged",
      sorted((f.method, f.category) for f in flagged) == [("eth_getBalance", "null"), ("eth_getBalance", "ok")],
      str(flagged))

rows, flagged, uncompared = mod.compare(
    [report("erigon", "eth_call", ok=1000), report("geth", "eth_call", http_error=1000)], 2.0)
check("compare: a client without any JSON-RPC response is flagged",
      [f.category for f in flagged] == ["responses"], str(flagged))

rows, flagged, uncompared = mod.compare(
    [report("erigon", "eth_call", ok=1000, http_error=500), report("geth", "eth_call", ok=1000)], 2.0)
check("compare: HTTP errors do not count against the JSON-RPC shares", flagged == [], str(flagged))

rows, flagged, uncompared = mod.compare([report("erigon", "eth_getLogs", ok=10)], 2.0)
check("compare: a method run on one client only is not compared",
      flagged == [] and uncompared == ["eth_getLogs"], str(uncompared))

# --- main classify -----------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    bin_dir = os.path.join(tmp, "bin")
    os.makedirs(bin_dir)
    open(os.path.join(bin_dir, "20260922104741_mainnet_geth_eth_call_1_1_1.bin"), "w").close()
    fixture = os.path.join(tmp, "encoded.jsonl")
    with open(fixture, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    fake_vegeta = os.path.join(tmp, "vegeta")
    with open(fake_vegeta, "w", encoding="utf-8") as fh:
        fh.write(f"#!{sys.executable}\nimport sys\nsys.stdout.write(open({fixture!r}).read())\n")
    os.chmod(fake_vegeta, os.stat(fake_vegeta).st_mode | stat.S_IEXEC)
    out_file = os.path.join(tmp, "geth-eth_call-responses.json")

    code, out = run_main(["classify", "--bin-dir", bin_dir, "--client", "geth", "--method", "eth_call",
                          "--vegeta", fake_vegeta, "--output", out_file])
    check("main classify: exits 0", code == 0, out)
    with open(out_file, encoding="utf-8") as fh:
        written = json.load(fh)
    check("main classify: writes client, method and counts",
          written["client"] == "geth" and written["method"] == "eth_call" and written["counts"]["error"] == 2,
          json.dumps(written))

    code, out = run_main(["classify", "--bin-dir", bin_dir, "--client", "geth", "--method", "eth_getLogs",
                          "--vegeta", fake_vegeta, "--output", out_file])
    check("main classify: no result file is an error", code == 1, out)

# --- main compare ------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    for sub, rep in [("test-results-mainnet-erigon/mainnet", report("erigon", "eth_call", ok=100)),
                     ("test-results-mainnet-geth/mainnet", report("geth", "eth_call", null=100))]:
        os.makedirs(os.path.join(tmp, sub))
        with open(os.path.join(tmp, sub, f"{rep['client']}-eth_call-responses.json"), "w", encoding="utf-8") as fh:
            json.dump(rep, fh)
    summary = os.path.join(tmp, "summary.md")
    code, out = run_main(["compare", "--input-dir", tmp, "--summary-file", summary])
    check("main compare: a diverging method fails", code == 1, out)
    with open(summary, encoding="utf-8") as fh:
        md = fh.read()
    check("main compare: summary lists the method and the diverging category",
          "eth_call" in md and "null" in md, md)

with tempfile.TemporaryDirectory() as tmp:
    code, out = run_main(["compare", "--input-dir", tmp])
    check("main compare: nothing to compare is not a failure", code == 0, out)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
