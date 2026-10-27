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


def ok(result, rid=1):
    return json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}).encode()


def err(message, code=-32000, rid=1):
    return json.dumps({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}).encode()


def record(body, code=200):
    return {"code": code, "body": base64.b64encode(body).decode()}


def state(method, body):
    return mod.classify_response(method, body)[0]


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


def report(client, method, data=(), no_data=None, transport=0):
    no_data = no_data or {}
    return {"client": client, "method": method,
            "counts": {"data": len(data), "no_data": sum(len(v) for v in no_data.values()), "transport": transport},
            "data_ids": list(data), "no_data_ids": {r: list(ids) for r, ids in no_data.items()},
            "transport_reasons": []}


# --- classify_response: plain results ----------------------------------------

check("receipt object is data",
      state("eth_getTransactionReceipt", ok({"status": "0x1", "logs": []})) == "data")
check("null result is no data",
      mod.classify_response("eth_getTransactionReceipt", ok(None))[:2] == ("no_data", "result: null"))
check("zero balance is no data", state("eth_getBalance", ok("0x0")) == "no_data")
check("non-zero balance is data", state("eth_getBalance", ok("0x1bc16d674ec80000")) == "data")
check("empty logs are no data", state("eth_getLogs", ok([])) == "no_data")
check("logs are data", state("eth_getLogs", ok([{"address": "0x01"}])) == "data")
check("empty call output is no data", state("eth_call", ok("0x")) == "no_data")
check("call output is data", state("eth_call", ok("0x" + "00" * 31 + "01")) == "data")

# --- classify_response: objects with fields but no data ----------------------

check("storageRangeAt with null storage is no data",
      mod.classify_response("debug_storageRangeAt", ok({"storage": None, "nextKey": None}))[:2]
      == ("no_data", "result: storage empty"))
check("storageRangeAt with empty storage is no data",
      state("debug_storageRangeAt", ok({"storage": {}, "nextKey": None})) == "no_data")
check("storageRangeAt with entries is data",
      state("debug_storageRangeAt", ok({"storage": {"0xab": {"key": None, "value": "0x01"}}, "nextKey": None})) == "data")
check("block without transactions is no data",
      state("eth_getBlockByNumber", ok({"number": "0xc65d40", "transactions": []})) == "no_data")
check("block with transactions is data",
      state("eth_getBlockByHash", ok({"number": "0xc65d40", "transactions": [{"hash": "0x01"}]})) == "data")
check("trace without struct logs is no data",
      state("debug_traceTransaction", ok({"gas": 21000, "failed": False, "returnValue": "", "structLogs": []})) == "no_data")
check("trace with struct logs is data",
      state("debug_traceTransaction", ok({"gas": 50000, "failed": False, "structLogs": [{"op": "PUSH1"}]})) == "data")
check("empty access list is no data",
      state("eth_createAccessList", ok({"accessList": [], "gasUsed": "0x5208"})) == "no_data")
check("access list of a reverted execution is data",
      state("eth_createAccessList", ok({"accessList": [], "error": "execution reverted", "gasUsed": "0x6000"})) == "data")
check("access list with entries is data",
      state("eth_createAccessList", ok({"accessList": [{"address": "0x01", "storageKeys": []}], "gasUsed": "0x6000"})) == "data")

# --- classify_response: errors -----------------------------------------------

check("revert is data: the EVM ran", state("eth_call", err("execution reverted: STF", code=3)) == "data")
check("VM error is data: the EVM ran", state("eth_call", err("out of gas")) == "data")
check("pre-execution rejection is no data",
      state("eth_call", err("err: insufficient funds for gas * price + value: address 0xab have 0 want 1 (supplied gas 21000)"))
      == "no_data")
check("geth missing state is no data",
      mod.classify_response("eth_getBalance", err("historical state 1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d is not available"))[:2]
      == ("no_data", "error: historical state … is not available"))
check("erigon pruned state is no data",
      state("eth_call", err("old data not available due to pruning: requested block 14000000, history is available from block 23000000"))
      == "no_data")
check("geth pruned history is no data", state("eth_getLogs", err("pruned history unavailable", code=4444)) == "no_data")
check("a result together with an error is no data",
      mod.classify_response("debug_traceTransaction",
                            b'{"jsonrpc":"2.0","id":1,"result":{"structLogs":[{"op":"PUSH1"}]},'
                            b'"error":{"code":-32000,"message":"tracing failed: missing trie node"}}')[:2]
      == ("no_data", "partial result with error: tracing failed: missing trie node"))
check("not a JSON-RPC response is no data",
      mod.classify_response("eth_call", b"<html>bad gateway</html>")[:2] == ("no_data", "not a JSON-RPC response"))

# --- classify_response: bodies that cannot be parsed whole -------------------

truncated = ok({"structLogs": [{"op": "PUSH1", "pc": i} for i in range(100)]}, rid=7)[:1500]
check("a truncated body with a result is data", mod.classify_response("debug_traceTransaction", truncated)[0] == "data")
check("the id is read from a truncated body", mod.classify_response("debug_traceTransaction", truncated)[2] == 7)

big = ok({"structLogs": [{"op": "PUSH1", "pc": i} for i in range(20000)]}, rid=9)
check("a large body with a result is data", mod.classify_response("debug_traceTransaction", big)[::2] == ("data", 9))
big_with_error = big[:-1] + b',"error":{"code":-32000,"message":"tracing failed"}}'
check("a large body with a trailing error is no data",
      state("debug_traceTransaction", big_with_error) == "no_data")

# --- classify_record ---------------------------------------------------------

check("HTTP 503 is a transport failure", mod.classify_record("eth_call", record(b"busy", code=503))[0] == "transport")
check("code 0 is a transport failure",
      mod.classify_record("eth_call", {"code": 0, "body": "", "error": "timeout"})[0] == "transport")
check("HTTP 200 is classified by its body, with the id",
      mod.classify_record("eth_call", record(ok(None, rid=42))) == ("no_data", "result: null", 42))

# --- tally -------------------------------------------------------------------

lines = [json.dumps(r) for r in [
    record(ok("0x01", rid=1)),
    record(ok(None, rid=2)),
    record(err("historical state " + "aa" * 32 + " is not available", rid=3)),
    record(err("historical state " + "bb" * 32 + " is not available", rid=4)),
    record(b"busy", code=503),
    record(ok("0x01", rid=1)),
]]
t = mod.tally("eth_call", lines)
check("tally: counts data, no data and transport failures",
      t.counts == {"data": 2, "no_data": 3, "transport": 1}, str(t.counts))
check("tally: ids per state, grouped by reason",
      t.data_ids == {1} and t.no_data_ids == {"result: null": {2}, "error: historical state … is not available": {3, 4}},
      f"{t.data_ids} {t.no_data_ids}")

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

rows, mismatches, uncompared = mod.compare([report("erigon", "eth_call", data=[1, 2]),
                                            report("geth", "eth_call", data=[1, 2])])
check("compare: data on both sides passes", mismatches == [] and uncompared == [], str(mismatches))

rows, mismatches, uncompared = mod.compare([report("erigon", "eth_getLogs", no_data={"result: empty value": [1]}),
                                            report("geth", "eth_getLogs", no_data={"result: empty value": [1]})])
check("compare: no data on both sides passes", mismatches == [], str(mismatches))

rows, mismatches, uncompared = mod.compare(
    [report("erigon", "eth_getBalance", data=[1, 2, 3]),
     report("geth", "eth_getBalance", data=[1], no_data={"error: historical state … is not available": [2, 3]})])
check("compare: data on erigon only is a mismatch, with the reason on geth",
      [(m.method, m.data_client, m.empty_client, m.reason, sorted(m.ids)) for m in mismatches]
      == [("eth_getBalance", "erigon", "geth", "error: historical state … is not available", [2, 3])],
      str(mismatches))

rows, mismatches, uncompared = mod.compare(
    [report("erigon", "eth_getTransactionByHash", data=[1], no_data={"result: null": [2]}),
     report("geth", "eth_getTransactionByHash", data=[1, 2])])
check("compare: data on geth only is a mismatch too",
      [(m.data_client, m.empty_client, sorted(m.ids)) for m in mismatches] == [("geth", "erigon", [2])],
      str(mismatches))

rows, mismatches, uncompared = mod.compare([report("erigon", "eth_call", data=[1, 2, 3]),
                                            report("geth", "eth_call", no_data={"result: null": [4]})])
check("compare: requests sent to one client only are not paired", mismatches == [], str(mismatches))

rows, mismatches, uncompared = mod.compare([report("erigon", "eth_getLogs", data=[1])])
check("compare: a method run on one client only is not compared",
      mismatches == [] and uncompared == ["eth_getLogs"], str(uncompared))

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
    check("main classify: writes counts and ids",
          written["client"] == "geth" and written["method"] == "eth_call"
          and written["counts"] == {"data": 2, "no_data": 3, "transport": 1}
          and written["data_ids"] == [1] and written["no_data_ids"]["result: null"] == [2],
          json.dumps(written))

    code, out = run_main(["classify", "--bin-dir", bin_dir, "--client", "geth", "--method", "eth_getLogs",
                          "--vegeta", fake_vegeta, "--output", out_file])
    check("main classify: no result file is an error", code == 1, out)

# --- main compare ------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    for sub, rep in [("test-results-mainnet-erigon/responses", report("erigon", "eth_call", data=[1, 2])),
                     ("test-results-mainnet-geth/responses", report("geth", "eth_call", data=[1],
                                                                    no_data={"result: null": [2]}))]:
        os.makedirs(os.path.join(tmp, sub))
        with open(os.path.join(tmp, sub, f"{rep['client']}-eth_call-responses.json"), "w", encoding="utf-8") as fh:
            json.dump(rep, fh)
    summary = os.path.join(tmp, "summary.md")
    code, out = run_main(["compare", "--input-dir", tmp, "--summary-file", summary])
    check("main compare: a mismatch fails", code == 1, out)
    with open(summary, encoding="utf-8") as fh:
        md = fh.read()
    check("main compare: summary names the method, the reason and the ids",
          "eth_call" in md and "result: null" in md and "2" in md, md)

with tempfile.TemporaryDirectory() as tmp:
    code, out = run_main(["compare", "--input-dir", tmp])
    check("main compare: nothing to compare is not a failure", code == 0, out)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
