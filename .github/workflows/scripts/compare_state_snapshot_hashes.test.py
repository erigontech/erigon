#!/usr/bin/env python3
"""Fixture tests for compare_state_snapshot_hashes.py.

Run: python3 .github/workflows/scripts/compare_state_snapshot_hashes.test.py
"""
import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "compare_state_snapshot_hashes", os.path.join(HERE, "compare_state_snapshot_hashes.py"))
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


def write(tmp, rel, content):
    p = os.path.join(tmp, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(content)
    return p


def run_main(argv):
    out = io.StringIO()
    old = sys.argv
    sys.argv = ["compare_state_snapshot_hashes.py"] + argv
    try:
        with contextlib.redirect_stdout(out):
            code = mod.main()
    except SystemExit as e:
        code = e.code
    finally:
        sys.argv = old
    return code, out.getvalue()


# --- parse_hash_toml ---------------------------------------------------------

TOML = """\
'accessor/v1.1-code.0-256.vi' = 'aaaa'
'domain/v1.1-accounts.0-1024.kv' = 'bbbb'
'history/v1.1-storage.0-1024.v' = 'cccc'
'idx/v1.1-code.0-1024.ef' = 'dddd'
'v1.1-000000-000500-headers.seg' = 'eeee'
'caplin/v1.1-000000-000100-beaconblocks.seg' = 'ffff'
'salt-state.txt' = '9999'
"""

entries = mod.parse_hash_toml(TOML)
check("parse reads every entry", len(entries) == 7, entries)
check("parse keeps directory prefix",
      entries.get("domain/v1.1-accounts.0-1024.kv") == "bbbb", entries)

check("parse tolerates blank lines and comments",
      mod.parse_hash_toml("\n# a comment\n'domain/x.kv' = '1'\n") == {"domain/x.kv": "1"})

# --- state_entries -----------------------------------------------------------

state = mod.state_entries(entries)
check("state filter keeps the four state dirs", sorted(state) == [
    "accessor/v1.1-code.0-256.vi",
    "domain/v1.1-accounts.0-1024.kv",
    "history/v1.1-storage.0-1024.v",
    "idx/v1.1-code.0-1024.ef",
], sorted(state))
check("state filter drops blocks, caplin and salt files",
      not any(k.startswith(("caplin/", "salt", "v1.1-0")) for k in state), sorted(state))

check("state filter can be narrowed to given subdirs",
      sorted(mod.state_entries(entries, ("domain", "idx"))) == [
          "domain/v1.1-accounts.0-1024.kv",
          "idx/v1.1-code.0-1024.ef",
      ], sorted(mod.state_entries(entries, ("domain", "idx"))))

# --- compare -----------------------------------------------------------------

cmp_all_match = mod.compare({"domain/a.kv": "1", "idx/b.ef": "2"},
                            {"domain/a.kv": "1", "idx/b.ef": "2"})
check("compare reports full agreement",
      cmp_all_match.matched == ["domain/a.kv", "idx/b.ef"] and not cmp_all_match.mismatched,
      cmp_all_match)

cmp_bad = mod.compare({"domain/a.kv": "1", "idx/b.ef": "wrong"},
                      {"domain/a.kv": "1", "idx/b.ef": "2"})
check("compare reports a differing hash",
      [m.name for m in cmp_bad.mismatched] == ["idx/b.ef"], cmp_bad.mismatched)
check("mismatch carries both hashes",
      cmp_bad.mismatched[0].local == "wrong" and cmp_bad.mismatched[0].published == "2",
      cmp_bad.mismatched)

cmp_partial = mod.compare({"domain/a.kv": "1", "domain/new.kv": "3"},
                          {"domain/a.kv": "1", "domain/old.kv": "2"})
check("compare lists files only Erigon built", cmp_partial.local_only == ["domain/new.kv"], cmp_partial)
check("compare lists files only the TOML has", cmp_partial.published_only == ["domain/old.kv"], cmp_partial)

# --- verdict -----------------------------------------------------------------

check("all matching is a success", mod.verdict(cmp_all_match).outcome == "SUCCESS")
check("any mismatch is a failure", mod.verdict(cmp_bad).outcome == "FAILURE")
check("no file in common is an error",
      mod.verdict(mod.compare({"domain/a.kv": "1"}, {"domain/b.kv": "2"})).outcome == "ERROR")

result = mod.verdict(cmp_bad)
check("result carries measures", result.measures["mismatched"] == 1 and result.measures["matched"] == 1,
      result.measures)
check("result serialises to the QA result shape",
      set(json.loads(json.dumps(result.__dict__))) == {"outcome", "reason", "exit_code", "measures"},
      result.__dict__)

# --- resolve_snapshot_branch -------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    write(tmp, "db/version/app.go",
          'package version\n\nconst (\n\tDefaultSnapshotGitBranch = "release/3.6" // Branch of ...\n)\n')
    check("branch comes from db/version/app.go",
          mod.resolve_snapshot_branch(tmp) == "release/3.6", mod.resolve_snapshot_branch(tmp))

# --- main --------------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    local = write(tmp, "hashes.txt", "'domain/a.kv' = '1'\n'v1.1-000000-000500-headers.seg' = 'zzz'\n")
    published = write(tmp, "preverified.toml", "'domain/a.kv' = '1'\n'v1.1-000000-000500-headers.seg' = 'other'\n")
    res = os.path.join(tmp, "result.json")
    code, out = run_main(["--local-hashes", local, "--published-toml", published,
                          "--chain", "chiado", "--result-file", res])
    check("main exits 0 when state hashes agree", code == 0, out)
    check("main ignores non-state entries", json.load(open(res))["measures"]["matched"] == 1)

with tempfile.TemporaryDirectory() as tmp:
    local = write(tmp, "hashes.txt", "'domain/a.kv' = '1'\n'accessor/a.bt' = 'other-salt'\n")
    published = write(tmp, "preverified.toml", "'domain/a.kv' = '1'\n'accessor/a.bt' = '2'\n")
    res = os.path.join(tmp, "result.json")
    code, out = run_main(["--local-hashes", local, "--published-toml", published, "--chain", "chiado",
                          "--dirs", "domain,history,idx", "--result-file", res])
    check("--dirs narrows what is compared", code == 0, out)
    check("--dirs leaves the excluded subdir out of the measures",
          json.load(open(res))["measures"]["matched"] == 1)

with tempfile.TemporaryDirectory() as tmp:
    local = write(tmp, "hashes.txt", "'domain/a.kv' = 'wrong'\n")
    published = write(tmp, "preverified.toml", "'domain/a.kv' = '1'\n")
    res = os.path.join(tmp, "result.json")
    summary = os.path.join(tmp, "summary.md")
    code, out = run_main(["--local-hashes", local, "--published-toml", published,
                          "--chain", "chiado", "--result-file", res, "--summary-file", summary])
    check("main exits 1 on a mismatch", code == 1, out)
    check("main writes the result file", json.load(open(res))["outcome"] == "FAILURE")
    check("summary names the offending file", "domain/a.kv" in open(summary).read())
    check("console output names the offending file", "domain/a.kv" in out, out)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
