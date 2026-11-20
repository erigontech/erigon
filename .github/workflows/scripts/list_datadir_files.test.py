#!/usr/bin/env python3
"""Fixture tests for list_datadir_files.py.

Run: python3 .github/workflows/scripts/list_datadir_files.test.py
"""
import importlib.util
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "list_datadir_files.py")
spec = importlib.util.spec_from_file_location("list_datadir_files", SCRIPT)
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


def make_datadir(tmp):
    root = os.path.join(tmp, "erigon_data")
    for rel, data in [("snapshots/domain/v2.0-accounts.0-64.kv", b"x" * 10),
                      ("snapshots/domain/v2.0-accounts.0-64.kv.torrent", b"t"),
                      ("snapshots/preverified.toml", b"'a' = 'b'\n"),
                      ("chaindata/mdbx.dat", b"y" * 100)]:
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)
    return root


with tempfile.TemporaryDirectory() as tmp:
    root = make_datadir(tmp)
    out = os.path.join(tmp, "listing.txt")
    subprocess.run([sys.executable, SCRIPT, root, out], check=True)
    lines = open(out, encoding="utf-8").read().splitlines()

    check("header carries the file and byte totals", lines[0] == f"# 4 files, 121 bytes in {root}", lines[0])
    check("second line names the columns", lines[1] == "# size_bytes\tmtime_utc\tpath", lines[1])

    rows = [line.split("\t") for line in lines[2:]]
    check("every file is listed", len(rows) == 4, rows)
    check("paths are relative to the datadir and slash-separated",
          [r[2] for r in rows] == ["chaindata/mdbx.dat",
                                   "snapshots/domain/v2.0-accounts.0-64.kv",
                                   "snapshots/domain/v2.0-accounts.0-64.kv.torrent",
                                   "snapshots/preverified.toml"], rows)
    check("size comes first", [r[0] for r in rows] == ["100", "10", "1", "10"], rows)
    check("mtime is UTC in ISO form",
          all(r[1].endswith("Z") and len(r[1]) == 20 for r in rows), rows)

with tempfile.TemporaryDirectory() as tmp:
    root = os.path.join(tmp, "empty")
    os.makedirs(root)
    out = os.path.join(tmp, "listing.txt")
    subprocess.run([sys.executable, SCRIPT, root, out], check=True)
    check("an empty datadir yields a header and nothing else",
          open(out, encoding="utf-8").read() == f"# 0 files, 0 bytes in {root}\n# size_bytes\tmtime_utc\tpath\n",
          open(out, encoding="utf-8").read())

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
