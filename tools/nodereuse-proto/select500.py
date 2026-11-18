#!/usr/bin/env python3
"""Node-reuse prototype for the hive consume-rlp simulator, part 1: select fixtures.

Reads the fixture-set meta index, picks `count` (file, fork) combos spread over
the consume-rlp selection (same sim-limit the hive-eest rlp shard uses), extracts
the fixture files from the release tarball in one streaming pass and writes
/tmp/rlp-proto/manifest.json with per-test genesis/blocks/expected hashes.

Run:  python3 select500.py <meta-index.json> <tarball> [count]
"""
import io
import json
import re
import sys
import tarfile
from pathlib import Path

OUT = Path("/tmp/rlp-proto")
SIM_LIMIT_TEST = re.compile(r"(eip2930_access_list|eip4844_blobs)")


def extract_stream(tarball: str, wanted: set) -> dict:
    """One sequential pass over the tarball; gzip cannot seek, so per-member
    extractfile calls would restart decompression for every file."""
    found = {}
    with tarfile.open(tarball, "r:gz") as tf:
        for member in tf:
            if member.name in wanted:
                f = tf.extractfile(member)
                if f is not None:
                    found[member.name] = json.load(io.TextIOWrapper(f, "utf-8"))
    return found


def main() -> None:
    meta_path, tarball = sys.argv[1], sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    idx = json.load(open(meta_path))
    sel = [
        t
        for t in idx["test_cases"]
        if SIM_LIMIT_TEST.search(t["id"]) and t["format"] == "blockchain_test"
    ]
    # Dedupe to (fixture file, fork) pairs: one file holds several tests that
    # differ only by variant, and the rlp run imports the file per fork.
    combos = {}
    for t in sel:
        fork = re.search(r"fork_([A-Za-z0-9]+)", t["id"]).group(1)
        combos.setdefault((t["json_path"], fork), t)
    picked = list(combos.values())
    step = max(1, len(picked) // count)
    picked = picked[::step][:count]

    OUT.mkdir(parents=True, exist_ok=True)
    by_path = {}
    for t in picked:
        by_path.setdefault(t["json_path"], []).append(t)
    wanted = {"fixtures/" + p for p in by_path}
    raw = extract_stream(tarball, wanted)
    found = {k[len("fixtures/"):]: v for k, v in raw.items()}
    missing = wanted - {"fixtures/" + p for p in found}
    print(
        f"selected {len(picked)} (file, fork) pairs from {len(by_path)} files,"
        f" extracted {len(found)} files, missing {len(missing)} files"
    )

    manifest = []
    for i, (path, ts) in enumerate(sorted(by_path.items())):
        for t in ts:
            try:
                fixture = found[path][t["id"]]
            except TypeError:
                print(f"DEBUG path={path!r} type(found[path])={type(found[path])} type(t)={type(t)} t={t!r}"[:400])
                raise
            if not isinstance(fixture, dict) or "genesisBlockHeader" not in fixture:
                # Some files are a single test object instead of an id-keyed map.
                fixture = found[path]
            fork = fixture.get("network") or re.search(
                r"fork_([A-Za-z0-9]+)", t["id"]
            ).group(1)
            test_dir = OUT / f"test{i:04d}_{fork}"
            blocks_dir = test_dir / "blocks"
            blocks_dir.mkdir(parents=True, exist_ok=True)
            genesis = dict(fixture["genesisBlockHeader"])
            genesis["alloc"] = {
                k.replace("0x", ""): v for k, v in fixture["pre"].items()
            }
            for b, block in enumerate(fixture["blocks"], start=1):
                (blocks_dir / f"{b:04d}.rlp").write_bytes(
                    bytes.fromhex(block["rlp"][2:])
                )
            (test_dir / "genesis-input.json").write_text(json.dumps(genesis))
            manifest.append(
                {
                    "dir": str(test_dir),
                    "fork": fork,
                    "last_hash": fixture.get("lastblockhash"),
                    "id": t["id"],
                }
            )
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(f"manifest: {OUT}/manifest.json ({len(manifest)} tests)")


if __name__ == "__main__":
    main()
