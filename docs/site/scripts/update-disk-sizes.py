#!/usr/bin/env python3
"""Update docs/site/src/data/disk-sizes.json from CI artifact data.

Called by the update-disk-sizes GH Actions workflow after a successful
sync-from-scratch run. Reads disk-usage-<chain>-<mode>.txt files produced
by the sync workflows and updates the corresponding entries in the JSON.

Usage:
    python3 update-disk-sizes.py <artifacts-dir> <json-path> <prune-mode>

Arguments:
    artifacts-dir  Directory containing disk-usage-<chain>-<mode>.txt files
    json-path      Path to disk-sizes.json
    prune-mode     'full' or 'minimal'
"""
import json
import sys
from datetime import date
from pathlib import Path


def format_bytes(b: int) -> str:
    """Format bytes to human-readable SI units (powers of 1000)."""
    if b >= 1_000_000_000_000:
        return f"{b / 1_000_000_000_000:.2f} TB"
    if b >= 1_000_000_000:
        return f"{b / 1_000_000_000:.2f} GB"
    mb = round(b / 1_000_000)
    if mb >= 1000:
        return f"{b / 1_000_000_000:.2f} GB"
    return f"{mb} MB"


def main() -> None:
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <artifacts-dir> <json-path> <prune-mode>")
        sys.exit(1)

    artifacts_dir = Path(sys.argv[1])
    json_path = Path(sys.argv[2])
    prune_mode = sys.argv[3]

    if prune_mode not in ("full", "minimal"):
        print(f"Error: prune-mode must be 'full' or 'minimal', got {prune_mode!r}")
        sys.exit(1)

    today = date.today().isoformat()

    with open(json_path) as f:
        data = json.load(f)

    updated = False
    matched = False
    for artifact_file in sorted(artifacts_dir.glob(f"disk-usage-*-{prune_mode}.txt")):
        matched = True
        # filename: disk-usage-<chain>-<mode>.txt  e.g. disk-usage-mainnet-full.txt
        stem = artifact_file.stem                        # disk-usage-mainnet-full
        inner = stem[len("disk-usage-"):]                # mainnet-full
        chain = inner[:-(len(prune_mode) + 1)]           # mainnet

        if chain not in data["networks"]:
            print(f"Skipping unknown chain: {chain!r}")
            continue

        raw = artifact_file.read_text().strip()
        try:
            bytes_val = int(raw)
        except ValueError:
            print(f"Could not parse bytes from {artifact_file}: {raw!r}")
            continue

        data["networks"][chain][prune_mode] = {
            "bytes": bytes_val,
            "display": format_bytes(bytes_val),
            "measured_at": today,
            "source": "ci",
        }
        print(f"Updated {chain}/{prune_mode}: {format_bytes(bytes_val)} ({bytes_val:,} bytes)")
        updated = True

    if not updated:
        if matched:
            print("Error: matching artifact files were found, but none could be applied")
        else:
            print("Error: no matching artifact files found")
        sys.exit(1)

    data["ci_last_updated"] = today
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"Wrote {json_path}")


if __name__ == "__main__":
    main()
