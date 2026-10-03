#!/usr/bin/env python3
"""Check a runner's QA reference data without changing it."""

import argparse
import configparser
import os
from pathlib import Path
import re
import sys


def check_reference(reference):
    for directory in (reference / "datadir", reference / "datadir/snapshots"):
        if not directory.is_dir():
            raise ValueError(f"missing directory: {directory}")
    database = reference / "datadir/chaindata/mdbx.dat"
    with database.open("rb") as source:
        if not source.read(1):
            raise ValueError(f"empty database: {database}")
    metadata = configparser.ConfigParser(interpolation=None)
    with (reference / "production.ini").open(encoding="utf-8") as source:
        metadata.read_file(source)
    if not metadata.get("production", "erigon_repo_commit", fallback="").strip():
        raise ValueError(f"missing production.erigon_repo_commit in {reference / 'production.ini'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--chain", choices=("mainnet", "gnosis"), required=True)
    parser.add_argument("--versions-dir", type=Path,
                        default=os.environ.get("ERIGON_QA_VERSIONS_DIR", "/opt/erigon-versions"))
    parser.add_argument("--github-env", type=Path)
    args = parser.parse_args()

    name = "gnosis-reference-version" if args.chain == "gnosis" else "reference-version"
    if args.branch.startswith("release/"):
        if not re.fullmatch(r"release/[0-9]+\.[0-9]+", args.branch):
            parser.error("release branches must match release/N.N")
        name += "-" + args.branch.removeprefix("release/")
    reference = args.versions_dir / name
    datadir = reference / "datadir"
    try:
        check_reference(reference)
    except (OSError, ValueError, configparser.Error) as error:
        print(f"::error::QA reference data for {args.branch} ({args.chain}) is not ready: {error}. "
              "Provision the reference on this runner before running QA. "
              "See .github/workflows/readme.md#release-qa-reference-data.", file=sys.stderr)
        return 1

    if args.github_env:
        with args.github_env.open("a", encoding="utf-8") as env:
            env.write(f"ERIGON_REFERENCE_DIR={reference}\nERIGON_REFERENCE_DATA_DIR={datadir}\n")
    print(f"QA reference files ready: {reference}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
