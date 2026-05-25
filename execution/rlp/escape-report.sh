#!/usr/bin/env bash
# escape-report.sh — capture escape analysis for the RLP layer and the
# top consumers that drive its hot-path allocations.
#
# Outputs three files in the current directory:
#   escape-rlp.txt              — escapes inside execution/rlp
#   escape-types-header.txt     — escapes inside execution/types involving Header
#   escape-foreachheader.txt    — escapes inside the ForEachHeader callsite
#
# Run from the repo root.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

echo "==> Building with -gcflags='-m=2' for execution/rlp..."
go build -gcflags='-m=2' ./execution/rlp/... 2>&1 \
    | grep -E "escapes to heap|moved to heap" \
    > execution/rlp/escape-rlp.txt || true

echo "==> Building with -gcflags='-m=2' for execution/types..."
go build -gcflags='-m=2' ./execution/types/... 2>&1 \
    | grep -E "escapes to heap|moved to heap" \
    | grep -i "header\|hash\|decoderlp\|encoderlp" \
    > execution/rlp/escape-types-header.txt || true

echo "==> Building with -gcflags='-m=2' for db/snapshotsync/freezeblocks..."
go build -gcflags='-m=2' ./db/snapshotsync/freezeblocks/... 2>&1 \
    | grep -E "escapes to heap|moved to heap" \
    | grep -i "foreachheader\|header" \
    > execution/rlp/escape-foreachheader.txt || true

echo "==> Done."
echo
wc -l execution/rlp/escape-rlp.txt execution/rlp/escape-types-header.txt execution/rlp/escape-foreachheader.txt
