#!/usr/bin/env bash
# fork-from-twice-diff.sh — determinism check for `snapshots fork-from`.
#
# Runs fork-from TWICE against the same parent + same cut params, into
# two fresh datadirs, then diffs the outputs. Two independent operators
# forking a chain from the same spec must produce byte-identical
# pre-cut file sets and byte-identical derived chain.json — otherwise
# the swarm cannot converge on canonical after they publish V2 manifests.
#
# Doesn't spin up a parent itself — expects one already running on
# --parent-rpc. Pair with scripts/erigon-launch-hoodi-fork-parent.sh
# and scripts/fork-from-fresh.sh (which does one fork-from);
# fork-from-twice-diff.sh is the same shape but two runs + diff.
#
# On success: prints "PASS: byte-identical output". Non-zero on any
# diff.
#
# Usage:
#   PARENT_RPC=http://127.0.0.1:19645 \
#   PARENT_DATADIR=/erigon/tmp/erigon-hoodi-fork-parent \
#   CUT_BLOCK=3287776 \
#   FORK_CHAIN_NAME=hoodi-fork-determinism-test \
#   scripts/fork-from-twice-diff.sh

set -u

PARENT_RPC="${PARENT_RPC:?PARENT_RPC required — the parent erigon's --http.port RPC}"
PARENT_DATADIR="${PARENT_DATADIR:?PARENT_DATADIR required — the parent's on-disk datadir}"
CUT_BLOCK="${CUT_BLOCK:?CUT_BLOCK required — the EL block to fork at}"
FORK_CHAIN_NAME="${FORK_CHAIN_NAME:-hoodi-fork-determinism-$(date +%s)}"
FORK_ROOT="${FORK_ROOT:-/erigon/tmp/erigon-hoodi-fork-determinism}"
SNAPSHOTS_BIN="${SNAPSHOTS_BIN:-./build/bin/snapshots}"

DIR_A="$FORK_ROOT-A"
DIR_B="$FORK_ROOT-B"

stage() { echo; echo "===== $(date -u +%H:%M:%S) :: $1 ====="; }

# fork-from refuses to clobber a non-empty existing dir — wipe both first.
stage "Phase 0: wipe target datadirs"
rm -rf "$DIR_A" "$DIR_B"
mkdir -p "$(dirname "$DIR_A")"

# Freeze the parent-cut ONCE and reuse it for both runs so we're
# testing fork-from's determinism, not the parent's tip movement
# between runs.
CUT_FILE="$FORK_ROOT-cut.json"
rm -f "$CUT_FILE"

stage "Phase 1: capture parent-cut.json (from --parent-rpc, saved for reuse)"
"$SNAPSHOTS_BIN" fork-from \
    --parent-rpc "$PARENT_RPC" \
    --parent-chain hoodi \
    --parent-datadir "$PARENT_DATADIR" \
    --cut-block "$CUT_BLOCK" \
    --new-chain-name "$FORK_CHAIN_NAME" \
    --new-datadir "$DIR_A" \
    --save-parent-cut "$CUT_FILE" >/tmp/fork-from-A.log 2>&1 || {
    echo "FAIL: first fork-from (run A) returned non-zero"
    tail -20 /tmp/fork-from-A.log
    exit 1
}
echo "  run A wrote $DIR_A"
echo "  cut file: $CUT_FILE"

stage "Phase 2: second fork-from (frozen-file mode) into DIR_B"
"$SNAPSHOTS_BIN" fork-from \
    --parent-cut-file "$CUT_FILE" \
    --parent-datadir "$PARENT_DATADIR" \
    --new-chain-name "$FORK_CHAIN_NAME" \
    --new-datadir "$DIR_B" >/tmp/fork-from-B.log 2>&1 || {
    echo "FAIL: second fork-from (run B) returned non-zero"
    tail -20 /tmp/fork-from-B.log
    exit 1
}
echo "  run B wrote $DIR_B"

stage "Phase 3: diff outputs"
FAIL=0

# chain.json content
if ! diff -q "$DIR_A/chain.json" "$DIR_B/chain.json"; then
    echo "FAIL: chain.json differs"
    diff -u "$DIR_A/chain.json" "$DIR_B/chain.json" | head -30
    FAIL=1
else
    echo "  chain.json byte-identical"
fi

# Snapshot file set: names, sizes, and content.
list_snap() {
    find "$1/snapshots" -maxdepth 3 -type f -not -name '*.torrent' 2>/dev/null \
        | sed "s|^$1/||" | sort
}
LIST_A=$(list_snap "$DIR_A")
LIST_B=$(list_snap "$DIR_B")

if [[ "$LIST_A" != "$LIST_B" ]]; then
    echo "FAIL: snapshot file set differs"
    diff <(echo "$LIST_A") <(echo "$LIST_B") | head -30
    FAIL=1
else
    n=$(echo "$LIST_A" | wc -l)
    echo "  snapshot file set matches ($n files, ignoring .torrent sidecars)"
fi

# Byte-identity of every file that appears in both. Skips torrent
# sidecars (their infohash carries deterministic bytes but the file
# structure includes creation metadata that legitimately differs).
mismatches=0
while IFS= read -r rel; do
    if [[ -f "$DIR_A/$rel" && -f "$DIR_B/$rel" ]]; then
        if ! cmp -s "$DIR_A/$rel" "$DIR_B/$rel"; then
            echo "  DIFF: $rel"
            mismatches=$((mismatches + 1))
        fi
    fi
done <<<"$LIST_A"

if [[ "$mismatches" -gt 0 ]]; then
    echo "FAIL: $mismatches file(s) differ byte-for-byte"
    FAIL=1
else
    echo "  every listed file byte-identical"
fi

if [[ $FAIL -eq 0 ]]; then
    stage "Result: PASS: byte-identical output"
    exit 0
else
    stage "Result: FAIL"
    exit 1
fi
