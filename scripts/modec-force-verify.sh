#!/usr/bin/env bash
# modec-force-verify.sh — one-off targeted test for mode-C v4 emit.
#
# Fires ONE debug_setHead against a running erigon at a target block
# picked to guarantee a mid-step lastTxN inside a multi-step commitment
# file (regime 4 — the classification that yields actionRegenTruncate
# → mode-C v4 emit). Then greps the erigon log between RPC-fire and
# RPC-return for pass/fail markers:
#   - v4 emit markers → mode-C fired
#   - panic:          → MustSupport rejection (regression)
#   - Wrong trie root → commitment recompute mismatch
#   - snapshot step misalignment → the exact class this fix targets
#
# Does NOT wait for the post-unwind forward-exec recovery to complete
# — that's peer-bandwidth-dependent and orthogonal to what we're
# validating. Provider.Unwind's own logic (compute + trim + apply +
# regen + verify) either completed successfully (0 return + no panic
# in the RPC window) or didn't.
#
# Prerequisites: erigon running on $DATADIR, RPC reachable at $RPC,
# integration binary built at $INTEGRATION_BIN.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-modec-verify-preverified}"
RPC="${RPC:-http://127.0.0.1:19545}"
LOG="${LOG:-/tmp/erigon-hoodi-modec-verify-preverified.log}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
CHAIN="${CHAIN:-hoodi}"
SETHEAD_TIMEOUT_SEC="${SETHEAD_TIMEOUT_SEC:-1800}"

echo "===== modec-force-verify $(date -Is) ====="
echo "  DATADIR=$DATADIR RPC=$RPC"

# 1. Compute regime 4 target (multi-step commitment file straddler).
#    TARGET_BLOCK env-var override skips regime-depths — useful when
#    the datadir isn't in a state regime-depths can classify (e.g.
#    head pinned at changesetFloor after an in-flight unwind's
#    recovery hasn't caught up). Caller then picks a target block
#    known to be inside a multi-step commitment .kv file's step range.
if [[ -n "${TARGET_BLOCK:-}" ]]; then
    target="$TARGET_BLOCK"
    in_file="explicit"
    depth="—"
    echo "==> TARGET_BLOCK override: target=$target"
else
    echo "==> computing regime 4 target via integration regime-depths"
    regime_out=$("$INTEGRATION_BIN" regime-depths --datadir="$DATADIR" --chain="$CHAIN" 2>&1)
    regime4_line=$(echo "$regime_out" | grep '^regime=4 ' || true)
    if [[ -z "$regime4_line" ]]; then
        echo "FAIL: regime 4 not reachable (retire hasn't produced a multi-step commitment .kv, or datadir is fresh)"
        echo "$regime_out" | tail -10 >&2
        exit 2
    fi
    target=$(echo "$regime4_line" | sed -n 's/.*target=\([0-9]*\).*/\1/p')
    in_file=$(echo "$regime4_line" | sed -n 's/.*in=\([^ ]*\).*/\1/p')
    depth=$(echo "$regime4_line" | sed -n 's/.*depth=\([0-9]*\).*/\1/p')
    if [[ -z "$target" ]]; then
        echo "FAIL: couldn't parse target from regime-depths output"
        echo "$regime4_line" >&2
        exit 2
    fi
    echo "  regime4: target=$target depth=$depth in=$in_file"
fi

# 2. Sanity: verify RPC is up and head is well past target.
head_hex=$(curl -sS --max-time 10 -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    "$RPC" 2>&1 | sed -n 's/.*"result":"\([^"]*\)".*/\1/p' || true)
if [[ -z "$head_hex" ]]; then
    echo "FAIL: RPC not reachable at $RPC"
    exit 2
fi
head=$((head_hex))
echo "  current head=$head target=$target (unwinding $((head - target)) blocks)"
if (( target >= head )); then
    echo "FAIL: target $target >= head $head — regime-depths returned an above-head target"
    exit 2
fi

# 3. Snapshot log offset so we only grep post-RPC log content.
log_offset=$(stat -c%s "$LOG" 2>/dev/null || echo 0)
echo "  log offset before setHead: $log_offset"

# 4. Fire debug_setHead. Provider.Unwind runs synchronously within
#    this RPC — success/failure is signalled by the response.
target_hex=$(printf '0x%x' "$target")
echo "==> firing debug_setHead($target_hex)"
start_ts=$(date +%s)
resp=$(curl -sS --max-time "$SETHEAD_TIMEOUT_SEC" -X POST -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"debug_setHead\",\"params\":[\"$target_hex\"],\"id\":1}" \
    "$RPC" 2>&1)
rpc_rc=$?
elapsed=$(( $(date +%s) - start_ts ))
echo "  setHead returned in ${elapsed}s (curl rc=$rpc_rc)"
echo "  response: $resp"

# 5. Give the log a moment to flush the last lines.
sleep 5

# 6. Grep the log slice between offset and now for markers.
tail_bytes=$(($(stat -c%s "$LOG" 2>/dev/null || echo 0) - log_offset))
new_log=""
if (( tail_bytes > 0 )); then
    new_log=$(tail -c "$tail_bytes" "$LOG" 2>/dev/null || true)
fi

v4_emit=$(echo "$new_log" | grep -cE "mode-C commitment v4 emit|v4\\.0-[a-z]+\\.[0-9]+-[0-9]+\\.kv" || true)
panic_ct=$(echo "$new_log" | grep -cE "^panic:" || true)
wrong_root=$(echo "$new_log" | grep -c "Wrong trie root" || true)
misalign=$(echo "$new_log" | grep -c "snapshot step misalignment" || true)
provider_ok=$(echo "$new_log" | grep -cE "\\[storage\\] Provider\\.Unwind: commitment-anchor applied toBlock=$target" || true)

echo ""
echo "==> log analysis (bytes=$tail_bytes since offset $log_offset):"
echo "    v4 emit markers:            $v4_emit"
echo "    Provider.Unwind completed:  $provider_ok"
echo "    panics:                     $panic_ct"
echo "    wrong roots:                $wrong_root"
echo "    step misalignment errors:   $misalign"

# 7. Verdict.
if (( panic_ct > 0 )); then
    echo "FAIL: erigon panicked during setHead (MustSupport pivot regression?)"
    echo "$new_log" | grep -A 5 "^panic:" | head -20 >&2
    exit 1
fi
if (( wrong_root > 0 )); then
    echo "FAIL: wrong trie root during setHead recovery (commitment recompute mismatch)"
    exit 1
fi
if (( misalign > 0 )); then
    echo "FAIL: snapshot step misalignment (the exact class mode-C fix targets)"
    exit 1
fi
if [[ ! "$resp" =~ \"result\": ]]; then
    echo "FAIL: setHead RPC did not return a success result: $resp"
    exit 1
fi
if (( provider_ok == 0 )); then
    echo "WARN: no Provider.Unwind completion marker found for target $target"
    echo "      (RPC returned OK but log context may be truncated; not a hard fail)"
fi
if (( v4_emit == 0 )); then
    echo "WARN: no v4 emit markers — target may have been step-aligned"
    echo "      (Provider.Unwind succeeded without emitting v4 files; mode-C didn't fire)"
    echo "      To force mode-C, pick a target with target's lastTxN mid-step."
    exit 3
fi
echo ""
echo "PASS: mode-C v4 emit fired, Provider.Unwind completed, no panic / wrong-root / misalignment"
exit 0
