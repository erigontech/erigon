#!/usr/bin/env bash
# verify-modec-mini-soak.sh — stage 6 of the mode-C v4 history-family
# reconstitute plan (docs/plans/20260821-reconstitute-v4-history-family.md).
#
# Runs a fresh-sync soak alternating mode-C (regime 3) with mode-B
# (regime 2) so both the WRITE path (mode-C emits paired v4 .v/.ef) and
# the READ path (subsequent mode-C AND mode-B read the produced files
# back) get exercised. Each iter picks a random target in its regime's
# [lo, hi] band, so mode-C runs hit distinct straddler ranges across
# iters rather than repeating the same point.
#
# Pass criteria per cycle:
#   1. Fresh sync + Phase 3.5 succeed.
#   2. Every scenario_test row in soak.csv = "PASS".
#   3. Zero `[dbg-dual-root] compute mismatch` lines in erigon.log
#      (the compute mismatch is the wrong-root signal — its absence is
#      the load-bearing invariant this fix restores).
#
# Verify criterion across cycles: pass 3 consecutive cycles on
# independent fresh datadirs.
#
# Env overrides:
#   ITER          — iters per cycle (default 6). Cycled through
#                   REGIME_CYCLE (default 3,3,2) so ITER=6 gives
#                   [C, C, B, C, C, B] — four mode-Cs and two mode-Bs.
#   REGIME_CYCLE  — per-iter regime CSV. Default: 3,3,2. Set to 3,2 for
#                   1:1 alternation, or 3 for pure mode-C.
#   CYCLE_LABEL   — human tag inserted into RESULTS_DIR path.

set -u

RESULTS_DIR=${RESULTS_DIR:-/erigon/tmp/mode-c-verify}
mkdir -p "$RESULTS_DIR"

cycle=$(printf '%03d' "$(ls "$RESULTS_DIR" 2>/dev/null | grep -c '^cycle-')")
cycle=$((10#$cycle + 1))
cycle_str=$(printf '%03d' "$cycle")
label="${CYCLE_LABEL:-}"
if [[ -n "$label" ]]; then
    out="$RESULTS_DIR/cycle-${cycle_str}-${label}"
else
    out="$RESULTS_DIR/cycle-$cycle_str"
fi
mkdir -p "$out/snapshots-between-iters"

echo "[verify-modec] cycle $cycle → $out $(date -u +%FT%TZ)"

cd /erigon/mark/hive/clients/erigon/erigon

# Fresh datadir per cycle — the 3× verify criterion requires INDEPENDENT
# fresh syncs, not reusing the same on-disk state.
DATADIR=/erigon/tmp/erigon-hoodi-modec-verify.cycle-${cycle_str}
ITER=${ITER:-6}
REGIME_CYCLE=${REGIME_CYCLE:-3,3,2}

echo "[verify-modec] ITER=$ITER REGIME_CYCLE=$REGIME_CYCLE DATADIR=$DATADIR"

DATADIR="$DATADIR" \
  LOG_DIR="$out" \
  LOG="$out/erigon.log" \
  LAUNCH_CMD=scripts/erigon-launch-hoodi-soak.sh \
  ITER="$ITER" \
  REGIME_CYCLE="$REGIME_CYCLE" \
  RANDOMIZE_DEPTHS=true \
  SNAPSHOT_BETWEEN_ITERS_DIR="$out/snapshots-between-iters" \
  RANDOM_SEED="modec-verify-cycle-${cycle}-$(date -u +%s | head -c 10)" \
  SETHEAD_CALL_TIMEOUT_SEC=3600 \
  bash scripts/unwind-fresh-sync-then-soak.sh > "$out/soak.log" 2>&1
# Note: STRESS_MODE deliberately not set — verify needs the FULL recovery
# window (default 1800s scaled by depth via recovery_timeout_for_depth) so
# Caplin's post-unwind historical download completes before the next iter
# fires. STRESS_MODE=1 caps recovery at STRESS_INTER_ITER_SEC=90s which
# is fine for stress-testing setHead throughput but too short for a
# regime-3 unwind that needs Caplin to re-download 30k+ historical
# blocks (~15 min at hoodi cadence).
rc=$?
echo "$rc" > "$out/exit-code"

echo "[verify-modec] driver rc=$rc"

# Pass criterion 3: no [dbg-dual-root] mismatches.
mismatch_count=0
if [[ -f "$out/erigon.log" ]]; then
    mismatch_count=$(grep -c '\[dbg-dual-root\] compute mismatch' "$out/erigon.log" || true)
fi
echo "[verify-modec] dual-root-mismatches=$mismatch_count"

# Pass criterion 2: every scenario_test row's note starts with "ok"
# (may include annotations like "ok+errors=N" or "ok+inv_missing=..." —
# non-fatal notes still begin with ok). Rows starting with "fail:" or
# "abort:" are failures. CSV shape:
# iter,phase,target,pre_head,post_head,duration,errors,note
soak_csv=""
for candidate in "$out/soak.csv" /tmp/unwind-soak-*.csv; do
    if [[ -f "$candidate" ]]; then
        soak_csv="$candidate"
        break
    fi
done
csv_fails=0
csv_total=0
if [[ -n "$soak_csv" && -f "$soak_csv" ]]; then
    csv_total=$(grep -cv '^#' "$soak_csv" || true)
    # Field 8 = note. Success: starts with "ok". Failure: starts with "fail:" or "abort:".
    csv_fails=$(grep -v '^#' "$soak_csv" | awk -F, '$8 !~ /^ok/ && $8 != "" {c++} END{print c+0}')
    echo "[verify-modec] soak.csv=$soak_csv rows=$csv_total fails=$csv_fails"
else
    echo "[verify-modec] soak.csv not found — treating as failure"
    csv_fails=1
fi

echo "[verify-modec] snapshots preserved at $out/snapshots-between-iters/"

if [[ "$rc" -eq 0 && "$mismatch_count" -eq 0 && "$csv_fails" -eq 0 && "$csv_total" -gt 0 ]]; then
    echo "[verify-modec] PASS: cycle $cycle"
    exit 0
fi

echo "[verify-modec] FAIL: cycle $cycle (rc=$rc mismatches=$mismatch_count csv_fails=$csv_fails/$csv_total)"
exit 1
