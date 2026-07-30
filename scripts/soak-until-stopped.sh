#!/usr/bin/env bash
# soak-until-stopped.sh — loop unwind-fresh-sync-then-soak.sh with a fresh
# random seed each cycle. Every cycle is a clean fresh-sync + N-iter
# randomized-depth soak. Runs until either interrupted OR a cycle fails
# (default), leaving its datadir intact for post-mortem.
#
# Design goals:
#   * Stop-on-fail by default — the failing cycle's datadir stays on disk
#     for inspection (integration commands, log walk, snapshot diff). Set
#     KEEP_GOING=true to override and accumulate diverse-seed evidence
#     across many cycles (the original mode; only useful when you already
#     know the fix stack is stable and you want a green-cadence sample).
#   * Each cycle's seed is captured up-front and printed to $OUT_DIR/index.tsv
#     so a green-run cadence is auditable later without opening every log.
#   * Cycles use their own datadir path so a still-running iteration can be
#     inspected after the driver has moved on to the next cycle.
#   * Each cycle's erigon log lives at $OUT_DIR/erigon.<cycle-tag>.log so
#     abort-with-null-head cases (F3 class) can be characterised without
#     racing the next cycle's fresh-sync overwrite of the shared
#     /tmp/erigon-hoodi.log.
#
# Env knobs (all optional):
#   BIN                 — erigon binary path (default: ./build/bin/erigon)
#   DATADIR_ROOT        — parent dir for per-cycle datadirs
#                         (default: /erigon/tmp/erigon-hoodi-continuous-soak)
#   OUT_DIR             — per-cycle log/CSV dir (default: /tmp/continuous-soak)
#   ITER                — iters per cycle (default: 50)
#   SETHEAD_CALL_TIMEOUT_SEC — passed through to unwind-soak.sh
#   MAX_CYCLES          — stop after N cycles (default: unlimited)
#   SLEEP_BETWEEN_SEC   — pause between cycles (default: 60)
#   KEEP_GOING          — "true" to loop past failed cycles (default: false)

set -u

BIN="${BIN:-./build/bin/erigon}"
DATADIR_ROOT="${DATADIR_ROOT:-/erigon/tmp/erigon-hoodi-continuous-soak}"
OUT_DIR="${OUT_DIR:-/tmp/continuous-soak}"
ITER="${ITER:-50}"
MAX_CYCLES="${MAX_CYCLES:-0}"
SLEEP_BETWEEN_SEC="${SLEEP_BETWEEN_SEC:-60}"
KEEP_GOING="${KEEP_GOING:-false}"
FRESH_SYNC_SCRIPT="${FRESH_SYNC_SCRIPT:-scripts/unwind-fresh-sync-then-soak.sh}"
LAUNCH_CMD="${LAUNCH_CMD:-scripts/erigon-launch-hoodi-soak.sh}"

mkdir -p "$OUT_DIR"
INDEX="$OUT_DIR/index.tsv"
if [[ ! -f "$INDEX" ]]; then
    echo -e "cycle\tstart\tend\trc\tseed\tdatadir\tlog\tcsv\terigon_log" >"$INDEX"
fi

cycle=0
while :; do
    cycle=$((cycle + 1))
    if [[ "$MAX_CYCLES" -gt 0 && "$cycle" -gt "$MAX_CYCLES" ]]; then
        echo "soak-until-stopped: reached MAX_CYCLES=$MAX_CYCLES; exiting"
        break
    fi

    SEED="$(openssl rand -hex 16)"
    STAMP="$(date +%Y%m%dT%H%M%S)"
    CYCLE_TAG="cycle$(printf '%04d' "$cycle")-$STAMP"
    CYCLE_DD="$DATADIR_ROOT.$CYCLE_TAG"
    CYCLE_LOG="$OUT_DIR/soak.$CYCLE_TAG.log"
    CYCLE_CSV="$OUT_DIR/soak.$CYCLE_TAG.csv"
    CYCLE_ERIGON_LOG="$OUT_DIR/erigon.$CYCLE_TAG.log"

    START="$(date -Iseconds)"
    echo "=== cycle $cycle starting at $START seed=$SEED datadir=$CYCLE_DD ==="

    BIN="$BIN" \
    DATADIR="$CYCLE_DD" \
    LAUNCH_CMD="$LAUNCH_CMD" \
    LOG="$CYCLE_ERIGON_LOG" \
    ITER="$ITER" \
    RANDOMIZE_DEPTHS=true \
    RANDOM_SEED="$SEED" \
    SETHEAD_CALL_TIMEOUT_SEC="${SETHEAD_CALL_TIMEOUT_SEC:-1800}" \
    "$FRESH_SYNC_SCRIPT" \
        >"$CYCLE_LOG" 2>&1
    rc=$?

    # unwind-fresh-sync-then-soak.sh writes its CSV under $DATADIR/…; copy
    # it into OUT_DIR so a single dir carries all cycle evidence.
    ORIG_CSV=$(find "$CYCLE_DD" -maxdepth 2 -name "unwind-*.csv" 2>/dev/null | head -1 || true)
    if [[ -n "$ORIG_CSV" && -f "$ORIG_CSV" ]]; then
        cp "$ORIG_CSV" "$CYCLE_CSV" 2>/dev/null || true
    fi

    END="$(date -Iseconds)"
    printf "%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n" \
        "$cycle" "$START" "$END" "$rc" "$SEED" "$CYCLE_DD" "$CYCLE_LOG" "$CYCLE_CSV" "$CYCLE_ERIGON_LOG" \
        >>"$INDEX"
    echo "=== cycle $cycle ended at $END rc=$rc ==="

    if [[ "$rc" -ne 0 && "$KEEP_GOING" != "true" ]]; then
        echo "soak-until-stopped: cycle $cycle failed (rc=$rc); stopping so the datadir + logs are preserved for post-mortem."
        echo "  datadir:    $CYCLE_DD"
        echo "  driver log: $CYCLE_LOG"
        echo "  erigon log: $CYCLE_ERIGON_LOG"
        echo "  csv:        $CYCLE_CSV"
        echo "  (set KEEP_GOING=true to skip past failures on a future run)"
        exit "$rc"
    fi

    if [[ "$SLEEP_BETWEEN_SEC" -gt 0 ]]; then
        echo "sleeping ${SLEEP_BETWEEN_SEC}s before next cycle…"
        sleep "$SLEEP_BETWEEN_SEC"
    fi
done
