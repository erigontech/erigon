#!/usr/bin/env bash
# fork-test-teardown-parent.sh — SIGTERM the parent erigon spawned
# by fork-test-bootstrap-parent.sh and (optionally) wipe its datadir.
#
# WIPE_DATADIR=false (default) leaves the datadir in place so
# subsequent runs can reuse it. WIPE_DATADIR=true removes it —
# useful for CI where the disk should return to empty after each run.

set -uo pipefail

DATADIR="${DATADIR:-/erigon/tmp/erigon-fork-test-parent}"
PIDFILE="${PIDFILE:-/tmp/erigon-fork-test-parent.pid}"
LOG="${LOG:-/tmp/erigon-fork-test-parent.log}"
WIPE_DATADIR="${WIPE_DATADIR:-false}"
SHUTDOWN_TIMEOUT_SEC="${SHUTDOWN_TIMEOUT_SEC:-60}"

stage() { echo "[$(date -u +%H:%M:%S)] teardown: $1"; }

if [[ -f "$PIDFILE" ]]; then
    pid=$(cat "$PIDFILE")
    if kill -0 "$pid" 2>/dev/null; then
        stage "sending SIGTERM to erigon pid=$pid"
        kill "$pid" 2>/dev/null || true
        for i in $(seq 1 "$SHUTDOWN_TIMEOUT_SEC"); do
            if ! kill -0 "$pid" 2>/dev/null; then
                stage "erigon pid=$pid exited after ${i}s"
                break
            fi
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            stage "erigon pid=$pid did not exit within ${SHUTDOWN_TIMEOUT_SEC}s — SIGKILL"
            kill -9 "$pid" 2>/dev/null || true
        fi
    else
        stage "no live erigon at pid=$pid"
    fi
    rm -f "$PIDFILE"
else
    # No PIDFILE — fall back to pgrep by datadir in case the pidfile was lost.
    if pid=$(pgrep -f "erigon.*--datadir=?$DATADIR" | head -1); [[ -n "$pid" ]]; then
        stage "no PIDFILE — SIGTERM discovered erigon pid=$pid at $DATADIR"
        kill "$pid" 2>/dev/null || true
        sleep 3
    else
        stage "no PIDFILE and no erigon holding $DATADIR — nothing to stop"
    fi
fi

if [[ "$WIPE_DATADIR" == "true" && -d "$DATADIR" ]]; then
    stage "wiping datadir $DATADIR"
    rm -rf "$DATADIR"
fi

stage "log preserved at $LOG"
