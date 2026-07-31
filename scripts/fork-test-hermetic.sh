#!/usr/bin/env bash
# fork-test-hermetic.sh — hermetic wrapper around fork-test-suite.sh.
#
# Phase 1 fork tests depend on a healthy parent erigon. Running against
# an operator-managed parent inherits whatever state that parent has
# accumulated: divergent execution, stuck currentContext, stale head.
# The 2026-07-30 fork-test-suite.sh run failed F2/F3 (Tier 3b/3c) on a
# 13-day-old parent stuck in a nonce-too-high retry loop.
#
# This wrapper owns the parent lifecycle end-to-end:
#   Phase 0: kill any running erigon on parent ports.
#   Phase 1: wipe (or archive) the parent datadir.
#   Phase 2: launch parent from clean state via the standard launcher.
#   Phase 3: wait for parent to reach live tip (chain-cadence gate).
#   Phase 4: invoke fork-test-suite.sh --with-e2e.
#   Cleanup: trap-kill parent on any exit path.
#
# See docs/plans/20260731-fork-test-scope-and-leaks.md for the Phase 1
# scope this closes (leaks L1 + L4).
#
# Usage:
#   scripts/fork-test-hermetic.sh [--archive|--wipe] [--with-soak]
#
# Env (all optional):
#   BIN                     — erigon binary (default: ./build/bin/erigon)
#   INTEGRATION_BIN         — integration binary (default: ./build/bin/integration)
#   PARENT_LAUNCH_CMD       — launcher script (default: ./scripts/erigon-launch-hoodi-fork-parent.sh)
#   PARENT_DATADIR          — parent datadir (default: /erigon/tmp/erigon-hoodi-fork-parent)
#   PARENT_RPC              — parent JSON-RPC (default: http://127.0.0.1:19645)
#   PARENT_LOG              — parent log path (default: /tmp/erigon-hoodi-fork-parent.log)
#   SYNC_HARD_DEADLINE_SEC  — max wall-clock for parent sync (default: 14400 = 4h)
#   SYNC_STAGNATION_POLL_LIMIT — polls with no head advance before failing (default: 10)
#   ARCHIVE_DIR             — where to move existing datadir (default: <datadir>.archived-<stamp>)
#   PARENT_PORTS            — ports to free before launch (default: 19645 19651 11690 43469 31603 4850 4851 8590 6360 9360)
#
# Flags:
#   --archive               — move existing datadir aside instead of wiping (default)
#   --wipe                  — rm -rf the existing datadir
#   --with-soak             — passed through to fork-test-suite.sh (adds Tier 4)

set -uo pipefail

MODE="archive"
WITH_SOAK=""
for arg in "$@"; do
    case "$arg" in
      --archive) MODE="archive" ;;
      --wipe) MODE="wipe" ;;
      --with-soak) WITH_SOAK="--with-soak" ;;
      -h|--help) sed -n '2,35p' "$0"; exit 0 ;;
      *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

BIN="${BIN:-./build/bin/erigon}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
PARENT_LAUNCH_CMD="${PARENT_LAUNCH_CMD:-./scripts/erigon-launch-hoodi-fork-parent.sh}"
PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
PARENT_LOG="${PARENT_LOG:-/tmp/erigon-hoodi-fork-parent.log}"
SYNC_HARD_DEADLINE_SEC="${SYNC_HARD_DEADLINE_SEC:-14400}"
SYNC_STAGNATION_POLL_LIMIT="${SYNC_STAGNATION_POLL_LIMIT:-10}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_DIR="${ARCHIVE_DIR:-${PARENT_DATADIR}.archived-${STAMP}}"

# Ports the fork-parent launcher binds. Any of them held by a stray
# process must be freed before launch or the new parent silently loses
# the port race and we end up talking to the wrong process.
PARENT_PORTS="${PARENT_PORTS:-19645 19651 11690 43469 31603 4850 4851 8590 6360 9360}"

stage() {
    echo
    echo "===== $(date -u +%H:%M:%S) :: $1 ====="
}

# Track parent PID so we can SIGTERM on any exit path (success, failure,
# ctrl-C). Without this the nohup'd parent survives our exit and mutates
# the datadir we're supposed to be leaving in a clean state. Same trap
# pattern as unwind-fresh-sync-then-soak.sh (dda36cc1de).
ELPID=""
cleanup_parent() {
    if [[ -n "$ELPID" ]] && kill -0 "$ELPID" 2>/dev/null; then
        echo
        echo "===== $(date -u +%H:%M:%S) :: cleanup: kill parent pid=$ELPID ====="
        kill "$ELPID" 2>/dev/null || true
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            kill -0 "$ELPID" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$ELPID" 2>/dev/null; then
            echo "  SIGKILL parent pid=$ELPID"
            kill -9 "$ELPID" 2>/dev/null || true
        fi
    fi
}
trap cleanup_parent EXIT INT TERM

eth_block_number() {
    curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        "$PARENT_RPC" | jq -r '.result // "null"'
}

# progress_block reports the parent's real sync progress in decimal.
# During OtterSync's initial-sync phase, eth_blockNumber stays at 0
# because Caplin hasn't cut its first forkchoice yet — the exec stage
# is executing frozen blocks but the "head" hasn't been declared.
# eth_syncing.currentBlock reflects the actual exec progress and
# tracks all the way up to live tip. Once at tip, eth_syncing returns
# {result: false} and we fall back to eth_blockNumber.
# Returns "null" only if the RPC is unreachable.
progress_block() {
    local resp cur h_hex
    resp=$(curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
        "$PARENT_RPC" 2>/dev/null)
    if [[ -z "$resp" ]]; then
        echo "null"
        return
    fi
    # eth_syncing returns either false (at tip) or an object with currentBlock.
    cur=$(echo "$resp" | jq -r '.result.currentBlock // empty')
    if [[ -n "$cur" ]]; then
        printf '%d\n' "$cur"
        return
    fi
    # At tip — fall back to eth_blockNumber.
    h_hex=$(eth_block_number)
    if [[ "$h_hex" == "null" || -z "$h_hex" ]]; then
        echo "null"
        return
    fi
    printf '%d\n' "$h_hex"
}

hex_to_dec() { printf '%d\n' "$1"; }

stage "Phase 0: kill any running erigon on parent ports + datadir"

# Datadir-owning process: even if it holds different ports (dev-mode
# variant, port collision debug session), kill it or the wipe in Phase
# 1 will race an ongoing MDBX write.
existing_pids=$(pgrep -af "build/bin/erigon.*$(basename "$PARENT_DATADIR")" | awk '{print $1}' || true)
if [[ -n "$existing_pids" ]]; then
    echo "  killing datadir-owning pids: $existing_pids"
    echo "$existing_pids" | xargs -r kill 2>/dev/null || true
    sleep 5
    still=$(pgrep -af "build/bin/erigon.*$(basename "$PARENT_DATADIR")" | awk '{print $1}' || true)
    if [[ -n "$still" ]]; then
        echo "  SIGKILL stragglers: $still"
        echo "$still" | xargs -r kill -9 2>/dev/null || true
        sleep 3
    fi
else
    echo "  no datadir-owning erigon"
fi

# Any process holding one of the parent ports (leftover from a wedged
# session, a mis-configured launcher, or a colliding second erigon) must
# be freed. Otherwise the new parent's bind fails silently.
for port in $PARENT_PORTS; do
    holder_pid=$(ss -tlnp 2>/dev/null | awk -v p=":$port" '$4 ~ p {print $NF}' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2 || true)
    if [[ -n "$holder_pid" ]]; then
        cmd=$(ps -o comm= -p "$holder_pid" 2>/dev/null || echo "?")
        echo "  port $port held by pid=$holder_pid ($cmd) — killing"
        kill -9 "$holder_pid" 2>/dev/null || true
        sleep 1
    fi
done
echo "  ports clear"

stage "Phase 1: clean parent datadir ($MODE)"

if [[ -d "$PARENT_DATADIR" ]]; then
    case "$MODE" in
      archive)
        echo "  moving existing datadir to $ARCHIVE_DIR"
        mv "$PARENT_DATADIR" "$ARCHIVE_DIR"
        ;;
      wipe)
        echo "  rm -rf $PARENT_DATADIR"
        rm -rf "$PARENT_DATADIR"
        ;;
    esac
fi
mkdir -p "$PARENT_DATADIR"
echo "  fresh datadir at $PARENT_DATADIR"

stage "Phase 2: launch parent ($PARENT_LAUNCH_CMD)"

# The launcher exec's erigon so its own pid is erigon's pid. We nohup
# it into the background and capture the pid for the exit trap.
# shellcheck disable=SC2086
BIN="$BIN" INTEGRATION_BIN="$INTEGRATION_BIN" \
    DATADIR="$PARENT_DATADIR" LOG="$PARENT_LOG" \
    nohup $PARENT_LAUNCH_CMD </dev/null >/dev/null 2>&1 &
ELPID=$!
echo "  launched parent pid=$ELPID"
echo "  parent log: $PARENT_LOG"

stage "Phase 3: wait for parent to reach live tip (deadline ${SYNC_HARD_DEADLINE_SEC}s, stagnation stall $((SYNC_STAGNATION_POLL_LIMIT * 30))s)"

# Same chain-cadence gate as unwind-fresh-sync-then-soak.sh Phase 3:
#   a. wait for RPC alive (head > 0)
#   b. capture first non-zero head as bootstrap_floor (snapshot tip)
#   c. require head > bootstrap_floor AND delta in [1,5] for 2
#      consecutive 30s polls (post-snapshot chain-cadence progress)
# Stagnation gate: SYNC_STAGNATION_POLL_LIMIT polls with no advancement
# fails fast so we don't wait the full hard deadline on a wedged parent.
sync_end=$(( $(date +%s) + SYNC_HARD_DEADLINE_SEC ))
prev_head=0
bootstrap_floor=0
stable_count=0
stagnation_polls=0
prev_head_for_progress=0

while [[ $(date +%s) -lt $sync_end ]]; do
    head=$(progress_block)
    if [[ "$head" == "null" || -z "$head" ]]; then
        sleep 5
        continue
    fi

    if [[ $bootstrap_floor -eq 0 && $head -gt 0 ]]; then
        bootstrap_floor=$head
        echo "  $(date -u +%H:%M:%S) bootstrap_floor=$bootstrap_floor (snapshot tip; not yet live tip)"
    fi

    past_floor=$((head - bootstrap_floor))
    delta=$((head - prev_head))

    if [[ $head -gt $prev_head_for_progress ]]; then
        stagnation_polls=0
        prev_head_for_progress=$head
    else
        stagnation_polls=$((stagnation_polls + 1))
    fi

    echo "  $(date -u +%H:%M:%S) head=$head delta=$delta past_floor=$past_floor stagnation=${stagnation_polls}/${SYNC_STAGNATION_POLL_LIMIT}"

    # Live tip = head has advanced past bootstrap_floor AND delta is
    # chain-cadence (1-5 blocks per 30s poll) for 2 consecutive polls.
    if [[ $past_floor -gt 0 && $delta -ge 1 && $delta -le 5 ]]; then
        stable_count=$((stable_count + 1))
        if [[ $stable_count -ge 2 ]]; then
            echo "  live tip reached: head=$head delta=$delta past_floor=$past_floor (chain cadence for 2 polls)"
            break
        fi
    else
        stable_count=0
    fi

    if [[ $stagnation_polls -ge $SYNC_STAGNATION_POLL_LIMIT ]]; then
        echo "FAIL: parent sync stagnated — head=$head has not advanced for $((SYNC_STAGNATION_POLL_LIMIT * 30))s; parent likely wedged"
        exit 1
    fi

    prev_head=$head
    sleep 30
done

if [[ $(date +%s) -ge $sync_end ]]; then
    echo "FAIL: parent sync did not reach live tip within hard deadline ${SYNC_HARD_DEADLINE_SEC}s"
    exit 1
fi

stage "Phase 4: run fork-test-suite.sh --with-e2e $WITH_SOAK"

PARENT_DATADIR="$PARENT_DATADIR" PARENT_RPC="$PARENT_RPC" \
    ./scripts/fork-test-suite.sh --with-e2e $WITH_SOAK
suite_rc=$?

stage "Result"
echo "fork-test-hermetic: suite exit rc=$suite_rc"
echo "  parent datadir: $PARENT_DATADIR (owned by this run; will be torn down)"
echo "  parent log:     $PARENT_LOG"
if [[ "$MODE" == "archive" ]]; then
    echo "  archived prior: $ARCHIVE_DIR (if it existed)"
fi
exit "$suite_rc"
