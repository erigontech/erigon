#!/usr/bin/env bash
# fork-test-suite.sh — single entry point for the fork test reshape
# (docs/plans/20260728-fork-test-reshape.md). Runs each tier in
# sequence, skipping tiers that require a live parent erigon when
# none is running.
#
# Usage:
#   scripts/fork-test-suite.sh              # unit + integration only (no live parent)
#   scripts/fork-test-suite.sh --with-e2e   # + Tier 3b, 3c (requires PARENT_RPC live)
#   scripts/fork-test-suite.sh --with-soak  # + Tier 4 (small ITER; requires live parent)
#
# Env (all optional):
#   PARENT_DATADIR  — datadir for the parent erigon (Tier 3+)
#   PARENT_RPC      — JSON-RPC of the parent (Tier 3+)
#   SOAK_ITER       — iterations for Tier 4 (default 3 for smoke, 50+ for real soak)

set -uo pipefail

WITH_E2E=false
WITH_SOAK=false
for arg in "$@"; do
    case "$arg" in
      --with-e2e)   WITH_E2E=true ;;
      --with-soak)  WITH_SOAK=true; WITH_E2E=true ;;
      --help|-h)
        sed -n '2,15p' "$0"
        exit 0
        ;;
      *) echo "unknown arg: $arg"; exit 2 ;;
    esac
done

PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
SOAK_ITER="${SOAK_ITER:-3}"

pass_tiers=()
fail_tiers=()
skip_tiers=()

stage() { echo; echo "########## $1 ##########"; }
report() {
    local tier="$1" status="$2"
    case "$status" in
      pass) pass_tiers+=("$tier") ;;
      fail) fail_tiers+=("$tier") ;;
      skip) skip_tiers+=("$tier") ;;
    esac
}

# Tier 1 + Tier 2: pure Go unit + integration tests. Always run — no
# live erigon required.
stage "Tier 1 + Tier 2 — Controller unit + integration tests"
if go test ./node/components/fork/ -count=1 -short 2>&1 | tail -5; then
    report "Tier 1+2 (fork Controller)" pass
else
    report "Tier 1+2 (fork Controller)" fail
fi

# Tier 1 also covers each captor's Restartable/Reconfigurable trio.
# Run those adjacent suites so a regression there doesn't hide behind
# green fork Controller tests.
stage "Tier 1 adjacent — component Restartable/Reconfigurable tests"
if go test ./node/components/sentry/ ./node/components/storage/ ./node/components/caplin/ ./node/components/downloader/ ./node/components/manifest_exchange/ ./txnprovider/txpool/ -count=1 -short 2>&1 | tail -10; then
    report "Tier 1 adjacent (component contracts)" pass
else
    report "Tier 1 adjacent (component contracts)" fail
fi

if ! $WITH_E2E; then
    stage "Skipping Tier 3+ (--with-e2e not set)"
    report "Tier 3b (fork-rpc-transition)" skip
    report "Tier 3c (fork-restart-transition)" skip
    report "Tier 4  (fork-soak)" skip
else
    # Tier 3 requires a live parent erigon. Probe first — if the parent
    # isn't up, mark E2E as skipped rather than launching one from here
    # (starting an erigon + waiting for sync is a multi-hour operation
    # the operator should drive explicitly via erigon-launch-hoodi-fork-parent.sh).
    stage "Probing live parent erigon at $PARENT_RPC"
    if ! curl -s --max-time 5 -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        "$PARENT_RPC" | grep -q '"result"'; then
        echo "  no parent erigon at $PARENT_RPC — skipping Tier 3+"
        echo "  launch one first via: scripts/erigon-launch-hoodi-fork-parent.sh"
        report "Tier 3b (fork-rpc-transition)" skip
        report "Tier 3c (fork-restart-transition)" skip
        report "Tier 4  (fork-soak)" skip
    else
        stage "Tier 3b — fork-rpc-transition.sh"
        if PARENT_DATADIR="$PARENT_DATADIR" PARENT_RPC="$PARENT_RPC" \
           scripts/fork-rpc-transition.sh; then
            report "Tier 3b (fork-rpc-transition)" pass
        else
            report "Tier 3b (fork-rpc-transition)" fail
        fi

        stage "Tier 3c — fork-restart-transition.sh"
        if PARENT_DATADIR="$PARENT_DATADIR" PARENT_RPC="$PARENT_RPC" \
           scripts/fork-restart-transition.sh; then
            report "Tier 3c (fork-restart-transition)" pass
        else
            report "Tier 3c (fork-restart-transition)" fail
        fi

        if $WITH_SOAK; then
            stage "Tier 4 — fork-soak-until-stopped.sh (ITER=$SOAK_ITER)"
            if PARENT_DATADIR="$PARENT_DATADIR" PARENT_RPC="$PARENT_RPC" \
               ITER="$SOAK_ITER" scripts/fork-soak-until-stopped.sh; then
                report "Tier 4 (fork-soak ITER=$SOAK_ITER)" pass
            else
                report "Tier 4 (fork-soak ITER=$SOAK_ITER)" fail
            fi
        else
            report "Tier 4  (fork-soak)" skip
        fi
    fi
fi

echo
echo "########## Summary ##########"
for t in "${pass_tiers[@]}"; do echo "PASS  $t"; done
for t in "${skip_tiers[@]}"; do echo "SKIP  $t"; done
for t in "${fail_tiers[@]}"; do echo "FAIL  $t"; done

if (( ${#fail_tiers[@]} > 0 )); then
    exit 1
fi
