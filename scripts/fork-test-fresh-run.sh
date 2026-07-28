#!/usr/bin/env bash
# fork-test-fresh-run.sh — the standalone, single-command,
# repeatable fork test entry point.
#
# One command:
#   scripts/fork-test-fresh-run.sh
#
# What it does:
#   1. Builds erigon + integration if the binaries are older than
#      the current source tree (--skip-build to bypass).
#   2. Bootstraps a fresh hoodi parent erigon
#      (scripts/fork-test-bootstrap-parent.sh): wipes DATADIR,
#      launches, waits for head to reach HEAD_MIN (default 2000).
#      Fresh hoodi sync can take up to WAIT_TIMEOUT_SEC (default 4h).
#   3. Runs the fork test suite
#      (scripts/fork-test-suite.sh --with-e2e).
#   4. Optionally runs Tier 4 soak (--with-soak, SOAK_ITER iterations).
#   5. Tears down the parent
#      (scripts/fork-test-teardown-parent.sh).
#      WIPE_DATADIR=true clears the datadir at the end (useful for CI).
#
# Exit code = 0 iff every stage passed.

set -uo pipefail

WITH_SOAK=false
SKIP_BUILD=false
for arg in "$@"; do
    case "$arg" in
      --with-soak)  WITH_SOAK=true ;;
      --skip-build) SKIP_BUILD=true ;;
      --help|-h)
        sed -n '2,25p' "$0"
        exit 0
        ;;
      *) echo "unknown arg: $arg"; exit 2 ;;
    esac
done

# Export for child scripts. Defaults chosen to match
# erigon-launch-hoodi-fork-parent.sh port layout so nothing conflicts
# with the standard soak launcher.
export DATADIR="${DATADIR:-/erigon/tmp/erigon-fork-test-parent}"
export RPC="${RPC:-http://127.0.0.1:19645}"
export HEAD_MIN="${HEAD_MIN:-2000}"
export WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-14400}"
export WIPE_DATADIR="${WIPE_DATADIR:-false}"

# fork-test-suite.sh reads PARENT_DATADIR + PARENT_RPC — remap so the
# consumer of this script doesn't need to know the internal naming.
export PARENT_DATADIR="$DATADIR"
export PARENT_RPC="$RPC"

stage() { echo; echo "########## $(date -u +%H:%M:%S) :: $1 ##########"; }

if ! $SKIP_BUILD; then
    stage "Build erigon + integration"
    make erigon integration
fi

stage "Bootstrap fresh parent erigon"
if ! scripts/fork-test-bootstrap-parent.sh; then
    echo "FAIL: parent bootstrap did not reach HEAD_MIN=$HEAD_MIN within ${WAIT_TIMEOUT_SEC}s"
    scripts/fork-test-teardown-parent.sh || true
    exit 1
fi

suite_args=(--with-e2e)
if $WITH_SOAK; then
    suite_args=(--with-soak)
fi

stage "Run fork test suite ${suite_args[*]}"
suite_rc=0
scripts/fork-test-suite.sh "${suite_args[@]}" || suite_rc=$?

stage "Teardown parent"
scripts/fork-test-teardown-parent.sh || true

if (( suite_rc != 0 )); then
    echo
    echo "FAIL: fork test suite exited $suite_rc"
    exit "$suite_rc"
fi

echo
echo "PASS: fresh-run complete (bootstrap + suite + teardown)"
