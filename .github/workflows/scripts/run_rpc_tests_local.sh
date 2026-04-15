#!/bin/bash
# Run QA RPC integration tests against a synced Erigon datadir.
# Backs up chaindata, runs migrations, starts rpcdaemon, runs the test suite,
# then restores chaindata on exit. Used by both CI and local developers.
#
# Usage:
#   run_rpc_tests_local.sh [--datadir DIR] [--chain CHAIN] [--workspace DIR] [--result-dir DIR] [--backup-dir DIR] [--skip-backup]
#
# Options:
#   --datadir DIR      Path to synced Erigon datadir (or set ERIGON_REFERENCE_DATA_DIR)
#   --chain CHAIN      mainnet (default) or gnosis
#   --workspace DIR    Directory for rpc-tests clone (default: auto temp dir, deleted on exit)
#   --result-dir DIR   Directory to save test results (default: auto temp dir, kept on failure)
#   --backup-dir DIR   Directory for chaindata backup (default: auto temp dir, deleted on exit)
#   --skip-backup      Skip chaindata backup/restore (use when datadir is ephemeral/disposable)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BUILD_BIN="$REPO_ROOT/build/bin"

CHAIN=mainnet
DATADIR="${ERIGON_REFERENCE_DATA_DIR:-}"
WORKSPACE=""
RESULT_DIR=""
BACKUP_DIR_OPT=""
SKIP_BACKUP=false

usage() {
  echo "Usage: $0 [--datadir DIR] [--chain CHAIN] [--workspace DIR] [--result-dir DIR] [--backup-dir DIR] [--skip-backup]"
  echo
  echo "  --datadir DIR      Path to synced Erigon datadir (or set ERIGON_REFERENCE_DATA_DIR)"
  echo "  --chain CHAIN      mainnet (default) or gnosis"
  echo "  --workspace DIR    Directory for rpc-tests clone (default: auto temp dir)"
  echo "  --result-dir DIR   Directory to save test results (default: auto temp dir)"
  echo "  --backup-dir DIR   Directory for chaindata backup (default: auto temp dir)"
  echo "  --skip-backup      Skip chaindata backup/restore (use when datadir is ephemeral/disposable)"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --datadir)     DATADIR="$2";      shift 2 ;;
    --chain)       CHAIN="$2";        shift 2 ;;
    --workspace)   WORKSPACE="$2";    shift 2 ;;
    --result-dir)  RESULT_DIR="$2";   shift 2 ;;
    --backup-dir)  BACKUP_DIR_OPT="$2"; shift 2 ;;
    --skip-backup) SKIP_BACKUP=true;  shift ;;
    *) echo "Unknown argument: $1"; usage ;;
  esac
done

if [ -z "$DATADIR" ]; then
  echo "Error: --datadir is required (or set ERIGON_REFERENCE_DATA_DIR)"
  echo
  usage
fi

if [ ! -d "$DATADIR" ]; then
  echo "Error: datadir does not exist: $DATADIR"
  exit 1
fi

for bin in rpcdaemon integration; do
  if [ ! -f "$BUILD_BIN/$bin" ]; then
    echo "Error: $BUILD_BIN/$bin not found — run 'make rpcdaemon integration' first"
    exit 1
  fi
done

case "$CHAIN" in
  mainnet) TEST_SCRIPT="$SCRIPT_DIR/run_rpc_tests_ethereum.sh" ;;
  gnosis)  TEST_SCRIPT="$SCRIPT_DIR/run_rpc_tests_gnosis.sh" ;;
  *) echo "Unknown chain: $CHAIN (supported: mainnet, gnosis)"; exit 1 ;;
esac

BACKUP_NEEDED=false
RPC_DAEMON_PID=""
BACKUP_DIR=""
OWN_BACKUP_DIR=false
OWN_WORKSPACE=false
OWN_RESULT_DIR=false

cleanup() {
  if [ -n "$RPC_DAEMON_PID" ] && kill -0 "$RPC_DAEMON_PID" 2>/dev/null; then
    echo "Stopping rpcdaemon..."
    kill "$RPC_DAEMON_PID"
  fi
  if $BACKUP_NEEDED && [ -d "$BACKUP_DIR/chaindata" ]; then
    echo "Restoring chaindata..."
    rm -rf "$DATADIR/chaindata"
    mv "$BACKUP_DIR/chaindata" "$DATADIR/chaindata"
  fi
  if $OWN_BACKUP_DIR && [ -n "$BACKUP_DIR" ]; then
    rm -rf "$BACKUP_DIR"
  fi
  if $OWN_WORKSPACE && [ -n "$WORKSPACE" ]; then
    rm -rf "$WORKSPACE"
  fi
  if $OWN_RESULT_DIR && [ -n "$RESULT_DIR" ]; then
    rm -rf "$RESULT_DIR"
  fi
}
trap cleanup EXIT

if [ -n "$BACKUP_DIR_OPT" ]; then
  BACKUP_DIR="$BACKUP_DIR_OPT"
  rm -rf "$BACKUP_DIR"
  mkdir -p "$BACKUP_DIR"
else
  BACKUP_DIR="$(mktemp -d)"
  OWN_BACKUP_DIR=true
fi
if [ -z "$WORKSPACE" ]; then
  WORKSPACE="$(mktemp -d)"
  OWN_WORKSPACE=true
fi
if [ -z "$RESULT_DIR" ]; then
  RESULT_DIR="$(mktemp -d)"
  OWN_RESULT_DIR=true
fi

if ! $SKIP_BACKUP; then
  echo "Backing up chaindata..."
  cp -r "$DATADIR/chaindata" "$BACKUP_DIR/chaindata"
  BACKUP_NEEDED=true
fi

echo "Running migrations..."
"$BUILD_BIN/integration" run_migrations --datadir "$DATADIR" --chain "$CHAIN"

echo "Starting rpcdaemon..."
mkdir -p "$RESULT_DIR"
"$BUILD_BIN/rpcdaemon" --datadir "$DATADIR" \
  --http.api admin,debug,eth,parity,erigon,trace,web3,txpool,ots,net \
  --ws > "$RESULT_DIR/rpcdaemon.log" 2>&1 &
RPC_DAEMON_PID=$!

echo "Waiting for port 8545..."
for i in {1..30}; do
  if nc -z localhost 8545 2>/dev/null; then
    echo "Port 8545 is open"
    break
  fi
  sleep 5
done
if ! nc -z localhost 8545 2>/dev/null; then
  echo "Error: port 8545 did not open — check $RESULT_DIR/rpcdaemon.log"
  exit 1
fi

echo "Running RPC integration tests (chain=$CHAIN)..."
set +e
"$TEST_SCRIPT" "$WORKSPACE" "$RESULT_DIR"
TEST_EXIT=$?
set -e

echo
if [ $TEST_EXIT -eq 0 ]; then
  echo "All RPC integration tests passed."
else
  echo "Some tests failed. Results: $RESULT_DIR/output.log"
  # Keep auto-created result dir on failure so user can inspect
  OWN_RESULT_DIR=false
fi

exit $TEST_EXIT
