#!/bin/bash
set -e # Enable exit on error

# Sanity check for mandatory parameters
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <CHAIN> <RPC_VERSION> [DISABLED_TESTS] [WORKSPACE] [RESULT_DIR] [TESTS_TYPE] [REFERENCE_HOST] [COMPARE_ERROR_MESSAGE] [DUMP_RESPONSE] [TRANSPORT_TYPES]"
  echo
  echo "  CHAIN:                 The chain identifier (possible values: mainnet, gnosis)"
  echo "  RPC_VERSION:           The rpc-tests repository version or branch (e.g., v1.66.0, main)"
  echo "  DISABLED_TESTS:        Comma-separated list of disabled tests (optional, default: empty)"
  echo "  WORKSPACE:             Workspace directory where repository checkout will happen (optional, default: /tmp)"
  echo "  RESULT_DIR:            Result directory where mismatching test results are saved (optional, default: empty)"
  echo "  TESTS_TYPE:            Test type (optional, default: empty, possible values: latest or all)"
  echo "  REFERENCE_HOST:        Host address of client node used as reference system (optional, default: empty)"
  echo "  COMPARE_ERROR_MESSAGE: Verify the error message (optional, default: empty, possible values: do-not-compare-error-message)"
  echo "  DUMP_RESPONSE:         Dump each test response (optional, default: empty, possible values: always-dump-response)"
  echo "  TRANSPORT_TYPES:       Comma-separated list of transport types (optional, default: empty, e.g. http,http_comp,websocket)"
  echo
  exit 1
fi

CHAIN="$1"
RPC_VERSION="$2"
DISABLED_TESTS="$3"
WORKSPACE="${4:-/tmp}"
RESULT_DIR="$5"
TEST_TYPE="$6"
REFERENCE_HOST="$7"
COMPARE_ERROR_MESSAGE="$8"
DUMP_RESPONSE="$9"
TRANSPORT_TYPES="${10:-${RPC_TRANSPORT_TYPES:-}}"

OPTIONAL_FLAGS=""
NUM_OF_RETRIES=2

# Check if REFERENCE_HOST is not empty (-n)
if [ -n "$REFERENCE_HOST" ]; then
    # If it's not empty, then check if TESTS_ON_LATEST is empty (-z)
    if [ -z "$TEST_TYPE" ]; then
        echo "Error: REFERENCE_HOST is set, but TEST_TYPE is empty."
        exit 1 # Exit the script with an error code
    fi
fi

if [ -n "$REFERENCE_HOST" ]; then
    OPTIONAL_FLAGS+="--verify-external-provider $REFERENCE_HOST"
fi

if [ "$TEST_TYPE" = "latest" ]; then
    OPTIONAL_FLAGS+=" --tests-on-latest-block"
    if [ -n "$REFERENCE_HOST" ]; then
        NUM_OF_RETRIES=5
    fi
fi

if [ "$COMPARE_ERROR_MESSAGE" = "do-not-compare-error-message" ]; then
    OPTIONAL_FLAGS+=" --do-not-compare-error"
fi

if [ "$DUMP_RESPONSE" = "always-dump-response" ]; then
    OPTIONAL_FLAGS+=" --dump-response"
fi

if [ -n "$TRANSPORT_TYPES" ]; then
    OPTIONAL_FLAGS+=" --transport-type $TRANSPORT_TYPES"
fi

if [ "${RPC_COMMITMENT_HISTORY:-false}" = "true" ]; then
    OPTIONAL_FLAGS+=" --erigon.commitment-history"
fi

echo "Setup the test execution environment..."

# Clone rpc-tests repository at specific tag/branch, reusing existing clone if already at the right version
_rpc_tests_cached=false
if [ -d "$WORKSPACE/rpc-tests/.git" ]; then
  _exact_tag="$(git -C "$WORKSPACE/rpc-tests" describe --tags --exact-match 2>/dev/null || true)"
  _current_branch="$(git -C "$WORKSPACE/rpc-tests" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  if [ "$_exact_tag" = "$RPC_VERSION" ] || [ "$_current_branch" = "$RPC_VERSION" ]; then
    _rpc_tests_cached=true
  fi
fi
if $_rpc_tests_cached; then
  echo "Using cached rpc-tests at $RPC_VERSION"
  # For branch references (not tags) pull the latest commits so the cache stays fresh.
  if [ -z "$_exact_tag" ]; then
    git -C "$WORKSPACE/rpc-tests" fetch --depth 1 origin "$RPC_VERSION" >/dev/null 2>&1 && \
      git -C "$WORKSPACE/rpc-tests" reset --hard FETCH_HEAD >/dev/null 2>&1 || true
  fi
  # Remove stale untracked test fixtures left by runs using a different branch/version,
  # but preserve build/ which holds the compiled rpc_int binary.
  git -C "$WORKSPACE/rpc-tests" clean -fd -e build >/dev/null 2>&1
else
  rm -rf "$WORKSPACE/rpc-tests" >/dev/null 2>&1
  git -c advice.detachedHead=false clone --depth 1 --branch "$RPC_VERSION" https://github.com/erigontech/rpc-tests "$WORKSPACE/rpc-tests" >/dev/null 2>&1
fi
cd "$WORKSPACE/rpc-tests"

# Remove the local results directory if any
cd "$WORKSPACE/rpc-tests/integration"
rm -rf ./"$CHAIN"/results/*
mkdir -p ./"$CHAIN"/results

# Determine the output log path
if [ -n "$RESULT_DIR" ]; then
  mkdir -p "$RESULT_DIR"
  LOG_FILE="$RESULT_DIR/output.log"
else
  LOG_FILE="$WORKSPACE/rpc-tests/integration/$CHAIN/results/output.log"
fi

# Run the RPC integration tests
set +e # Disable exit on error for test run


# Move to home dir of rpc-tests
cd ..
make rpc_int

retries=0
attempt=0
while true; do
   attempt=$((attempt + 1))
   echo "" | tee -a "$LOG_FILE"
   echo "Attempt $attempt" | tee -a "$LOG_FILE"
   ./build/bin/rpc_int --blockchain "$CHAIN" --port 8545 --engine-port 8545 --continue --display-only-fail --verbose 1 $OPTIONAL_FLAGS --exclude-api-list "$DISABLED_TESTS" | tee -a "$LOG_FILE"
   RUN_TESTS_EXIT_CODE=${PIPESTATUS[0]}

   if [ "$RUN_TESTS_EXIT_CODE" -eq 0 ]; then
        break
   fi
   retries=$((retries + 1))

   if [ $retries -ge $NUM_OF_RETRIES ]; then
        break
   fi

   # rpc_int wipes the results dir at startup, so save this attempt's artifacts before retrying.
   if [ -n "$RESULT_DIR" ]; then
        cp -r "$WORKSPACE/rpc-tests/integration/$CHAIN/results" "$RESULT_DIR/results_attempt_$attempt"
   fi
done

set -e # Re-enable exit on error after test run

# Save any results to the requested result directory if provided
if [ -n "$RESULT_DIR" ]; then
  # Copy the results to the requested result directory
  cp -r "$WORKSPACE/rpc-tests/integration/$CHAIN/results/" "$RESULT_DIR"
  # Clean up the local result directory
  rm -rf "$WORKSPACE/rpc-tests/integration/$CHAIN/results/"
fi

exit "$RUN_TESTS_EXIT_CODE"
