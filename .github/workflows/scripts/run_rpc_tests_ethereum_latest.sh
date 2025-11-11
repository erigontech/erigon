#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"
# The host address where the reference client runs
REFERENCE_HOST="$3"
# The response dump mode
DUMP_RESPONSE="$4"

if [ -z "$REFERENCE_HOST" ]; then
    echo "*WARNING*: REFERENCE_HOST is not set, RPC tests on latest will run without reference comparison"
    echo "*WARNING*: RPC responses are available for inspection in results directory"
    DUMP_RESPONSE="always-dump-response"
fi

# Disabled tests for Ethereum mainnet
DISABLED_TEST_LIST=(
   debug_traceBlockByNumber/test_30.json # huge JSON response => slow diff
   debug_traceCall/test_22.json
   debug_traceCall/test_38.json # see https://github.com/erigontech/erigon-qa/issues/274
   debug_traceCallMany
   erigon_
   eth_blobBaseFee/test_01.json # debug mismatch
   eth_callBundle
   ots_
   parity_
   trace_
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" mainnet v1.98.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR" "latest" "$REFERENCE_HOST" "do-not-compare-error-message" "$DUMP_RESPONSE"
