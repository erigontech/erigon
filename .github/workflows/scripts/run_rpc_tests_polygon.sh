#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"

# Disabled tests for Polygon chain
DISABLED_TEST_LIST=(
  bor_getAuthor
  bor_getSnapshot
  # Temporarily disabled because of pruned node on runner n-4 (should be archive instead)
  debug_traceTransaction/test_02
  eth_getTransactionReceipt/test_01
)
# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" polygon-pos release/3.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"
