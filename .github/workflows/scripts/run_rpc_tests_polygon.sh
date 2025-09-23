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
  eth_getTransactionReceipt/test_01.json
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" polygon-pos v1.71.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"
