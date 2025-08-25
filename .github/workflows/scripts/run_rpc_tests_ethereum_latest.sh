#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"
# The REFERENCE_HOST that hosts the reference client
REFERENCE_HOST="$3"

# Disabled tests for Ethereum mainnet
DISABLED_TEST_LIST=(
   debug_traceCall/test_22.json
   debug_traceCallMany
   erigon_
   eth_callBundle
   eth_getProof/test_04.json
   eth_getProof/test_08.json
   eth_getProof/test_09.json
   ots_
   parity_
   trace_
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" mainnet v1.77.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR" "latest" "$REFERENCE_HOST" 
