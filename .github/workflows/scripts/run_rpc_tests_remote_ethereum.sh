#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"

# Disabled tests for Ethereum mainnet in remote (grpc) configuration
DISABLED_TEST_LIST=(
  # Erigon2 and Erigon3 never supported this api methods
  trace_rawTransaction
  engine_
  eth_getProof
  eth_simulateV1
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" mainnet v1.97.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"
