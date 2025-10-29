#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"

# Disabled tests for Ethereum mainnet in remote (grpc) configuration
DISABLED_TEST_LIST=(
  # Failing after the PR https://github.com/erigontech/erigon/pull/13617 that fixed this incompatibility
  # issues https://hive.pectra-devnet-5.ethpandaops.io/suite.html?suiteid=1738266984-51ae1a2f376e5de5e9ba68f034f80e32.json&suitename=rpc-compat
  net_listening/test_1.json
  # Erigon2 and Erigon3 never supported this api methods
  trace_rawTransaction
  # to investigate
  engine_exchangeCapabilities/test_1.json
  engine_exchangeTransitionConfigurationV1/test_01.json
  engine_getClientVersionV1/test_1.json
  eth_getProof
  eth_simulateV1
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" mainnet v1.97.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"
