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
  # these tests/apis are disabled because some methods are not implmented on grpc
  eth_getProof
  eth_simulateV1
  # Temporary disable required block 24298763
  debug_traceBlockByNumber/test_51.json
  erigon_getLogsByHash/test_01.json
  eth_getBlockReceipts/test_01.json
  eth_getBlockReceipts/test_06.json
  ots_getBlockDetails/test_01.json
  ots_getBlockDetailsByHash/test_01.json
  ots_getBlockTransactions/test_02.json 
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" mainnet canepat/v2 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"
