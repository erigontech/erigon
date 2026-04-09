#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"

# Disabled tests for Geth on Ethereum mainnet
DISABLED_TEST_LIST=(
  # Erigon-specific namespace - not supported by Geth
  erigon_
  # Otterscan-specific namespace - not supported by Geth
  ots_
  # OpenEthereum/Parity-specific namespace - not supported by Geth
  parity_
  # OpenEthereum-style trace API - not supported by Geth
  trace_
  # Engine API runs on authenticated port 8551, not 8545
  engine_
  # Admin info format differs between clients
  admin_nodeInfo/test_01.json
  admin_peers/test_01.json
  # Mining/PoW endpoints not applicable on PoS mainnet
  eth_coinbase/test_01.json
  eth_getWork/test_01.json
  eth_mining/test_01.json
  eth_submitHashrate/test_1.json
  eth_submitWork/test_1.json
  # Temporary disable required block 24298763
  debug_traceBlockByNumber/test_51.json
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
# Use do-not-compare-error-message since Geth error messages differ from Erigon
"$(dirname "$0")/run_rpc_tests.sh" mainnet v2.2.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR" "" "" "do-not-compare-error-message"
