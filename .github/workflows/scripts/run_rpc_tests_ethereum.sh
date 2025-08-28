#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"

# Disabled tests for Ethereum mainnet
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
  # these tests requires Erigon active
  admin_nodeInfo/test_01.json
  admin_peers/test_01.json
  erigon_nodeInfo/test_1.json
  eth_coinbase/test_01.json
  eth_createAccessList/test_16.json
  eth_getProof/test_04.json
  eth_getProof/test_08.json
  eth_getProof/test_09.json
  eth_getTransactionByHash/test_02.json
  # Small prune issue that leads to wrong ReceiptDomain data at 16999999 (probably at every million) block: https://github.com/erigontech/erigon/issues/13050
  ots_searchTransactionsBefore/test_04.tar
  eth_getWork/test_01.json
  eth_mining/test_01.json
  eth_protocolVersion/test_1.json
  eth_submitHashrate/test_1.json
  eth_submitWork/test_1.json
  net_peerCount/test_1.json
  net_version/test_1.json
  txpool_status/test_1.json
  web3_clientVersion/test_1.json
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" mainnet v1.78.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"
