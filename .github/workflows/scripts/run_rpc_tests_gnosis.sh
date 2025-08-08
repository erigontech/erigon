#!/bin/bash
set -e # Enable exit on error

# The workspace directory, no default because run_rpc_tests has it
WORKSPACE="$1"
# The result directory, no default because run_rpc_tests has it
RESULT_DIR="$2"

# Disabled tests for Gnosis chain
DISABLED_TEST_LIST=(
  # These tests require Erigon active
  eth_mining
  eth_submitHashrate
  eth_submitWork
  net_peerCount
  net_listening
  net_version
  web3_clientVersion
)

# Transform the array into a comma-separated string
DISABLED_TESTS=$(IFS=,; echo "${DISABLED_TEST_LIST[*]}")

# Call the main test runner script with the required and optional parameters
"$(dirname "$0")/run_rpc_tests.sh" gnosis v1.74.0 "$DISABLED_TESTS" "$WORKSPACE" "$RESULT_DIR"

