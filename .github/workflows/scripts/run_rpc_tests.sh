#!/bin/bash
set -e

# Accept WORKSPACE as the first argument, default to /tmp directory if not provided
WORKSPACE="${1:-/tmp}"
# Accept RESULT_DIR as the second argument, do not set a default
RESULT_DIR="$2"

RPC_VERSION="v1.66.0"

# Clone rpc-tests repository at specific tag/branch
rm -rf "$WORKSPACE/rpc-tests" >/dev/null 2>&1
git -c advice.detachedHead=false clone --depth 1 --branch $RPC_VERSION https://github.com/erigontech/rpc-tests "$WORKSPACE/rpc-tests" >/dev/null 2>&1
cd "$WORKSPACE/rpc-tests"

# Try to create and activate a Python virtual environment or install packages globally if it fails (not ideal but works)
if python3 -m venv .venv >/dev/null 2>&1; then :
elif python3 -m virtualenv .venv >/dev/null 2>&1; then :
elif virtualenv .venv >/dev/null 2>&1; then :
else
  echo "Failed to create a virtual environment, installing packages globally."
  pip3 install -r requirements.txt 1>/dev/null
fi

# Activate virtual environment if it was created
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
  pip3 install -r requirements.txt
fi

# Remove the local results directory if any
cd "$WORKSPACE/rpc-tests/integration"
rm -rf ./mainnet/results/

# Array of disabled tests
disabled_tests=(
    # Erigon3 temporary disable waiting fix on expected test on rpc-test (PR https://github.com/erigontech/rpc-tests/pull/411)
    erigon_getHeaderByNumber
    erigon_getHeaderByHash
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
    web3_clientVersion/test_1.json)

# Transform the array into a comma-separated string
disabled_test_list=$(IFS=,; echo "${disabled_tests[*]}")

# Run the RPC integration tests
set +e # Disable exit on error for test run

python3 ./run_tests.py --port 8545 --engine-port 8545 --continue -f --json-diff -x "$disabled_test_list"
RUN_TESTS_EXIT_CODE=$?

set -e # Re-enable exit on error after test run

# Save any failed results to the requested result directory if provided
if [ $RUN_TESTS_EXIT_CODE -ne 0 ] && [ -n "$RESULT_DIR" ]; then
  # Copy the results to the requested result directory
  cp -r "$WORKSPACE/rpc-tests/integration/mainnet/results/" "$RESULT_DIR"
  # Clean up the local result directory
  rm -rf "$WORKSPACE/rpc-tests/integration/mainnet/results/"
fi

# Always deactivate the Python virtual environment at the end
cd "$WORKSPACE/rpc-tests"
if [ -f ".venv/bin/activate" ]; then
  deactivate 2>/dev/null || :
fi

exit $RUN_TESTS_EXIT_CODE
