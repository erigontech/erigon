#!/bin/bash
set -e # Enable exit on error

# Sanity check for mandatory parameters
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <CHAIN> <RPC_VERSION> [DISABLED_TESTS] [WORKSPACE] [RESULT_DIR]"
  echo
  echo "  CHAIN:          The chain identifier (possible values: mainnet, gnosis, polygon)"
  echo "  RPC_VERSION:    The rpc-tests repository version or branch (e.g., v1.66.0, main)"
  echo "  DISABLED_TESTS: Comma-separated list of disabled tests (optional, default: empty)"
  echo "  WORKSPACE:      Workspace directory (optional, default: /tmp)"
  echo "  RESULT_DIR:     Result directory (optional, default: empty)"
  echo
  exit 1
fi

CHAIN="$1"
RPC_VERSION="$2"
DISABLED_TESTS="$3"
WORKSPACE="${4:-/tmp}"
RESULT_DIR="$5"

echo "Setup the test execution environment..."

# Clone rpc-tests repository at specific tag/branch
rm -rf "$WORKSPACE/rpc-tests" >/dev/null 2>&1
git -c advice.detachedHead=false clone --depth 1 --branch "$RPC_VERSION" https://github.com/erigontech/rpc-tests "$WORKSPACE/rpc-tests" >/dev/null 2>&1
cd "$WORKSPACE/rpc-tests"

# Try to create and activate a Python virtual environment or install packages globally if it fails
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
  pip3 install --upgrade pip 1>/dev/null
  pip3 install -r requirements.txt 1>/dev/null
fi

# Remove the local results directory if any
cd "$WORKSPACE/rpc-tests/integration"
rm -rf ./"$CHAIN"/results/

# Run the RPC integration tests
set +e # Disable exit on error for test run

python3 ./run_tests.py --blockchain "$CHAIN" --port 8545 --engine-port 8545 --continue --display-only-fail --json-diff --exclude-api-list "$DISABLED_TESTS"
RUN_TESTS_EXIT_CODE=$?

set -e # Re-enable exit on error after test run

# Save any failed results to the requested result directory if provided
if [ $RUN_TESTS_EXIT_CODE -ne 0 ] && [ -n "$RESULT_DIR" ]; then
  # Copy the results to the requested result directory
  cp -r "$WORKSPACE/rpc-tests/integration/$CHAIN/results/" "$RESULT_DIR"
  # Clean up the local result directory
  rm -rf "$WORKSPACE/rpc-tests/integration/$CHAIN/results/"
fi

# Deactivate the Python virtual environment if it was created
cd "$WORKSPACE/rpc-tests"
if [ -f ".venv/bin/activate" ]; then
  deactivate 2>/dev/null || :
fi

exit $RUN_TESTS_EXIT_CODE
