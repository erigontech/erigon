#!/bin/bash
set -e # Enable exit on error

# Sanity check for mandatory parameters
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <CHAIN> <RPC_VERSION> [DISABLED_TESTS] [WORKSPACE] [RESULT_DIR] [TESTS_TYPE] [REFERENCE_HOST]"
  echo
  echo "  CHAIN:           The chain identifier (possible values: mainnet, gnosis, polygon)"
  echo "  RPC_VERSION:     The rpc-tests repository version or branch (e.g., v1.66.0, main)"
  echo "  DISABLED_TESTS:  Comma-separated list of disabled tests (optional, default: empty)"
  echo "  WORKSPACE:       Workspace directory (optional, default: /tmp)"
  echo "  RESULT_DIR:      Result directory (optional, default: empty)"
  echo "  TESTS_TYPE:      Test type (optional, default: empty, possible values: latest or all)"
  echo "  REFERENCE_HOST:  IP Address of HOST (optional, default: empty)"
  echo
  exit 1
fi

CHAIN="$1"
RPC_VERSION="$2"
DISABLED_TESTS="$3"
WORKSPACE="${4:-/tmp}"
RESULT_DIR="$5"
TEST_TYPE="$6"
REFERENCE_HOST="$7"

OPTIONAL_FLAGS=""
NUM_OF_RETRIES=1

# Check if REFERENCE_HOST is not empty (-n)
if [ -n "$REFERENCE_HOST" ]; then
    # If it's not empty, then check if TESTS_ON_LATEST is empty (-z)
    if [ -z "$TEST_TYPE" ]; then
        echo "Error: REFERENCE_HOST is set, but TEST_TYPE is empty."
        exit 1 # Exit the script with an error code
    fi
fi

if [ -n "$REFERENCE_HOST" ]; then
    #OPTIONAL_FLAGS+="--verify-external-provider $REFERENCE_HOST"
    OPTIONAL_FLAGS+="-e $REFERENCE_HOST"
fi

if [ "$TEST_TYPE" = "latest" ]; then
    OPTIONAL_FLAGS+=" --tests-on-latest-block"
    NUM_OF_RETRIES=3
fi

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

retries=0
while true; do
   python3 ./run_tests.py --blockchain "$CHAIN" --port 8545 --engine-port 8545 --continue --display-only-fail --json-diff $OPTIONAL_FLAGS --exclude-api-list "$DISABLED_TESTS"
   RUN_TESTS_EXIT_CODE=$?

   if [ $RUN_TESTS_EXIT_CODE -eq 0 ]; then
        break
   fi
   retries=$((retries + 1))

   if [ $retries -ge $NUM_OF_RETRIES ]; then
        break
   fi
done

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
