#!/bin/bash

set +e # Disable exit on error

manual=false
for arg in "$@"; do
  if [[ $arg == "--manual" ]]; then
    manual=true
  fi
done

if $manual; then
  echo "Running manual setup…"
  python3 -m venv .venv
  source .venv/bin/activate
  pip3 install -r ../requirements.txt
  echo "Manual setup complete."
fi

# Array of disabled tests
disabled_tests=(
  bor_getAuthor
  bor_getSnapshot
)

# Transform the array into a comma-separated string
disabled_test_list=$(IFS=,; echo "${disabled_tests[*]}")

python3 ./run_tests.py --blockchain polygon-pos --port 8545 --engine-port 8545 --continue -f --json-diff --serial -x "$disabled_test_list"
RUN_TESTS_EXIT_CODE=$?
if $manual; then
  echo "deactivating…"
  deactivate 2>/dev/null || echo "No active virtualenv"
  echo "deactivating complete."
fi
exit $RUN_TESTS_EXIT_CODE
