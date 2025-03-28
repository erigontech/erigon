#!/bin/bash

set +e # Disable exit on error

# Array of disabled tests
disabled_tests=(
  bor_getAuthor
  bor_getSnapshot
)

# Transform the array into a comma-separated string
disabled_test_list=$(IFS=,; echo "${disabled_tests[*]}")

python3 ./run_tests.py --blockchain polygon-pos --port 8545 --engine-port 8545 --continue -f --json-diff --serial -x "$disabled_test_list"

exit $?
