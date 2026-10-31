#!/bin/bash
# Runs one rpc_perf load pattern, archives its results and saves them on the DB.
# Usage: run_rpc_perf.sh <method> <pattern> <load-sequence> [label] [verbose]
# Expects CHAIN, CLIENT, RUNNER_WS, ERIGON_QA_PATH, ERIGON_REFERENCE_DIR, COMMIT,
# RESULT_DIR, PAST_TEST_DIR, BIN_PAST_TEST_DIR and OUTPUT_LOG in the environment.

method=$1
pattern=$2
sequence=$3
label=${4:-$method} # names the results; set it to run the same method with another pattern
verbose=${5:-false}

failed=0

verbose_flag=()
if [ "$verbose" == "true" ]; then
  verbose_flag=(--verbose)
fi

result_file=$CLIENT-$label-result.json

# clean temporary area
cd "$RUNNER_WS/rpc-tests/perf"
rm -rf "./reports/$CHAIN/"*

# Move to home dir of rpc-tests
cd ..
./build/bin/rpc_perf --blockchain "$CHAIN" \
                     --test-type "$method" \
                     --pattern-file perf/pattern/"$CHAIN"/"$pattern".tar \
                     --test-sequence "$sequence" \
                     --repetitions 4 \
                     --response-timeout 30s \
                     --client-build-dir "/" \
                     --test-report \
                     --json-report "$RESULT_DIR/$result_file" \
                     --testing-client "$CLIENT" \
                     --more-percentiles \
                     "${verbose_flag[@]}" 2>&1 | tee -a "$OUTPUT_LOG"
perf_exit_status=${PIPESTATUS[0]}

# Detect the pre-built db version
if [ "$CLIENT" == "erigon" ]; then
  db_version=$(python3 "$ERIGON_QA_PATH/test_system/qa-tests/uploads/prod_info.py" "$ERIGON_REFERENCE_DIR/production.ini" production erigon_repo_commit)
else
  db_version=$COMMIT
fi

if [ "$perf_exit_status" -eq 0 ]; then
  outcome=success

  # Save all vegeta binary reports
  echo "Save current vegeta binary files"
  if [ -d "$RUNNER_WS/rpc-tests/perf/reports/bin" ]; then
    cp -r "$RUNNER_WS/rpc-tests/perf/reports/bin" "$BIN_PAST_TEST_DIR"
  else
    echo "::warning::vegeta binary reports directory '$RUNNER_WS/rpc-tests/perf/reports/bin' not found; skipping copy."
  fi

  echo "Execute Latency Percentile HDR Analysis"
  (cd "$RESULT_DIR" && python3 "$ERIGON_QA_PATH/test_system/qa-tests/rpc-tests/perf_hdr_analysis.py" \
    --test_name "$CLIENT-$label" \
    --input_file "./$result_file" \
    --output_file "./$CLIENT-$label-latency_hdr_analysis.pdf")
  perf_hdr_status=$?
  if [ $perf_hdr_status -ne 0 ]; then
    echo "::warning::perf_hdr_analysis.py failed with exit code $perf_hdr_status"
  fi
else
  failed=1
  outcome=failure
  echo "::error::rpc_perf failed on $label with exit code $perf_exit_status"
fi

echo "Save test result on DB"

if [ "$CLIENT" == "erigon" ]; then
  branch_name="$GITHUB_REF_NAME"
  commit_hash=$(git -C "$RUNNER_WS/erigon" rev-parse HEAD)
else
  branch_name="release"
  commit_hash=$COMMIT
fi

echo branch_name=$branch_name
echo commit_hash=$commit_hash
echo method=$method
echo db_version=$db_version
echo outcome=$outcome
echo result_file=$result_file

if ! python3 "$ERIGON_QA_PATH/test_system/qa-tests/uploads/upload_test_results.py" \
  --repo "$CLIENT" \
  --branch "$branch_name" \
  --commit "$commit_hash" \
  --test_name "rpc-performance-test-latest-$label" \
  --chain "$CHAIN" \
  --runner "$RUNNER_NAME" \
  --db_version "$db_version" \
  --outcome "$outcome" \
  --result_file "$RESULT_DIR/$result_file"; then
  failed=1
  echo "::error::Failure saving the $label test results on DB"
fi

# Save test results to a directory with timestamp and commit hash
if [ -d "$RESULT_DIR" ]; then
  cp -r "$RESULT_DIR" "$PAST_TEST_DIR"
else
  echo "::warning::Result directory '$RESULT_DIR' does not exist; skipping archival to $PAST_TEST_DIR"
fi

exit $failed
