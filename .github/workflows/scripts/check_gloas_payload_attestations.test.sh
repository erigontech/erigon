#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poll_script="$script_dir/check_gloas_payload_attestations.sh"

new_fixture() {
  fixture_dir=$(mktemp -d)
  mkdir -p "$fixture_dir/bin" "$fixture_dir/api"
  export GLOAS_TEST_STATE_DIR="$fixture_dir"
  export PATH="$fixture_dir/bin:$original_path"

  cat >"$fixture_dir/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

output=
write_out=false
max_time=
max_filesize=
url=
while (($#)); do
  case "$1" in
    --output)
      output=$2
      shift 2
      ;;
    --write-out)
      write_out=true
      shift 2
      ;;
    --max-time)
      max_time=$2
      shift 2
      ;;
    --max-filesize)
      max_filesize=$2
      shift 2
      ;;
    http://*|https://*)
      url=$1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if [[ "$url" == */head ]]; then
  count_file="$GLOAS_TEST_STATE_DIR/head-count"
  count=$(($(cat "$count_file" 2>/dev/null || echo 0) + 1))
  echo "$count" >"$count_file"
  if [[ "$GLOAS_TEST_SCENARIO" == initial_deadline && "$max_time" -gt "$GLOAS_POLL_TIMEOUT_SECONDS" ]]; then
    exit 87
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == initial_deadline && "$max_filesize" -ne 8388608 ]]; then
    exit 88
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == initial_retry && "$count" -eq 1 ]]; then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == terminal_initial_failure ]]; then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == initial_late_response && "$count" -eq 1 ]]; then
    sleep 3
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == head_retry_recovery ]] && ((count == 2)); then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == terminal_head_failure ]] && ((count > 1)); then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == malformed_head ]] && ((count == 2)); then
    printf '{}\n' >"$output"
    exit 0
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == fractional_head ]] && ((count == 2)); then
    printf '{"data":{"message":{"slot":"1.5"}}}\n' >"$output"
    exit 0
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == exponent_head ]] && ((count == 2)); then
    printf '{"data":{"message":{"slot":"1e3"}}}\n' >"$output"
    exit 0
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == noncanonical_head ]] && ((count == 2)); then
    printf '{"data":{"message":{"slot":"01"}}}\n' >"$output"
    exit 0
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == noncanonical_head ]] && ((count == 3)); then
    printf '{"data":{"message":{"slot":"1\\n"}}}\n' >"$output"
    exit 0
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == multi_document_head ]] && ((count == 2)); then
    printf '{}\n{"data":{"message":{"slot":"1"}}}\n' >"$output"
    exit 0
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == invalid_head_roots ]]; then
    case "$count" in
      2) : >"$output"; exit 0 ;;
      3) printf 'null\n' >"$output"; exit 0 ;;
      4) printf '[]\n' >"$output"; exit 0 ;;
    esac
  fi
  case "$GLOAS_TEST_SCENARIO" in
    deadline)
      slot=$((count == 1 ? 0 : 100000))
      ;;
    permanent_then_later)
      slot=$((count == 1 ? 0 : 2))
      ;;
    near_head_404)
      slot=$((count == 1 ? 0 : count == 2 ? 1 : 2))
      ;;
    initial_retry)
      slot=$((count == 2 ? 0 : 1))
      ;;
    overflow_head)
      if ((count == 1)); then
        slot=9223372036854775807
      else
        slot=$((count == 2 ? 0 : 1))
      fi
      ;;
    head_retry_recovery)
      slot=$((count == 1 ? 0 : 1))
      ;;
    head_retry_liveness)
      slot=0
      ;;
    *)
      slot=$((count == 1 ? 0 : 1))
      ;;
  esac
  printf '{"data":{"message":{"slot":"%s"}}}\n' "$slot" >"$output"
  exit 0
fi

candidate_count_file="$GLOAS_TEST_STATE_DIR/candidate-count"
candidate_count=$(($(cat "$candidate_count_file" 2>/dev/null || echo 0) + 1))
echo "$candidate_count" >"$candidate_count_file"
candidate_slot=${url##*/}
slot_count_file="$GLOAS_TEST_STATE_DIR/candidate-$candidate_slot-count"
slot_count=$(($(cat "$slot_count_file" 2>/dev/null || echo 0) + 1))
echo "$slot_count" >"$slot_count_file"

if [[ "$GLOAS_TEST_SCENARIO" == deadline ]]; then
  : >"$output"
  $write_out && printf '404'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == retry* ]] && ((slot_count == 1)); then
  $write_out && printf '000'
  exit 7
fi

if [[ "$GLOAS_TEST_SCENARIO" == retry_empty ]]; then
  printf '{"version":"gloas","data":{"message":{"slot":"%s","parent_root":"0xabababababababababababababababababababababababababababababababab","body":{"payload_attestations":[]}}}}\n' \
    "$candidate_slot" >"$output"
  $write_out && printf '200'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == permanent_then_later && "$candidate_slot" == 1 ]]; then
  : >"$output"
  $write_out && printf '500'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == near_head_404 && "$candidate_slot" == 1 ]]; then
  : >"$output"
  $write_out && printf '404'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == malformed ]] && ((slot_count == 1)); then
  printf '{}\n' >"$output"
  $write_out && printf '200'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == multi_document_candidate ]] && ((slot_count == 1)); then
  signature=$(printf '01%.0s' {1..96})
  printf '{}\n{"version":"gloas","data":{"message":{"slot":"1","body":{"payload_attestations":[{"aggregation_bits":"0x01","data":{"slot":"0","payload_present":true},"signature":"0x%s"}]}}}}\n' \
    "$signature" >"$output"
  $write_out && printf '200'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == invalid_candidate_roots ]]; then
  case "$slot_count" in
    1) : >"$output"; $write_out && printf '200'; exit 0 ;;
    2) printf 'null\n' >"$output"; $write_out && printf '200'; exit 0 ;;
    3) printf '[]\n' >"$output"; $write_out && printf '200'; exit 0 ;;
  esac
fi

signature=$(printf '01%.0s' {1..96})
aggregation_bits=$(printf '01%.0s' {1..64})
beacon_block_root=$(printf 'ab%.0s' {1..32})
response_slot=$candidate_slot
attestation_slot=$((candidate_slot - 1))
parent_root=$beacon_block_root
if [[ "$GLOAS_TEST_SCENARIO" == wrong_slot ]] && ((slot_count == 1)); then
  response_slot=$((candidate_slot + 1))
  attestation_slot=$((response_slot - 1))
fi
if [[ "$GLOAS_TEST_SCENARIO" == invalid_attestation ]] && ((slot_count == 1)); then
  signature=bad
fi
if [[ "$GLOAS_TEST_SCENARIO" == short_aggregation_bits ]] && ((slot_count == 1)); then
  aggregation_bits=01
fi
if [[ "$GLOAS_TEST_SCENARIO" == trailing_newline_fields ]]; then
  case "$slot_count" in
    1) aggregation_bits="${aggregation_bits}\\n" ;;
    2) beacon_block_root="${beacon_block_root}\\n" ;;
    3) signature="${signature}\\n" ;;
  esac
fi
if [[ "$GLOAS_TEST_SCENARIO" == mismatched_attestation_root ]] && ((slot_count == 1)); then
  beacon_block_root=$(printf 'cd%.0s' {1..32})
fi
if [[ "$GLOAS_TEST_SCENARIO" == case_variant_matching_root ]] && ((slot_count == 1)); then
  beacon_block_root=$(printf 'AB%.0s' {1..32})
fi
if [[ "$GLOAS_TEST_SCENARIO" == noncanonical_slot ]] && ((slot_count == 1)); then
  response_slot=1.0
fi
if [[ "$GLOAS_TEST_SCENARIO" == leading_zero_slot ]] && ((slot_count == 1)); then
  response_slot=01
fi
if [[ "$GLOAS_TEST_SCENARIO" == exponent_attestation ]] && ((slot_count == 1)); then
  attestation_slot=0e0
fi
if [[ "$GLOAS_TEST_SCENARIO" == noncanonical_attestation ]] && ((slot_count == 1)); then
  attestation_slot='0\n'
fi
payload_present=true
if [[ "$GLOAS_TEST_SCENARIO" == late_empty_candidate_parse ]] && ((slot_count == 1)); then
  payload_present=false
fi
beacon_block_root_field=",\"beacon_block_root\":\"0x$beacon_block_root\""
blob_data_available_field=',"blob_data_available":true'
if [[ "$GLOAS_TEST_SCENARIO" == missing_attestation_fields ]] && ((slot_count == 1)); then
  beacon_block_root_field=
fi
if [[ "$GLOAS_TEST_SCENARIO" == missing_attestation_fields ]] && ((slot_count == 2)); then
  blob_data_available_field=
fi
cat >"$output" <<EOF_JSON
{"version":"gloas","data":{"message":{"slot":"$response_slot","parent_root":"0x$parent_root","body":{"payload_attestations":[{"aggregation_bits":"0x$aggregation_bits","data":{"slot":"$attestation_slot"$beacon_block_root_field,"payload_present":$payload_present$blob_data_available_field},"signature":"0x$signature"}]}}}}
EOF_JSON
$write_out && printf '200'
EOF
  cat >"$fixture_dir/bin/jq" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

last_arg=${!#}
if [[ "$GLOAS_TEST_SCENARIO" == late_candidate_parse || "$GLOAS_TEST_SCENARIO" == late_empty_candidate_parse ]] && \
  [[ "$last_arg" == */candidate.json && ! -e "$GLOAS_TEST_STATE_DIR/jq-delayed" ]]; then
  touch "$GLOAS_TEST_STATE_DIR/jq-delayed"
  sleep 3
fi
if [[ "$GLOAS_TEST_SCENARIO" == late_head_parse && "$last_arg" == */head.json ]]; then
  count_file="$GLOAS_TEST_STATE_DIR/jq-head-count"
  count=$(($(cat "$count_file" 2>/dev/null || echo 0) + 1))
  echo "$count" >"$count_file"
  if ((count == 2)); then
    sleep 3
  fi
fi
exec "$GLOAS_TEST_REAL_JQ" "$@"
EOF
  chmod +x "$fixture_dir/bin/curl" "$fixture_dir/bin/jq"
}

cleanup() {
  rm -rf "${fixture_dir:-}"
}

original_path=$PATH
export GLOAS_TEST_REAL_JQ
GLOAS_TEST_REAL_JQ=$(command -v jq)
trap cleanup EXIT

new_fixture
export GLOAS_TEST_SCENARIO=deadline
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected deadline scenario to fail" >&2
  exit 1
fi
grep -q "Payload-attestation scan incomplete" <<<"$output"
if grep -Eq "Chain liveness failure|No canonical Gloas block" <<<"$output"; then
  echo "deadline scenario used a misleading diagnosis: $output" >&2
  exit 1
fi

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=retry
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=permanent_then_later
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=2 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 2" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 1
test "$(cat "$fixture_dir/candidate-2-count")" -eq 1

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=near_head_404
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=2 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 2" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=multi_document_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=invalid_head_roots
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 5

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=multi_document_candidate
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=invalid_candidate_roots
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 4

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=initial_deadline
output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=fractional_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=exponent_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=noncanonical_slot
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=leading_zero_slot
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=exponent_attestation
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=terminal_head_failure
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected terminal head failure scenario to fail" >&2
  exit 1
fi
grep -q "Head polling was incomplete before the deadline; last observed head slot 0" <<<"$output"

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=late_head_parse
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected head parsed after the deadline to fail" >&2
  exit 1
fi
grep -q "Head polling was incomplete before the deadline; last observed head slot 0" <<<"$output"
if grep -q "Chain liveness failure" <<<"$output"; then
  echo "late head parsing was misclassified as a liveness failure: $output" >&2
  exit 1
fi

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=initial_retry
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=terminal_initial_failure
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected initial head failure scenario to fail" >&2
  exit 1
fi
grep -q "no valid head slot was observed" <<<"$output"

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=late_candidate_parse
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected candidate parsed after the deadline to fail" >&2
  exit 1
fi
grep -q "Payload-attestation scan incomplete before the deadline" <<<"$output"
if grep -q "Found available payload attestations" <<<"$output"; then
  echo "candidate parsed after the deadline was reported as successful: $output" >&2
  exit 1
fi

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=late_empty_candidate_parse
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected empty candidate parsed after the deadline to fail" >&2
  exit 1
fi
grep -q "Payload-attestation scan incomplete before the deadline" <<<"$output"
if grep -q "No canonical Gloas block" <<<"$output"; then
  echo "empty candidate parsed after the deadline was treated as a complete scan: $output" >&2
  exit 1
fi

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=initial_late_response
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected late initial head response to miss the deadline" >&2
  exit 1
fi
grep -q "no valid head slot was observed" <<<"$output"

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=noncanonical_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 4

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=overflow_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=noncanonical_attestation
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=malformed_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=head_retry_recovery
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=head_retry_liveness
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=2 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected stalled head scenario to fail" >&2
  exit 1
fi
grep -q "Chain liveness failure" <<<"$output"
if grep -q "Payload-attestation scan incomplete" <<<"$output"; then
  echo "head retry polluted candidate scan state: $output" >&2
  exit 1
fi

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=wrong_slot
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=invalid_attestation
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=missing_attestation_fields
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=short_aggregation_bits
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=trailing_newline_fields
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 4

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=mismatched_attestation_root
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=case_variant_matching_root
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 1

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=malformed
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/candidate-1-count")" -eq 2

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=retry_empty
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1); then
  echo "expected scan without an attestation to fail" >&2
  exit 1
fi
grep -q "No canonical Gloas block exposed a payload attestation" <<<"$output"
test "$(cat "$fixture_dir/candidate-count")" -eq 2
