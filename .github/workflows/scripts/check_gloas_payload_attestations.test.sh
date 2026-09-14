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
  if [[ "$GLOAS_TEST_SCENARIO" == head_retry_liveness ]] && ((count == 2)); then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == malformed_head ]] && ((count == 2)); then
    printf '{}\n' >"$output"
    exit 0
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
  printf '{"version":"gloas","data":{"message":{"slot":"%s","body":{"payload_attestations":[]}}}}\n' \
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

signature=$(printf '01%.0s' {1..96})
response_slot=$candidate_slot
if [[ "$GLOAS_TEST_SCENARIO" == wrong_slot ]] && ((slot_count == 1)); then
  response_slot=$((candidate_slot + 1))
fi
if [[ "$GLOAS_TEST_SCENARIO" == invalid_attestation ]] && ((slot_count == 1)); then
  signature=bad
fi
cat >"$output" <<EOF_JSON
{"version":"gloas","data":{"message":{"slot":"$response_slot","body":{"payload_attestations":[{"aggregation_bits":"0x01","data":{"slot":"$((response_slot - 1))","payload_present":true},"signature":"0x$signature"}]}}}}
EOF_JSON
$write_out && printf '200'
EOF
  chmod +x "$fixture_dir/bin/curl"
}

cleanup() {
  rm -rf "${fixture_dir:-}"
}

original_path=$PATH
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
export GLOAS_TEST_SCENARIO=malformed_head
output=$(GLOAS_POLL_TIMEOUT_SECONDS=30 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
  "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
grep -q "Found available payload attestations in Gloas block at slot 1" <<<"$output"
test "$(cat "$fixture_dir/head-count")" -ge 3

cleanup
new_fixture
export GLOAS_TEST_SCENARIO=head_retry_liveness
if output=$(GLOAS_POLL_TIMEOUT_SECONDS=2 GLOAS_POLL_TARGET_ADVANCE=1 GLOAS_POLL_SLEEP_SECONDS=0 \
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
