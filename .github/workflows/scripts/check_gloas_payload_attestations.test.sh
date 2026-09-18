#!/usr/bin/env bash
set -uo pipefail

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
  if [[ "$GLOAS_TEST_SCENARIO" == initial_deadline ]]; then
    if [[ -z "$max_time" ]] || ((max_time > GLOAS_POLL_TIMEOUT_SECONDS)); then
      exit 87
    fi
    if [[ -z "$max_filesize" ]] || ((max_filesize != 8388608)); then
      exit 88
    fi
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == initial_retry && "$count" -eq 1 ]]; then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == terminal_initial_failure ]]; then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == initial_late_response && "$count" -eq 1 ]]; then
    sleep 5
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == head_retry_recovery ]] && ((count == 2)); then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == terminal_head_failure ]] && ((count > 1)); then
    exit 7
  fi
  if [[ "$GLOAS_TEST_SCENARIO" == api_hole_then_head_failure ||
    "$GLOAS_TEST_SCENARIO" == near_head_404_then_head_failure ]] && ((count > 2)); then
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
    printf '{"data":{"message":{"slot":"1"}}}\n{}\n' >"$output"
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
    stalled_near_head_404)
      slot=$((count == 1 ? 0 : 2))
      ;;
    api_hole)
      slot=$((count == 1 ? 0 : 2))
      ;;
    api_hole_then_head_failure)
      slot=$((count == 1 ? 0 : 2))
      ;;
    near_head_404_then_head_failure)
      slot=$((count == 1 ? 0 : 2))
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

if { [[ "$GLOAS_TEST_SCENARIO" == permanent_then_later ]] ||
  [[ "$GLOAS_TEST_SCENARIO" == api_hole ]] ||
  [[ "$GLOAS_TEST_SCENARIO" == api_hole_then_head_failure ]]; } &&
  [[ "$candidate_slot" == 1 ]]; then
  : >"$output"
  $write_out && printf '500'
  exit 0
fi

if [[ "$GLOAS_TEST_SCENARIO" == near_head_404 && "$candidate_slot" == 1 ]] ||
  [[ "$GLOAS_TEST_SCENARIO" == stalled_near_head_404 && "$candidate_slot" == 1 ]] ||
  [[ "$GLOAS_TEST_SCENARIO" == near_head_404_then_head_failure && "$candidate_slot" == 1 ]]; then
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
  aggregation_bits=$(printf '01%.0s' {1..64})
  root=$(printf 'ab%.0s' {1..32})
  printf '{"version":"gloas","data":{"message":{"slot":"1","parent_root":"0x%s","body":{"payload_attestations":[{"aggregation_bits":"0x%s","data":{"slot":"0","beacon_block_root":"0x%s","payload_present":true,"blob_data_available":true},"signature":"0x%s"}]}}}}\n{}\n' \
    "$root" "$aggregation_bits" "$root" "$signature" >"$output"
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
if [[ "$GLOAS_TEST_SCENARIO" == api_hole ||
  "$GLOAS_TEST_SCENARIO" == api_hole_then_head_failure ||
  "$GLOAS_TEST_SCENARIO" == stalled_near_head_404 ||
  "$GLOAS_TEST_SCENARIO" == near_head_404_then_head_failure ]]; then
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
  sleep 5
fi
if [[ "$GLOAS_TEST_SCENARIO" == late_head_parse && "$last_arg" == */head.json ]]; then
  count_file="$GLOAS_TEST_STATE_DIR/jq-head-count"
  count=$(($(cat "$count_file" 2>/dev/null || echo 0) + 1))
  echo "$count" >"$count_file"
  if ((count == 2)); then
    sleep 5
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
pass=0
fail=0

# run_scenario <name> <scenario> <exit> <want-regex|-> <reject-regex|->
#              <timeout> <target-advance> <poll-sleep> [<count-file> <eq|ge> <value> ...]
run_scenario() {
  local name=$1 scenario=$2 want_exit=$3 want_re=$4 reject_re=$5
  local timeout=$6 target_advance=$7 poll_sleep=$8
  local output status why= file comparison expected actual
  shift 8

  export GLOAS_TEST_SCENARIO=$scenario
  cleanup
  if ! new_fixture; then
    printf 'FAIL - %s: fixture setup failed\n' "$name"
    fail=$((fail + 1))
    return
  fi

  output=$(GLOAS_POLL_TIMEOUT_SECONDS=$timeout \
    GLOAS_POLL_TARGET_ADVANCE=$target_advance \
    GLOAS_POLL_SLEEP_SECONDS=$poll_sleep \
    "$poll_script" http://beacon.test "$fixture_dir/api" 2>&1)
  status=$?

  if ((status != want_exit)); then
    why="want exit $want_exit, got $status"
  elif [[ "$want_re" != - ]] && ! grep -Eq "$want_re" <<<"$output"; then
    why="output missing /$want_re/"
  elif [[ "$reject_re" != - ]] && grep -Eq "$reject_re" <<<"$output"; then
    why="output unexpectedly matched /$reject_re/"
  fi

  while [[ -z "$why" && $# -gt 0 ]]; do
    file=$1
    comparison=$2
    expected=$3
    shift 3
    if [[ "$file" == output ]]; then
      case "$comparison" in
        matches) grep -Eq "$expected" <<<"$output" || why="output missing /$expected/" ;;
        rejects) grep -Eq "$expected" <<<"$output" && why="output unexpectedly matched /$expected/" ;;
        *) why="unknown output comparison $comparison" ;;
      esac
      continue
    fi
    if [[ ! -f "$fixture_dir/$file" ]]; then
      why="missing count file $file"
      break
    fi
    actual=$(<"$fixture_dir/$file")
    case "$comparison" in
      eq) ((actual == expected)) || why="$file: want $expected, got $actual" ;;
      ge) ((actual >= expected)) || why="$file: want >= $expected, got $actual" ;;
      *) why="unknown count comparison $comparison" ;;
    esac
  done

  if [[ -z "$why" ]]; then
    printf 'ok   - %s\n' "$name"
    pass=$((pass + 1))
  else
    printf 'FAIL - %s: %s\n' "$name" "$why"
    printf '%s\n' "$output" | sed 's/^/       | /'
    fail=$((fail + 1))
  fi
}

run_scenario "deadline exhaustion reports incomplete scan" deadline 1 \
  'Payload-attestation scan incomplete' 'Chain liveness failure|No canonical Gloas block' 4 1 0
run_scenario "candidate transport failure retries" retry 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-count eq 2
run_scenario "later candidate progress survives an earlier hole" permanent_then_later 0 \
  'Found available payload attestations in Gloas block at slot 2' - 30 2 0 \
  candidate-1-count eq 1 candidate-2-count eq 1
run_scenario "near-head 404 is revisited" near_head_404 0 \
  'Found available payload attestations in Gloas block at slot 2' - 30 2 0 \
  candidate-1-count eq 2
run_scenario "stalled chain with near-head 404 reports liveness" stalled_near_head_404 1 \
  'Chain liveness failure' 'Payload-attestation scan incomplete' 4 8 4
run_scenario "stalled chain with API hole reports incomplete scan" api_hole 1 \
  'Payload-attestation scan incomplete' 'Chain liveness failure' 4 8 4
run_scenario "candidate hole remains primary after head polling fails" api_hole_then_head_failure 1 \
  'Payload-attestation scan incomplete' - 4 8 0 \
  output matches 'Head polling was incomplete'
run_scenario "near-head 404 remains incomplete after head polling fails" near_head_404_then_head_failure 1 \
  'Payload-attestation scan incomplete' 'Chain liveness failure' 4 8 0 \
  output matches 'Head polling was incomplete'
run_scenario "multi-document head response is rejected" multi_document_head 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "invalid head JSON roots are rejected" invalid_head_roots 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 5
run_scenario "multi-document candidate response is rejected" multi_document_candidate 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "invalid candidate JSON roots are rejected" invalid_candidate_roots 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 4
run_scenario "initial request obeys the poll deadline" initial_deadline 0 \
  'Found available payload attestations in Gloas block at slot 1' - 4 1 0
run_scenario "fractional head slot is rejected" fractional_head 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "exponent head slot is rejected" exponent_head 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "fractional candidate slot is rejected" noncanonical_slot 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "leading-zero candidate slot is rejected" leading_zero_slot 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "exponent attestation slot is rejected" exponent_attestation 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2

run_scenario "terminal head failure reports polling" terminal_head_failure 1 \
  'Head polling was incomplete before the deadline; last observed head slot 0' - 4 1 0
run_scenario "late head parse reports polling" late_head_parse 1 \
  'Head polling was incomplete before the deadline; last observed head slot 0' \
  'Chain liveness failure' 4 1 0
run_scenario "initial head transport failure retries" initial_retry 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "terminal initial failure reports no head" terminal_initial_failure 1 \
  'no valid head slot was observed' - 4 1 0
run_scenario "late candidate parse reports incomplete scan" late_candidate_parse 1 \
  'Payload-attestation scan incomplete before the deadline' \
  'Found available payload attestations' 4 1 0
run_scenario "late empty candidate parse reports incomplete scan" late_empty_candidate_parse 1 \
  'Payload-attestation scan incomplete before the deadline' \
  'No canonical Gloas block' 4 1 0
run_scenario "late initial head response misses deadline" initial_late_response 1 \
  'no valid head slot was observed' - 4 1 0
run_scenario "noncanonical head slot is rejected" noncanonical_head 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 4
run_scenario "overflow head slot is rejected" overflow_head 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "noncanonical attestation slot is rejected" noncanonical_attestation 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "malformed head response retries" malformed_head 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "head transport failure recovers" head_retry_recovery 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  head-count ge 3
run_scenario "stalled head reports liveness" head_retry_liveness 1 \
  'Chain liveness failure' 'Payload-attestation scan incomplete' 4 1 4
run_scenario "wrong candidate slot is rejected" wrong_slot 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "invalid signature is rejected" invalid_attestation 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "missing attestation fields are rejected" missing_attestation_fields 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 3
run_scenario "short aggregation bits are rejected" short_aggregation_bits 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "newline-tainted fixed-width fields are rejected" trailing_newline_fields 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 4
run_scenario "mismatched attestation root is rejected" mismatched_attestation_root 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "case-variant matching root is accepted" case_variant_matching_root 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 1
run_scenario "malformed candidate response retries" malformed 0 \
  'Found available payload attestations in Gloas block at slot 1' - 30 1 0 \
  candidate-1-count eq 2
run_scenario "complete scan without payload reports absence" retry_empty 1 \
  'No canonical Gloas block exposed a payload attestation' - 30 1 0 \
  candidate-count eq 2

cleanup
echo "----"
printf '%d passed, %d failed\n' "$pass" "$fail"
[[ "$fail" -eq 0 ]]
