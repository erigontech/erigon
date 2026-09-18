#!/usr/bin/env bash
set -euo pipefail

beacon_url=${1:?beacon URL is required}
api_tmp_dir=${2:?temporary output directory is required}
poll_timeout=${GLOAS_POLL_TIMEOUT_SECONDS:-180}
target_advance=${GLOAS_POLL_TARGET_ADVANCE:-8}
poll_sleep=${GLOAS_POLL_SLEEP_SECONDS:-3}

normalize_poll_config() {
  local name=$1 value=$2
  case "$value" in
    ''|*[!0-9]*)
      echo "$name must be an unsigned decimal integer" >&2
      return 2
      ;;
  esac
  if ((${#value} > 9)); then
    echo "$name is too large" >&2
    return 2
  fi
  printf '%d' "$((10#$value))"
}

poll_timeout=$(normalize_poll_config GLOAS_POLL_TIMEOUT_SECONDS "$poll_timeout")
target_advance=$(normalize_poll_config GLOAS_POLL_TARGET_ADVANCE "$target_advance")
poll_sleep=$(normalize_poll_config GLOAS_POLL_SLEEP_SECONDS "$poll_sleep")
max_head_slot=$((9223372036854775806 - target_advance))

curl_args=(
  --silent
  --show-error
  --connect-timeout 5
  --max-filesize 8388608
)

deadline=$((SECONDS + poll_timeout))
scan_incomplete=false
scan_incomplete_near_head_404_only=false
head_fetch_unavailable=false

wait_for_next_poll() {
  local remaining_time sleep_time
  remaining_time=$((deadline - SECONDS))
  if ((remaining_time <= 0)); then
    return 1
  fi
  sleep_time=$((poll_sleep < remaining_time ? poll_sleep : remaining_time))
  sleep "$sleep_time"
}

read_head_slot() {
  jq -ser --arg max_head_slot "$max_head_slot" '
    def canonical_decimal:
      type == "string" and
      length > 0 and
      (explode | all(.[]; . >= 48 and . <= 57)) and
      (. == "0" or (startswith("0") | not));
    select(length == 1) |
    .[0] |
    .data.message.slot as $slot |
    select($slot | canonical_decimal) |
    select(
      ($slot | length) < ($max_head_slot | length) or
      (($slot | length) == ($max_head_slot | length) and $slot <= $max_head_slot)
    ) |
    $slot
  ' "$api_tmp_dir/head.json"
}

initial_head_slot=
while ((SECONDS < deadline)); do
  remaining=$((deadline - SECONDS))
  request_timeout=$((remaining < 15 ? remaining : 15))
  if curl --fail "${curl_args[@]}" \
    --max-time "$request_timeout" \
    "$beacon_url/eth/v2/beacon/blocks/head" \
    --output "$api_tmp_dir/head.json" && initial_head_slot=$(read_head_slot); then
    if ((SECONDS >= deadline)); then
      initial_head_slot=
      head_fetch_unavailable=true
      break
    fi
    head_fetch_unavailable=false
    break
  fi
  echo "Initial head block response was unavailable or invalid; retrying"
  head_fetch_unavailable=true
  wait_for_next_poll || break
done

if [ -z "$initial_head_slot" ]; then
  echo "::error::Head polling was incomplete before the deadline; no valid head slot was observed"
  exit 1
fi

target_head_slot=$((initial_head_slot + target_advance))
last_head_slot=$initial_head_slot
next_slot=$((initial_head_slot > 8 ? initial_head_slot - 8 : 1))

while ((SECONDS < deadline)); do
  remaining=$((deadline - SECONDS))
  request_timeout=$((remaining < 15 ? remaining : 15))
  if ! curl --fail "${curl_args[@]}" \
    --max-time "$request_timeout" \
    "$beacon_url/eth/v2/beacon/blocks/head" \
    --output "$api_tmp_dir/head.json"; then
    head_fetch_unavailable=true
    wait_for_next_poll || break
    continue
  fi
  if ! head_slot=$(read_head_slot); then
    echo "Head block response was invalid; retrying"
    head_fetch_unavailable=true
    wait_for_next_poll || break
    continue
  fi
  if ((SECONDS >= deadline)); then
    head_fetch_unavailable=true
    break
  fi
  head_fetch_unavailable=false
  last_head_slot=$head_slot
  pass_incomplete=false
  pass_near_head_404_only=true
  first_unresolved_slot=0

  if [ "$scan_incomplete" = true ] && ((next_slot > head_slot)); then
    pass_incomplete=true
    pass_near_head_404_only=$scan_incomplete_near_head_404_only
    first_unresolved_slot=$next_slot
  fi

  for ((slot = next_slot; slot <= head_slot; slot++)); do
    if ((SECONDS >= deadline)); then
      scan_incomplete=true
      scan_incomplete_near_head_404_only=false
      break 2
    fi

    remaining=$((deadline - SECONDS))
    request_timeout=$((remaining < 15 ? remaining : 15))
    slot_unresolved=false
    slot_near_head_404=false
    if candidate_http_code=$(curl "${curl_args[@]}" \
      --max-time "$request_timeout" \
      --write-out '%{http_code}' \
      "$beacon_url/eth/v2/beacon/blocks/$slot" \
      --output "$api_tmp_dir/candidate.json"); then
      if ((SECONDS >= deadline)); then
        scan_incomplete=true
        scan_incomplete_near_head_404_only=false
        break 2
      fi

      case "$candidate_http_code" in
        200)
          if ! payload_available=$(jq -sr \
            --arg requested_slot "$slot" \
            --arg attestation_slot "$((slot - 1))" '
            def canonical_decimal:
              type == "string" and
              length > 0 and
              (explode | all(.[]; . >= 48 and . <= 57)) and
              (. == "0" or (startswith("0") | not));
            select(length == 1) |
            .[0] |
            .data.message.parent_root as $parent_root |
            (
              (.version | type == "string") and
              (.version == "gloas") and
              (.data | type == "object") and
              (.data.message | type == "object") and
              (.data.message.slot | canonical_decimal) and
              (.data.message.slot == $requested_slot) and
              (.data.message.parent_root | type == "string") and
              (.data.message.parent_root | test("^0x[0-9a-fA-F]{64}\\z")) and
              (.data.message.body | type == "object") and
              (.data.message.body.payload_attestations | type == "array") and
              all(.data.message.body.payload_attestations[];
                (. | type == "object") and
                (.aggregation_bits | type == "string") and
                (.signature | type == "string") and
                (.data | type == "object") and
                (.data.beacon_block_root | type == "string") and
                (.data.beacon_block_root | test("^0x[0-9a-fA-F]{64}\\z")) and
                (.data.slot | canonical_decimal) and
                (.data.slot == $attestation_slot) and
                (.data.payload_present | type == "boolean") and
                (.data.blob_data_available | type == "boolean") and
                (.data.beacon_block_root | ascii_downcase) == ($parent_root | ascii_downcase) and
                (.aggregation_bits |
                  test("^0x[0-9a-fA-F]{128}\\z") and
                  (test("^0x0+\\z"; "i") | not)
                ) and
                (.signature |
                  test("^0x[0-9a-fA-F]{192}\\z") and
                  (test("^0x0+\\z"; "i") | not)
                )
              )
            ) as $valid |
            select($valid) |
            any(.data.message.body.payload_attestations[];
              .data.payload_present == true
            )
          ' "$api_tmp_dir/candidate.json"); then
            echo "Candidate block response for slot $slot was invalid; retrying"
            slot_unresolved=true
          elif [[ "$payload_available" != true && "$payload_available" != false ]]; then
            echo "Candidate block response for slot $slot was invalid; retrying"
            slot_unresolved=true
          else
            if ((SECONDS >= deadline)); then
              scan_incomplete=true
              scan_incomplete_near_head_404_only=false
              break 2
            fi
            if [ "$payload_available" = true ]; then
              echo "Found available payload attestations in Gloas block at slot $slot"
              exit 0
            fi
          fi
          ;;
        404)
          if ((slot + 1 >= head_slot)); then
            slot_unresolved=true
            slot_near_head_404=true
          fi
          ;;
        *)
          echo "Candidate block fetch for slot $slot returned HTTP $candidate_http_code; retrying"
          slot_unresolved=true
          ;;
      esac
    else
      echo "Candidate block fetch for slot $slot failed; retrying"
      slot_unresolved=true
    fi

    if [ "$slot_unresolved" = true ]; then
      pass_incomplete=true
      if [ "$slot_near_head_404" = false ]; then
        pass_near_head_404_only=false
      fi
      if ((first_unresolved_slot == 0)); then
        first_unresolved_slot=$slot
      fi
    elif ((first_unresolved_slot == 0)); then
      next_slot=$((slot + 1))
    fi
  done

  scan_incomplete=$pass_incomplete
  scan_incomplete_near_head_404_only=$pass_near_head_404_only
  if [ "$scan_incomplete" = true ]; then
    next_slot=$first_unresolved_slot
    wait_for_next_poll || break
    continue
  fi
  if ((head_slot >= target_head_slot)); then
    break
  fi
  echo "Waiting for payload attestations: head slot $head_slot, target slot $target_head_slot"
  wait_for_next_poll || break
done

if [ "$scan_incomplete" = true ] && [ "$head_fetch_unavailable" = true ]; then
  echo "::error::Payload-attestation scan incomplete before the deadline; last head slot $last_head_slot, next unverified slot $next_slot"
  echo "::error::Head polling was incomplete before the deadline; last observed head slot $last_head_slot"
elif [ "$scan_incomplete" = true ] && [ "$scan_incomplete_near_head_404_only" = true ] && \
  [ "$head_fetch_unavailable" = false ] && ((last_head_slot < target_head_slot)); then
  echo "::error::Chain liveness failure: head only advanced from slot $initial_head_slot to $last_head_slot before the deadline"
elif [ "$scan_incomplete" = true ]; then
  echo "::error::Payload-attestation scan incomplete before the deadline; last head slot $last_head_slot, next unverified slot $next_slot"
elif [ "$head_fetch_unavailable" = true ]; then
  echo "::error::Head polling was incomplete before the deadline; last observed head slot $last_head_slot"
elif ((last_head_slot < target_head_slot)); then
  echo "::error::Chain liveness failure: head only advanced from slot $initial_head_slot to $last_head_slot before the deadline"
else
  echo "::error::No canonical Gloas block exposed a payload attestation by head slot $last_head_slot"
fi
exit 1
