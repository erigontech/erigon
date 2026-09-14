#!/usr/bin/env bash
set -euo pipefail

beacon_url=${1:?beacon URL is required}
api_tmp_dir=${2:?temporary output directory is required}
poll_timeout=${GLOAS_POLL_TIMEOUT_SECONDS:-180}
target_advance=${GLOAS_POLL_TARGET_ADVANCE:-8}
poll_sleep=${GLOAS_POLL_SLEEP_SECONDS:-3}

curl_args=(
  --silent
  --show-error
  --connect-timeout 5
)

curl --fail "${curl_args[@]}" \
  --retry 4 \
  --retry-delay 1 \
  --retry-connrefused \
  --retry-max-time 30 \
  --max-time 15 \
  "$beacon_url/eth/v2/beacon/blocks/head" \
  --output "$api_tmp_dir/head.json"
initial_head_slot=$(jq -er '.data.message.slot | tonumber' "$api_tmp_dir/head.json")
target_head_slot=$((initial_head_slot + target_advance))
last_head_slot=$initial_head_slot
next_slot=$((initial_head_slot > 8 ? initial_head_slot - 8 : 1))
deadline=$((SECONDS + poll_timeout))
scan_incomplete=false
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
  if ! head_slot=$(jq -er '.data.message.slot | tonumber' "$api_tmp_dir/head.json"); then
    echo "Head block response was invalid; retrying"
    head_fetch_unavailable=true
    wait_for_next_poll || break
    continue
  fi
  head_fetch_unavailable=false
  last_head_slot=$head_slot
  pass_incomplete=false
  first_unresolved_slot=0

  if [ "$scan_incomplete" = true ] && ((next_slot > head_slot)); then
    pass_incomplete=true
    first_unresolved_slot=$next_slot
  fi

  for ((slot = next_slot; slot <= head_slot; slot++)); do
    if ((SECONDS >= deadline)); then
      scan_incomplete=true
      break 2
    fi

    remaining=$((deadline - SECONDS))
    request_timeout=$((remaining < 15 ? remaining : 15))
    slot_unresolved=false
    if candidate_http_code=$(curl "${curl_args[@]}" \
      --max-time "$request_timeout" \
      --write-out '%{http_code}' \
      "$beacon_url/eth/v2/beacon/blocks/$slot" \
      --output "$api_tmp_dir/candidate.json"); then
      if ((SECONDS >= deadline)); then
        scan_incomplete=true
        break 2
      fi

      case "$candidate_http_code" in
        200)
          if ! jq -e '
            (.version | type == "string") and
            (.data | type == "object") and
            (.data.message | type == "object") and
            (.data.message.slot | type == "string") and
            ((try (.data.message.slot | tonumber) catch null) != null) and
            (.data.message.body | type == "object") and
            (.data.message.body.payload_attestations | type == "array") and
            all(.data.message.body.payload_attestations[];
              (. | type == "object") and
              (.aggregation_bits | type == "string") and
              (.signature | type == "string") and
              (.data | type == "object") and
              (.data.slot | type == "string") and
              ((try (.data.slot | tonumber) catch null) != null) and
              (.data.payload_present | type == "boolean")
            )
          ' "$api_tmp_dir/candidate.json"; then
            echo "Candidate block response for slot $slot was invalid; retrying"
            slot_unresolved=true
          elif ! jq -e --argjson requested_slot "$slot" '
            (.data.message.slot | tonumber) as $block_slot |
            .version == "gloas" and
            $block_slot == $requested_slot and
            $block_slot > 0 and
            all(.data.message.body.payload_attestations[];
              (.data.slot | tonumber) == ($block_slot - 1) and
              (.aggregation_bits |
                test("^0x[0-9a-fA-F]+$") and
                (test("^0x0+$"; "i") | not)
              ) and
              (.signature |
                test("^0x[0-9a-fA-F]{192}$") and
                (test("^0x0+$"; "i") | not)
              )
            )
          ' "$api_tmp_dir/candidate.json"; then
            echo "Candidate block response for slot $slot was invalid; retrying"
            slot_unresolved=true
          elif jq -e '
            any(.data.message.body.payload_attestations[];
              .data.payload_present == true
            )
          ' "$api_tmp_dir/candidate.json"; then
            echo "Found available payload attestations in Gloas block at slot $slot"
            exit 0
          fi
          ;;
        404)
          if ((slot + 1 >= head_slot)); then
            slot_unresolved=true
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
      if ((first_unresolved_slot == 0)); then
        first_unresolved_slot=$slot
      fi
    elif ((first_unresolved_slot == 0)); then
      next_slot=$((slot + 1))
    fi
  done

  scan_incomplete=$pass_incomplete
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

if [ "$scan_incomplete" = true ]; then
  echo "::error::Payload-attestation scan incomplete before the deadline; last head slot $last_head_slot, next unverified slot $next_slot"
elif [ "$head_fetch_unavailable" = true ]; then
  echo "::error::Head polling was incomplete before the deadline; last observed head slot $last_head_slot"
elif ((last_head_slot < target_head_slot)); then
  echo "::error::Chain liveness failure: head only advanced from slot $initial_head_slot to $last_head_slot before the deadline"
else
  echo "::error::No canonical Gloas block exposed a payload attestation by head slot $last_head_slot"
fi
exit 1
