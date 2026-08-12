#!/usr/bin/env bash
# Decide whether the CI gate passes for a PR or merge-queue run.
#
# Exit 0 = gate passes (including a benign merge-queue reshuffle that cancelled
# this run). Exit 1 = real failure; the caller treats it as a failed check.
#
# Inputs (env): NEEDS (toJSON(needs)), RUN_CANCELLED ("true" when cancelled()),
# GITHUB_EVENT_NAME (reshuffle fast-path is merge_group-only),
# GH_TOKEN/GITHUB_REPOSITORY/GITHUB_RUN_ID to fetch the jobs list.
# Test seam: CI_GATE_NO_FETCH=1 uses CI_GATE_JOBS_JSON verbatim instead of the
# API (an empty value simulates a failed fetch) and CI_GATE_ANNOTATIONS_JSON
# ({"<job id>": ["<message>"]}) instead of the per-job annotations endpoint.
set -eo pipefail

needs="${NEEDS:-}"
[ -n "$needs" ] || needs="{}"
failed=$(jq -r 'to_entries[] | select(.value.result == "failure") | .key' <<<"$needs")
cancelled=$(jq -r 'to_entries[] | select(.value.result == "cancelled") | .key' <<<"$needs")

if [ -z "$failed" ] && [ -z "$cancelled" ]; then
  echo "All required jobs passed or were skipped."
  exit 0
fi

if [ -n "${CI_GATE_NO_FETCH:-}" ]; then
  raw="${CI_GATE_JOBS_JSON:-}"
else
  raw=$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs" --paginate) || raw=""
fi

# --paginate emits one JSON object per page; merge into a single {jobs:[...]}
# so a failure on any page is seen, and fail closed on an empty/invalid fetch.
jobs=""
if [ -n "$raw" ]; then
  jobs=$(jq -s '{jobs: [.[].jobs[]]}' <<<"$raw" 2>/dev/null) || jobs=""
fi

# A leaf that fast-cancels the run on its own failure rolls up to "cancelled",
# so a failed step still signals a real failure.
failed_steps=$(jq -r '.jobs[] | select(.name != "ci-gate") | select(any(.steps[]?; .conclusion == "failure")) | .name' <<<"$jobs" 2>/dev/null || true)

annotations_of() {
  if [ -n "${CI_GATE_NO_FETCH:-}" ]; then
    jq -r --arg id "$1" '.[$id] // [] | .[]' <<<"${CI_GATE_ANNOTATIONS_JSON:-{\}}" 2>/dev/null || true
    return 0
  fi
  gh api "repos/${GITHUB_REPOSITORY}/check-runs/$1/annotations" --jq '.[].message' 2>/dev/null || true
}

# `timeout-minutes` kills a job with conclusion "cancelled" — the same value an
# external cancel or a reshuffle produces — and lets its reporting step run on,
# so the failed step points at the report rather than the overrun. The runner's
# annotation is the only reliable discriminator.
timed_out_names=""
timeouts=""
while IFS=$'\t' read -r job_id job_name; do
  [ -n "$job_id" ] && [ "$job_id" != "null" ] || continue
  case "$(annotations_of "$job_id")" in
    *"exceeded the maximum execution time"*) ;;
    *) continue ;;
  esac
  overran=$(jq -r --arg n "$job_name" \
    '[.jobs[] | select(.name == $n) | .steps[]? | select(.conclusion == "cancelled") | .name] | join("; ")' \
    <<<"$jobs" 2>/dev/null || true)
  timed_out_names="${timed_out_names}${job_name}"$'\n'
  timeouts="${timeouts}::error title=CI timeout::${job_name} — exceeded its timeout-minutes budget${overran:+ while running: $overran}"$'\n'
done < <(jq -r '.jobs[] | select(.name != "ci-gate") | select(.conclusion == "cancelled") | "\(.id)\t\(.name)"' <<<"$jobs" 2>/dev/null || true)

# Run cancelled with no failure = GitHub tore down a superseded merge group
# (reshuffle); failing here would spuriously evict the PR. Scope to merge_group
# (reshuffles only happen in the queue) and require a successful jobs fetch so an
# empty failed_steps can be trusted. A timed-out leaf looks identical from the
# needs rollup, so it is excluded explicitly or it would merge unchecked.
if [ -z "$failed" ] && [ -n "$jobs" ] && [ -z "$failed_steps" ] && [ -z "$timeouts" ] && [ "${RUN_CANCELLED:-}" = "true" ] && [ "${GITHUB_EVENT_NAME:-}" = "merge_group" ]; then
  echo "::notice::Merge-queue reshuffle cancelled this run (no failed jobs or steps); passing the gate so the PR stays queued."
  echo "Cancelled jobs: $(tr '\n' ' ' <<<"$cancelled")"
  exit 0
fi

printf '%s' "$timeouts"
echo "The following gate jobs failed or were cancelled:"
echo "$failed"
echo "$cancelled"
# The leaf that fast-cancelled the run is the true root cause; other failed
# steps may be collateral. Timed-out leaves are already reported above, and
# their reporting step would otherwise be blamed for the overrun.
root_cause='"::error title=CI root cause::" + .name + " — failed step: " + ([.steps[] | select(.conclusion == "failure") | .name] | join("; "))'
# $n and $to are jq variables, not shell.
# shellcheck disable=SC2016
not_timed_out='select(.name as $n | ($to | split("\n") | map(select(length > 0))) | index($n) | not)'
root=$(jq -r --arg to "$timed_out_names" ".jobs[] | $not_timed_out | select(any(.steps[]?; .name == \"Cancel workflow run on failure\" and .conclusion == \"success\")) | $root_cause" <<<"$jobs" 2>/dev/null || true)
if [ -z "$root" ]; then
  root=$(jq -r --arg to "$timed_out_names" ".jobs[] | select(.name != \"ci-gate\") | $not_timed_out | select(any(.steps[]?; .conclusion == \"failure\")) | $root_cause" <<<"$jobs" 2>/dev/null || true)
fi
if [ -n "$root" ]; then
  echo "Root-cause job(s):"
  echo "$root"
fi
exit 1
