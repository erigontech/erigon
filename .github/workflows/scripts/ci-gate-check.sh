#!/usr/bin/env bash
# Decide whether the CI gate passes for a PR or merge-queue run.
#
# Exit 0 = gate passes (including a benign merge-queue reshuffle that cancelled
# this run). Exit 1 = real failure; the caller treats it as a failed check.
#
# Inputs (env): NEEDS (toJSON(needs)), RUN_CANCELLED ("true" when cancelled()),
# GH_TOKEN/GITHUB_REPOSITORY/GITHUB_RUN_ID to fetch the jobs list.
# Test seam: CI_GATE_NO_FETCH=1 uses CI_GATE_JOBS_JSON verbatim instead of the
# API (an empty value simulates a failed fetch).
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

# Run cancelled with no failure = GitHub tore down a superseded merge group
# (reshuffle); failing here would spuriously evict the PR. Require a successful
# jobs fetch so an empty failed_steps can be trusted.
if [ -z "$failed" ] && [ -n "$jobs" ] && [ -z "$failed_steps" ] && [ "${RUN_CANCELLED:-}" = "true" ]; then
  echo "::notice::Merge-queue reshuffle cancelled this run (no failed jobs or steps); passing the gate so the PR stays queued."
  echo "Cancelled jobs: $(tr '\n' ' ' <<<"$cancelled")"
  exit 0
fi

echo "The following gate jobs failed or were cancelled:"
echo "$failed"
echo "$cancelled"
# The leaf that fast-cancelled the run is the true root cause; other failed
# steps may be collateral.
root=$(jq -r '.jobs[] | select(any(.steps[]?; .name == "Cancel workflow run on failure" and .conclusion == "success")) | "::error title=CI root cause::" + .name + " — failed step: " + ([.steps[] | select(.conclusion == "failure") | .name] | join("; "))' <<<"$jobs" 2>/dev/null || true)
if [ -z "$root" ]; then
  root=$(jq -r '.jobs[] | select(.name != "ci-gate") | select(any(.steps[]?; .conclusion == "failure")) | "::error title=CI root cause::" + .name + " — failed step: " + ([.steps[] | select(.conclusion == "failure") | .name] | join("; "))' <<<"$jobs" 2>/dev/null || true)
fi
echo "Root-cause job(s):"
echo "$root"
exit 1
