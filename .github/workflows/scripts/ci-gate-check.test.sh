#!/usr/bin/env bash
# Local test harness for ci-gate-check.sh — feeds crafted needs/jobs JSON
# through the real gate script and asserts its exit code.
# Run: bash .github/workflows/scripts/ci-gate-check.test.sh
set -uo pipefail

here=$(dirname -- "$0")
script="$here/ci-gate-check.sh"
pass=0
fail=0

# run_case <name> <want_exit> [VAR=VAL ...]
run_case() {
  local name="$1" want="$2"
  shift 2
  local out rc
  out=$(env -i PATH="$PATH" CI_GATE_NO_FETCH=1 "$@" bash "$script" 2>&1)
  rc=$?
  if [ "$rc" -eq "$want" ]; then
    printf 'ok   - %s (exit %d)\n' "$name" "$rc"
    pass=$((pass + 1))
  else
    printf 'FAIL - %s: want exit %d, got %d\n' "$name" "$want" "$rc"
    printf '%s\n' "$out" | sed 's/^/       | /'
    fail=$((fail + 1))
  fi
}

# run_output <name> <want_exit> <want_regex|-> <reject_regex|-> [VAR=VAL ...]
run_output() {
  local name="$1" want="$2" want_re="$3" reject_re="$4"
  shift 4
  local out rc why=""
  out=$(env -i PATH="$PATH" CI_GATE_NO_FETCH=1 "$@" bash "$script" 2>&1)
  rc=$?
  [ "$rc" -eq "$want" ] || why="want exit $want, got $rc"
  if [ -z "$why" ] && [ "$want_re" != "-" ] && ! grep -qE "$want_re" <<<"$out"; then
    why="output missing /$want_re/"
  fi
  if [ -z "$why" ] && [ "$reject_re" != "-" ] && grep -qE "$reject_re" <<<"$out"; then
    why="output unexpectedly matched /$reject_re/"
  fi
  if [ -z "$why" ]; then
    printf 'ok   - %s (exit %d)\n' "$name" "$rc"
    pass=$((pass + 1))
  else
    printf 'FAIL - %s: %s\n' "$name" "$why"
    printf '%s\n' "$out" | sed 's/^/       | /'
    fail=$((fail + 1))
  fi
}

# Everything green -> pass.
run_case "all success" 0 \
  NEEDS='{"lint":{"result":"success"},"tests":{"result":"success"}}'

# Skipped + success -> pass.
run_case "skipped + success" 0 \
  NEEDS='{"docs-site":{"result":"skipped"},"lint":{"result":"success"}}'

# A real failure -> fail (evict).
run_case "real failure" 1 \
  NEEDS='{"lint":{"result":"failure"},"tests":{"result":"success"}}' \
  CI_GATE_JOBS_JSON='{"jobs":[]}'

# Reshuffle: all-cancelled, run cancelled, no failed steps -> pass (stay queued).
run_case "reshuffle cancellation" 0 \
  NEEDS='{"hive":{"result":"cancelled"},"bench":{"result":"cancelled"},"lint":{"result":"success"}}' \
  RUN_CANCELLED=true \
  GITHUB_EVENT_NAME=merge_group \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"bench / benchmarks","steps":[{"name":"run","conclusion":"cancelled"}]}]}'

# Same cancellation on a non-merge_group run (PR supersede / manual cancel) is
# NOT a benign reshuffle -> fail closed even with no failures.
run_case "cancelled non-merge_group run -> fail closed" 1 \
  NEEDS='{"hive":{"result":"cancelled"},"lint":{"result":"success"}}' \
  RUN_CANCELLED=true \
  GITHUB_EVENT_NAME=pull_request \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"hive","steps":[{"name":"run","conclusion":"cancelled"}]}]}'

# Leaf timeout: cancelled need but run NOT cancelled -> fail (real problem).
run_case "leaf timeout (run not cancelled)" 1 \
  NEEDS='{"hive":{"result":"cancelled"},"lint":{"result":"success"}}' \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"hive","steps":[{"name":"run","conclusion":"cancelled"}]}]}'

# Fail-fast self-cancel: all-cancelled + run cancelled, but a failed step -> fail.
run_case "fail-fast self-cancel" 1 \
  NEEDS='{"hive":{"result":"cancelled"},"bench":{"result":"cancelled"}}' \
  RUN_CANCELLED=true \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"hive","steps":[{"name":"unit","conclusion":"failure"},{"name":"Cancel workflow run on failure","conclusion":"success"}]}]}'

# Reshuffle signal but jobs fetch failed (empty) -> fail closed.
run_case "reshuffle w/ failed jobs fetch (fail closed)" 1 \
  NEEDS='{"hive":{"result":"cancelled"}}' \
  RUN_CANCELLED=true \
  CI_GATE_JOBS_JSON=''

# --paginate emits one object per page; a failure on a later page must be seen.
run_case "multi-page, failure on page 2 -> fail" 1 \
  NEEDS='{"hive":{"result":"cancelled"},"bench":{"result":"cancelled"}}' \
  RUN_CANCELLED=true \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"hive","steps":[{"name":"run","conclusion":"cancelled"}]}]}
{"jobs":[{"name":"bench","steps":[{"name":"unit","conclusion":"failure"}]}]}'

# Multi-page with no failure on any page -> still a benign reshuffle.
run_case "multi-page reshuffle (no failures)" 0 \
  NEEDS='{"hive":{"result":"cancelled"},"bench":{"result":"cancelled"}}' \
  RUN_CANCELLED=true \
  GITHUB_EVENT_NAME=merge_group \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"hive","steps":[{"name":"run","conclusion":"cancelled"}]}]}
{"jobs":[{"name":"bench","steps":[{"name":"run","conclusion":"cancelled"}]}]}'

# An error page (no .jobs) among the pages must fail closed, not silently pass.
run_case "error page mid-pagination -> fail closed" 1 \
  NEEDS='{"hive":{"result":"cancelled"}}' \
  RUN_CANCELLED=true \
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"hive","steps":[{"name":"run","conclusion":"cancelled"}]}]}
{"message":"Server Error"}'

# A job killed by timeout-minutes reports conclusion "cancelled" and, because
# its reporting step still runs, a failed step that is not the real cause.
# Name the timeout and the step that overran; don't blame the reporting step.
run_output "leaf timeout is named as a timeout" 1 \
  'CI timeout::hive-eest .*glamsterdam-devnet.*Run hive tests and parse output' \
  'root cause' \
  NEEDS='{"hive-eest":{"result":"cancelled"},"lint":{"result":"success"}}' \
  CI_GATE_JOBS_JSON='{"jobs":[{"id":1,"name":"hive-eest / test-hive-eest (glamsterdam-devnet, serial)","conclusion":"cancelled","steps":[{"name":"Run hive tests and parse output","conclusion":"cancelled"},{"name":"Test Results","conclusion":"failure"}]}]}' \
  CI_GATE_ANNOTATIONS_JSON='{"1":["The job has exceeded the maximum execution time of 1h0m0s"]}'

# A cancelled leaf with no timeout annotation (runner dropped the job, external
# cancel) must not be mislabelled as a timeout.
run_output "external cancel is not a timeout" 1 \
  '-' 'CI timeout' \
  NEEDS='{"hive":{"result":"cancelled"},"lint":{"result":"success"}}' \
  CI_GATE_JOBS_JSON='{"jobs":[{"id":2,"name":"hive / test-hive (engine, api, parallel)","conclusion":"cancelled","steps":[{"name":"Set up job","conclusion":"cancelled"}]}]}' \
  CI_GATE_ANNOTATIONS_JSON='{"2":["The operation was canceled."]}'

# A timeout leaves no failed step at all when the reporting step is skipped, so
# the reshuffle fast-path would otherwise swallow it -> must still fail.
run_output "timeout is not absorbed by the reshuffle fast-path" 1 \
  'CI timeout::tests / tests-mac-linux' '-' \
  NEEDS='{"tests":{"result":"cancelled"},"lint":{"result":"success"}}' \
  RUN_CANCELLED=true \
  GITHUB_EVENT_NAME=merge_group \
  CI_GATE_JOBS_JSON='{"jobs":[{"id":3,"name":"tests / tests-mac-linux (windows-2025, parallel)","conclusion":"cancelled","steps":[{"name":"Run tests","conclusion":"cancelled"}]}]}' \
  CI_GATE_ANNOTATIONS_JSON='{"3":["The job has exceeded the maximum execution time of 1h0m0s"]}'

echo "----"
printf '%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
