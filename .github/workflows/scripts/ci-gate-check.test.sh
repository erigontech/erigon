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
  CI_GATE_JOBS_JSON='{"jobs":[{"name":"bench / benchmarks","steps":[{"name":"run","conclusion":"cancelled"}]}]}'

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

echo "----"
printf '%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
