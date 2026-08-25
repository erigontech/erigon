#!/usr/bin/env bash
# Local test harness for merge-queue-requeue.sh — feeds crafted run/job/annotation
# JSON through the real script and asserts its exit code and decision output.
# Run: bash .github/workflows/scripts/merge-queue-requeue.test.sh
set -uo pipefail

here=$(dirname -- "$0")
script="$here/merge-queue-requeue.sh"
pass=0
fail=0

# Default fixtures: one matching gate run; overridden per case where needed.
RUNS_DEFAULT='{"workflow_runs":[
  {"id":111,"path":".github/workflows/ci-gate.yml","head_branch":"gh-readonly-queue/main/pr-23294-cafe","created_at":"2026-08-25T11:50:39Z","html_url":"https://example.test/runs/111"},
  {"id":90,"path":".github/workflows/lint.yml","head_branch":"gh-readonly-queue/main/pr-23294-cafe","created_at":"2026-08-25T11:55:00Z","html_url":"https://example.test/runs/90"},
  {"id":110,"path":".github/workflows/ci-gate.yml","head_branch":"gh-readonly-queue/main/pr-23294-beef","created_at":"2026-08-25T07:42:29Z","html_url":"https://example.test/runs/110"}
]}'

# A hosted runner died mid-job: the job is "failure" with no failed step, and
# the runner's annotation is the only signature (logs are never uploaded).
JOBS_RUNNER_DEATH='{"jobs":[
  {"id":1,"name":"tests / tests-mac-linux (macos-15, serial)","conclusion":"failure","steps":[{"name":"Set up job","conclusion":"success"}]},
  {"id":2,"name":"lint / lint","conclusion":"success","steps":[{"name":"Set up job","conclusion":"success"}]},
  {"id":3,"name":"ci-gate","conclusion":"failure","steps":[{"name":"Check all required jobs","conclusion":"failure"}]}
]}'
ANN_RUNNER_DEATH='{"1":["The hosted runner lost communication with the server. Anything in your workflow that terminates the runner process, starves it for CPU/Memory, or blocks its network access can cause this error."]}'

# The runner network broke: module downloads timed out, the job failed its test
# step and fast-cancelled the run, so it rolls up as "cancelled" with a
# succeeded cancel step. The hive-eest sibling is collateral of the cancel: its
# "Test Results" step failed only because the run was torn down around it.
JOBS_NETWORK_CASCADE='{"jobs":[
  {"id":10,"name":"tests / tests-mac-linux (macos-15, serial)","conclusion":"cancelled","steps":[{"name":"Set up job","conclusion":"success"},{"name":"Run all tests on macos-15 (serial commitment)","conclusion":"failure"},{"name":"Cancel workflow run on failure","conclusion":"success"}]},
  {"id":11,"name":"hive-eest / test-hive-eest (cancun, parallel)","conclusion":"cancelled","steps":[{"name":"Set up job","conclusion":"success"},{"name":"Test Results","conclusion":"failure"},{"name":"Cancel workflow run on failure","conclusion":"skipped"}]},
  {"id":12,"name":"ci-gate","conclusion":"failure","steps":[{"name":"Check all required jobs","conclusion":"failure"}]}
]}'
ANN_NETWORK='{"10":["github.com/RoaringBitmap/roaring/v2@v2.24.0: Get \"https://storage.googleapis.com/proxy-golang-org-prod/blob.zip\": read tcp 192.168.64.2:49196->142.251.219.155:443: read: operation timed out","unable to access '"'"'https://github.com/erigontech/erigon/'"'"': Could not resolve host: github.com"]}'

# A runner disappeared during job assignment: the only step is a cancelled
# "Set up job" and no runner name is ever recorded.
JOBS_LOST_AT_SETUP='{"jobs":[
  {"id":20,"name":"hive / test-hive (ethereum/engine, withdrawals, serial)","conclusion":"cancelled","steps":[{"name":"Set up job","conclusion":"cancelled"}]},
  {"id":21,"name":"hive / test-hive (ethereum/engine, withdrawals, parallel)","conclusion":"success","steps":[{"name":"Set up job","conclusion":"success"}]},
  {"id":22,"name":"ci-gate","conclusion":"failure","steps":[{"name":"Check all required jobs","conclusion":"failure"}]}
]}'

JOBS_REAL_FAILURE='{"jobs":[
  {"id":30,"name":"eest-spec-tests / eest-spec-blocktests-devnet-parallel","conclusion":"failure","steps":[{"name":"Set up job","conclusion":"success"},{"name":"Run tests","conclusion":"failure"}]},
  {"id":31,"name":"ci-gate","conclusion":"failure","steps":[{"name":"Check all required jobs","conclusion":"failure"}]}
]}'
ANN_REAL_FAILURE='{"30":["Process completed with exit code 2.","TestStateGasAuthBase failed: post-state root mismatch"]}'

# Mixed: an infra-lost job plus a real failure in the same run. The real
# failure must veto the re-queue no matter the order.
JOBS_MIXED='{"jobs":[
  {"id":20,"name":"hive / test-hive (ethereum/engine, withdrawals, serial)","conclusion":"cancelled","steps":[{"name":"Set up job","conclusion":"cancelled"}]},
  {"id":30,"name":"eest-spec-tests / eest-spec-blocktests-devnet-parallel","conclusion":"failure","steps":[{"name":"Set up job","conclusion":"success"},{"name":"Run tests","conclusion":"failure"}]},
  {"id":31,"name":"ci-gate","conclusion":"failure","steps":[{"name":"Check all required jobs","conclusion":"failure"}]}
]}'

# A timeout-minutes kill: cancelled with "Set up job" succeeded, no failed step
# and no succeeded cancel step. Not classifiable as infra — a hang can be the
# PR's own bug.
JOBS_TIMEOUT_ONLY='{"jobs":[
  {"id":40,"name":"kurtosis / assertoor","conclusion":"cancelled","steps":[{"name":"Set up job","conclusion":"success"},{"name":"Run","conclusion":"cancelled"}]},
  {"id":41,"name":"ci-gate","conclusion":"failure","steps":[{"name":"Check all required jobs","conclusion":"failure"}]}
]}'

MARKER='<!-- merge-queue-auto-requeue -->'
CAP_MARKER='<!-- merge-queue-auto-requeue-cap -->'
# MQ_NOW below is 2026-08-25T12:00:00Z; the 07:00Z comments are inside the
# 24 h window, the 08-20 ones far outside it.
COMMENTS_AT_LIMIT='[
  {"body":"requeue one '"$MARKER"'","created_at":"2026-08-25T06:00:00Z"},
  {"body":"requeue two '"$MARKER"'","created_at":"2026-08-25T07:00:00Z"},
  {"body":"requeue three '"$MARKER"'","created_at":"2026-08-25T11:00:00Z"}
]'
COMMENTS_AT_LIMIT_NOTIFIED='[
  {"body":"requeue one '"$MARKER"'","created_at":"2026-08-25T06:00:00Z"},
  {"body":"requeue two '"$MARKER"'","created_at":"2026-08-25T07:00:00Z"},
  {"body":"requeue three '"$MARKER"'","created_at":"2026-08-25T11:00:00Z"},
  {"body":"limit reached '"$CAP_MARKER"'","created_at":"2026-08-25T11:05:00Z"}
]'
COMMENTS_OLD='[
  {"body":"requeue one '"$MARKER"'","created_at":"2026-08-20T06:00:00Z"},
  {"body":"requeue two '"$MARKER"'","created_at":"2026-08-20T07:00:00Z"},
  {"body":"requeue three '"$MARKER"'","created_at":"2026-08-20T08:00:00Z"}
]'

# run_output <name> <want_exit> <want_regex|-> <reject_regex|-> [VAR=VAL ...]
# Later VAR=VAL pairs override the defaults injected before them.
run_output() {
  local name="$1" want="$2" want_re="$3" reject_re="$4"
  shift 4
  local out rc why=""
  out=$(env -i PATH="$PATH" MQ_NO_FETCH=1 \
    GITHUB_REPOSITORY=erigontech/erigon \
    PR_NUMBER=23294 \
    PR_NODE_ID=PR_test \
    MQ_NOW=1787659200 \
    MQ_PR_STATE_JSON='{"state":"OPEN","mergeQueueEntry":null,"id":"PR_test"}' \
    MQ_COMMENTS_JSON='[]' \
    MQ_RUNS_JSON="$RUNS_DEFAULT" \
    MQ_ANNOTATIONS_JSON='{}' \
    "$@" bash "$script" 2>&1)
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

run_file_match() {
  local name="$1" file="$2" pattern="$3"
  if grep -qE "$pattern" "$file"; then
    printf 'ok   - %s\n' "$name"
    pass=$((pass + 1))
  else
    printf 'FAIL - %s: %s does not match /%s/\n' "$name" "$file" "$pattern"
    fail=$((fail + 1))
  fi
}

# Hosted runner death (job "failure" + lost-communication annotation) -> requeue.
run_output "runner death -> requeue" 0 \
  "would enqueue PR #23294" "not re-queuing" \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# The requeue comment names the guilty job and carries the marker.
run_output "runner death -> comment with marker" 0 \
  "would comment" - \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Runner network death behind a fast-cancel cascade -> requeue; the collateral
# hive-eest job (failed step, no succeeded cancel step) must not veto it.
run_output "network death with cascade -> requeue" 0 \
  "would enqueue PR #23294" "not re-queuing" \
  MQ_JOBS_JSON="$JOBS_NETWORK_CASCADE" \
  MQ_ANNOTATIONS_JSON="$ANN_NETWORK"

# Runner lost during job assignment (cancelled "Set up job") -> requeue.
run_output "runner lost at setup -> requeue" 0 \
  "runner lost before the job started" "not re-queuing" \
  MQ_JOBS_JSON="$JOBS_LOST_AT_SETUP"

# A real test failure has no infra signature -> stay out of the queue.
run_output "real failure -> no requeue" 0 \
  "without an infrastructure signature" "would enqueue" \
  MQ_JOBS_JSON="$JOBS_REAL_FAILURE" \
  MQ_ANNOTATIONS_JSON="$ANN_REAL_FAILURE"

# Infra-lost job + real failure in one run -> the real failure vetoes.
run_output "mixed infra + real failure -> no requeue" 0 \
  "without an infrastructure signature" "would enqueue" \
  MQ_JOBS_JSON="$JOBS_MIXED" \
  MQ_ANNOTATIONS_JSON="$ANN_REAL_FAILURE"

# Timeout-only cancellation leaves no guilty job -> no requeue.
run_output "timeout-only -> no requeue" 0 \
  "No root-cause job" "would enqueue" \
  MQ_JOBS_JSON="$JOBS_TIMEOUT_ONLY"

# Manual removal from the queue is never overridden.
run_output "manual dequeue reason -> skip" 0 \
  "not a CI failure" "would enqueue" \
  DEQUEUE_REASON=MANUAL \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# A CI-failure reason must pass the reason filter.
run_output "ci failure reason -> proceeds" 0 \
  "would enqueue PR #23294" - \
  DEQUEUE_REASON=CI_FAILURE \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Someone already put the PR back -> do not enqueue twice.
run_output "already queued again -> skip" 0 \
  "already" "would enqueue" \
  MQ_PR_STATE_JSON='{"state":"OPEN","mergeQueueEntry":{"position":1},"id":"PR_test"}' \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Closed/merged PRs are left alone.
run_output "merged PR -> skip" 0 \
  "not open" "would enqueue" \
  MQ_PR_STATE_JSON='{"state":"MERGED","mergeQueueEntry":null,"id":"PR_test"}' \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# At the 24 h limit: no enqueue, and a one-time cap notice is posted.
run_output "rate limit reached -> cap notice, no enqueue" 0 \
  "limit reached" "would enqueue" \
  MQ_COMMENTS_JSON="$COMMENTS_AT_LIMIT" \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

run_output "rate limit reached -> posts the cap notice" 0 \
  "would comment" - \
  MQ_COMMENTS_JSON="$COMMENTS_AT_LIMIT" \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Cap notice already posted -> stay silent.
run_output "rate limit already notified -> silent skip" 0 \
  "limit reached" "would comment|would enqueue" \
  MQ_COMMENTS_JSON="$COMMENTS_AT_LIMIT_NOTIFIED" \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Markers older than 24 h do not count against the limit.
run_output "old markers ignored -> requeue" 0 \
  "would enqueue PR #23294" "limit reached" \
  MQ_COMMENTS_JSON="$COMMENTS_OLD" \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Unreadable annotations -> fail closed (cannot prove infra).
run_output "annotations fetch failure -> no requeue" 0 \
  "Could not read annotations" "would enqueue" \
  MQ_ANNOTATIONS_FAIL=1 \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH"

# No gate run found for this PR -> nothing to classify.
run_output "no matching gate run -> skip" 0 \
  "No merge-queue gate run" "would enqueue" \
  MQ_RUNS_JSON='{"workflow_runs":[{"id":7,"path":".github/workflows/ci-gate.yml","head_branch":"gh-readonly-queue/main/pr-999-cafe","created_at":"2026-08-25T11:00:00Z","html_url":"https://example.test/runs/7"}]}' \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH"

# The newest matching gate run is the one classified.
run_output "picks newest matching run" 0 \
  "run 111" - \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH" \
  MQ_ANNOTATIONS_JSON="$ANN_RUNNER_DEATH"

# Garbage PR number is an operational error.
run_output "invalid PR number -> error" 1 \
  - "would enqueue" \
  PR_NUMBER=abc \
  MQ_JOBS_JSON="$JOBS_RUNNER_DEATH"

run_file_match "failed gate dispatches trusted handler" "$here/../ci-gate.yml" \
  'gh workflow run merge-queue-requeue\.yml'
run_file_match "handler accepts workflow dispatch" "$here/../merge-queue-requeue.yml" \
  '^  workflow_dispatch:$'
run_file_match "handler mints a GitHub App token" "$here/../merge-queue-requeue.yml" \
  'actions/create-github-app-token@'
run_file_match "App token can manage the merge queue" "$here/../merge-queue-requeue.yml" \
  'permission-merge-queues:[[:space:]]*write'
run_file_match "handler passes a separate enqueue token" "$here/../merge-queue-requeue.yml" \
  'ENQUEUE_TOKEN:.*steps\.app_token\.outputs\.token'
run_file_match "enqueue mutation uses the App token" "$script" \
  'GH_TOKEN="[$]ENQUEUE_TOKEN" gh api graphql'

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
