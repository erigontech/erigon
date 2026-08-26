#!/usr/bin/env bash
# Put a PR back into the merge queue when the queue evicted it for a CI
# infrastructure failure — a dead runner or a broken runner network — rather
# than a failure of the PR's own checks. Called by merge-queue-requeue.yml.
#
# Exit 0 = handled (re-queued, or intentionally skipped with a logged reason).
# Exit 1 = operational error (bad input, enqueue mutation failed).
#
# Inputs (env): PR_NUMBER; PR_NODE_ID (resolved via the API when empty);
# EXPECTED_HEAD_OID; DEQUEUE_REASON (optional, from the event payload);
# GH_TOKEN for API reads and comments; ENQUEUE_TOKEN for enqueuePullRequest;
# GITHUB_REPOSITORY for the API; MAX_REQUEUES (default 3 per rolling 24 h);
# MQ_RUN_ID to classify a specific gate run instead of looking one up;
# MQ_DRY_RUN=1 to log mutations without performing them.
# Test seam: MQ_NO_FETCH=1 reads MQ_RUNS_JSON, MQ_JOBS_JSON,
# MQ_ANNOTATIONS_JSON ({"<job id>": ["<message>"]}), MQ_COMMENTS_JSON,
# MQ_PR_STATE_JSON and MQ_NOW instead of the API, and never mutates;
# MQ_ANNOTATIONS_FAIL=1 simulates the annotations endpoint erroring.
set -eo pipefail

if ! [[ "${PR_NUMBER:-}" =~ ^[0-9]+$ ]]; then
  echo "::error::PR_NUMBER must be a number, got '${PR_NUMBER:-}'"
  exit 1
fi

# Only CI-caused removals qualify. The reason enum's spelling is not pinned
# down by the docs, so deny-list the clearly non-CI reasons and let the
# job-level classification below gate everything else.
if grep -qiE 'manual|conflict|already[ _]?merged|branch[ _]?protect|queue[ _]?cleared|roll[ _]?back|invalid' \
  <<<"${DEQUEUE_REASON:-}"; then
  echo "::notice::Dequeue reason '${DEQUEUE_REASON}' is not a CI failure; not re-queuing PR #${PR_NUMBER}."
  exit 0
fi
if ! [[ "${EXPECTED_HEAD_OID:-}" =~ ^[[:xdigit:]]{40,64}$ ]]; then
  echo "::error::EXPECTED_HEAD_OID must be a Git object ID, got '${EXPECTED_HEAD_OID:-}'"
  exit 1
fi

dry_run() { [ -n "${MQ_NO_FETCH:-}" ] || [ -n "${MQ_DRY_RUN:-}" ]; }

if [ -n "${MQ_RUN_ID:-}" ]; then
  run_id="$MQ_RUN_ID"
  run_url="https://github.com/${GITHUB_REPOSITORY}/actions/runs/${run_id}"
else
  if [ -n "${MQ_NO_FETCH:-}" ]; then
    runs="${MQ_RUNS_JSON:-}"
  else
    runs=$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs?event=merge_group&per_page=100") || runs=""
  fi
  run=$(jq -c --arg pr "$PR_NUMBER" '
    [.workflow_runs[]?
     | select(.path == ".github/workflows/ci-gate.yml")
     | select(.head_branch // "" | test("^gh-readonly-queue/.+/pr-" + $pr + "-"))]
    | sort_by(.created_at) | last // empty' <<<"$runs" 2>/dev/null) || run=""
  if [ -z "$run" ]; then
    echo "::notice::No merge-queue gate run found for PR #${PR_NUMBER}; not re-queuing."
    exit 0
  fi
  run_id=$(jq -r '.id' <<<"$run")
  run_url=$(jq -r '.html_url' <<<"$run")
fi
echo "::notice::Classifying merge-queue gate run ${run_id} (${run_url})"

if [ -n "${MQ_NO_FETCH:-}" ]; then
  raw="${MQ_JOBS_JSON:-}"
else
  raw=$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${run_id}/jobs?per_page=100" --paginate) || raw=""
fi
jobs=""
if [ -n "$raw" ]; then
  jobs=$(jq -s '{jobs: [.[].jobs[]]}' <<<"$raw" 2>/dev/null) || jobs=""
fi
if [ -z "$jobs" ]; then
  echo "::warning::Could not fetch the jobs of run ${run_id}; not re-queuing."
  exit 0
fi

# Root-cause ("guilty") jobs, mirroring the shapes ci-gate-check.sh knows:
# - a leaf that fast-cancelled the run on its own failure (rolls up "cancelled"
#   but its succeeded cancel step marks it as the root cause);
# - a plainly failed leaf;
# - a leaf whose runner disappeared during assignment: its only recorded step
#   is a cancelled "Set up job".
# Zero-step cancellations, cascade victims, and timeout kills are not guilty
# here: a timed-out job may be hung by the PR's own bug, so it never qualifies
# for a re-queue.
guilty=$(jq -c '
  .jobs[]
  | select(.name != "ci-gate")
  | if any(.steps[]?; .name == "Cancel workflow run on failure" and .conclusion == "success")
      then {id, name, kind: "failed"}
    elif .conclusion == "failure"
      then {id, name, kind: "failed"}
    elif .conclusion == "cancelled"
         and (.steps | length) == 1
         and .steps[0].name == "Set up job"
         and .steps[0].conclusion == "cancelled"
      then {id, name, kind: "lost-at-setup"}
    else empty end' <<<"$jobs" 2>/dev/null) || guilty=""
if [ -z "$guilty" ]; then
  echo "::notice::No root-cause job found in run ${run_id} (reshuffle, timeout, or manual removal); not re-queuing."
  exit 0
fi

# An Actions job id is also its check-run id, so the annotations are one call
# away. Returns non-zero when the fetch itself failed, which must be read as
# "cannot classify", never as "no infra signature".
annotations_of() {
  if [ -n "${MQ_NO_FETCH:-}" ]; then
    [ -z "${MQ_ANNOTATIONS_FAIL:-}" ] || return 1
    jq -r --arg id "$1" '.[$id] // [] | .[]' <<<"${MQ_ANNOTATIONS_JSON:-{\}}" 2>/dev/null || true
    return 0
  fi
  gh api "repos/${GITHUB_REPOSITORY}/check-runs/$1/annotations" --paginate --jq '.[].message'
}

# Signatures of runner-infrastructure death. A failed job with none of these in
# its annotations is treated as a real failure, so the PR stays evicted.
infra_pattern='lost communication with the server|runner has received a shutdown signal|Could not resolve host|(proxy\.golang\.org|storage\.googleapis\.com).*(operation timed out|i/o timeout)'

infra_only=1
details=""
while IFS= read -r g; do
  [ -n "$g" ] || continue
  job_id=$(jq -r '.id' <<<"$g")
  job_name=$(jq -r '.name' <<<"$g")
  kind=$(jq -r '.kind' <<<"$g")
  if [ "$kind" = "lost-at-setup" ]; then
    details="${details}- \`${job_name}\` — runner lost before the job started"$'\n'
    continue
  fi
  if ! anns=$(annotations_of "$job_id"); then
    echo "::notice::Could not read annotations of job '${job_name}'; treating its failure as real and not re-queuing."
    infra_only=0
    break
  fi
  match=$(grep -iE "$infra_pattern" <<<"$anns" | head -1) || true
  if [ -z "$match" ]; then
    echo "::notice::Job '${job_name}' failed without an infrastructure signature; not re-queuing."
    infra_only=0
    break
  fi
  details="${details}- \`${job_name}\` — ${match:0:160}"$'\n'
done <<<"$guilty"

if [ "$infra_only" -ne 1 ]; then
  exit 0
fi
echo "::notice::All root-cause jobs of run ${run_id} are infrastructure failures:"
printf '%s' "$details"

if [ -n "${MQ_NO_FETCH:-}" ]; then
  pr_state="${MQ_PR_STATE_JSON:-}"
else
  # shellcheck disable=SC2016
  pr_state=$(gh api graphql \
    -F owner="${GITHUB_REPOSITORY%%/*}" \
    -F name="${GITHUB_REPOSITORY#*/}" \
    -F number="$PR_NUMBER" \
    -f query='query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) { id state headRefOid mergeQueueEntry { position } }
      }
    }' --jq '.data.repository.pullRequest') || pr_state=""
fi
if [ -z "$pr_state" ]; then
  echo "::warning::Could not read the state of PR #${PR_NUMBER}; not re-queuing."
  exit 0
fi
if [ "$(jq -r '.state' <<<"$pr_state")" != "OPEN" ]; then
  echo "::notice::PR #${PR_NUMBER} is not open; not re-queuing."
  exit 0
fi
if [ "$(jq -r '.mergeQueueEntry' <<<"$pr_state")" != "null" ]; then
  echo "::notice::PR #${PR_NUMBER} is already back in the merge queue; nothing to do."
  exit 0
fi
current_head_oid=$(jq -r '.headRefOid // empty' <<<"$pr_state")
if [ -z "$current_head_oid" ]; then
  echo "::warning::Could not read the head commit of PR #${PR_NUMBER}; not re-queuing."
  exit 0
fi
if [ "$current_head_oid" != "$EXPECTED_HEAD_OID" ]; then
  echo "::notice::PR #${PR_NUMBER} head changed from ${EXPECTED_HEAD_OID} to ${current_head_oid}; not re-queuing."
  exit 0
fi
if [ -z "${PR_NODE_ID:-}" ]; then
  PR_NODE_ID=$(jq -r '.id' <<<"$pr_state")
fi

post_comment() {
  if dry_run; then
    printf 'DRY-RUN: would comment:\n%s\n' "$1"
    return 0
  fi
  gh api "repos/${GITHUB_REPOSITORY}/issues/${PR_NUMBER}/comments" -f body="$1" >/dev/null
}

ledger_error() {
  echo "::error::Could not read the re-queue ledger for PR #${PR_NUMBER}; not re-queuing."
  exit 1
}

# Marker comments from the workflow bot double as the rate-limit ledger: at
# most MAX_REQUEUES automatic attempts per rolling 24 h, so a systematically
# broken runner pool cannot keep a PR looping through the queue.
marker='<!-- merge-queue-auto-requeue -->'
cap_marker='<!-- merge-queue-auto-requeue-cap -->'
ledger_author='github-actions[bot]'
now=${MQ_NOW:-$(date +%s)}
max=${MAX_REQUEUES:-3}
if [ -n "${MQ_NO_FETCH:-}" ]; then
  comments="${MQ_COMMENTS_JSON:-[]}"
else
  if ! comments=$(gh api "repos/${GITHUB_REPOSITORY}/issues/${PR_NUMBER}/comments?per_page=100" \
    --paginate --jq '.[] | {body, created_at, author: (.user.login // "")}' | jq -s '.'); then
    comments=""
  fi
fi
if ! jq -e 'type == "array"' <<<"$comments" >/dev/null 2>&1; then
  ledger_error
fi
recent_count() {
  jq --argjson now "$now" --arg m "$1" --arg author "$ledger_author" \
    '[.[] | select(.author == $author) | select(.body | contains($m)) | select((.created_at | fromdateiso8601) > ($now - 86400))] | length' \
    <<<"$comments" 2>/dev/null
}
if ! recent=$(recent_count "$marker"); then
  ledger_error
fi
if [ "$recent" -ge "$max" ]; then
  echo "::warning::Automatic re-queue limit reached (${recent}/${max} in 24 h) for PR #${PR_NUMBER}; leaving it out of the queue."
  if ! cap_recent=$(recent_count "$cap_marker"); then
    ledger_error
  fi
  if [ "$cap_recent" -eq 0 ] && ! post_comment "The merge queue evicted this PR on a CI infrastructure failure again, but the automatic re-queue limit (${max} per 24 h) is reached. Please investigate the runner infrastructure and re-queue manually.

${cap_marker}"; then
    echo "::warning::Could not post the automatic re-queue limit notice on PR #${PR_NUMBER}."
  fi
  exit 0
fi

if ! dry_run && [ -z "${ENQUEUE_TOKEN:-}" ]; then
  echo "::error::ENQUEUE_TOKEN is required to re-queue PR #${PR_NUMBER}."
  exit 1
fi

attempt=$((recent + 1))
if ! post_comment "The merge queue removed this PR after a CI **infrastructure** failure — a dead runner or a broken runner network, not a failure of the PR's own checks — so an automatic re-queue attempt is starting (attempt ${attempt}/${max} in 24 h).

Gate run: ${run_url}
${details}
${marker}"; then
  echo "::error::Could not record the re-queue attempt for PR #${PR_NUMBER}; not re-queuing."
  exit 1
fi

if dry_run; then
  echo "DRY-RUN: would enqueue PR #${PR_NUMBER} (${PR_NODE_ID})"
else
  # shellcheck disable=SC2016
  GH_TOKEN="$ENQUEUE_TOKEN" gh api graphql \
    -F id="$PR_NODE_ID" \
    -F expectedHeadOid="$EXPECTED_HEAD_OID" \
    -f query='mutation($id: ID!, $expectedHeadOid: GitObjectID!) {
      enqueuePullRequest(input: {pullRequestId: $id, expectedHeadOid: $expectedHeadOid}) {
        mergeQueueEntry { position }
      }
    }' >/dev/null || {
    echo "::error::enqueuePullRequest failed for PR #${PR_NUMBER}; re-queue it manually."
    exit 1
  }
fi

echo "::notice::Re-queued PR #${PR_NUMBER} after an infrastructure-classified gate failure."
