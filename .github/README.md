# CI Notes

## Required jobs

This can block on workflows or jobs that won't run because they haven't been appropriately triggered. This is a tricky spot. I think merge queues would fix this by providing a final gate where everything should run.

Required checks are opt-in, configured in repo settings under branch protection rules. By default no checks are required and PRs can merge regardless of workflow status.

If a workflow is skipped entirely (e.g. due to path filters), no check is reported for that commit SHA, and a required status check stays pending indefinitely, blocking the PR. To avoid this, don't use workflow-level `paths`/`paths-ignore` on workflows with required checks. Instead, use a paths-filter step inside the job (e.g. `dorny/paths-filter`) so the job always runs and reports a status, but skips the expensive steps when no relevant paths changed.

## Merge queues

Merge queues still rely on required checks. You select which checks the queue runs in branch protection settings. There's no "everything must pass" mode.

The queue creates a temporary branch with the PR merged on top of the latest base branch, runs checks against that speculative merge commit, and if they pass, fast-forwards the target branch to it. This prevents the "PR was green but broke after merge" problem.

Merge queues batch multiple PRs. When a batch fails, GitHub bisects: it splits the batch in half and retries each subset, continuing until it identifies which PR(s) caused the failure. The failing PR is ejected from the queue and the remaining PRs are re-batched and retested. Larger batches are more efficient when everything passes, but a single failure triggers multiple bisection rounds.

### Running tests only in the merge queue

The `merge_group` event triggers when a PR is added to the merge queue and GitHub creates the speculative merge commit:

```yaml
on:
  merge_group:
```

Remove `pull_request` and `push` from a workflow's `on:` triggers to run it exclusively in the queue. The tradeoff is that developers get no feedback until the PR enters the merge queue. A practical middle ground: run fast checks (lint, build) on `pull_request` for quick feedback, and defer expensive tests (full test suite, integration, hive) to `merge_group` only.

## Triggering events

Tests that can be run locally should run as late in the pull request phase as possible. So for example the all tests currently can run exclusively on auto-merge enabled. And any other event that indicates that it could be merged imminently.

### `pull_request` event types

If `types:` is omitted, the defaults are `opened`, `synchronize`, and `reopened`. Most CI workflows add `ready_for_review` so that draft PRs don't run CI until they're marked ready. The full list:

- `opened` — PR created
- `reopened` — closed PR reopened
- `synchronize` — new commit pushed to head branch (or force-push)
- `closed` — PR closed (merged or not; check `github.event.pull_request.merged`)
- `edited` — title, body, or base branch changed
- `ready_for_review` — PR moved from draft to non-draft
- `converted_to_draft` — PR moved from non-draft to draft
- `assigned` / `unassigned` — assignee added/removed
- `labeled` / `unlabeled` — label added/removed
- `locked` / `unlocked` — conversation locked/unlocked
- `review_requested` / `review_request_removed` — reviewer requested/removed
- `auto_merge_enabled` / `auto_merge_disabled`
- `milestoned` / `demilestoned`
- `enqueued` / `dequeued` — added to/removed from merge queue

### Path filters

For `pull_request` events, `paths`/`paths-ignore` compare the entire PR diff against the base branch, not just the latest commit. If any commit in the PR touches a matching path, the workflow triggers on every `synchronize` event, even if the new commit only changes unrelated files.

For `push` events, path filters compare against the previous commit on that branch (the before/after of that specific push), so filtering is more granular per-push.

### `push` vs `pull_request`

`push`-triggered runs satisfy required status checks on PRs. GitHub matches checks by commit SHA + check name, not by event type. The `push` event creates a check on the same SHA that is the PR's head commit.

`push` fires on every push to every matching branch regardless of whether a PR exists. `pull_request` only fires when there's an associated PR, making it the right trigger for "only run on branches with open PRs."

`github.ref` differs by event: `push` gives `refs/heads/<branch>`, `pull_request` gives `refs/pull/<number>/merge`. This matters for concurrency groups that use `github.event.pull_request.number` — that field is empty under `push` events.

## Go module caching

`$(go env GOMODCACHE)/cache/download` contains only the downloaded zip archives and metadata. Caching this instead of the full GOMODCACHE avoids storing the extracted source trees, roughly halving cache size. `go mod download` is self-healing: it checks what's already present and fills in any missing zips, so incomplete restores recover automatically.

## Cache key formats

### Go module cache (`setup-go` action)

| | Key |
|-|-----|
| Restore | `gomodcache-{os}-{hash(go.sum)}` |
| Restore fallback | `gomodcache-{os}-` |
| Save | `gomodcache-{os}-{hash(go.sum)}-{mod-contents-hash}` |

The save key appends a hash of the installed module directories so that cache entries are deduplicated when `go.sum` changes but the resolved set of modules doesn't.

### Go build cache (`restore-build-cache` / `save-build-cache` actions)

| | Key |
|-|-----|
| Save | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{branch}-{sha}-{run_id}` |
| Restore (exact) | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{branch}-{sha}-{run_id}` |
| Restore fallback 1 | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{branch}-{sha}-` |
| Restore fallback 2 | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{branch}-{parent}-` |
| Restore fallback 3 | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{branch}-` |
| Restore fallback 4 | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{base_ref}-{parent}-` |
| Restore fallback 5 | `gocache-{workflow_file}-{job}-{goversion}-{os}-{arch}-{base_ref}-` |

Each workflow+job combination gets its own cache namespace automatically via the workflow filename (e.g. `test-all-erigon`) and `github.job`. Branch-aware keys let PR caches restore from the base branch when no PR-specific cache exists.

The build cache contains a `testexpire.txt` file that controls test cache staleness. It holds a Unix timestamp in nanoseconds; cached test results from before that time are re-run. It's only updated by `go clean -testcache`. Separately, `trim.txt` controls disk eviction of old cache entries (both build and test) that haven't been accessed recently.

### golangci-lint cache (`lint` workflow)

| | Key |
|-|-----|
| Restore | `golangci-lint-{os}-{hash(go.mod)}` |
| Restore fallback | `golangci-lint-{os}-` |
| Save | same as restore exact key |

## TODO

- Most workflows should run on pull request to any branch.
- Tighten timeouts where possible.

## Debugging workflows

Re-run a failed run with debug logging enabled:

```bash
gh run rerun <run-id> --debug
```

Or in the UI, use the "Re-run jobs" dropdown and check "Enable debug logging."

This can also be enabled persistently by setting the repo secret/variable `ACTIONS_STEP_DEBUG` to `true` (verbose step output) or `ACTIONS_RUNNER_DEBUG` to `true` (runner diagnostic logs).

Raw logs (`gh run view <run-id> --log`) contain timestamps on every line, which can be used to measure how long individual operations take within a step (e.g. per-submodule checkout times).

## Notes that could be claude rules too

* Tests that can be reproduced locally, should always fail-fast and terminate the rest to not waste runner time.

https://github.com/erigontech/erigon/actions/metrics/performance?tab=jobs&filters=-runner_type%3Aself-hosted

GODEBUG with gocachehash, gocachetest, gocacheverify
