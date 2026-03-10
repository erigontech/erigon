# CI Guidelines

## Workflow triggers

Each workflow should serve a distinct purpose with a distinct trigger pattern. Avoid
duplicating workflows that fire on the same events — merge them instead.

**Trigger ladder** (fastest feedback to most comprehensive):

| Stage | Trigger | Suitable for |
|---|---|---|
| All commits | `push` to `main`/`release/**`, `pull_request` on `ready_for_review`/`synchronize` | Lint, build, fast unit tests |
| Merge queue | `merge_group` | Full test suite, race tests, integration tests |
| Scheduled | `schedule` | Flaky-test discovery, regression detection, QA |

Run tests that developers can reproduce locally as late as possible — ideally only in
the merge queue. This avoids burning runner time on every push and concentrates
deterministic blocking checks at the point where they matter most.

All workflows should include `workflow_dispatch` so they can be triggered manually
without code changes. For workflows with inputs (e.g. QA), use dispatch inputs instead
of separate workflow files.

### Draft PRs

If a job uses `if: ${{ !github.event.pull_request.draft }}` to skip on draft PRs, the
workflow **must** include `ready_for_review` in its `pull_request` event types:

```yaml
on:
  pull_request:
    types:
      - opened
      - reopened
      - synchronize
      - ready_for_review
```

Without `ready_for_review`, converting a draft PR to ready-for-review fires an event
the workflow doesn't subscribe to, so the job never runs — the PR appears to have
skipped CI until the next push.

### Required checks and path filters

Required checks must always report a status or they block the PR indefinitely.
Do **not** use workflow-level `paths`/`paths-ignore` on workflows with required checks.
Instead, use a step-level filter (e.g. `dorny/paths-filter`) so the job always runs and
can report "skipped" rather than going missing.

## Runtime goals

| Run type | Target |
|---|---|
| Cached (source unchanged) | < 5 minutes |
| Cold / no cache | ≤ 30 minutes |

If a job regularly exceeds 30 minutes cold, fix the underlying cause — don't just raise
the timeout:

- Split large packages into smaller ones so Go's test cache works at finer granularity.
- Break a single large job into parallel jobs.
- Move slow tests to the merge queue so they don't block developer iteration.

PRs that make tests significantly slower should include workflow changes to compensate.

## Go test caching

Go's test cache keys each package result by the compiled test binary hash **plus** the
`mtime`/size of every file the test opens at runtime. A single file with a wrong mtime
invalidates the cache for the entire package.

### Fixture mtimes

**Main repo fixtures** — run `git restore-mtime` over testdata paths so each file's
mtime equals the commit that last modified it. This is deterministic and
content-sensitive.

**Submodule fixtures** — shallow clones (`--depth 1`) don't have enough history for
`git restore-mtime`. Instead, set every file's mtime to the submodule's HEAD commit
timestamp. All files in a submodule share the same mtime, which is stable as long as
the pinned commit doesn't change.

**Directory mtimes** — normalize all directories to a fixed epoch (e.g.
`200902132331.30`). Git doesn't track directory mtimes, so without this step they
reflect checkout time, which varies between runs.

### Keeping packages small

Go caches test results at the package level. A package with hundreds of test cases
that reads many fixture files will miss the cache if *any* fixture changes. Splitting
it into sub-packages (one per fixture set) means unrelated fixtures don't cause
unnecessary re-runs.

This is also why the `execution/tests/` package is broken into focused sub-packages
rather than one monolithic package.

## Required checks

Required checks exist to block *regressions*, not to enforce perfection.

- **Don't add a required check for a test that is already flaky.** Quarantine flaky
  tests first (skip them, move them to a scheduled run, or fix them), then make the
  check required.
- **Checks must not be time-sensitive.** A test that passed yesterday on the same code
  should pass today. Non-deterministic failures (timing, network, external state) must
  not be required.
- **Required checks should run on `pull_request` or `merge_group`, never only on
  `push` to `main`.** Checking after merge is too late — it creates a broken `main`
  that blocks everyone else.

### Flaky tests

Flaky tests should be discovered on scheduled runs that repeat the test suite, not
tolerated silently in required checks. When a flaky test is identified:

1. Skip or quarantine it so it no longer blocks PRs.
2. File a bug and fix it separately.
3. Re-enable it as a required check once it is stable.

## Local reproducibility

Every CI job should have a local equivalent so developers can pre-check before pushing.
If you're changing code in `execution/`, you should be able to run the corresponding
test group locally (`make test-group TEST_GROUP=execution-tests`) and get the same
result as CI.

CI workflows should use the same Makefile targets that developers use locally (e.g.
`make test-group`, `make lint`) rather than inline shell commands. This keeps CI and
local test runs as similar as possible, reducing "works locally but fails in CI"
surprises.

Tests that can be reproduced locally should preferably only block in the merge queue,
not on every PR push — this avoids burning runner time on checks developers can run
themselves.

## Debugging

Re-run with debug logging:

```bash
gh run rerun <run-id> --debug
```

Or in the UI: "Re-run jobs" → enable "Debug logging".

Raw logs include per-line timestamps useful for profiling slow steps:

```bash
gh run view <run-id> --log
```
