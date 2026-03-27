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

### CI gate and workflow_call

All PR and merge-queue checks run through `.github/workflows/ci-gate.yml`. It calls
each sub-workflow via `uses:` (reusable workflow call) rather than inlining the job
definitions. This avoids duplicating job definitions that also appear in standalone
`push`/`schedule` runs.

Use `workflow_call:` on a workflow (and call it from ci-gate) when the workflow has
other triggers besides PRs — `push` to `main`, `schedule`, etc. — so there is one
source of truth for the job definition.

Only inline job definitions directly into ci-gate if the workflow is exclusively
PR-gated with no other triggers, since there is then no duplication concern.

Workflows called by ci-gate must not have `pull_request:` triggers — ci-gate owns
PR coverage and a `pull_request:` trigger would cause every job to run twice.

ci-gate has a workflow-level `concurrency:` group that cancels the entire previous
run (all sub-workflow jobs) when a new PR push or merge_group event arrives. Sub-
workflows called via `workflow_call` must **not** define their own job-level
`concurrency:` for this purpose — `github.workflow` and `github.job` resolve to
the caller's values (or empty) in a `workflow_call` context, which causes collisions
that cancel sibling jobs within the same run.

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

### Cache warming and GitHub's cache isolation rules

GitHub Actions caches are scoped by branch. A workflow run can restore caches from:

1. **Its own branch** — caches saved by earlier runs on the same branch.
2. **The base branch** (`main`) — caches saved by any workflow run on `main`.

It cannot access caches from other PRs or other branches.

**The cold-start problem** — If a workflow only triggers on `pull_request`, it never
runs on `main` and therefore never populates the base-branch cache. Every new PR
opens to a cold cache, even if the codebase hasn't changed at all. Subsequent pushes
to the *same* PR do find each other's caches, but only until the GitHub-hosted cache
is evicted (repos share a 10 GB limit; busy repos evict older entries within hours).

**The fix** — add `push: {branches: [main, release/**]}` as a trigger. After each
merge, a run warms the build and module caches on `main`. All future PR first-pushes
restore from that warm base-branch cache instead of starting cold.

**Cache warming without test validation** — For workflows where correctness is already
enforced by the merge queue, the push-to-main run exists purely for cache warming. To
prevent benchmark or test flakiness from showing as failures on main commits, use
compile-only steps on non-PR events:

```yaml
- name: Run benchmarks
  if: github.event_name == 'pull_request'
  run: make test-bench

- name: Build test binaries (cache warming)
  if: github.event_name != 'pull_request'
  run: go test -run=^$ ./...
```

`go test -run=^$` compiles every test binary (identical GOCACHE entries to a full
run) but executes nothing. The job always succeeds, and the commit on `main` shows
green. If a benchmark were broken post-merge it would have been caught by the PR or
merge queue run before landing.

**Merge queues** — `merge_group` runs use a temporary synthetic branch
(`refs/heads/gh-readonly-queue/main/pr-N-SHA`). They can read from the base-branch
(`main`) cache, so they benefit from the warm cache created by the `push` trigger.
However, caches *saved* during a merge-group run are stored under that temporary
branch and become inaccessible once the merge group completes — they do not
contribute to the `main` cache. Only the `push`-triggered run after the merge creates
a durable base-branch cache.

**Implication for workflow design**: any workflow that should benefit from caching
across PRs and merge queue runs needs a `push` trigger on `main` (or a nightly
schedule on `main`). Without it, each PR and each merge-queue batch is always a cold
start.

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

## Memory- and disk-intensive tests

Some test packages allocate large databases or hold many files open simultaneously.
Running too many of them in parallel can exhaust RAM or IOPS and cause OOM kills or
spurious timeouts.

Use `-p` to limit the number of packages tested in parallel (default: `GOMAXPROCS`),
and `-parallel` to limit concurrency *within* a single package (default: `GOMAXPROCS`):

```bash
# At most 2 packages at a time, at most 4 subtests in parallel within each
go test -p 2 -parallel 4 ./...
```

These flags can be passed via `GO_FLAGS` in the Makefile:

```bash
make test-all GO_FLAGS="-p 2 -parallel 4"
```

Consider setting tighter defaults in the workflow matrix for jobs that are known to
be memory- or disk-heavy, rather than working around pressure by adjusting unrelated
constraints like timeouts or GC tuning.

## Checking benchmarks

The purpose of `make test-bench` in CI is to verify that benchmarks compile and
execute at least one iteration — not to produce meaningful performance numbers.

### Why benchmarks are slow by default

Many benchmarks are sized for profiling or comparison work: a single iteration can
take minutes. Go's benchmark runner will execute exactly 1 iteration in that case
(`-benchtime=1x`), so the `ns/op` figure is meaningless and the run just wastes
time.

The fix is to keep benchmark iteration work small enough to be loopable, and use
`testing.Short()` to trim parameter sweeps when all we need is a smoke test:

```go
if testing.Short() {
    totalSteps = 10        // instead of 200+
    keyCount = 10_000      // instead of 1_000_000
}
```

`make test-bench` passes `-short` so these guards are active in CI.

### Why you cannot parallelize across packages

`go test` forces benchmark packages to run **serially** regardless of the `-p` flag.
The serialization is enforced at the action-graph level in `cmd/go` when `-bench` is
set — each package run is added as a dependency of the previous one. Passing
`-p N` only affects compilation parallelism, not execution order.

The only way to reduce `make test-bench` wall time is to reduce the work done per
benchmark iteration, which is why right-sizing benchmarks (via `testing.Short()`) is
the correct approach rather than parallelizing the runner.

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
