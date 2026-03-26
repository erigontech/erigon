# CI Guidelines

## The four trigger buckets

Every job belongs to exactly one of four buckets based on what it is trying to guarantee.
Mixing concerns across buckets undermines each bucket's contract.

| Bucket | Trigger | Contract |
|---|---|---|
| Pull request | `pull_request` | Fast signal on things devs cannot easily check locally |
| Merge queue | `merge_group` | Deterministic gate — a failure means the code is wrong |
| Push to main/release | `push` to `main`/`release/**` | Cache warming only — no test validation |
| Scheduled | `schedule` | Long-running or repeated runs not feasible on every commit |

All workflows should include `workflow_dispatch` so they can be triggered manually
without code changes. For workflows with inputs (e.g. QA), use dispatch inputs instead
of separate workflow files.

---

### Pull requests

PR checks exist to give developers early signal on things they cannot easily reproduce
locally — primarily cross-platform and cross-OS coverage. They are not the right place
for the full correctness gate.

**Belongs here:**
- Builds and smoke tests on operating systems other than the developer's own (Windows,
  Linux, macOS where not the primary dev platform)
- Lint (fast, not reproducible identically outside CI toolchain versions)
- Checks that fail quickly and cheaply, reducing wasted time submitting broken code to
  the merge queue

**Flaky tests belong here too.** A flaky test that runs on a PR is visible to the author
and can be prioritized. The same test running on `main` or a release branch produces a
failure that cannot be corrected and signals low-quality code to outside observers. Prefer
surfacing flakiness on the PR where it can be acted on.

**Does not belong here:**
- Tests that developers can run locally with `make test-short` or equivalent — those
  belong in the merge queue so runner time is not burned on every push.

#### Draft PRs

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

#### Required checks and path filters

Required checks must always report a status or they block the PR indefinitely.
Do **not** use workflow-level `paths`/`paths-ignore` on workflows with required checks.
Instead, use a step-level filter (e.g. `dorny/paths-filter`) so the job always runs and
can report "skipped" rather than going missing.

---

### Merge queue

Merge queue checks are the correctness gate. Their contract is strict:

- **A failure means the code is wrong.** If something fails in the merge queue, merging
  it would break `main`. There are no false positives.
- **A pass means the code is correct.** Code that passes the merge queue must not then
  fail on `main` or a release branch. There are no false negatives.

This means only deterministic, reproducible checks belong here. A flaky test does not
belong in the merge queue — by definition it cannot provide a reliable signal.

**Belongs here:**
- The full unit and integration test suite (things developers can reproduce with
  `make test-all` or equivalent)
- Race detector tests (`-race`), which are expensive but deterministic
- Any check where a failure must block the merge

**Does not belong here:**
- Flaky tests — they produce false positives and erode trust in the gate
- Checks that are not locally reproducible (those belong in the PR bucket)

GitHub's merge queue supports running jobs multiple times before deciding on a result.
This can mask flakiness in the short term but is not a long-term solution — it increases
queue latency and still occasionally lets flaky failures block the queue. The right fix
is to move flaky tests to the PR bucket and fix them there.

**Merge queue batching** — multiple PRs can be grouped into a single merge-group run,
reducing CI cost proportional to batch size. This works well when the merge queue gate
is fast and reliable; a flaky gate undermines batching by causing entire batches to be
re-queued.

---

### Push to main/release (cache warming)

After a PR is merged through the merge queue, a `push` event fires on `main` (or the
release branch). Correctness has already been established by the PR and merge queue
gates. The sole purpose of running CI gating workflows on this trigger is to **warm
caches** for future PR and merge queue runs on that branch. There is no value in
re-running the gate itself — it already passed.

Only workflows that already run as PR or merge queue checks should add a push-to-main
trigger, and only for cache warming. Workflows that do not gate PRs or the merge queue
have no reason to run on push to main at all.

**Never run tests on this trigger.** If a test fails on `main`, there is no PR to fix it
in, no author to notify, and no way to correct the commit. Failures here are noise that
misleads outside observers into thinking `main` is broken, when in fact the code passed
all required checks before landing.

For CI gating jobs that need a push-to-main run for cache warming, use conditional steps:

```yaml
- name: Run tests
  if: github.event_name != 'push'
  run: make test-all

- name: Build test binaries (cache warming only)
  if: github.event_name == 'push'
  run: go test -run=^$ ./...
```

`go test -run=^$` compiles every test binary — producing identical `GOCACHE` entries
to a full run — but executes nothing. The job always succeeds.

#### GitHub cache isolation

GitHub Actions caches are scoped by branch. A workflow run can restore caches from:

1. **Its own branch** — caches saved by earlier runs on the same branch.
2. **The base branch** (`main`) — caches saved by any workflow run on `main`.

It cannot access caches from other PRs or other branches. Without a `push` trigger on
`main`, every new PR starts cold. The push-to-main run exists to populate the
base-branch cache that all future PRs and merge-queue runs can restore from.

**Merge queue cache note** — `merge_group` runs use a temporary synthetic branch and
cannot save durable caches back to `main`. Only the `push`-triggered run after the
merge creates the base-branch cache entry.

---

### Scheduled

Scheduled jobs cover work that is too long-running or too expensive to attach to every
commit, or that is specifically designed to discover problems through repetition.

**Belongs here:**
- Very long-running test suites (multi-hour jobs) where per-commit triggering is not
  feasible
- Repeated runs of the test suite to surface flaky tests by statistical observation
- QA and regression detection workflows that aggregate results over time

**Known problem: scheduled failures are not a development priority.** Currently, QA
collects and reports on scheduled failures, but there is no mechanism that links a
scheduled failure back to the commit that introduced it. As a result, failures tend to
accumulate rather than get fixed.

Two directions under consideration:

1. **Fix the flaky or slow tests** so they can be promoted into the PR or merge queue
   buckets, where failures have a clear owner and block progress until resolved.

2. **Retroactively annotate commits.** Using an alternate GitHub token, a scheduled
   workflow could post a commit status or check run against the specific commit that
   first introduced a failure. This surfaces the regression at the point where it landed
   rather than at the point where the scheduled job happened to run, making it harder
   to ignore.

Neither approach excludes the other. Tests that can be made fast and deterministic
should be promoted; tests that are inherently long-running can remain scheduled but
with better failure attribution.

---

## CI gate and workflow_call

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

---

## Runtime goals

| Bucket | Cold target | Cached target |
|---|---|---|
| Pull request | ≤ 15 minutes | < 5 minutes |
| Merge queue | ≤ 30 minutes | < 10 minutes |
| Push to main/release | ≤ 15 minutes | < 5 minutes |
| Scheduled | no hard limit | — |

If a job regularly exceeds its cold target, fix the underlying cause — don't just raise
the timeout:

- Split large packages into smaller ones so Go's test cache works at finer granularity.
- Break a single large job into parallel jobs.
- Move tests that can be reproduced locally to the merge queue so they only run once
  per merge rather than on every PR push.

PRs that make tests significantly slower should include workflow changes to compensate.

---

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

---

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

---

## Checking benchmarks

The purpose of `make test-bench` in CI is to verify that benchmarks compile and
execute at least one iteration — not to produce meaningful performance numbers.

Benchmarks sized for profiling work can take minutes per iteration. Use
`testing.Short()` to trim parameter sweeps in CI:

```go
if testing.Short() {
    totalSteps = 10        // instead of 200+
    keyCount = 10_000      // instead of 1_000_000
}
```

`make test-bench` passes `-short` so these guards are active in CI.

`go test` forces benchmark packages to run **serially** regardless of the `-p` flag —
the serialization is enforced at the action-graph level when `-bench` is set. Reducing
per-iteration work (via `testing.Short()`) is the only effective way to reduce wall time.

---

## Local reproducibility

Every CI job should have a local equivalent. If you're changing code in `execution/`,
you should be able to run the corresponding test group locally
(`make test-group TEST_GROUP=execution-tests`) and get the same result as CI.

CI workflows should use the same Makefile targets that developers use locally rather
than inline shell commands. This keeps CI and local runs as similar as possible.

---

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
