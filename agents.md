# Erigon Agent Guidelines

This file provides guidance for AI agents working with this codebase.

**Requirements**: Go 1.25+, GCC 10+ or Clang, 32GB+ RAM, SSD/NVMe storage

## Build & Test

```bash
make erigon              # Build main binary (./build/bin/erigon)
make integration         # Build integration test binary
make lint                # Run golangci-lint + mod tidy check
make test-short          # Quick unit tests (-short -failfast)
make test-all            # Full test suite with coverage
make gen                 # Generate all auto-generated code (mocks, grpc, etc.)
```

Before committing, always verify changes with: `make lint && make erigon integration`

## Architecture Overview

- Erigon is an Ethereum execution client
- Data flow: `db -> snapshots`
- `snapshots` are immutable
- `Unwind` beyond data in snapshots not allowed

## Local Spec Maps

For consensus-critical changes, especially under `cl/`, follow the local
`CLAUDE.md` spec maps required by the component `agents.md`. Local tests are
not sufficient evidence of consensus correctness.

## Key Directories

| Directory | Purpose | Component Docs |
|-----------|---------|----------------|
| `cmd/` | Entry points: erigon, rpcdaemon, caplin, sentry, downloader | - |
| `execution/stagedsync/` | Staged sync pipeline | [agents.md](execution/stagedsync/agents.md) |
| `db/` | Storage: MDBX, snapshots, ETL | [agents.md](db/agents.md) |
| `cl/` | Consensus layer (Caplin) | [agents.md](cl/agents.md) |
| `p2p/` | P2P networking (DevP2P) | [agents.md](p2p/agents.md) |
| `rpc/jsonrpc/` | JSON-RPC API | - |

## Running

```bash
./build/bin/erigon --datadir=./data --chain=mainnet
./build/bin/erigon --datadir=dev --chain=dev  # PoS dev mode
```

## Test-Driven Development

When fixing bugs or adding new features, follow the test-driven development (TDD) cycle: **Red → Green → Refactor**.

1. **Red** — write a failing test that specifies the desired behavior. Confirm it fails *for the right reason* (the behavior is missing or wrong), not because of a typo, missing import, or wrong setup.
2. **Green** — write the minimum production code needed to make the test pass. Do not write code the current failing test does not demand.
3. **Refactor** — clean up code and tests with the suite staying green. Skipping this step is how technical debt accumulates.

### For bug fixes

Reproduce the bug as a failing test **before** touching the fix. This proves three things at once: (a) the bug exists, (b) the agent/contributor understands it, and (c) the fix actually addresses it — the test flips red → green when the fix lands.

Never write the fix first and then "add a test for it" — that test only proves the code matches itself, not that it fixes the original defect. If the bug cannot be reproduced as a test, stop and either get more information or escalate; do not guess at a fix.

### For new features

- Drive the API shape from how the test wants to call it (outside-in).
- Start with the simplest meaningful behavior, not the full design.
- Add edge cases as separate Red → Green cycles, not bundled into one giant test.

### Anti-patterns

- **Test-after development**: writing code first, then "adding a test" — this is not TDD; the test merely echoes the code's existing behavior.
- **Tests that pass on the first run**: means the test did not actually drive anything; either the behavior already existed or the assertion is wrong.
- **Skipping the refactor step**: the suite is green but the design didn't improve.
- **Bundling many behaviors into one test**: makes failures hard to localize and refactors brittle.
- **Adding `t.Skip` instead of fixing a failing test**: forbidden for automated agents — see [Test skips](#test-skips) below.

### When pragmatism applies

TDD is the default for behavior changes (bug fixes, new logic, new endpoints). It applies less cleanly to:
- Pure refactors with no behavior change — existing tests are the safety net; do not write new tests just to satisfy the cycle.
- Exploratory spikes — throw the spike away and TDD the real implementation.
- Mechanical changes — renames, generated code regeneration, dependency bumps.

When skipping TDD for one of these reasons, say so explicitly in the PR description.

## Test skips

These rules apply project-wide — to every contributor and every automated agent (LLM coding assistants, CI bots, etc.).

### Why skips are dangerous

A failing test is a real failure to be diagnosed and fixed. Skipping it converts a loud "this is broken" signal into silence, then into a later surprise for whoever removes the skip and finds the bug still there. Concrete case: `#21153` removed a `t.Skip` for `TestGeneratedTraceApiCollision` documenting a known parallel-exec CREATE2-reincarnation bug that was never actually fixed (the inline comment claimed a fix on a branch that never merged), so removing it broke CI across downstream PRs.

### Valid reasons a skip may exist

The first two reasons require an explicit, linked tracking issue and apply to human contributors; an automated agent may not invoke them on its own initiative (only when a user explicitly directs it — see below). The third is test-suite partitioning and applies to every contributor, including automated agents.

1. **External test suites we import where we can't pass all the tests** — usually a feature we haven't implemented yet (e.g. an upstream Ethereum spec test). The skip documents the gap rather than hiding a regression.

2. **Flaky tests** — partially valid, with very low (not zero) tolerance. The rule is **reproduce locally and fix**; a skip is a last resort only after serious root-cause analysis (not "I ran it three times and it passed"), with that investigation attached to the tracking issue. The test owner is responsible for un-skipping once the flake is fixed.

3. **Long-running tests excluded from short mode** — a test too slow or resource-heavy for the quick `-short` suite may use the canonical guard `if testing.Short() { t.Skip("...") }` (message such as `"long-running test"`). It must still run in the full (non-`-short`) suite, the guard must depend only on `testing.Short()`, and no tracking issue is needed because this partitions the suite rather than hiding a defect. Never add the guard in response to a failure, hang, or flake.

For the first two reasons, the skip carries an inline comment linking the tracking issue, which is closed by removing the skip — not by closing it with the skip still in place.

### Rule for automated agents (LLM assistants etc.)

**Automated agents must never, on their own initiative, add a skip to hide unsupported behavior, a failure, or a flake** — not with a "the user can review it" framing, not as an `AskUserQuestion` option, not as a "tactical unblock" in a text answer, not behind a conditional or env-var gate. The sole standing exception is the canonical `testing.Short()` guard described above, for genuinely long-running tests.

When an agent encounters a failing test:
- Investigate: read logs, reproduce locally, narrow to a minimal repro
- Fix the underlying bug
- If genuinely unfixable in-session: (a) escalate with the findings, or (b) report a tracked blocker — never (c) skip it

If a flaky test blocks the agent's own CI iteration, the agent reproduces locally and either fixes it or hands off with the repro recipe. Adding a skip "just to get CI green" is exactly the pattern that produced #21153's surprise.

Except for that `testing.Short()` convention, this applies to all forms of test muting: `t.Skip`, `t.SkipNow`, `t.Skipf`, `SkipLoad`, `bt.SkipLoad`, build-tag exclusions, conditional bypasses behind `dbg.*` env flags, and removing tests from a runner matrix without a tracking issue. **All are off-limits for automated agents.**

If a user explicitly directs an agent to add any other skip in the current turn (overriding this rule for a specific case), the agent should still flag the trade-off and ensure a tracking issue exists.

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`

Do not add `Co-Authored-By: Claude` or `🤖 Generated with Claude Code` lines to commits, PRs, or issues — Claude attribution is disabled repo-wide via `.claude/settings.json` (`includeCoAuthoredBy: false`).

Run `make lint` before every push. The linter is non-deterministic — run it repeatedly until clean.

**Important**: Always run `make lint` after making code changes and before committing. Fix any linter errors before proceeding. PRs must pass `make lint` before being opened or updated.

## Code Style

### Comments

**Default: no comment.** Clear names and small focused functions read on their own. The vast majority of code — including code written by automated agents — should carry zero new comments. Before adding one, ask whether renaming, extracting a helper, or restructuring would remove the need. Almost always, it does.

A comment may be warranted for: a non-obvious invariant the types don't enforce; a workaround for a bug in a dependency or the runtime (link the issue/commit); a surprising edge case a reader would otherwise miss; a performance-sensitive choice where the obvious implementation would be wrong.

When a comment is genuinely required, it MUST be:

- **Concise, but err on the side of clarity.** Most comments fit in a sentence or two; keep an extra sentence when cutting it would cost understanding, and cut it when it only adds length — two clear sentences beat both a cryptic one-liner and a six-line essay. An explanation that needs sub-headings (`// Concurrency:` / `// Why:`) or bullet lists to stay organized has outgrown source: put the full story in the commit message, the PR description, or a design doc, and keep the distilled why in the code.
- **High-level, not scenario-specific.** Explain the invariant or gotcha in general terms. Don't walk through specific call sites, sequences of operations, or particular situations a reader could find with `grep`. The right level of abstraction is "what must remain true," not "what happened to me last Tuesday."
- **Free of forensic detail.** Strip dates (`// found on 2026-05-21`), devnet/branch names (`// seen on bal-devnet-7`), PR/issue/review references (`// flagged in #21314 round-4 review`), incident anecdotes, and "used by X, Y, Z" callsite lists. That history belongs in the commit message and PR description, where it survives intact; in source it rots, misleads later readers, and bloats the file.
- **Not a restatement of the code.** If a reader could delete the comment without losing information, delete it. Standard Go idioms and well-known library behavior don't need annotation.

A good comment:

```go
// Safe to close while read views are still iterating: the memStore backing
// makes Rollback a no-op on the data.
func (m *MemoryMutation) Rollback() { ... }
```

The same constraint written badly — long, name-dropping internal callers, threading review history through the source:

```go
// Rollback releases this mutation's local cursor cache and forwards Rollback
// to the backing in-memory tx / db. Concurrency invariant (load-bearing —
// see also Filters.WithOverlay): for a MemoryMutation created via
// NewMemoryBatch (the pure-Go memStore backing used by
// SharedDomains.blockOverlay), memTx.Rollback and memDb.Close are no-ops on
// the in-memory data — see memory_store.go. That is what makes it safe for
// the FCU bg-commit goroutine to call Close on the published BlockOverlay
// while concurrent RPC readers are still iterating views obtained via
// NewReadView / NewTemporalReadView. If this is ever switched to
// NewMemoryBatchMDBX (real MDBX backing, where Rollback DOES invalidate
// cursors), the bg-commit close + concurrent-RPC-reader pattern becomes
// unsafe and refcounting/drain logic is required...
```

If a constraint really needs to be enforced for the codebase's safety, prefer **code that enforces it** (a runtime assert, a type the caller can't misuse, a single private constructor) over a comment that describes it. A `panic` survives refactors; a long comment doesn't.

Function docstrings follow the same rule: a one-line summary, plus param/return notes only when the signature doesn't already say it. A docstring that needs sections is a sign the function does too much, or the explanation belongs elsewhere.

`// TODO` notes are only acceptable with a linked tracking issue and an owner. Better: file the issue and don't add the TODO; or fix it now.

**For automated agents specifically:** previous iterations of this guidance were not enough — agents kept producing multi-paragraph block comments enumerating call sites and incident history. The forensic-detail and scenario rules above are hard limits; length is a judgment call with clarity as the tiebreaker. When a comment grows, look at what the growth is made of: call-site inventories and incident history move to the commit message; a sentence that saves the reader a wrong guess stays.

## Pull Requests & Workflows

When manually dispatching a workflow that is not part of the PR's automatic check list, add a comment on the PR explaining which workflow was dispatched, why it was chosen, and include a direct link to the workflow run.

### Referring to numbered points in GitHub text

To refer to a numbered list item, point, step, or nit in PR descriptions, issue descriptions, or comments, write it in words — "point 1", "item 1", "the first nit". Use `#N` only as a reference to a GitHub issue or PR number; GitHub auto-links it, so a bare `#1` would point at an unrelated PR.

### Backport PRs to release branches

When opening a PR against `release/3.4` (or another release branch):

- **Title**: prefix with `[r3.4]` — e.g. `[r3.4] db/version: enforce upper-bound file version check`. Variants `[r34]` and `[3.4]` also appear but `[r3.4]` is the dominant form.
- **Body**: keep it short. For a straight cherry-pick: `Cherry-pick of #<N> to release/3.4.` When release-branch adjustments were needed (different APIs, skipped tests, etc.), add a `## r3.4-specific adaptations` section listing the deltas. When the change is not a cherry-pick of a merged PR (e.g. a dependency bump pulling a fix), describe what's being pulled in and why it's needed on the release branch.
- **Branch name**: keep a `3.4` marker. Common forms: `cherry-pick-<PR#>-to-release-3.4`, `cherry-pick-<PR#>-r34`, `cp/<PR#>-to-3.4`, or `<user>/<short>_34`.
- **Base branch**: `release/3.4`.

## Pre-push

Before running `git push`, always run `make lint` first and fix all issues. Run lint multiple times if needed — it is non-deterministic.

## Lint Notes

The linter (`make lint`) is non-deterministic in which files it scans — new issues may appear on subsequent runs. Run lint repeatedly until clean.

Common lint categories and fixes:
- **ruleguard (defer tx.Rollback/cursor.Close):** The error check must come *before* `defer tx.Rollback()`. Never remove an explicit `.Close()` or `.Rollback()` — add `defer` as a safety net alongside it, since the timing of the explicit call may matter.
- **prealloc:** Pre-allocate slices when the length is known from a range.
- **unslice:** Remove redundant `[:]` on variables that are already slices.
- **newDeref:** Replace `*new(T)` with `T{}`.
- **appendCombine:** Combine consecutive `append` calls into one.
- **rangeExprCopy:** Use `&x` in `range` to avoid copying large arrays.
- **dupArg:** For intentional `x.Equal(x)` self-equality tests, suppress with `//nolint:gocritic`.
- **Loop ruleguard in benchmarks:** For `BeginRw`/`BeginRo` inside loops where `defer` doesn't apply, suppress with `//nolint:gocritic`.

## Workflows

Make sure all scripts and shell code used from GitHub workflows is cross platform, for macOS, Windows and Linux.

Read [`CI-GUIDELINES.md`](CI-GUIDELINES.md) for guidelines before making changes to workflows.

## Go Test Caching

Go's test result cache keys on the mtime+size of every file read (via Go stdlib) during a test run. CI normalizes mtimes via `git restore-mtime` in `.github/actions/setup-erigon/action.yml` so that unchanged files get stable mtimes across runs.

**When a test reads a data file at runtime** (via `os.Open`, `os.ReadFile`, `os.Stat`, etc.) that lives outside a `testdata/` directory, it must be added to the `git restore-mtime` pattern list in `setup-erigon/action.yml`. Otherwise that package's test results will never be cached in CI.

Covered patterns already include `**/testdata/**`, `execution/tests/test-corners/**`, `cl/spectest/**/data_*/**`, `cl/transition/**/test_data/**`, `cl/utils/eth2shuffle/spec/**`, and `execution/state/genesiswrite/*.json`.
