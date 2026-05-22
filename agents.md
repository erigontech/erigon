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
./build/bin/erigon --datadir=dev --chain=dev --beacon.api=beacon,validator,node,config  # PoS dev mode
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

These rules apply project-wide — to every contributor and to every automated agent (LLM coding assistants, CI bots, etc.) working in this repository.

### Why skips are dangerous

A failing test is a real failure that must be diagnosed and fixed. Skipping it hides the failure and pushes the cost onto whoever later removes the skip — at which point the underlying bug is still there and now also surprises them. Skipping converts a loud "this is broken" signal into silence, then back into surprise. Concrete case: `#21153` removed a `t.Skip` for `TestGeneratedTraceApiCollision` that had documented a known parallel-exec SD/CREATE2-reincarnation bug; the underlying bug was never actually fixed (the comment said "fixed on `exec3/remove-rwtx-threading` branch" — that branch's fix never merged), so removing the skip suddenly red'd CI across downstream PRs (notably #21017).

### Two valid reasons a skip may exist

Both apply to human contributors. Both require an explicit, linked tracking issue. Neither permits an automated agent to add the skip on its own.

1. **External test suites we import where we can't pass all the tests** — typically because we haven't done the corresponding development yet (e.g., an upstream Ethereum spec test for a feature we haven't implemented). The skip documents the gap rather than hiding a regression.

2. **Flaky tests** — partially valid, with very low (not zero) tolerance. The general rule for flakes is **reproduce locally and fix**. Only after a serious attempt at local repro and root-cause analysis (not "I ran it three times and it passed") should a skip be considered. The tracking issue must include the local-repro investigation attached, and the test owner accepts responsibility for un-skipping once the flake is fixed.

In both cases the skip carries an inline comment with the linked tracking issue, and the issue gets closed by removing the skip — not by closing the issue with the skip still in place.

### Rule for automated agents (LLM assistants etc.)

**Automated agents must never add a skip. Period.** Not even with a "the user can review it" framing. Not as an option in `AskUserQuestion` menus. Not as a "tactical unblock" suggestion in text answers. Not behind any conditional or env-var gate.

When an agent encounters a failing test:
- Investigate the failure: read logs, reproduce locally, narrow to a minimal repro
- Fix the underlying bug
- If the agent genuinely can't fix it in-session, the correct outcomes are: (a) escalate to the user with the investigation findings, or (b) report it as a tracked blocker — never (c) skip it

If a flaky test is blocking the agent's own CI iteration, the agent reproduces locally and either fixes the flake or hands off to the user with the repro recipe. Adding a skip "just to get CI green" is exactly the pattern that produced #21153's surprise.

Applies to all forms of test muting: `t.Skip`, `t.SkipNow`, `t.Skipf`, `SkipLoad`, `bt.SkipLoad`, build-tag exclusions, conditional bypasses behind `dbg.*` env flags, removing tests from a runner matrix without a tracking issue. **All off-limits for automated agents.**

If a user explicitly directs an agent to add a skip in the current turn (overriding this rule for a specific case), the agent should still flag the trade-off and ensure a tracking issue exists.

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`

Run `make lint` before every push. The linter is non-deterministic — run it repeatedly until clean.

**Important**: Always run `make lint` after making code changes and before committing. Fix any linter errors before proceeding. PRs must pass `make lint` before being opened or updated.

## Code Style

### Comments

Prefer self-explanatory code over comments. Use clear names and small, focused functions so the code reads on its own. Default to writing no comment.

Add a comment only when the code itself can't tell the reader *why*:
- Workarounds for bugs in dependencies, the runtime, or other parts of the codebase (link the issue or commit when possible)
- Non-obvious invariants or constraints not enforced by types
- Surprising edge cases that are easy to miss when reading
- Performance-sensitive choices where the straightforward implementation would be wrong

Avoid:
- Restating what the code does (`// increment counter`)
- Referencing the current task, PR, or caller (`// added for the X flow`) — that belongs in the commit message
- Documenting standard Go idioms or well-known library behavior
- `// TODO` notes without a linked issue or owner

When a comment is warranted, keep it short and focused on the *why*. If a reader could delete the comment without losing information, it shouldn't have been written.

## Pull Requests & Workflows

When manually dispatching a workflow that is not part of the PR's automatic check list, add a comment on the PR explaining which workflow was dispatched, why it was chosen, and include a direct link to the workflow run.

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
