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
