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

Run specific tests:
```bash
go test ./execution/stagedsync/...
go test -run TestName ./path/to/package/...
```

## Architecture Overview

Erigon is a high-performance Ethereum execution client with embedded consensus layer. Key design principles:
- **Flat KV storage** instead of tries (reduces write amplification)
- **Staged synchronization** (ordered pipeline, independent unwind)
- **Modular services** (sentry, txpool, downloader can run separately)

### Domain State Layer

State is stored across 4 domains (Accounts, Storage, Code, Commitment). Each domain has a values table in MDBX (chaindata) and frozen snapshot files.

**Read path**: `SharedDomains.GetLatest` → in-memory batch → parent batch → state cache → `DomainRoTx.getLatestFromDb` (MDBX) → `getLatestFromFiles` (frozen snapshots, newest-first scan). Steps are encoded as inverted uint64 (`^uint64(step)`) for DupSort ordering.

**Write path**: `SharedDomains.DomainPut/DomainDel` → `TemporalMemBatch` → `DomainBufferedWriter.PutWithPrev/DeleteWithPrev` → `addValue` (ETL collector) + `DomainDiff.DomainUpdate` (changeset for unwind). Deletions are stored as 8-byte step-only entries (no value content).

**Unwind path**: Changesets (`ChangeSets3` table) are collected backwards per block, merged via `MergeDiffSets`, then applied by `DomainRoTx.unwind()` which deletes the current-step entry and restores the previous value. In changeset entries, `nil` value means the key belongs to a different step (skip restore), while `[]byte{}` means the key was deleted at that point (restore an empty tombstone to prevent fallthrough to stale file data). The `unwind()` function distinguishes these via `value != nil` rather than `len(value) > 0`.

**Frozen files lack tombstones**: deleted keys are simply absent from frozen/snapshot files. If a deletion entry in MDBX is discarded or missing, `getLatestFromFiles` falls through to older files and returns stale pre-deletion data. This is the root cause of stale-value bugs where `gas used mismatch` diffs equal exactly `SSTORE_SET - SSTORE_RESET = 17100`.

**Domain config** (`db/state/statecfg/state_schema.go`): StorageDomain and AccountsDomain use `LargeValues=false` (DupSort); CodeDomain and RCacheDomain use `LargeValues=true`.

**Changeset formats**: V0 (legacy, dictionary-based with prevStep) and V1 (post-df770fadfe, hasValue flag + value). `DeserializeDiffSet` auto-detects via first two bytes. V1 distinguishes nil (hasValue=0, different step) from `[]byte{}` (hasValue=1 + valLen=0, new key or was-deleted).

## Directory Structure

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
./build/bin/erigon --datadir=dev --chain=dev --mine  # Development
```

## Conventions

Commit messages: prefix with package(s) modified, e.g., `eth, rpc: make trace configs optional`

Cherry-pick PRs: when opening a PR that cherry-picks a commit to a `release/X.Y` branch, prepend the PR title with `[rX.Y]`, e.g., a cherry-pick to `release/3.4` → `[r3.4] eth, rpc: make trace configs optional`

**Important**: Always run `make lint` after making code changes and before committing. Fix any linter errors before proceeding. PRs must pass `make lint` before being opened or updated.

## Pull Requests & Workflows

When manually dispatching a workflow that is not part of the PR's automatic check list, add a comment on the PR explaining which workflow was dispatched, why it was chosen, and include a direct link to the workflow run.

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
