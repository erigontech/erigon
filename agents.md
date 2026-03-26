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

Read [`.github/README.md`](.github/README.md) for guidelines before making changes to workflows.

## Go Test Caching

Go's test result cache keys on the mtime+size of every file read (via Go stdlib) during a test run. CI normalizes mtimes via `git restore-mtime` in `.github/actions/setup-erigon/action.yml` so that unchanged files get stable mtimes across runs.

**When a test reads a data file at runtime** (via `os.Open`, `os.ReadFile`, `os.Stat`, etc.) that lives outside a `testdata/` directory, it must be added to the `git restore-mtime` pattern list in `setup-erigon/action.yml`. Otherwise that package's test results will never be cached in CI.

Covered patterns already include `**/testdata/**`, `execution/tests/test-corners/**`, `cl/spectest/**/data_*/**`, `cl/transition/**/test_data/**`, `cl/utils/eth2shuffle/spec/**`, and `execution/state/genesiswrite/*.json`.

## Concurrent Commitment Architecture

`ConcurrentPatriciaHashed` (`execution/commitment/hex_concurrent_patricia_hashed.go`) splits the hex trie into 16 subtries by first nibble. During `ParallelHashSort`, each subtrie runs in its own goroutine with an independent `TrieContext` for **reads** (own `roTx`). **Writes** (`PutBranch`) are isolated via per-goroutine `etl.Collector` instances (field `TrieContext.localCollector`). The `concurrentTrieContextFactory` in `commitmentdb/commitment_context.go` creates these collectors and tracks them in a mutex-protected slice. After all goroutines complete, `ComputeCommitment` self-drains the collectors by loading each entry and writing it via the main `trieContext.PutBranch` (which routes through `DomainPut` since the main context has no `localCollector`). This happens automatically for all callers. The root fold after `g.Wait()` is single-threaded and uses the normal `DomainPut` path (no local collector).

### Activation

- **Normal sync/execution:** `--experimental.concurrent-commitment` CLI flag sets `statecfg.ExperimentalConcurrentCommitment` globally.
- **Rebuild path:** `ERIGON_REBUILD_CONCURRENT_COMMITMENT=true` env var enables it only for `RebuildCommitmentFiles` (in `db/state/squeeze.go`). This also calls `domains.EnableParaTrieDB(rwDb)` to provide the DB handle for per-goroutine read transactions.

Key packages:
- `execution/commitment` — trie logic, `PatriciaContext` interface, `Trie` interface
- `execution/commitment/commitmentdb` — `TrieContext` (implements `PatriciaContext`), `SharedDomainsCommitmentContext`, `trieContextFactory`, `concurrentTrieContextFactory`
- `db/state` — `DomainBufferedWriter`, `TemporalMemBatch`, squeeze/rebuild (`rebuildCommitmentShard`)
- `db/state/execctx` — `SharedDomains`, `temporalPutDel` (bridges `TrieContext.PutBranch` to domain writers)

The `sd` interface in `commitmentdb` is the abstraction boundary to `SharedDomains` (provides `AsGetter`, `AsPutDel`, `StepSize`, `TxNum`).

## Domain Value Format (ETL Encoding)

For `LargeValues=false` domains (**Accounts, Storage, Commitment**), `DomainBufferedWriter.addValue` prepends an 8-byte inverted step (`^step` as big-endian uint64) to the **value** before collecting into ETL. `collateETL` strips these 8 bytes when building `.kv` files.

For `LargeValues=true` domains (**Code, RCache**), the 8-byte inverted step is appended to the **key** instead; values are stored unchanged.

This format mirrors the MDBX table layout where DupSort uses the step prefix for versioning within the same key.

**Per-goroutine collectors (concurrent commitment):** Store raw `(prefix, branchData)` pairs *without* the step prefix. After `Process` completes, `ComputeCommitment` self-drains the collectors via `trieContext.PutBranch(k, v, nil)`, which routes through `DomainPut` → `PutWithPrev` → `addValue` (adding the step prefix automatically). Do not pre-encode the step in `localCollector.Collect` calls.
