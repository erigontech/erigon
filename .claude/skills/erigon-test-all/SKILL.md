---
name: erigon-test-all
description: Run the full Erigon test suite locally using GOGC=80 make test-all. Use this before marking a PR ready for review. Equivalent to the "All tests" CI workflow.
---

# Erigon Full Test Suite

Runs the complete test suite with 60-minute timeout and coverage output. Takes ~30 minutes.

## Prerequisite: Test fixtures

`make test-all` no longer downloads any fixture tarballs. EEST spec tests (state/blockchain/engine-x) moved out of `go test ./...` and into the dedicated `eest-spec-*` Makefile targets driven by the **EEST spec tests** workflow (`test-eest-spec.yml`); the consensus spec test (`cl/spectest`) is skipped here via `ERIGON_SKIP_CL_SPECTEST=true` (set automatically by the Makefile) and runs only in `test-integration-caplin.yml`.

To exercise the EEST suites locally, see `erigon-eest-spec` (or run a specific shard directly):

```bash
make eest-spec-statetests-stable             # state tests vs eest_stable fixtures
make eest-spec-blocktests-stable-sequential  # blockchain tests vs eest_stable fixtures (ERIGON_EXEC3_PARALLEL=false)
make eest-spec-blocktests-stable-parallel    # same, but with ERIGON_EXEC3_PARALLEL=true
make eest-spec-enginextests-stable-sequential # engine-x tests vs eest_stable (ERIGON_EXEC3_PARALLEL=false)
make eest-spec-enginextests-stable-parallel  # same, but with ERIGON_EXEC3_PARALLEL=true
make eest-spec-statetests-devnet             # …vs eest_devnet fixtures
make eest-spec-blocktests-devnet             # devnet blocktests (always parallel exec3)
make eest-spec-enginextests-benchmark-1m-sequential
                                             # engine-x benchmark fixtures @ 1M gas target
                                             # (with per-test --time stats);
                                             # -5m/-10m/-30m/-60m/-100m/-150m variants too,
                                             # each with a "-sequential" / "-parallel" pair
make eest-spec-blocktests-stable-race-cancun-sequential
                                             # race-detector variant, sharded per fork:
                                             # -pre-cancun/-cancun/-prague/-osaka, plus
                                             # eest-spec-blocktests-devnet-race-amsterdam.
                                             # Each stable-race sub-shard has a
                                             # "-sequential" / "-parallel" pair
                                             # (e.g. ...-race-cancun-{sequential,parallel})
```

The shard list / failure budgets / `exec3-parallel` flags live in `tools/eest-spec-shards.yml` (single source of truth for both this workflow and `tools/run-eest-spec-test.sh`). See `EEST_SPEC_SHARDS` / `EEST_SPEC_RACE_SHARDS` in the root `Makefile` for the partition into race vs non-race targets.

**Pitfall: stale `evm` / `evm.race` binary.** Always invoke shards via `make eest-spec-<shard>` — the Makefile lists `evm` (or `evm.race`) as a prereq and `go build` is cache-aware, so a stale binary gets rebuilt automatically. Calling `bash tools/run-eest-spec-test.sh <shard>` directly **bypasses** the rebuild and silently exercises whatever `build/bin/evm{,.race}` happens to be on disk against current fixtures, inflating failures or hiding regressions. After pulling code, switching branches, or any time you suspect the binary is older than HEAD: `rm -f build/bin/evm build/bin/evm.race && make evm evm.race` before re-running.

One side prerequisite still applies for tests `make test-all` does run:

```bash
git submodule update --init --recursive --force            # only for legacy-tests (TestLegacyCancunState)
```

The CI workflow handles this in `setup-erigon`; locally you must do it yourself.

## Prerequisite: Create RAM Disk

Before running `make test-all`, create a RAM disk and export its path as `ERIGON_EXECUTION_TESTS_TMPDIR`:

```bash
path=$(bash tools/create-ramdisk)
```

Then prepend `ERIGON_EXECUTION_TESTS_TMPDIR=$path` to the test command (see below). The `execution/tests` suite does heavy temp-file I/O; backing that with tmpfs avoids disk bottlenecks and matches how CI runs the same workflow (`ramdisk: true` in `test-all-erigon.yml`, which invokes the same `tools/create-ramdisk` script via `setup-erigon`).

The script is cross-platform (Linux tmpfs, macOS hdiutil, Windows ImDisk). Linux requires `sudo` to mount tmpfs at `/mnt/erigon-ramdisk`. Override size with `RAMDISK_SIZE_MB` (default 2048).

## Command

```bash
ERIGON_EXECUTION_TESTS_TMPDIR=$path GOGC=80 make test-all
```

Equivalent to the **"All tests"** GitHub Actions workflow (`test-all-erigon.yml`).

## When Tests Fail — Drill Down

When `make test-all` fails, identify and re-run just the failing package/test:

```bash
# Re-run just the failing package
go test --timeout 60m ./execution/stagedsync/...

# Re-run a specific test by name
go test --timeout 60m -run TestStagedSyncFoo ./execution/stagedsync/...

# Run with verbose output to see what's happening
go test --timeout 60m -v -run TestName ./path/to/package/...

# Run with GOGC to match CI memory pressure
GOGC=80 go test --timeout 60m ./path/to/package/...
```

## Find Which Package Failed

The output shows the failing package path. Look for lines like:
```
FAIL    github.com/erigontech/erigon/execution/stagedsync [build failed]
--- FAIL: TestName (12.34s)
```

Extract the import path after `github.com/erigontech/erigon/` and convert to a local path:
```
github.com/erigontech/erigon/execution/stagedsync → ./execution/stagedsync/
```

## Common Skips

Tests skipped via `-short` in `test-short` run fully here. If a test passes in `test-short` but fails here, it likely tests a slow path (large dataset, timeout, DB heavy).

## When to Use

- Before marking a PR ready for review
- After significant logic changes to verify no edge cases break
- Full gate: `git submodule update --init --recursive --force && path=$(bash tools/create-ramdisk) && make lint && make erigon integration && ERIGON_EXECUTION_TESTS_TMPDIR=$path GOGC=80 make test-all`

## CI Equivalent

| Local command | CI workflow | File |
|---------------|-------------|------|
| `GOGC=80 make test-all` | All tests | `test-all-erigon.yml` |
| `make eest-spec-<suite>-<fixtures>` | EEST spec tests | `test-eest-spec.yml` |
| `cd cl/spectest && make tests && make mainnet` | Consensus spec | `test-integration-caplin.yml` |

To dispatch remotely:
```bash
gh workflow run "All tests" --ref <branch>
gh workflow run "EEST spec tests" --ref <branch>
gh workflow run "Consensus spec" --ref <branch>
```
