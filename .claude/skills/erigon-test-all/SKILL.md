---
name: erigon-test-all
description: Run the full Erigon test suite locally using GOGC=80 make test-all. Use for broad changes, failures in the full CI test suite, or an explicitly requested full local run. Equivalent to the "All tests" CI workflow.
---

# Erigon Full Test Suite

Runs the complete test suite with 60-minute timeout and coverage output. Takes ~30 minutes.

## Prerequisite: Test fixtures

`make test-all` no longer downloads any fixture tarballs. Execution spec tests moved out of `go test ./...` and into the dedicated `eest-spec-*` Makefile targets driven by the **EEST spec tests** workflow (`test-eest-spec.yml`); the consensus spec test (`cl/spectest`) is skipped here via `ERIGON_SKIP_CL_SPECTEST=true` (set automatically by the Makefile) and runs only in `test-integration-caplin.yml`.

To exercise the EEST suites locally, see `erigon-eest-spec` (or run a specific shard directly):

```bash
make eest-spec-statetests-stable-parallel
                                             # state tests vs eest_stable fixtures
make eest-spec-blocktests-stable-parallel    # blockchain tests vs eest_stable fixtures
make eest-spec-enginextests-stable-parallel  # engine-x tests vs eest_stable
make eest-spec-enginextests-devnet-parallel
                                             # devnet engine-x tests
make eest-spec-statetests-devnet-parallel
                                             # state tests vs eest_devnet fixtures
make eest-spec-transactiontests-{stable,devnet}-race
                                             # transaction tests with evm.race
make eest-spec-blocktests-devnet-parallel
                                             # devnet blocktests
make eest-spec-statetests-legacy-parallel
                                             # pinned legacy Cancun state-test archive
make eest-spec-rlptests-legacy-race          # complete pinned legacy RLP suite
make eest-spec-transactiontests-legacy-race  # complete pinned legacy transaction suite
make eest-spec-difficultytests-legacy-race   # complete pinned legacy difficulty suite
make eest-spec-blocktests-legacy-consensus-parallel
                                             # Hive consensus fixture selection;
                                             # -parallel and
                                             # -race-parallel variants too
make eest-spec-blocktests-legacy-constantinople-parallel
                                             # Hive legacy fixture selection;
                                             # -parallel plus three race partitions:
                                             # ...-race-constantinople,
                                             # ...-race-constantinople-fix, and
                                             # ...-race-other-forks
make eest-spec-blocktests-legacy-cancun-parallel
                                             # Hive legacy-cancun selection;
                                             # -parallel plus six race partitions:
                                             # ...-race-{berlin,shanghai,cancun,
                                             # london,paris,other-forks}
make eest-spec-enginextests-benchmark-1m-parallel
                                             # engine-x benchmark fixtures @ 1M gas target
                                             # (with per-test --time stats);
                                             # -5m/-10m/-30m/-60m/-100m/-150m variants too
make eest-spec-blocktests-stable-race-cancun-parallel
                                             # race-detector variant, sharded per fork:
                                             # -pre-cancun/-cancun/-prague/-osaka, plus
                                             # eest-spec-blocktests-devnet-race-amsterdam-parallel
```

The shard list / failure budgets / `commitment-parallel` flags live in `tools/eest-spec-shards.yml` (single source of truth for both this workflow and `tools/run-eest-spec-test.sh`). See `EEST_SPEC_SHARDS` / `EEST_SPEC_RACE_SHARDS` in the root `Makefile` for the partition into race vs non-race targets.

**Pitfall: stale `evm` / `evm.race` binary.** Always invoke shards via `make eest-spec-<shard>` — the Makefile lists `evm` (or `evm.race`) as a prereq and `go build` is cache-aware, so a stale binary gets rebuilt automatically. Calling `bash tools/run-eest-spec-test.sh <shard>` directly **bypasses** the rebuild and silently exercises whatever `build/bin/evm{,.race}` happens to be on disk against current fixtures, inflating failures or hiding regressions. After pulling code, switching branches, or any time you suspect the binary is older than HEAD: `rm -f build/bin/evm build/bin/evm.race && make evm evm.race` before re-running.

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

- When full local test verification is requested or needed to reproduce CI failures
- After significant logic changes to verify no edge cases break
- Select local lint and command builds using [Local verification in AGENTS.md](../../../AGENTS.md#local-verification).

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
