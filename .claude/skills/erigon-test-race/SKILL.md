---
name: erigon-test-race
description: Run Erigon tests with Go race detector to find data races and concurrency bugs. Use this for concurrency-sensitive changes (parallel executor, p2p, txpool). Takes 30-60 minutes.
---

# Erigon Race Detector Tests

Runs the full test suite with Go's `-race` flag. Catches concurrency bugs that normal tests miss. Takes 30–60 minutes.

## Prerequisite: Test fixtures

`make test-all-race` no longer downloads any fixture tarballs. EEST spec tests moved out of `go test ./...` and into the dedicated `eest-spec-*` Makefile targets driven by the **EEST spec tests** workflow (`test-eest-spec.yml`); the consensus spec test (`cl/spectest`) is skipped here via `ERIGON_SKIP_CL_SPECTEST=true` (set automatically by the Makefile) and runs only in `test-integration-caplin.yml`.

Use the dedicated EEST race shards:

```bash
make eest-spec-{rlptests,transactiontests,difficultytests}-legacy-race
make eest-spec-blocktests-stable-race-{pre-cancun,cancun,prague,osaka}-{sequential,parallel}
make eest-spec-blocktests-devnet-race-amsterdam-{sequential,parallel}
make eest-spec-blocktests-legacy-consensus-race-{sequential,parallel}
make eest-spec-blocktests-legacy-constantinople-race-{constantinople,constantinople-fix,other-forks}-{sequential,parallel}
make eest-spec-blocktests-legacy-cancun-race-{berlin,shanghai,cancun,london,paris,other-forks}-{sequential,parallel}
```

These targets build a race-instrumented `evm.race` binary automatically (see `EEST_SPEC_RACE_SHARDS` in the root `Makefile`). The stable, devnet, and legacy blocktest `-sequential` / `-parallel` pairs pin both commitment modes; execution remains parallel in every pair. For the consensus spec suite or other Go packages, pass `GOFLAGS='-race'` or invoke `go test -race` against the relevant package directly.

**Pitfall: stale `evm.race` binary.** `make eest-spec-<race-shard>` lists `evm.race` as a prereq and `go build` is cache-aware, so a stale binary gets rebuilt. Calling `bash tools/run-eest-spec-test.sh <shard>` directly with `EVM_BIN=build/bin/evm.race` **bypasses** the rebuild and silently runs an old race-instrumented binary against current fixtures — race reports against code that no longer exists, missed races against code that does. After pulling or switching branches: `rm -f build/bin/evm.race && make evm.race` before re-running.

## Prerequisite: Create RAM Disk

Before running `make test-all-race`, create a RAM disk and export its path as `ERIGON_EXECUTION_TESTS_TMPDIR`:

```bash
path=$(bash tools/create-ramdisk)
```

Then prepend `ERIGON_EXECUTION_TESTS_TMPDIR=$path` to the test command (see below). The `execution/tests` suite does heavy temp-file I/O; backing that with tmpfs avoids disk bottlenecks and matches how CI runs the same workflow (`ramdisk: true` in `test-all-erigon-race.yml`, which invokes the same `tools/create-ramdisk` script via `setup-erigon`).

The script is cross-platform (Linux tmpfs, macOS hdiutil, Windows ImDisk). Linux requires `sudo` to mount tmpfs at `/mnt/erigon-ramdisk`. Override size with `RAMDISK_SIZE_MB` (default 2048).

## Command

```bash
ERIGON_EXECUTION_TESTS_TMPDIR=$path make test-all-race
```

## When Tests Fail — Drill Down

Race detector output includes the goroutine stack traces that caused the race:

```
==================
WARNING: DATA RACE
Read at 0x00c0001a2b40 by goroutine 45:
  github.com/erigontech/erigon/execution/exec3.(*parallelExecutor).applyLoop()
      /path/to/exec3_parallel.go:123 +0x4f8

Previous write at 0x00c0001a2b40 by goroutine 23:
  github.com/erigontech/erigon/execution/exec3.(*parallelExecutor).executeBlocks()
      /path/to/exec3.go:456 +0x2a0
==================
```

Re-run just the failing package with race:
```bash
go test -race --timeout 60m ./execution/exec3/...
go test -race --timeout 60m -run TestName ./path/to/package/...
```

Run without race first to confirm the test passes normally, then add `-race`:
```bash
# 1. Confirm test logic is correct (fast)
go test -short -run TestName ./path/to/package/...

# 2. Check for races (slower)
go test -race -run TestName ./path/to/package/...
```

## Known Race-Prone Areas

Areas historically susceptible to races in Erigon:
- `execution/exec3/` — parallel executor, `SharedDomains`, `AsyncTx`
- `p2p/` — sentry, peer manager
- `txpool/` — concurrent transaction pool
- `cl/` — caplin consensus layer goroutines

## When to Use

- After changes to the parallel executor or concurrent code paths
- For concurrency-sensitive fixes before merging
- Race check gate: `path=$(bash tools/create-ramdisk) && make lint && ERIGON_EXECUTION_TESTS_TMPDIR=$path make test-all-race`

## CI Equivalent

The **All tests (with -race)** workflow (`test-all-erigon-race.yml`) runs Go package race checks, while **EEST spec tests** (`test-eest-spec.yml`) runs the `evm.race` fixture shards above. Both are part of the CI gate.
