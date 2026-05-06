---
name: erigon-test-all
description: Run the full Erigon test suite locally using GOGC=80 make test-all. Use this before marking a PR ready for review. Equivalent to the "All tests" CI workflow.
---

# Erigon Full Test Suite

Runs the complete test suite with 60-minute timeout and coverage output. Takes ~30 minutes.

## Prerequisite: Test fixtures

`make test-all` declares `test-fixtures` as a prerequisite, so fixture tarballs pinned in `test-fixtures.json` are downloaded into `test-fixtures-cache/` (sha256-verified, no-op on cache hit) automatically. No submodule sync needed for `execution/tests/`.

Two side prerequisites still apply:

```bash
git submodule update --init --recursive --force            # only for legacy-tests (TestLegacyCancunState)
git lfs pull --include='execution/tests/test-corners/**'   # for TestInvalidReceiptHashHighMgas
```

The CI workflow handles both in `setup-erigon`; locally you must do them yourself.

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
- Full gate: `git submodule update --init --recursive --force && git lfs pull --include='execution/tests/test-corners/**' && path=$(bash tools/create-ramdisk) && make lint && make erigon integration && ERIGON_EXECUTION_TESTS_TMPDIR=$path GOGC=80 make test-all`

## CI Equivalent

| Local command | CI workflow | File |
|---------------|-------------|------|
| `GOGC=80 make test-all` | All tests | `test-all-erigon.yml` |

To dispatch remotely:
```bash
gh workflow run "All tests" --ref <branch>
```
