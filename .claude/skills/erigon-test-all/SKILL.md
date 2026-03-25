---
name: erigon-test-all
description: Run the full Erigon test suite locally using GOGC=80 make test-all. Use this before marking a PR ready for review. Equivalent to the "All tests" CI workflow.
---

# Erigon Full Test Suite

Runs the complete test suite with 60-minute timeout and coverage output. Takes ~30 minutes.

## Command

```bash
GOGC=80 make test-all
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
- Full gate: `make lint && make erigon integration && GOGC=80 make test-all`

## CI Equivalent

| Local command | CI workflow | File |
|---------------|-------------|------|
| `GOGC=80 make test-all` | All tests | `test-all-erigon.yml` |

To dispatch remotely:
```bash
gh workflow run "All tests" --ref <branch>
```
