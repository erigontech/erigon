---
name: erigon-test-race
description: Run Erigon tests with Go race detector to find data races and concurrency bugs. Use this for concurrency-sensitive changes (parallel executor, p2p, txpool). Takes 30-60 minutes.
---

# Erigon Race Detector Tests

Runs the full test suite with Go's `-race` flag. Catches concurrency bugs that normal tests miss. Takes 30–60 minutes.

## Command

```bash
make test-all-race
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
- Race check gate: `make lint && make test-all-race`

## CI Equivalent

There is no dedicated race CI workflow — the "All tests" workflow does not run with `-race` by default. This is a local-only check.

To run a subset with race via CI, consider running locally or using:
```bash
go test -race --timeout 60m ./execution/exec3/... ./execution/stagedsync/...
```
