## Warmuper Flow Diagram

```
                    ┌──────────────────┐
                    │  HashSort start  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Load key[i]     │◀─────────────────────┐
                    └────────┬─────────┘                      │
                             │                                │
              ┌──────────────┴──────────────┐                 │
              │                             │                 │
              ▼                             ▼                 │
     ┌────────────────┐           ┌─────────────────┐         │
     │ WarmKey(hk)    │           │ Process(hk)     │         │
     │ (non-blocking) │           │ (state root)    │         │
     └───────┬────────┘           └────────┬────────┘         │
             │                             │                  │
             ▼                             │                  │
     ┌────────────────┐                    │                  │
     │ Worker Pool    │                    │                  │
     │ ┌────────────┐ │                    │                  │
     │ │ Prefetch:  │ │                    │                  │
     │ │ • Branch   │ │                    │                  │
     │ │ • Account  │ │                    │                  │
     │ │ • Storage  │ │                    │                  │
     │ └────────────┘ │                    │                  │
     └───────┬────────┘                    │                  │
             │                             │                  │
             ▼                             │                  │
     ┌────────────────┐                    │                  │
     │ WarmupCache    │◀ ─ ─ cache hit ─ ─ ┤                  │
     └────────────────┘                    │                  │
                                           │                  │
                             ┌─────────────┴─────────┐        │
                             │ i++ < batch_size?     │────────┘
                             └─────────────┬─────────┘  yes
                                           │ no
                                           ▼
                             ┌─────────────────────────┐
                             │ DrainPending()          │
                             │ (sync before next batch)│
                             └─────────────┬───────────┘
                                           │
                                           ▼
                             ┌─────────────────────────┐
                             │ more batches?           │──────▶ next batch
                             └─────────────┬───────────┘
                                           │ no
                                           ▼
                                    ┌─────────────┐
                                    │    Done     │
                                    └─────────────┘
```

**Key insight:** WarmKey and Process run in parallel - the main routine does not wait for prefetch.
Workers warm the cache ahead of time; processing benefits from cache hits on subsequent keys.

## Summary

- Consolidate `HashSort` and `HashSortWithPrefetch` into a single `HashSort(ctx, warmuper, fn)` function
- Process keys in batches of 10k to prevent memory explosion during warmup/commitment
- Add `DrainPending()` method to warmuper to ensure batch completion before processing
- Update tests to use warmuper for more realistic testing

## Changes

### commitment.go
- Added `warmupBatchSize = 10_000` constant to control batch sizes
- Merged `HashSortWithPrefetch` into `HashSort` with optional `warmuper` parameter
- Keys are now collected and processed in batches of 10k instead of all at once
- Added `DrainPending()` calls before processing each batch to ensure warmup completes

### warmuper.go
- Added `DrainPending()` method that drains the work channel before returning

### hex_patricia_hashed.go
- Updated all `HashSort` calls to use new signature with warmuper parameter

### commitment_test.go
- Added `noopPatriciaContext` mock for testing
- Updated `TestUpdates_TouchPlainKey` to use a real warmuper

## Test plan

- [x] `go build ./execution/commitment/...` passes
- [x] `go test ./execution/commitment/...` passes
