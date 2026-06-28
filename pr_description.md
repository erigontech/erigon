## Title
db/state: invalidate BranchCache entries on Flush to fix stale trie reads

## Root Cause

The `BranchCache` introduced in #21380 (State Cache Consolidation) is an
aggregator-scoped commitment-branch cache that lives across batch boundaries.
`SharedDomains.Commit()` correctly updates the cache after a successful commit,
but the `Flush()` path — used by the `integration stage_exec` from-0 loop — did
not invalidate or update the cache at all.

In the from-0 execution loop (`cmd/integration/commands/stages.go:802–837`):
1. Batch N reads a commitment branch from MDBX → cached in BranchCache
2. Batch N updates the branch via execution → written to `sd.mem`
3. `Flush()` writes `sd.mem` to the MDBX tx (but does NOT touch BranchCache)
4. `ClearRam()` clears `sd.mem` (but NOT the BranchCache)
5. `tx.Commit()` persists to disk
6. Batch N+1 reads the same branch → BranchCache returns the stale value from
   step 1, bypassing MDBX which has the correct value from step 3

This produced wrong trie roots because the trie computation used stale
intermediate branch nodes instead of the freshly committed ones.

## Fix

`Flush()` now uses the existing `flushMemWithCallback` path to invalidate every
commitment-domain key it writes. Subsequent reads fall through the (now-empty)
cache to MDBX and get the correct value.

The invalidation-only approach (vs. populating the cache like `Commit()` does)
is deliberately conservative: `Flush()` callers own the commit, so we cannot
guarantee the tx will actually be committed. Invalidation is safe regardless of
commit outcome.

## Verification

- `TestBranchCacheFlushInvalidatesStaleEntries` — new unit test that reproduces
  the exact stale-cache scenario. Confirmed: FAILS without the fix (returns
  stale `v1`), PASSES with the fix (returns correct `v2`).
- `TestBranchCacheCommitRefreshesAfterReadThrough` — existing test, still passes.
- `TestFromZero_GenesisAllocPreservedAfterResetReExec` — passes (serial + parallel).
- All `db/state/execctx` tests pass with `-count=3 -race`.
- All `execution/commitment` tests pass with `-count=3 -race`.
- `make lint` clean, `make erigon integration` build clean.
