## db/state: clear BranchCache on Flush to prevent stale trie data

### What was failing

The `stage-exec-test (from-0, parallel)` CI job failed with:
```
[EROR] Wrong trie root of block 263641: f743cd41... expected f362852c...
```
All four `from-0` and `resume-nonchaintip` matrix jobs failed with wrong trie roots at different blocks (263641, 513814, 25314648). The `chaintip, serial` and `resume-nonchaintip, parallel` jobs passed.

### Root cause

PR #21380 (State Cache Consolidation, commit `193d04a`) introduced an aggregator-scoped `BranchCache` that caches commitment-trie branch data across transactions. The `SharedDomains.Commit()` method correctly updates the cache after flushing, but `SharedDomains.Flush()` did not — it wrote new commitment branches to the transaction without invalidating or updating the cache.

The integration tool's `stage_exec` command (non-chaintip mode) uses a Flush+ClearRam+Commit loop:
```go
for {
    SpawnExecuteBlocksStage(...)  // reads branches → populates cache via read-through
    doms.Flush(ctx, tx)           // writes updated branches to tx, cache NOT touched
    doms.ClearRam(true)           // clears in-memory batch, NOT the cache
    tx.Commit()                   // commits to disk
    tx = db.BeginTemporalRw(ctx)  // new tx; cache still holds pre-flush data
}
```

On the next batch iteration, `GetLatest` for `CommitmentDomain` keys hit the BranchCache (line 837 of `domain_shared.go`) and returned stale branch data from the read-through population in the previous iteration. The trie walker then computed an incorrect state root from these stale branches.

### Fix

Clear the `BranchCache` after a successful `Flush` in `SharedDomains.Flush()` (`db/state/execctx/domain_shared.go:657`). This forces the next read to go to the backing transaction/files, which contain the correct post-flush data. The cache re-warms naturally via read-through during the next execution batch.

`Commit()` is unaffected — it already maintains cache consistency via its `CommitmentFlushCallback`.

### Verification

- `TestFromZero_GenesisAllocPreservedAfterResetReExec` (serial + parallel): PASS ×10
- `TestBranchCacheCommitRefreshesAfterReadThrough`: PASS ×10
- All `db/state/execctx` tests: PASS
- All `execution/commitment` tests (including BranchCache, DecodeBranch, HPH): PASS
- `make erigon` + `make integration`: build clean
- `make lint`: 0 issues (2 consecutive runs)

The actual CI failure cannot be reproduced locally (requires a mainnet reference datadir), but the fix is deterministic: the stale-cache read path is eliminated by clearing on Flush.
