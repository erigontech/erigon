## PR Description: Parallel Commitment Warmup Cache

### Summary

This PR introduces a **warmup cache** for the commitment trie processing that reduces redundant DB reads during block execution. Data fetched during the warmup phase is cached and reused during the actual trie computation, improving performance.

### Key Changes

- **New `WarmupCache`** - A cache that stores pre-fetched branch, account, and storage data using `maphash.Map` for efficient byte slice lookups without allocations
- **Cache integration in `Warmuper`** - The warmuper now optionally caches data it reads, making it available during trie processing
- **`BranchEncoder` cache support** - Branch lookups check the cache first before hitting the DB
- **Key eviction** - Keys being modified are evicted from the cache to ensure consistency

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Block Execution                               │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          Warmuper                                    │
│  ┌──────────────────┐    ┌──────────────────┐                       │
│  │  Parallel Workers │───▶│   WarmupCache    │                       │
│  │  (prefetch data)  │    │                  │                       │
│  └──────────────────┘    │  ┌────────────┐  │                       │
│                          │  │  branches  │  │                       │
│                          │  ├────────────┤  │                       │
│                          │  │  accounts  │  │                       │
│                          │  ├────────────┤  │                       │
│                          │  │  storage   │  │                       │
│                          │  └────────────┘  │                       │
│                          └────────┬─────────┘                       │
└───────────────────────────────────┼─────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │         Cache Lookup          │
                    ▼                               ▼
         ┌─────────────────┐             ┌─────────────────┐
         │   Cache Hit     │             │   Cache Miss    │
         │  (return data)  │             │   (read DB)     │
         └─────────────────┘             └─────────────────┘
                                                  │
                                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     HexPatriciaHashed Trie                          │
│                                                                      │
│   Process() ──▶ BranchEncoder.CollectUpdate()                       │
│                        │                                             │
│                        ▼                                             │
│              ┌─────────────────┐                                    │
│              │ Check WarmupCache│                                    │
│              │   for branch     │                                    │
│              └────────┬────────┘                                    │
│                       │                                              │
│           ┌───────────┴───────────┐                                 │
│           ▼                       ▼                                 │
│     Cache Hit               Cache Miss                              │
│   (skip DB read)          (read from DB)                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Key Eviction Flow                              │
│                                                                      │
│   Updates.HashSort() ──▶ For each modified key:                     │
│                              cache.EvictKey(key)                    │
│                                                                      │
│   (Ensures modified keys are re-read from DB, not stale cache)     │
└─────────────────────────────────────────────────────────────────────┘
```

### Files Changed

| File | Changes |
|------|---------|
| `execution/commitment/warmup_cache.go` | **New** - WarmupCache implementation with maphash-based maps |
| `execution/commitment/warmup_cache_test.go` | **New** - Comprehensive tests for the cache |
| `execution/commitment/warmuper.go` | Added cache integration, `*FromCacheOrDB` helpers |
| `execution/commitment/commitment.go` | `BranchEncoder` now checks cache before DB reads |
| `execution/commitment/hex_patricia_hashed.go` | Added `SetEnableWarmupCache` support |
| `execution/commitment/commitmentdb/commitment_context.go` | Context factory updates |
| `execution/stagedsync/exec3.go` | Enable warmup cache during execution |
| `execution/execmodule/forkchoice.go` | Warmup cache config propagation |

### Test Plan

- [ ] Run existing commitment tests
- [ ] Run `warmup_cache_test.go` tests
- [ ] Benchmark block execution with/without warmup cache
- [ ] Verify no stale data issues with key eviction
