## Global Active Validator Caches

### Summary

Introduces a global `ActiveValidatorsCache` for Caplin to avoid redundant validator set computations across beacon states.

### Architecture & Rationale

Previously, each `CachingBeaconState` independently computed active validators and total active balance. This led to repeated expensive iterations over the full validator set (~1M+ validators on mainnet) whenever states were copied or epochs changed.

The new `ActiveValidatorsCacheGlobal` is keyed by `(epoch, blockRootAtPrevEpoch)` and shared across all beacon states. This deduplicates the computation since states at the same epoch with the same history will have identical active validator sets.

The parallel worker pattern for validator iteration was also removed—the sequential loop is simpler and the global cache makes the parallelism unnecessary.

## TxPool Message Batching & Deduplication

### Summary

Batches and deduplicates incoming transaction messages to reduce redundant computation and MDBX transaction overhead.

### Architecture & Rationale

Two problems were identified in profiling:

1. **Blob verification allocations**: Duplicate transactions arriving from multiple peers caused the same expensive blob verification to run multiple times, leading to excessive allocations.

2. **MDBX transaction overhead**: Each incoming message opened its own read transaction. Profiling showed this accounted for ~15% of CPU time.

The fix introduces:

- **LRU-based deduplication**: An LRU cache (4096 entries) filters duplicate messages before they enter the processing batch. Uses `maphash` on the raw message data to avoid string allocations.

- **1-second batching window**: Instead of processing messages immediately, they accumulate in a batch and are flushed every second (or on stream close). A single MDBX read transaction is opened per batch and reused for all messages in that batch.

This ensures each unique transaction is processed exactly once per batch window, and the MDBX transaction cost is amortized across potentially hundreds of messages.

## Reduced Map Updates in Beacon State Copying

### Summary

Reduces redundant map updates when copying beacon states into a parent state by detecting ancestry and minimizing public key indices cache rebuilding.

### Architecture & Rationale

When updating head state, we frequently copy a child state back into its parent. Previously, this would clear and fully rebuild the `publicKeyIndicies` map (pubkey → validator index), causing ~1M+ map insertions every time.

The fix detects parent-child relationship by comparing block roots:

```go
blockRoot, _ := bs.BlockRoot()
fixedCachesUnwind = blockRoot == b.LatestBlockHeader().ParentRoot
```

When copying into a parent state, the existing cache entries remain valid since validator indices only grow. Instead of rebuilding the entire map, we only update the last 256 entries (covering any new validators):

```go
if bs.publicKeyIndicies != nil && fixedCachesUnwind {
    startIdx := len(bs.publicKeyIndicies) - 256
    for i := startIdx; i < bs.ValidatorLength(); i++ {
        v := bs.Validators().Get(i)
        bs.publicKeyIndicies[v.PublicKey()] = uint64(i)
    }
}
```

This reduces map operations from O(n) to O(1) for the common case of advancing head state by one block.
