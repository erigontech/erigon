# Deferred-write API simplification — next-plan marker

Not for the current `20260710-commitment-write-opt` run. Recorded because the wins are real but the
confidence is not: the profile's headline write cost is a bench artifact and the biggest idea is
unmeasured. Verify the gates below before promoting any of this to an executable plan.

Parallel-only throughout: the serial path uses `CollectUpdate` (immediate encode + `PutBranch`) and
never touches the deferred struct/pool/arena, so all of this is serial-byte-identical by construction.

## Ground truth this rests on (verified 2026-07-11 @ awskii/commitment-write-opt)

- One fresh branch's encoded bytes are copied 3×: `getDeferredUpdate` raw-clone → `putLatest`
  (`common.Copy(val)`, temporal_mem_batch.go:171) → ETL `addValue` (domain.go:526). The arena only
  kills the first; `putLatest` and ETL copies are downstream and mandatory (they outlive the arena).
- Fresh 1M whale (w18): merge pass = **0 CPU** (prev empty). `mergeDeferredUpdate` on fresh is just
  `encoded = raw`. Seeded 120k whale: merge pass = 4.25 ms, the only place merge-under-fold pays.

## Items (ranked)

1. **Value-struct + append-only encoder replaces arena-beside-pool.** Make `EncodeBranch` append to a
   per-worker buffer instead of `Reset`-per-branch, and hold `DeferredBranchUpdate` as value structs
   with sub-slices into that buffer. Subsumes the current plan's arena AND drops `deferredUpdatePool`,
   `putDeferredUpdate`, and the per-branch `getDeferredUpdateCount` atomic in one move. One reset at
   flush. Smaller surface than arena-plus-pool.
   - **Gate:** confirm `be.buf`'s reused slice has no reader other than the immediate copy-out, i.e.
     that switching Reset→append doesn't alias a live consumer. Prototype and diff roots byte-for-byte.

2. **Fresh path needs only `{prefix, raw}`.** `prev`/`encoded`/`mergeDeferredUpdate` are dead weight on
   a whole-fresh build (prev always empty). Downscope current-plan Task 3 (merge-under-fold) to
   **seeded-only** — it delivers nothing for the fresh fork we shipped.
   - **Gate:** none beyond item 1; the 0-CPU fresh merge already confirms the downscope.

3. **The untouched headroom is the sequential write drain, not the clones.** 436k `DomainPut`/block,
   each taking `latestStateLock` + `common.Copy` + ETL Collect, serialized. A bulk-put on
   `TemporalMemBatch` (one lock acquisition, N puts) would be additive and parallel-only.
   - **Gate (blocking, do first):** the fresh 34 ms "sequential write loop" in the baseline is a
     **MockState map-insert artifact**, NOT production `sd.mem`. Measure the real `sd.mem` write half
     on a production-shaped path before investing. If it isn't the tail, drop this item.

## Why not now

- Item 3's premise (write drain is the tail) is **unmeasured** — bench used a map, not `sd.mem`.
- Item 1 is byte-safe by construction but not prototyped; the encoder append change needs the aliasing
  gate cleared.
- Current run's arena (Task 2) is a valid, smaller step that lands the fresh GC win; let it finish,
  then decide whether to fold items 1–2 in as a follow-up.

4. **Thread the owning trie's `accountKeyLen`/`TrieConfig` into the direct-fold `foldCtx`s.**
   `newFoldCtx` and the fold wrappers hardcode `NewHexPatriciaHashed(length.Addr, nil,
   DefaultTrieConfig())`; a non-20-byte-key instance (or a config the fold-time hashing must honor)
   would silently diverge from the replay path. Latent only — every production instance uses
   `length.Addr` — flagged at the 2026-07-11 review. Fold into the forkjoin plan's Task 4–6 rewiring
   of the same constructors rather than churning them twice.
