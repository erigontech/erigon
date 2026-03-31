# Objective: 2-Level Prefix Index for BpsTree

## Problem

BpsTree performs binary search over sorted `.kv` files using cached pivot nodes (every M-th key). For large files (>60k keys), the initial `bs()` call on cached nodes narrows the search range from N to ~M (256), then a second binary search narrows within that range. For files with millions of keys, the `bs()` over `N/M` cached nodes still requires ~log2(N/M) comparisons — each involving byte-by-byte key comparison against variable-length keys.

For a 12M key file: `log2(12M/256) = log2(46875) ≈ 15.5` comparisons in `bs()` alone, plus ~8 more in the inner binary search. Each comparison fetches a `Node.key` slice from a potentially cache-cold memory location.

## Goal

Add a 2-level prefix index (first 2 bytes of each key) that maps byte prefixes to `di` ranges, providing O(1) range narrowing before any binary search. This eliminates the `bs()` phase for most lookups and reduces the inner binary search range dramatically.

**Expected speedup**: For uniformly distributed keys, the prefix index narrows the range to `N/65536` — for 12M keys, that's ~183 entries. The subsequent binary search needs only `log2(183) ≈ 7.5` comparisons (vs ~24 total today). For skewed distributions, the improvement varies but is always >= current performance since we take the tighter of prefix-index vs `bs()` bounds.

## Success Criteria

1. `BenchmarkBpsTreeSeek` with 12M keys shows measurable improvement in ns/op
2. Memory overhead ≤ 512KB per file (65536 entries × 8 bytes)
3. All existing tests pass unchanged
4. Prefix index is only built for files with >60k keys (small files see no overhead)
5. Prefix index integrates cleanly with existing `bs()` — whichever gives tighter bounds wins

## Non-Goals

- Changing the on-disk `.bti` format (prefix index is runtime-only, built during WarmUp)
- Supporting keys shorter than 2 bytes (these are vanishingly rare in Ethereum KV files — account hashes, storage hashes, and composite keys are all ≥20 bytes)
- Replacing the cached node system (prefix index complements it)
