# Objective: Standalone PrefixIndex Replacing BpsTree

## Problem

The prefix index introduced in PR #20171 is embedded inside `BpsTree` in `bps_tree.go`. This creates several issues:

1. **Tight coupling**: `prefixIndex` is a private type inside BpsTree, making it impossible to test or use independently. BpsTree has two code paths — prefix index for large files, `bs()`+`mx` for small files — with interleaved logic in `Seek()` and `Get()`.

2. **Suboptimal node distribution**: The current approach picks every M-th key globally as cached nodes, then distributes them into prefix buckets. This means popular prefixes (e.g., keys starting with `0x00` in Ethereum) get many nodes while sparse prefixes get zero — exactly the wrong allocation. A prefix with 1M keys might get 3900 nodes (only 8 kept, rest discarded), while a prefix with 100 keys gets zero nodes.

3. **BpsTree complexity**: BpsTree maintains two search paths (`prefix != nil` vs `mx` + `bs()`), two construction paths (`WarmUp` vs `NewBpsTreeWithNodes`), and dead weight (`M` field, `trace` field) that a prefix-first design doesn't need.

## Goal

Extract the prefix index into a standalone `PrefixIndex` type in its own file (`prefix_index.go`) that:

1. Is a self-contained search engine: given an EliasFano offset index and a `seg.Reader`, provides `Seek()` and `Get()` with no dependency on BpsTree
2. Builds nodes with **even distribution per prefix bucket**: scan all keys, group by 2-byte prefix, pick 8 evenly spaced keys per bucket — every non-empty prefix gets good narrowing
3. Replaces BpsTree as the search engine inside `BtIndex` — domain code (`FilesItem.bindex`, `DomainRoTx.getLatestFromFile`) is unchanged
4. Has a single code path (no small-file vs large-file fork)

## Key Insight: Per-Prefix Even Distribution

Current (global M-step sampling):
```
Prefix 0x00ab: 50,000 keys → gets ~195 M-th nodes → 8 kept, 187 discarded
Prefix 0xff01: 3 keys → gets 0 nodes → no narrowing, full binary search
```

New (per-prefix even distribution):
```
Prefix 0x00ab: 50,000 keys → pick 8 at positions 0, 6250, 12500, ... → even spacing
Prefix 0xff01: 3 keys → pick all 3 as nodes → exact coverage
```

Every non-empty prefix gets up to 8 well-spaced nodes. Binary search within any bucket starts from a tight range.

## Success Criteria

1. `PrefixIndex` is a standalone type in `prefix_index.go` — no imports from BpsTree, no dependency on `mx`/`bs()`
2. `BtIndex` uses `PrefixIndex` instead of `BpsTree` — all existing `BtIndex` tests pass unchanged
3. `BenchmarkBpsTreeSeek` with 12M keys: equal or better performance vs current prefix index
4. Memory: prefix buckets ~2.5MB (same as now) + nodes ~8 per active bucket (well under 1MB total)
5. `make lint && make erigon integration` passes
6. Same `.bt` file format — EliasFano section read as before; node section from `.bt` is optional (PrefixIndex builds its own nodes from `.kv` scan)

## Non-Goals

- Changing the `.bt` on-disk format (PrefixIndex reads EliasFano from `.bt`, builds nodes at runtime)
- Changing domain-level code (`FilesItem`, `DomainRoTx`) — BtIndex public API stays the same
- Supporting keys shorter than 2 bytes (not relevant for Ethereum key types)
- Removing BpsTree entirely in this PR (can coexist, remove in follow-up)
