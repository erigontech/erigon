# QMTree Domain Integration Plan

## Goal

Make qmtree data flow through the same pipeline as accounts/storage/code
domains: hot writes to MDBX → collation to snapshot files → pruning →
merging → torrent distribution. Eliminates the separate `stage_exec_replay`
process — qmtree is built as a side-effect of normal block execution.

## Current State (HPFile-based)

```
block execution → Tracker.AppendLeaf() → HPFile segments (entries/, twigs/)
                → Tracker.NotifyKeyWrites() → KeyIndexFile (.kv/.kvi)
```

Problems:
- Separate file format (HPFile) not integrated with snapshot lifecycle
- No collation/pruning/merging pipeline
- Files not distributable via torrent
- Requires `stage_exec_replay` to generate from scratch
- No crash recovery beyond what HPFile provides

## Target State (Domain-integrated)

```
block execution → MDBX qmtree tables (hot)
                     ↓ collation (per step)
                  .kv/.kvi snapshot files (frozen)
                     ↓ pruning
                  MDBX hot data removed
                     ↓ merging
                  larger frozen files
                     ↓ torrent
                  distributed to peers
```

The qmtree becomes two new Erigon domains:

| Domain | Key | Value | Purpose |
|--------|-----|-------|---------|
| `QMTreeEntry` | serialNum (8B) | preStateHash \|\| stateChangeHash \|\| transitionHash (96B) | Leaf components |
| `QMTreeKeyIndex` | keyHash (32B) | txNum (8B) | Latest-write tracking |

Upper-tree nodes and twig Merkle data are derived (computed from entries
on collation), not stored as a separate domain.

## Migration Phases

### Phase 1: MDBX Tables for Hot Data

**What:** Write qmtree entry components and key-index updates to MDBX tables
during block execution, alongside the existing HPFile writes.

**Tables:**
```
QMTreeEntries:   serialNum (8B BE) → pre(32B) || sc(32B) || trans(32B)
QMTreeKeyIndex:  keyHash (32B)     → txNum (8B BE)
QMTreeMeta:      "nextSN"          → uint64
                 "prevLeaf"        → hash
```

**Changes:**
- `Tracker` gains a `kv.RwTx` reference (set per block batch, like SharedDomains)
- `AppendLeaf` writes to both HPFile and MDBX
- `NotifyKeyWrites` writes to both KeyIndexFile and MDBX
- `LoadFromDisk` reads from MDBX first, falls back to HPFile

**Why dual-write:** HPFile continues working as before. MDBX is the new
source of truth. Once Phase 2 is validated, HPFile writes are removed.

**Estimated effort:** 2-3 days

### Phase 2: Collation — Freeze Completed Steps to Snapshot Files

**What:** When a step completes (1,562,500 txns), freeze the MDBX data
for that step range into standard `.kv`/`.kvi` snapshot files.

**Entry collation:**
1. Iterate `QMTreeEntries` for serialNums in `[step*stepSize, (step+1)*stepSize)`
2. Write sorted entries to `v1-qmtree-entries.{fromStep}-{toStep}.kv`
3. Build RecSplit index → `.kvi`
4. Compute twig Merkle trees for all twigs in the step → write upper-tree
   edge nodes to a small metadata file (or embed in the .kv as a trailer)

**KeyIndex collation:**
1. Scan `QMTreeKeyIndex` for keys with txNum in the step's range
2. Write to `v1-qmtree-keyindex.{fromStep}-{toStep}.kv` + `.kvi`
3. Same format as current KeyIndexFile segments

**Integration point:** hook into `Aggregator.collate()` or register as a
custom domain via the existing `DomainCfg` mechanism.

**Estimated effort:** 3-5 days

### Phase 3: Pruning — Remove Frozen Data from MDBX

**What:** After collation succeeds and snapshot files are verified, delete
the corresponding entries from MDBX to keep the hot dataset small.

**Prune logic:**
- Delete `QMTreeEntries` rows with serialNum < frozenUpToSN
- For `QMTreeKeyIndex`, only prune entries where a newer write exists in
  a frozen file (the key's latest txNum moved to a newer step)

**Integration point:** hook into `Aggregator.prune()`.

**Estimated effort:** 1-2 days

### Phase 4: Merging — Combine Small Step Files

**What:** Merge multiple small step files into larger frozen files, same
as domains do today.

**Entry merging:** concatenate sorted step files (entries are already
ordered by serialNum which is monotonically increasing across steps).
Rebuild RecSplit index over merged file.

**KeyIndex merging:** merge-sort by keyHash, keep latest txNum per key
(same as current `PopulateKeyIndex` logic). Rebuild RecSplit.

**Integration point:** hook into `Aggregator.merge()`.

**Estimated effort:** 2-3 days

### Phase 5: Remove HPFile — MDBX/Snapshots Only

**What:** Remove the HPFile-based storage entirely. All reads come from
MDBX (hot) or snapshot files (frozen).

**Changes:**
- Remove `entryFile`, `twigFile` from Tracker
- Remove HPFile dependency
- `getLeafData` reads from MDBX or snapshot files
- `getProof` reads twig data from snapshot trailers or recomputes
- Remove `stage_exec_replay` (no longer needed — data is built inline)

**Estimated effort:** 2-3 days

### Phase 6: Torrent Distribution

**What:** Register qmtree snapshot files with the downloader for torrent
distribution, matching the existing domain/history/idx pattern.

**File registration:**
```
snapshots/domain/v1-qmtree-entries.0-64.kv       → torrent
snapshots/domain/v1-qmtree-entries.0-64.kvi       → torrent
snapshots/domain/v1-qmtree-keyindex.0-64.kv       → torrent
snapshots/domain/v1-qmtree-keyindex.0-64.kvi      → torrent
```

**Changes:**
- Add `QMTreeEntry` and `QMTreeKeyIndex` to `kv.Domain` enum
- Register file patterns in `snaptype` (snapshot type registry)
- Downloader discovers and seeds qmtree files alongside other domains
- New nodes download qmtree snapshots during initial sync instead of
  replaying from genesis

**Verification:** downloading node builds the upper tree from entry files,
computes root, and verifies against the expected root for the frozen block
range (published in the torrent manifest or derived from block headers once
the EIP is active).

**Estimated effort:** 3-5 days

## Phase Summary

| Phase | Description | Effort | Depends On |
|-------|-------------|--------|------------|
| 1 | MDBX hot tables | 2-3 days | — |
| 2 | Collation to snapshots | 3-5 days | Phase 1 |
| 3 | Pruning | 1-2 days | Phase 2 |
| 4 | Merging | 2-3 days | Phase 2 |
| 5 | Remove HPFile | 2-3 days | Phases 2-4 validated |
| 6 | Torrent distribution | 3-5 days | Phase 2 |

Phases 3, 4, 6 can proceed in parallel after Phase 2.

**Total: ~15-20 days of work.**

## Key Design Decisions

### Why two domains, not one?

`QMTreeEntry` is append-only and keyed by serialNum (monotonically increasing).
`QMTreeKeyIndex` is keyed by keyHash and gets updated (latest txNum per key).
These have fundamentally different access patterns and collation strategies —
entries are immutable once written, key-index entries are overwritten.

### Where do twig Merkle trees live?

Twig MT data (the 4096-node intra-twig Merkle tree) is **derived** from entry
components, not stored as a primary domain. During collation, twigs are computed
and stored as a trailer in the entry snapshot file or as a small auxiliary file.
This keeps the primary storage format simple (just the 96-byte entry components)
while still enabling O(1) proof generation from frozen files.

### What about the upper tree?

The upper tree (inter-twig nodes) is always in-memory. It's rebuilt from twig
roots on startup, which is O(N_twigs) = O(total_entries / 2048). For 2.5B
mainnet entries that's ~1.2M twigs, taking seconds to rebuild.

### Snapshot file directory

Follow the existing convention:
```
snapshots/domain/v1-qmtree-entries.{fromStep}-{toStep}.kv
snapshots/domain/v1-qmtree-keyindex.{fromStep}-{toStep}.kv
```

Or create a new subdirectory:
```
snapshots/qmtree/v1-entries.{fromStep}-{toStep}.kv
snapshots/qmtree/v1-keyindex.{fromStep}-{toStep}.kv
```

The latter keeps qmtree files separate and makes the `--experimental.qmtree`
flag's scope clear. Recommend starting with a separate subdirectory and
migrating to `domain/` once the feature is stable.

### Backward compatibility

Nodes without `--experimental.qmtree` ignore the tables and files entirely.
The domains are only created when the flag is set. Torrent distribution
includes qmtree files as optional — downloaders that don't request them
simply skip.
