# KeyIndex Persistence Plan

## Problem

The KeyIndex (`keyHash → lastTxNum`) is rebuilt from scratch on every restart
by replaying all entry files. For mainnet (~300M unique keys), this means:
- ~15-20 GB in-memory map
- Minutes-long replay on startup
- No crash resilience — kill during execution loses the in-memory state

## Design: Segmented Data File + RecSplit Index

Follow the Domain "latest values" pattern used throughout Erigon's state
storage. The KeyIndex is structurally identical to a Domain's latest snapshot:
one value (txNum) per key (keyHash), built incrementally during execution.

### File Layout

```
snapshots/qmtree/keyindex/
├── v1-keyindex.0-1.kv       # data: sorted (keyHash, txNum) pairs for step 0→1
├── v1-keyindex.0-1.kvi      # RecSplit perfect hash index into .kv
├── v1-keyindex.1-2.kv       # data for step 1→2
├── v1-keyindex.1-2.kvi      # index for step 1→2
└── ...
```

Each `.kv` file contains all keys that were updated during that step range,
sorted by keyHash. The `.kvi` RecSplit index provides O(1) lookup by keyHash
into the `.kv` file.

### Record Format (.kv)

Fixed-size records, no compression needed (hashes are incompressible):

```
[32B keyHash][8B txNum_BE] = 40 bytes per entry
```

Written via `seg.Compressor` with `CompressNone` to match the Domain pattern
and integrate with existing file management (decompressor, visibility, cleanup).

### Index Format (.kvi)

Standard RecSplit perfect hash index (`db/recsplit`):
- Key: 32-byte keyHash
- Value: byte offset into the `.kv` file
- `Enums=false` (direct offset lookup, not ordinal)
- `LessFalsePositives=true` (existence filter for non-existent keys)

### Lookup: "What was the last txNum for keyHash K?"

Walk `.kvi` files in reverse chronological order (newest step first):

```go
for i := len(visibleFiles) - 1; i >= 0; i-- {
    offset, found := files[i].index.Lookup(keyHash)
    if found {
        // read 8 bytes at offset+32 in the .kv file
        return binary.BigEndian.Uint64(data[offset+32 : offset+40])
    }
}
// not found in any file → key was never written
```

This is O(1) per file × number of step files. After merging, typically 1-3
files to check.

### Merging

When step files are merged (same as Domain/InvertedIndex merge), the KeyIndex
files merge by taking the **latest txNum per keyHash** (max wins). Since files
are sorted by keyHash, this is a single-pass merge-sort:

```
for each keyHash in merged stream:
    keep the entry from the newest step file
```

The merged `.kv` is written, then a new `.kvi` RecSplit index is built over it.

## Implementation Steps

### Step 1: KeyIndexFile type (read/write layer)

New file: `execution/commitment/qmtree/keyindexfile.go`

```go
type KeyIndexFile struct {
    dir       string
    stepSize  uint64
}

// Flush writes the current in-memory KeyIndex to a .kv file for the given
// step range, then builds the .kvi RecSplit index.
func (f *KeyIndexFile) Flush(ki *KeyIndex, fromStep, toStep uint64) error

// Load reads all .kv/.kvi files from disk and populates a KeyIndex.
// Returns the loaded KeyIndex and the highest step found.
func (f *KeyIndexFile) Load() (*KeyIndex, uint64, error)

// Lookup searches .kvi files for keyHash, returning (txNum, found).
func (f *KeyIndexFile) Lookup(keyHash common.Hash) (uint64, bool)
```

**Flush implementation:**
1. Call `ki.ensureSorted()`
2. Create `seg.Compressor` → `seg.Writer` with `CompressNone`
3. Write each `(keyHash, txNum)` pair as key-then-value
4. `Compress()` → open as `seg.Decompressor`
5. Build RecSplit index via `buildHashMapAccessor` pattern:
   - `NewRecSplit(RecSplitArgs{KeyCount: ki.Len(), Enums: false, ...})`
   - Iterate `.kv`, `AddKey(keyHash, offset)` for each entry
   - `Build(ctx)`
6. Register files for visibility

**Load implementation:**
1. Glob `v1-keyindex.*.kv` files, sort by step range
2. For each file, open decompressor + RecSplit index
3. Iterate all entries, populate `KeyIndex.index` map (newest step wins)
4. Return populated KeyIndex

### Step 2: Wire into Tracker

Modify `tracker.go`:

```go
type Tracker struct {
    // ... existing fields ...
    keyIndexFile *KeyIndexFile  // persistent storage
}
```

**On Flush():** after flushing entries and twigs, also flush the KeyIndex
delta (keys updated since last flush) to a new step file.

**On LoadFromDisk():** instead of replaying entries to rebuild the KeyIndex,
call `keyIndexFile.Load()`. Fall back to replay if no `.kv` files exist
(migration path from existing data).

**On Truncate (unwind):** delete `.kv/.kvi` files for truncated steps.
Rebuild the in-memory KeyIndex from the remaining files.

### Step 3: Incremental flush (delta files)

Rather than writing the full KeyIndex on every flush (expensive for 300M keys),
write only the **delta** — keys updated since the last flush:

```go
type Tracker struct {
    keyIndexDirty map[common.Hash]uint64  // keys updated since last flush
}
```

On `AppendLeaf`, when `keyIndex.UpdateKey()` is called, also add to
`keyIndexDirty`. On `Flush()`, write only `keyIndexDirty` to a new step
file, then clear the map. This keeps flush time proportional to the number
of updated keys per step, not the total index size.

Lookup then walks step files newest-first (dirty keys in newest file shadow
older entries). Periodic merging collapses multiple step files into one.

### Step 4: Tests

1. **TestKeyIndexFile_FlushAndLoad** — flush N entries, reload, verify all
   present with correct txNums
2. **TestKeyIndexFile_IncrementalFlush** — flush two steps, verify second
   step's entries shadow first
3. **TestKeyIndexFile_Lookup** — verify O(1) RecSplit lookup without loading
   full index into memory
4. **TestKeyIndexFile_Truncate** — flush, truncate, verify entries from
   truncated step are gone
5. **TestKeyIndexFile_MergeSteps** — flush multiple steps, merge, verify
   single merged file has correct latest-wins semantics

### Step 5: Migration

Existing qmtree data has no `.kv/.kvi` files. On first `LoadFromDisk`:
1. Detect missing `.kv` files
2. Fall back to current replay-based reconstruction
3. Immediately flush the reconstructed KeyIndex to `.kv/.kvi`
4. Subsequent loads use the persisted files

## File Dependencies

```
keyindexfile.go (new)
├── db/seg          — Compressor, Decompressor, Writer
├── db/recsplit     — NewRecSplit, OpenIndex, IndexReader
├── keyindex.go     — KeyIndex, KeyIndexEntry (existing)
└── hpfile          — NOT used (RecSplit replaces offset-based access)

tracker.go (modified)
├── keyindexfile.go — Flush, Load, Lookup
└── keyindex.go     — UpdateKey, Root, GetProof (unchanged)
```

## Size Estimates (mainnet)

| Metric | Value |
|--------|-------|
| Unique state keys | ~300M |
| Record size | 40 bytes |
| Full .kv file | ~12 GB |
| RecSplit .kvi overhead | ~3 bits/key ≈ ~113 MB |
| Per-step delta (100K blocks) | ~5-50 MB |
| Load time (mmap) | <1 second |

## Not In Scope

- **KeyIndex unwind** — still requires replay for affected keys. Persistence
  helps (can replay from persisted files instead of entry files) but doesn't
  eliminate the replay.
- **RPC exposure** — `GetProof()` works against the in-memory KeyIndex as
  before. Persistence is transparent to the proof layer.
