# QMTree Snapshot File Format

This document describes the on-disk format for qmtree snapshot files.
All files live in `snapshots/domain/` alongside other Erigon domain files.

## File Types

| File | Purpose | Key → Value |
|------|---------|-------------|
| `v1.0-qmtree.{from}-{to}.kv` | Entry data | txNum (8B BE) → pre(32B) \|\| sc(32B) \|\| trans(32B) |
| `v1.0-qmtree.{from}-{to}.kvi` | Entry index | txNum (8B) → byte offset into .kv (RecSplit) |
| `v1.0-qmtree-keyindex.{from}-{to}.kvi` | Key index | keyHash (32B) → txNum (RecSplit) |

`{from}` and `{to}` are step numbers (not block numbers or txNums).
One step = `stepSize` transactions (read from `erigondb.toml`).

## Entry Data (.kv)

Fixed-size records, uncompressed. Each record is 104 bytes:

```
Offset  Size  Field
0       8     txNum          (big-endian uint64)
8       32    preStateHash   (DeriveSha MPT root over state reads)
40      32    stateChangeHash (DeriveSha MPT root over state writes)
72      32    transitionHash (rolling keccak256 over EVM + transition records)
```

Records are ordered by txNum ascending (monotonically increasing within
each step). The file size is always a multiple of 104 bytes.

File size per step = `stepSize × 104` bytes. For the default step size
of 1,562,500 that's ~155 MB per step file.

## Entry Index (.kvi)

RecSplit perfect hash index mapping `txNum → byte offset` into the
corresponding `.kv` data file.

- **Key**: txNum as 8-byte big-endian
- **Value**: byte offset (uint64) into the `.kv` file
- **Lookup**: `offset, ok := reader.Lookup(txNumBytes)` → read 104 bytes
  at that offset in the `.kv` file
- **False positives**: ~0.4% with `LessFalsePositives=true`. Callers verify
  the txNum in the record matches the queried txNum.

Built with `recsplit.RecSplitArgs{Enums: false, LessFalsePositives: true}`.

## Key Index (.kvi)

RecSplit perfect hash index mapping `keyHash → txNum` directly. No
separate data file — the txNum is stored as the RecSplit "offset" value.

- **Key**: keyHash (32 bytes) = `keccak256(domain_byte || key_bytes)`
- **Value**: txNum (uint64) of the last transaction that wrote this key
- **Lookup**: `txNum, ok := reader.Lookup(keyHash)` → use txNum to look
  up the entry via the entry `.kvi`

Domain byte values: 0 = accounts, 1 = storage, 2 = code.

Key index files are flushed at quarter-step boundaries (`stepSize/4`
transactions). Multiple small segments are produced during execution;
these can be merged into larger files.

## Lookup Chain

To prove a state key was written:

```
keyHash → txNum     (qmtree-keyindex.kvi)
txNum   → offset    (qmtree.kvi)
offset  → entry     (qmtree.kv)
entry   → proof     (in-memory tree: GetProof(txNum))
```

## MDBX Hot Tables

During execution, entries and key-index updates are written to MDBX
tables before being frozen to snapshot files:

| Table | Key | Value |
|-------|-----|-------|
| `QMTreeEntries` | txNum (8B BE) | pre(32B) \|\| sc(32B) \|\| trans(32B) |
| `QMTreeKeyIndex` | keyHash (32B) | txNum (8B BE) |
| `QMTreeMeta` | `"nextTxNum"` | uint64 |
| `QMTreeMeta` | `"prevLeaf"` | hash (32B) |

At step boundaries, the `SnapshotManager` collates MDBX entries into
`.kv`/`.kvi` snapshot files and prunes the frozen rows from MDBX.

## Step Alignment

Snapshot files are aligned to the datadir's step size (from `erigondb.toml`).
The step size varies by datadir:

| Config | Step Size | Entries per file | .kv file size |
|--------|-----------|-----------------|---------------|
| Default | 1,562,500 | 1,562,500 | ~155 MB |
| Mainnet (some datadirs) | 390,625 | 390,625 | ~39 MB |

Key index files use quarter-step boundaries: `stepSize/4` transactions
per segment.

## Versioning

The `v1.0` prefix in filenames indicates the entry format version. If the
leaf hash construction changes (e.g., different hash function or component
ordering), the version is bumped and old files must be regenerated.

The RecSplit index format has its own internal versioning (salt, features
bitmask) independent of the qmtree version prefix.
