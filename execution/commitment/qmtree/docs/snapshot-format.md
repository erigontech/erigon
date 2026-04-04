# QMTree Snapshot File Format

This document describes the on-disk format for qmtree snapshot files.
All files live in `snapshots/domain/` alongside other Erigon domain files.

## File Types

| File | Purpose | Key → Value |
|------|---------|-------------|
| `v1.0-qmtree.{from}-{to}.kv` | Entry data | txNum (8B BE) → pre(32B) \|\| sc(32B) \|\| trans(32B) |
| `v1.0-qmtree.{from}-{to}.kvi` | Entry index | txNum (8B) → byte offset into .kv (RecSplit) |

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

## Key → TxNum Lookup

The separate keyindex has been removed. To find the latest txNum for a
state key, use Erigon's existing inverted index:

```go
iter, _ := tx.IndexRange(kv.AccountsHistoryIdx, key, -1, -1, order.Desc, 1)
txNum := iter.Next()  // most recent txNum that wrote this key
```

## Lookup Chain

To prove a state key was written:

```
key     → txNum     (Erigon inverted index: IndexRange desc limit=1)
txNum   → offset    (qmtree.kvi)
offset  → entry     (qmtree.kv)
entry   → proof     (in-memory tree: GetProof(txNum))
```

## Future: Twig Roots

Twig roots (~24 KB per step) will be stored in snapshots alongside entries.
These are needed for:
- **Fresh-start**: new nodes load twig roots from snapshots to build the
  upper tree without replaying all entries
- **Pruning**: non-archival nodes can prune old entries while keeping twig
  roots to maintain the correct qmtree root
- Twig roots are never pruned — they're small and required for the upper tree

## MDBX Hot Tables

During execution, entries are written to MDBX tables before being
frozen to snapshot files:

| Table | Key | Value |
|-------|-----|-------|
| `QMTreeEntries` | txNum (8B BE) | pre(32B) \|\| sc(32B) \|\| trans(32B) |
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
