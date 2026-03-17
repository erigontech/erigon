# Sparse Snapshot POC — Design Issues & Compromises

Issues discovered during implementation, captured for revisiting in later stages.

## 1. BlockReader bypasses VisibleSegment.Get()

`BlockReader.headerFromSnapshot()` and similar methods call `sn.Src().MakeGetter()` directly
on the `DirtySegment`, bypassing the `VisibleSegment.Get()` convenience method. This means
we can't simply override `Get()` — we need the `DirtySegment` itself to work with sparse data.

**Impact**: We need either a real `Decompressor` (or compatible shim) inside the `DirtySegment`,
or we need to modify BlockReader methods. The POC takes the approach of intercepting at a
higher level.

**Future**: Consider adding an interface (`SegmentDataProvider`) that both `Decompressor` and
`SparseDecompressor` implement, and making `DirtySegment` use the interface.

## 2. Getter requires contiguous []byte data slice

The `Getter` type reads directly from a `[]byte` data slice using index arithmetic
(`g.data[g.dataP]`). It cannot work with an `io.Reader` or any lazy-loading abstraction.
For sparse access, we must materialize the needed bytes into a buffer before creating the Getter.

**Impact**: Each sparse record access allocates a 64KB buffer, reads from the torrent into it,
then creates a temporary Getter. This is fine for POC but wasteful at scale.

**Future**: Consider a `LazyGetter` that reads from `io.ReaderAt` on demand, or use mmap on
a sparse file that gets pieces written into it by the torrent storage layer.

## 3. Thread safety of torrent Reader

The anacrolix/torrent `Reader` is not safe for concurrent use (documented in library).
Each sparse segment needs either a dedicated Reader per goroutine, or serialized access
behind a mutex.

**Impact**: POC uses a mutex around the reader. Under high concurrency, this becomes a
bottleneck.

**Future**: Use `torrent.NewReader()` per request, or use `Torrent.DownloadPieces()` +
direct storage access which avoids the Reader entirely.

## 4. Decompressor header parsing duplicated

The `SparseDecompressor.parseHeader()` reimplements the same Huffman dictionary parsing
logic from `NewDecompressorWithMetadata()`. This is fragile — if the format changes, both
must be updated.

**Impact**: Maintenance burden. If the seg format evolves (new version byte, new features),
the sparse path may break silently.

**Future**: Refactor `Decompressor` to accept an `io.ReaderAt` instead of requiring mmap.
Or extract dictionary parsing into a shared function that works on `[]byte`.

## 5. No mmap for sparse data

The standard path uses mmap which gives the OS full control over page caching, eviction,
and readahead. The sparse path reads into heap-allocated buffers which don't benefit from
these optimizations.

**Impact**: Higher memory pressure under load. No automatic eviction of old data.

**Future**: Use sparse files + mmap. Create a file of the full segment size (sparse, no disk
usage), mmap it, then write torrent pieces to the correct offsets. The mmap view sees the
data immediately. The OS handles caching.

## 6. Record size unknown before reading

We don't know how large a compressed record is before reading it. The POC reads a 64KB window
which is generous for most records, but extremely large records (e.g., blocks with many
transactions) could exceed this.

**Impact**: Could silently truncate large records.

**Future**: Read the first few bytes to determine record length (the Huffman-encoded word length),
then read exactly that much. Or use a growing-buffer approach.

## 7. Index files must be fully local

The recsplit `.idx` files must be fully present on disk for `OrdinalLookup()` to work (they're
mmap'd). There's no sparse access path for indexes.

**Impact**: Indexes are ~5-10GB for mainnet. This is the minimum disk requirement even for a
"sparse" node.

**Future**: Consider remote index access, or smaller index formats. Alternatively, download
indexes via BitTorrent normally (they're much smaller than segments).

## 8. Dictionary caching per-file

Each segment file has its own Huffman dictionaries in its header. The `SparseDecompressor`
parses and caches these per file. For mainnet with ~1000 segment files, this means ~1000
dictionary sets in memory.

**Impact**: ~100MB memory for all dictionaries (rough estimate). Acceptable.

**Future**: Could LRU-evict dictionaries for rarely accessed files.

## 9. Aggregator Merge Panic on Startup (Blocks Integration Testing)

The state aggregator's background merge goroutine panics with `index out of range`
in `db/recsplit/eliasfano32/elias_fano.go:819` during `InvertedIndexRoTx.mergeFiles`.
This happens when datadirs created by older binary versions are opened by the current code.
The merge state format has changed.

**Stack**: `elias_fano.go:819 → sequence_builder.go:129 → merge.go:681 → aggregator.go:1606`

**Impact**: Cannot reuse existing synced datadirs for sparse POC integration testing.
The startup process is known to be flakey. Refactoring it is a big functional change
affecting all prune modes and needs significant testing — not appropriate for the POC stage.

**Workaround**: Need a `--sync.no-merge` flag, or fix merge code to handle stale state
gracefully, or sync a fresh datadir from scratch.

## 10. SyncSnapshots Stage Blocks on Missing .seg Files

The `OtterSync` stage calls `SyncSnapshots` which waits for ALL files in the preverified
registry to download before proceeding. For sparse mode, we intentionally don't have
some .seg files — they should be loaded on-demand.

**Impact**: The node gets stuck in the download stage, can't reach RPC readiness.

**Solution options** (for future work):
- Add a `--snap.sparse` flag that skips the download wait for blocks-layer .seg files
- Make `SyncSnapshots` aware of sparse-capable segments and skip them
- Separate the "download everything" path from the "index snapshots" path

## 11. Canonical Hash Dependency for RPC

The RPC path `eth_getBlockByNumber → blockByNumberWithSenders → GetBlockNumber` resolves
block numbers to canonical hashes via the chaindata DB. These entries are populated during
the snapshot import sync stage. A fresh datadir has no canonical hashes for blocks 1+,
so RPC returns null even when the sparse path can find the data.

**Solution options**:
- Run snapshot import stage before entering sparse mode (needs merge fix first)
- Add a fallback that reads headers directly from snapshots without canonical hash lookup
- Pre-populate canonical hashes from snapshot headers during startup

## POC Validation Summary (2025-03-07)

### What Works (Validated with real Hoodi snapshot files)

- `SparseDecompressor` reads real .seg files correctly (v1.0, v1.1 formats, 86MB files)
- Sequential and random access patterns both work
- 500,000 record files load in <100ms
- Index lookup (OrdinalLookup) → SparseGet produces identical output to standard path
- v1.0 idx + v1.0 seg: PASS
- v2.0 idx + v1.1 seg: PASS (current production version combo)
- v1.0 bodies: PASS
- `AddIncompleteTorrentsFromDisk` correctly loads torrents without local .seg data
- `FindFileForBlock` scans torrentsByName for the right file/version
- `SparseProvider.ViewSingleFile` creates cached sparse segments on demand
- `DirtySegment.GetRecord` transparently handles both normal and sparse paths
- `BlockReader` methods converted to use `GetRecord` (7 methods updated)
- Erigon starts successfully with sparse provider wired in (on fresh datadir)
- Log output confirms `[sparse] created sparse segment file=v1.0-000000-000500-headers.seg`

### What Needs Startup Refactoring (Issues 9-11)

Full end-to-end RPC test blocked by:
1. Merge panic on existing datadirs (issue 9)
2. SyncSnapshots blocking on missing .seg files (issue 10)
3. Canonical hash lookup requiring sync stages to run first (issue 11)

These are general erigon startup issues, not specific to sparse snapshots.
