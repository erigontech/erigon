# POC: On-Demand Sparse Snapshot Loading via BitTorrent

## Goal Statement

Prove that an Erigon node can serve historical RPC requests (e.g., `eth_getBlockByNumber`, `eth_getTransactionReceipt`) **without downloading complete snapshot files upfront**, by fetching only the needed pieces on-demand from the BitTorrent swarm when an RPC request arrives.

A "minimal" node starts with only index files (`.idx`) and torrent metadata (`.torrent`), but no segment data (`.seg`). When an RPC request references a historical block, the node uses the index to calculate which byte offset in the segment contains the data, maps that to a BitTorrent piece, downloads just that piece (2 MiB) from peers, and serves the response. Subsequent requests for nearby data hit the already-downloaded piece — no additional network round-trip.

## Background

### Current Architecture

Erigon stores historical blockchain data in frozen **snapshot files**:

```
v1-000000-000500-headers.seg    (blocks 0-500k headers, compressed)
v1-000000-000500-headers.idx    (recsplit perfect hash → byte offset)
v1-000000-000500-bodies.seg     (block bodies)
v1-000000-000500-bodies.idx
v1-000000-000500-transactions.seg
v1-000000-000500-transactions.idx
```

**Read path** (RPC → data):
1. `eth_getBlockByNumber(N)` → `BlockReader.HeaderByNumber(N)`
2. `RoSnapshots.ViewSingleFile(Headers, N)` → finds `VisibleSegment` covering block N
3. `segment.Index().OrdinalLookup(N - baseDataID)` → byte offset in `.seg` file
4. `segment.MakeGetter().Reset(offset)` → seek in mmap'd file
5. `getter.Next()` → Huffman decompress → RLP decode → return header

If step 2 returns `ok=false` (file missing), the RPC returns `nil` (no data found).

**Download path** (BitTorrent):
- Each `.seg` file has a corresponding `.torrent` with **2 MiB pieces**
- The downloader calls `torrent.DownloadAll()` — always downloads the entire file
- Prune mode determines which files are downloaded (archive/full/minimal)

### Key Insight: The Library Already Supports Sparse Access

The anacrolix/torrent library (v1.61.1) provides:

```go
// Create a streaming reader that triggers on-demand piece downloads
reader := torrent.NewReader()
reader.Seek(byteOffset, io.SeekStart)  // Seek to any byte position
reader.SetReadahead(2 * 1024 * 1024)   // Only download 1 piece ahead
reader.SetResponsive()                  // Don't wait for piece hash verification
reader.Read(buf)                        // Blocks until piece is downloaded, then returns data

// Or explicit piece control:
torrent.DownloadPieces(begin, end)      // Request specific piece range
torrent.CancelPieces(begin, end)        // Cancel downloads
torrent.PieceState(i)                   // Query piece completion state
```

The Reader.Read() call **blocks until the data is available**, automatically requesting the needed pieces from peers. This is exactly the on-demand mechanism we need.

### Why This Is Interesting

| Mode | Disk Usage | Start Time | RPC Coverage |
|------|-----------|------------|--------------|
| Archive (current) | ~2 TB | Hours (download all) | Full history |
| Full (current) | ~500 GB | Hours (download subset) | Post-merge only |
| **Sparse (this POC)** | **~10 GB (indexes only)** | **Minutes** | **Full history, on-demand** |

A sparse node could serve as a lightweight historical data provider, paying a per-request latency cost (~1-5s for first access to a piece, then cached) instead of upfront bulk download cost.

## POC Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     RPC Layer                           │
│  eth_getBlockByNumber(N) → BlockReader.HeaderByNumber() │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│              RoSnapshots.ViewSingleFile()                │
│                                                         │
│  Currently: returns (nil, false) if .seg missing        │
│  POC:       returns a *VisibleSegment backed by         │
│             a SparseDirtySegment that fetches on demand  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│              SparseDirtySegment                          │
│                                                         │
│  Has: .idx file (mmap'd, fully local)                   │
│  Has: .torrent metadata (info-hash, piece hashes)       │
│  Has: torrent.Reader for on-demand piece access         │
│                                                         │
│  OrdinalLookup(N) → byte offset                        │
│  MakeGetter().Reset(offset) → fetches piece via BT     │
│  getter.Next() → decompress from downloaded piece       │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│          anacrolix/torrent (existing library)            │
│                                                         │
│  Reader.Seek(offset) → sets piece priority              │
│  Reader.Read(buf)    → blocks until piece downloaded    │
│  Piece downloaded from BitTorrent swarm (2 MiB)         │
│  Piece cached on disk via torrent storage               │
└──────────────────────────────────────────────────────────┘
```

### Integration Points (3 touch points)

#### 1. New Type: `SparseDecompressor` (wraps torrent Reader as data source)

**File**: `db/seg/sparse.go` (new)

The existing `Decompressor` opens a local file and mmaps it. The `SparseDecompressor` wraps a torrent Reader to provide the same `[]byte` data interface but fetches pieces on demand.

The key challenge: `Decompressor.data` is a `[]byte` slice (mmap'd), and `Getter` reads directly from it via index arithmetic (`g.data[g.dataP]`). We can't use a torrent Reader as a drop-in replacement for mmap because the Getter does random byte-level access within the data slice.

**Approach**: Pre-fetch the needed piece(s) into a local file, then mmap that file. The torrent's part-file storage already handles this — pieces land on disk as they're downloaded. We just need to:

1. Create a sparse/empty `.seg` file of the correct size (or use the torrent's part-file)
2. When a read is needed at byte offset X: calculate which piece contains X, trigger download of that piece, wait for completion
3. The Decompressor's mmap will then see the data (the OS page cache serves the newly-written piece)

**Simpler approach for POC**: Use `torrent.NewReader()` to read the needed byte range into a buffer, then pass that buffer to the Getter. This avoids mmap complexity entirely.

```go
// SparseSegment wraps a torrent to provide on-demand segment access.
type SparseSegment struct {
    t          *torrent.Torrent
    idx        *recsplit.Index     // locally available index file
    segType    snaptype.Type
    from, to   uint64              // block range
    pieceLen   int64               // piece length (2 MiB)

    mu         sync.Mutex
    dictBuf    []byte              // cached: decompression dictionary (file header)
    dictReady  bool
}

// Get fetches a single record by global ID, downloading pieces on demand.
func (ss *SparseSegment) Get(globalId uint64) ([]byte, error) {
    // 1. Index lookup (local, instant)
    offset := ss.idx.OrdinalLookup(globalId - ss.idx.BaseDataID())

    // 2. Ensure dictionary is loaded (one-time, first few pieces of file)
    if err := ss.ensureDictionary(); err != nil {
        return nil, err
    }

    // 3. Read compressed record from torrent at offset
    buf, err := ss.readAt(offset, maxRecordSize)
    if err != nil {
        return nil, err
    }

    // 4. Decompress using dictionary
    return ss.decompress(buf)
}

// readAt fetches bytes from the torrent, triggering piece download as needed.
func (ss *SparseSegment) readAt(offset int64, size int) ([]byte, error) {
    reader := ss.t.NewReader()
    defer reader.Close()
    reader.SetReadahead(int64(ss.pieceLen)) // download 1 piece ahead
    reader.SetResponsive()
    reader.Seek(offset, io.SeekStart)

    buf := make([]byte, size)
    n, err := io.ReadFull(reader, buf)
    if err == io.ErrUnexpectedEOF {
        return buf[:n], nil
    }
    return buf[:n], err
}
```

**Dictionary caching**: The `.seg` file header contains Huffman dictionaries needed for decompression. This is in the first few KB of the file. We fetch this once on first access and cache it. Subsequent record reads only need the dictionary + the record bytes.

#### 2. Modified: `RoSnapshots.ViewSingleFile()` — sparse fallback

**File**: `db/snapshotsync/snapshots.go` (modify ~10 lines)

Currently when `ViewSingleFile` can't find a segment, it returns `(nil, false, noop)`. We add a fallback that checks if a sparse segment is available:

```go
func (s *RoSnapshots) ViewSingleFile(t snaptype.Type, blockNum uint64) (
    segment *VisibleSegment, ok bool, close func(),
) {
    // Existing path: check visible segments
    s.visibleLock.RLock()
    defer s.visibleLock.RUnlock()
    segmentRotx := s.visible[t.Enum()].BeginRo()
    for _, seg := range segmentRotx.Segments {
        if blockNum >= seg.from && blockNum < seg.to {
            return seg, true, segmentRotx.Close
        }
    }
    segmentRotx.Close()

    // NEW: Sparse fallback
    if s.sparseProvider != nil {
        return s.sparseProvider.ViewSingleFile(t, blockNum)
    }

    return nil, false, noop
}
```

#### 3. New Type: `SparseProvider` — manages sparse segments

**File**: `db/snapshotsync/sparse_provider.go` (new)

Bridges the gap between the snapshot layer and the downloader's torrent client:

```go
type SparseProvider struct {
    torrentClient *torrent.Client
    snapDir       string

    mu       sync.RWMutex
    segments map[string]*SparseSegment  // "headers-000000-000500" -> SparseSegment
}

// ViewSingleFile returns a VisibleSegment backed by sparse on-demand loading.
func (sp *SparseProvider) ViewSingleFile(t snaptype.Type, blockNum uint64) (
    *VisibleSegment, bool, func(),
) {
    sp.mu.RLock()
    // Find the sparse segment covering this block range and type
    seg := sp.findSegment(t, blockNum)
    sp.mu.RUnlock()

    if seg == nil {
        return nil, false, noop
    }

    // Return a VisibleSegment that delegates Get() to the SparseSegment
    vs := &VisibleSegment{
        Range:   Range{from: seg.from, to: seg.to},
        segType: t,
        src:     seg.asDirtySegment(), // adapter
    }
    return vs, true, noop
}
```

### What Gets Downloaded vs What's Local

| Component | Size | Location | When |
|-----------|------|----------|------|
| `.idx` files | ~5-10 GB total | Local disk | Upfront (small, fast download) |
| `.torrent` files | ~few MB total | Local disk | Upfront (from preverified list) |
| `.seg` dictionary/header | ~1-10 KB per file | In-memory cache | First access per file |
| `.seg` piece (2 MiB) | 2 MiB per piece | Torrent part-file on disk | On-demand per RPC request |

### Request Flow (Concrete Example)

1. RPC: `eth_getBlockByNumber("0x1000", false)` (block 4096)
2. `BlockReader.HeaderByNumber(4096)`
3. `ViewSingleFile(Headers, 4096)` → no local segment → sparse fallback
4. `SparseProvider.findSegment(Headers, 4096)` → `SparseSegment{v1-000000-000500-headers}`
5. `sparseSegment.Get(4096)`:
   a. `idx.OrdinalLookup(4096)` → byte offset 847392 (local, instant)
   b. `ensureDictionary()` → reads first ~4KB of torrent (pieces 0-1, cached after first call)
   c. `readAt(847392, 4096)` → piece index = 847392 / 2097152 = **piece 0** (already cached!)
   d. Decompress with Huffman dictionary → RLP decode → return header
6. Total latency: ~1-5s first request (piece download), <1ms subsequent (cached)

### Decompression Without Full Decompressor

The biggest technical challenge is that `seg.Decompressor` is designed for full files with mmap. For the POC, we have two viable approaches:

**Option A: Piece-level file materialization (recommended for POC)**

When a piece is needed:
1. Use `torrent.DownloadPieces(pieceIdx, pieceIdx+1)` to download just that piece
2. The torrent storage backend writes it to the part-file on disk
3. Once the piece is complete, the bytes are available via `torrent.NewReader().ReadAt()`
4. Parse the dictionary from the file header (first access) and build a `patternTable`
5. Use a standalone decompression function that takes `(dictionaries, rawBytes, offset)` → decompressed record

This approach reuses the torrent storage layer and avoids reimplementing file I/O.

**Option B: Full sparse file with mmap**

Create a sparse file of the full segment size (appears large but occupies no disk). Mmap it. As pieces arrive, they're written to the correct offset. The mmap automatically sees the new data. Then the existing `Decompressor` works unchanged.

This is cleaner but requires coordinating between torrent storage writes and the mmap view. Viable but more complex for a POC.

**We go with Option A** for simplicity. A standalone `SparseGetter` that operates on byte buffers rather than mmap'd files.

### Files Summary

#### New Files

| File | Purpose | ~Lines |
|------|---------|--------|
| `db/seg/sparse.go` | `SparseSegment`: index + torrent reader + decompression | ~200 |
| `db/seg/sparse_test.go` | Unit tests for sparse decompression | ~100 |
| `db/snapshotsync/sparse_provider.go` | `SparseProvider`: manages sparse segments, bridges to torrent client | ~150 |
| `db/snapshotsync/sparse_provider_test.go` | Integration tests | ~100 |

#### Modified Files

| File | Change | ~Lines |
|------|--------|--------|
| `db/snapshotsync/snapshots.go` | Add `sparseProvider` field to `RoSnapshots`, fallback in `ViewSingleFile` | ~15 |
| `db/downloader/downloader.go` | Add `TorrentByName(name) (*torrent.Torrent, bool)` accessor | ~5 |

#### Untouched

| Component | Why |
|-----------|-----|
| `BlockReader` | Calls `ViewSingleFile` which now has sparse fallback — no changes needed |
| RPC handlers | Call `BlockReader` — no changes needed |
| `Decompressor` | Existing mmap-based code unchanged — sparse path uses its own decompression |
| Download flow | `SyncSnapshots` / `DownloadAll` unchanged — sparse is a separate path |

### POC Scope Boundaries

**In scope:**
- `eth_getBlockByNumber` (header + body) from sparse snapshots
- `eth_getBlockByHash` (via header index)
- `eth_getTransactionReceipt` (requires header + body + transaction segments)
- Piece-level caching (downloaded pieces persist on disk)
- Dictionary caching (parsed once per segment file)

**Out of scope (future work):**
- Historical state queries (`eth_getBalance` at old blocks) — requires state snapshots, different architecture
- Performance optimization (prefetching, parallel piece downloads, LRU eviction)
- Automatic index-only download mode (currently manual)
- Seeding of downloaded pieces back to the network
- Piece eviction / disk space management

### RPC Test Plan

Based on the [erigontech/rpc-tests](https://github.com/erigontech/rpc-tests) format, we'll create test cases that exercise historical block access. Each test is a JSON file with `test`, `request`, and `response` sections.

We target mainnet blocks in the 0-500k range (first snapshot segment) since these are well-known and deterministic.

| Test File | RPC Method | Block/Tx | What It Proves |
|-----------|-----------|----------|----------------|
| `test_sparse_01.json` | `eth_getBlockByNumber` | Block 1 (genesis+1) | Earliest block access |
| `test_sparse_02.json` | `eth_getBlockByNumber` | Block 46147 | First tx-containing block |
| `test_sparse_03.json` | `eth_getBlockByNumber` (fullTx=true) | Block 46147 | Full transaction objects |
| `test_sparse_04.json` | `eth_getBlockByHash` | Block 100000 hash | Hash-based lookup |
| `test_sparse_05.json` | `eth_getTransactionReceipt` | First mainnet tx | Receipt from sparse segments |
| `test_sparse_06.json` | `eth_getBlockByNumber` | Block 499999 | Last block in first segment |
| `test_sparse_07.json` | `eth_getBlockByNumber` | Block 500000 | First block in second segment |
| `test_sparse_08.json` | `eth_getBlockTransactionCountByNumber` | Block 46147 | Transaction count |

Test structure (example):
```json
[
  {
    "test": {
      "description": "Get block 46147 (first block with a transaction) from sparse snapshot",
      "reference": "https://etherscan.io/block/46147"
    },
    "request": {
      "jsonrpc": "2.0",
      "method": "eth_getBlockByNumber",
      "params": ["0xB443", false],
      "id": 1
    },
    "response": {
      "jsonrpc": "2.0",
      "id": 1,
      "result": {
        "number": "0xb443",
        "hash": "0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd",
        "transactions": ["0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060"]
      }
    }
  }
]
```

### Verification Plan

```bash
# Phase 1: Build
make erigon

# Phase 2: Unit tests
go test ./db/seg/... -run TestSparse -v
go test ./db/snapshotsync/... -run TestSparse -v

# Phase 3: Integration test
# Start a minimal node with only indexes + torrent metadata
# Requires at least 1 seeding peer on the network
./build/bin/erigon --datadir=./sparse-test \
  --chain=mainnet \
  --snap.sparse          # new flag: enable sparse snapshot mode \
  --http --http.api=eth  # enable RPC

# Phase 4: RPC tests
# Run against the sparse node
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0xB443",false],"id":1}'

# Expected: returns full block data (fetched on demand from BT peers)
# Previously: returns null (missing snapshot)
```

### Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Decompression without full mmap | Use standalone buffer-based decompression; dictionary cached separately |
| Record spans piece boundary | Read 2+ adjacent pieces; torrent Reader handles this transparently |
| No peers seeding pieces | POC requires at least 1 archive node seeding; production would need webseed fallback |
| Index files require segment for validation | Skip validation; trust preverified index hashes |
| First-request latency (~1-5s) | Acceptable for POC; production could prefetch |
| Huffman dictionary in file header | Cache after first fetch; ~4KB overhead per file |
