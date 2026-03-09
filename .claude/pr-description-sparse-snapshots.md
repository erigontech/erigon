# Sparse Execution State: DAS for Steps

## Problem

Ethereum execution clients store ~500GB+ of state data, most of which is never accessed
by any given node's workload. L2 bridge operators monitoring 10 accounts, shadow fork
testers validating protocol changes, and light infrastructure nodes all pay the same
500GB storage cost as full archive nodes. This limits who can run infrastructure and
where.

## Proposal

Extend Erigon's snapshot architecture with **sparse execution state** — a mode where
domain data files (`.kv`) are not stored locally but served on-demand from the P2P
network via BitTorrent. Index and accessor files (`.bt`, `.kvi`, `.kvei`) are stored
locally for fast key lookups, but the underlying data is fetched piece-by-piece when
needed.

Data availability for sparse state uses a custody model adapted from PeerDAS (EIP-7594):
**DAS for Steps**. Where PeerDAS assigns blob columns to nodes by slot, StepDAS assigns
domain file step ranges to nodes by txNum. The existing PeerDAS custody calculation,
peer selection, and gossip infrastructure in Caplin can be reused directly.

## POC Results

A proof-of-concept on branch `poc/sparse-snapshots` validated the end-to-end flow on
mainnet:

| Metric | Value |
|--------|-------|
| Local storage (indices + accessors) | ~180GB |
| Domain data stored locally | 0 (all sparse) |
| Block execution time (warm cache) | 22s |
| Block execution time (cold, first block) | 5m30s |
| Blocks processed before OOM | 19 |
| Memory at crash | alloc=90.5GB, sys=109.2GB |

The POC confirmed: sparse decompressors work with BTree accessors, domain lookups
resolve through torrent readers, commitment verification passes with all files from the
same preverified source, and the warm/cold access pattern matches torrent piece caching.

## Architecture

### Storage Model

Erigon's state is stored in domain files indexed by step range (txNum / stepSize):

```
v1-accounts.0-64.kv      (steps 0-64, ~2GB)
v1-accounts.64-128.kv    (steps 64-128, ~2GB)
v1-storage.0-64.kv       (steps 0-64, ~8GB)
...
```

Each file has companion accessor files:
- `.bt` — BTree index (key → offset in .kv)
- `.kvi` — Hash-map index (alternative lookup)
- `.kvei` — Existence filter (bloom filter for fast negative lookups)

In sparse mode:
- **Accessors**: downloaded locally (~180GB total)
- **Domain .kv data**: served on-demand via BitTorrent torrent readers
- **Commitment domain**: may be local or sparse depending on use case

### On-Demand Read Path

```
GetLatest(key) → BTree lookup → offset in .kv file
  → sparseReset(offset) → torrent Reader.Seek(offset) + Read(8MB window)
    → torrent piece request → peer/webseed download → data returned
```

Each `sparseReset()` reads an 8MB window from the torrent at the requested offset.
The torrent client downloads pieces (256KB each) on-demand, caching them locally in
pre-allocated `.kv.part` files.

### Latency Analysis

Per `sparseReset` call:
1. Piece availability check (condition variable wait)
2. Piece request to peer (16KB chunks via BitTorrent protocol)
3. Network RTT (dominant cost)
4. Piece verification (SHA1 hash per 256KB piece)
5. Storage I/O (write piece to disk, read back)

Per block with N transactions:
- Account/storage lookups: ~1-2 sparse reads per cache miss per tx
- Commitment shortened key derefs: 5-50 reads per commitment branch
- Total: 50-300 sparse reads × 8MB = 400MB-2.4GB of torrent data per block

### Memory Issue (OOM)

Each `sparseReset()` allocates a fresh 8MB `[]byte` buffer. At 200+ reads per block,
this creates 1.6GB+ of transient allocations per block. Combined with the torrent
client's unbounded piece cache, the process OOM'd at 109GB on a 128GB machine after
19 blocks.

## Use Cases

### Use Case 1: L2 Bridge Node

Bridge operators monitor a small whitelist of accounts (bridge contracts, token
contracts, rollup inbox/outbox). They execute L1 blocks to track state changes on
watched accounts and feed those into their own proving system.

**How it works with sparse:**
- Execute all transactions (required for correctness of watched accounts)
- Only persist state writes for whitelisted accounts (local MDBX)
- Read all other state on-demand from sparse torrent files
- Skip commitment/state root verification (bridge has its own proofs)
- Maintain indices for event queries

**Storage**: ~185GB (indices) + <1GB (whitelist state) vs ~500GB+ full node

**Changes needed:**
1. `FilteredStateWriter` — wraps `StateWriter`, only persists whitelisted accounts
2. `--sparse.no-commitment` flag — skip `ComputeCommitment`
3. `--sparse.accounts` config — account whitelist

### Use Case 2: Shadow Fork

Fork from mainnet at block N, replay blocks with modified EVM rules. Used for testing
protocol changes, MEV research, or upgrade validation.

**How it works with sparse:**
- All state before fork point N: served from sparse torrent reads
- New state after N: copy-on-write to local MDBX
- No state root verification needed (it's a fork)
- Recent blocks have small working set → torrent piece cache stays warm

**Storage**: ~185GB (indices) + grows only as modified accounts accumulate

**Changes needed:**
1. `--sparse.fork-block=N` — copy-on-write semantics after block N
2. First write to any account copies from sparse → local MDBX

### Use Case 3: Light Infrastructure

Nodes that serve RPC queries, index events, or provide API access but don't need full
state locally. They maintain indices for fast queries and fetch domain data on-demand.

### Use Case 4: Clustered Nodes

A deployment topology where sparse execution nodes are backed by a dedicated storage
layer. The primary nodes run sparse, handling block execution and serving RPC, while a
shared storage backend (local NAS, object store, or a full archive node) provides the
domain data.

**How it works with sparse:**
- Primary nodes: sparse mode with commitment skip, optimised for execution throughput
- Storage backend: serves torrent pieces or raw data over local network
- Latency profile: LAN reads (~0.1-1ms) instead of internet torrent reads (~50-500ms)
- Multiple primaries share one storage backend, reducing total storage cost linearly

**Storage**: N × ~185GB (indices per primary) + 1 × ~500GB (shared backend)
vs N × ~500GB (N independent full nodes)

The interaction between primary nodes and the storage backend — protocol choice (torrent
vs direct file serving), caching strategy, and failure modes — is future work. The
sparse read interface (`SparseTorrentLookup`) abstracts the data source, so the storage
backend could be a local torrent peer, an HTTP file server, or a direct filesystem
mount.

## BAL Integration (Block Access Lists)

Post-Amsterdam fork (EIP-7928), incoming blocks carry a `BlockAccessList` in the block
body containing every account address, storage slot key, balance/nonce/code change for
the block. This is a perfect prefetch oracle for sparse nodes:

```
Block arrives from network with BAL
  → Parse BAL: extract all account addresses + storage slot keys
  → BTree lookup: find offset in sparse .kv file for each
  → Map offsets → torrent piece indices
  → Request all pieces in parallel (before execution starts)
  → Execute block (all data already in piece cache)
```

BAL eliminates cold-read latency for post-Amsterdam blocks. The sparse node knows
exactly which pieces it needs before execution begins, and can download them all in
parallel. Estimated per-block time with BAL prefetch + no commitment: 5-10s.

**Limitation**: BAL is only available post-Amsterdam. Pre-Amsterdam historical blocks
don't carry access lists and will hit the cold-read pattern.

## Data Availability: DAS for Steps

### The Parallel with PeerDAS

PeerDAS (EIP-7594) assigns blob data columns to nodes using deterministic custody:

```
GetCustodyGroups(nodeID, cgc) → set of column indices
```

Erigon's domain files are indexed by step range — the same linear model:

```
PeerDAS:  custody(nodeID) → columns {3, 17, 42, 91} of slot S
StepDAS:  custody(nodeID) → steps {0-64, 128-192} of domain D
```

Both are linear, both use deterministic assignment from node ID, both provide the same
availability guarantee structure. The initial difference:
- **No erasure coding in Phase 5** — steps are canonical data, integrity via torrent
  info-hash (SHA1 per piece)
- Erasure coding and associated commitment proofs can be layered on top of the existing
  torrent transport in a later phase (see below)

### Custody Assignment

Each sparse node commits to storing and seeding a set of step ranges beyond what it
needs for its own use case. Assignment uses the same rendezvous hashing as PeerDAS's
`GetCustodyGroups()`:

```go
func GetCustodySteps(nodeID enode.ID, cgc uint64, totalSteps uint64) []StepRange {
    // Same deterministic hash-based assignment as PeerDAS
    // Maps nodeID → set of step ranges this node is responsible for
}
```

With future account-sharded snapshots, custody extends to two dimensions:

```
custody(nodeID) → set of (step_range, account_shard) pairs
```

This is exactly PeerDAS's `(slot, column)` in execution state terms.

### Reusable PeerDAS Infrastructure

| PeerDAS Component | StepDAS Reuse |
|-------------------|---------------|
| `GetCustodyGroups(nodeID, cgc)` | Assign step-range segments to nodes |
| `columnDataPeers` peer selection | Find peers holding specific segments |
| Gossip subnet infrastructure | Announce segment availability |
| `DataColumnStorage` patterns | Segment storage with existence/pruning |
| Request-response RPC | Request specific segments from peers |

### Network Cost Estimation

Over-fetch per sparse node to maintain K-redundancy:

```
O = (K × D) / N - S

Example: N=10000 nodes, K=3 redundancy, D=500GB total, S=5GB own use
  O = (3 × 500GB) / 10000 - 5GB ≈ 150MB per sparse node
```

| Network Size | % Sparse | Full Node Storage | Sparse Node Storage | Total Network |
|-------------|----------|-------------------|--------------------|-|
| 10,000 | 10% | 500GB × 9,000 | ~190GB × 1,000 | 4,690 TB |
| 10,000 | 30% | 500GB × 7,000 | ~190GB × 3,000 | 4,070 TB |
| 10,000 | 50% | 500GB × 5,000 | ~190GB × 5,000 | 3,450 TB |

Network-wide storage decreases as sparse adoption increases. Data availability is
maintained through custody commitment and webseed CDN as a baseline.

## Implementation Phases

### Phase 1: POC (DONE — branch `poc/sparse-snapshots`)
- Sparse domain file registration via `AddSparseMetadataTorrents`
- Blacklisting domain `.kv` files from download
- BTree accessors working with sparse decompressors
- `GetLatest` falling through to sparse files
- On-demand torrent reads for domain data
- **Result**: End-to-end validated on mainnet, 19 blocks processed

### Phase 2: Stability
- Buffer pooling (`sync.Pool` for 8MB buffers) — fix OOM
- Torrent piece cache size limit — prevent unbounded memory growth
- Larger readahead (1MB+ instead of 64KB)
- Concurrent readers per file (remove single mutex bottleneck)
- **Result**: Stable sparse execution at chain tip

### Phase 3: Use Case Enablement
- `FilteredStateWriter` with account predicate
- `--sparse.no-commitment` flag (skip trie computation)
- `--sparse.accounts` whitelist config
- `--sparse.fork-block` for shadow fork copy-on-write
- **Result**: L2/bridge and shadow fork use cases operational

### Phase 4: Performance (BAL Integration)
- BAL-driven piece prefetching before block execution
- Record-level LRU cache (avoid re-reading 8MB for <1KB records)
- Warm set for recently-accessed account/storage offsets
- Adaptive window size (64KB for accounts, 8MB for commitment)
- **Result**: 5-10s per block with BAL, sub-second for warm whitelist accounts

### Phase 5: Data Availability (DAS for Steps)
- Custody assignment adapted from PeerDAS (`GetCustodySteps`)
- Segment availability gossip via PeerDAS subnet infrastructure
- Over-fetch commitment per sparse node
- Peer selection with custody awareness
- **Result**: Decentralized data availability without CDN dependency

### Phase 6: Account Sharding (Future)
- Rendezvous hashing for content-based distribution
- Two-dimensional custody: `(step_range, account_shard)`
- **Result**: Full DAS for execution state

### Phase 7: Erasure Coding and Piece-Level Commitments (Future)

In a DAS-based world, the network needs to over-commit at the piece level — nodes must
be able to verify that peers are honestly serving data without downloading it all. This
requires erasure coding (so data can be recovered from any sufficient subset of pieces)
and commitment proofs (so individual pieces can be verified against a root).

Erasure coding and associated commitments can be built on top of the existing torrent
transport. The natural mapping:
- **Torrent pieces** (256KB each) as the unit of erasure coding
- **Piece commitments** (KZG or Merkle) committed in the torrent's info dict or a
  sidecar structure
- **Recovery** from any K-of-N encoded pieces (similar to PeerDAS's 50% threshold)

The exact mapping between raw torrent pieces and erasure-encoded pieces is not defined
at present. Key design questions include:
- Whether encoding is per-file or per-step-range
- How piece commitments integrate with torrent info-hashes
- Whether the torrent protocol's existing piece verification is sufficient or needs
  augmentation for DAS sampling proofs

## Cross-Domain Consistency and the Merge Problem

### The Problem

Domain files (`.kv`, `.bt`, `.kvi`, `.kvei`) from different sources are NOT
interchangeable, even when they represent the same blockchain state. Three coupling
mechanisms create this constraint:

1. **BTree offsets are layout-dependent**: A `.bt` index maps keys to byte offsets
   within the corresponding `.kv` file. Different compression settings, key ordering,
   or build parameters produce different `.kv` layouts, making their `.bt` files
   incompatible. A `.bt` built from one `.kv` will return wrong offsets when used with
   a different `.kv` covering the same step range.

2. **Commitment uses `ReplaceKeysInValues`**: The commitment domain stores trie branch
   nodes with shortened key references — file offsets pointing into accounts and storage
   domain `.kv` files. These references are build-specific. A commitment file built
   against one set of accounts/storage `.kv` files cannot resolve its shortened keys
   against a different set, even if the logical state is identical.

3. **Salt-dependent hash indices**: `.kvi` hash-map indices use a random salt for their
   hash function. Same keys + different salt = different index layout. The salt is chosen
   at build time and embedded in the index file.

### What Happens When Sources Mix

During the POC, mixing domain files from different sources produced:
- BTree panics (offset out of bounds in mismatched `.kv`)
- Commitment `empty branch data` errors (shortened key lookup failed)
- `replace back lost account full key` errors (key deref returned wrong data)
- Silent data corruption (wrong account data returned without error)

### The Merge Dimension

Erigon's aggregator periodically **merges** smaller step-range files into larger ones.
For example, `accounts.0-64.kv` + `accounts.64-128.kv` → `accounts.0-128.kv`. This
merge process:
- Reads from the source `.kv` files (would trigger sparse torrent reads)
- Produces new `.kv` files with different internal layout than the originals
- Rebuilds all accessors (`.bt`, `.kvi`, `.kvei`) against the new layout
- The merged files are now inconsistent with any remaining unmerged sparse files

This means a sparse node that runs merging would produce local files that are
incompatible with the preverified torrent files it depends on for sparse reads. The
commitment domain's shortened key references would break across the boundary between
locally-merged and remotely-served files.

### Current Mitigation

The POC uses `--snap.state.stop` to disable merging entirely. All domain files come
from the same preverified build pipeline, ensuring cross-domain consistency. This is
sufficient for the initial use cases (L2/bridge, shadow fork) where the node doesn't
need to produce its own merged state.

### Future Work: Merge Management for Sparse Nodes

For sparse nodes that need to run long-term at chain tip, merge management must be
resolved. Possible approaches:

1. **Never merge sparse ranges**: Only merge locally-produced step ranges (post-sync).
   Sparse ranges remain as-is from the preverified source. Requires the aggregator to
   distinguish sparse from local files in merge range selection.

2. **Merge with re-download**: When merging a range that includes sparse files,
   download the full `.kv` data for those files first, merge locally, then discard the
   downloaded data. Expensive but maintains consistency.

3. **Network-coordinated merges**: The preverified source periodically publishes new
   merged files. Sparse nodes adopt them atomically (swap old sparse files for new
   merged sparse files). Requires the preverified pipeline to publish merge generations.

4. **Commitment domain always local**: Keep commitment files local (not sparse) and
   rebuild shortened key references whenever the underlying domain files change. This
   decouples commitment from the sparse/local boundary but requires commitment to be
   re-derived from the current file layout.

The right approach likely depends on the use case: L2/bridge nodes (short-lived, no
merge needed), shadow forks (ephemeral, no merge), light infrastructure (long-lived,
needs option 1 or 3).

## Future Research Directions

### Sparse Indexing

The current design downloads all index and accessor files locally (~180GB). For use
cases with very narrow account whitelists (e.g. a bridge watching 10 contracts), most
of this index data is also dead weight — the node only ever looks up a handful of keys
across hundreds of BTree and hash-map index files.

Sparse indexing would extend the on-demand model to accessor files themselves: instead
of downloading all `.bt` and `.kvi` files, serve index lookups from the network. This
is a harder problem than sparse domain data because:
- BTree traversal requires multiple random reads within the index file
- Existence filters (bloom filters) must be local for fast negative lookups
- Index files are smaller individually but numerous (thousands of files)

The feasibility and performance characteristics of sparse indexing are a research topic.
Possible approaches include: hierarchical index summaries (top-level nodes local, leaf
nodes sparse), account-sharded index files (only download indices for your shard), or
pre-computed index subsets for known whitelist accounts.

## Files Modified (POC)

| File | Change |
|------|--------|
| `db/downloader/sparse_adapter.go` | Torrent reader creation, metadata-only registration |
| `db/downloader/downloader.go` | `sparseTorrents` exclusion from download stats |
| `db/snapshotsync/snapshotsync.go` | Blacklist building, step range extraction |
| `db/state/aggregator.go` | `OpenSparseFiles()` injects FilesItem entries |
| `db/state/dirty_files.go` | `sparse` flag, visibility bypass, preserve across OpenFolder |
| `db/state/domain.go` | `SparseTorrentLookup` interface, skip sparse without btree |
| `db/seg/decompress.go` | `Getter.Size()` fix for sparse, `sparseReset` read window |
| `node/eth/backend.go` | Wiring: metadata registration, wait, open sparse files |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| OOM from buffer allocation | Phase 2: sync.Pool + piece cache limit |
| Webseed CDN goes down | Phase 5: DAS custody provides decentralized availability |
| Torrent pieces arrive slowly | Phase 4: BAL prefetch eliminates cold reads |
| Cross-domain data consistency | All domain files must come from same source; merging disabled (see Merge Problem section) |
| Commitment overhead for full validation | Phase 3: skip commitment for use cases with own proofs |
| Pre-Amsterdam blocks lack BAL | Fallback to cold-read pattern; L2/bridge only care about tip |
