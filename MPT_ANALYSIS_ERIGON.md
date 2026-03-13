# Merkle Patricia Trie Implementation Analysis: Erigon

## Executive Summary

Erigon's Merkle Patricia Trie (MPT) implementation is a ground-up redesign of Ethereum's state commitment mechanism. Rather than storing the trie as a traditional pointer-based tree (as in go-ethereum), Erigon uses a **grid-based, fold/unfold architecture** backed by flat key-value storage. This design eliminates the massive write amplification inherent in classical MPT implementations while remaining fully compatible with Ethereum's state root computation.

The implementation lives primarily in two layers:
- **`execution/commitment/`** — The `HexPatriciaHashed` engine that computes state roots using a 2D grid
- **`execution/commitment/trie/`** — A traditional in-memory trie used for proof generation and witness building

---

## Architecture Overview

### The Two-Layer Design

Erigon maintains **two distinct trie representations** that serve different purposes:

1. **`HexPatriciaHashed`** (`hex_patricia_hashed.go`, ~3,258 lines) — The primary commitment engine. Uses a radix-16 grid (`[128][16]cell`) to incrementally compute state roots without ever materializing the full trie in memory. This is the hot path for block processing.

2. **`trie.Trie`** (`trie/trie.go`, ~1,440 lines) — A traditional in-memory MPT with `FullNode`, `ShortNode`, `DuoNode`, `HashNode`, `AccountNode`, and `ValueNode` types. Used primarily for proof generation (`eth_getProof`) and witness building. Marked as deprecated in comments but still actively used for RPC.

### Core Data Structures

**The Grid** (`hex_patricia_hashed.go:79`):
```
grid [128][16]cell   // 128 rows x 16 columns
```
- Rows 0-63: account trie (keccak256 of address = 32 bytes = 64 nibbles)
- Rows 64-127: storage trie (keccak256 of storage key = 64 more nibbles)
- Columns 0-15: the 16 hex nibbles at each depth level

**The Cell** (`hex_patricia_hashed.go:150-168`):
Each grid cell holds:
- `hashedExtension[128]` — hashed extension key (nibbles)
- `extension[64]` — plain extension key
- `accountAddr` / `storageAddr` — plain keys for state lookups
- `hash` — computed cell hash (keccak256 of RLP-encoded node)
- `stateHash` — memoized hash for skip optimization
- `Update` — embedded state change (balance, nonce, code, storage)

**Branch Data** (`commitment.go:796`):
A compact binary encoding of trie nodes stored in the `CommitmentDomain`:
- Touch map (2 bytes): which cells were modified
- Bitmap (2 bytes): which cells exist
- Per-cell field flags + variable-length encoded data

---

## Strengths

### 1. Elimination of Write Amplification

**The fundamental innovation.** Traditional MPT implementations (go-ethereum) store every trie node as a separate database entry keyed by its hash. Updating a single leaf requires rewriting every node on the path to the root — typically 7-10 nodes. Each node write is a separate random I/O to the database.

Erigon's approach stores **flat state** (accounts, storage) in sorted tables and only persists **branch data** (the compact node encodings) in the `CommitmentDomain`. The trie structure is reconstructed on-demand through the fold/unfold mechanism. This reduces database writes per state change from O(depth) to O(1) for the state itself, plus incremental branch updates.

**Impact**: 5-10x reduction in trie-related storage I/O.

### 2. The Fold/Unfold Mechanism

The `fold()` (`hex_patricia_hashed.go:1970`) and `unfold()` (`hex_patricia_hashed.go:1770`) functions are the algorithmic heart of the system:

- **`unfold()`**: Loads branch data from the database into the grid, expanding the active frontier along the path being traversed. Only the cells needed for the current update are materialized.
- **`fold()`**: Collapses grid rows by computing hashes, encoding branch data, and writing updates. The `updateKind` system (`updateKindDelete`, `updateKindPropagate`, `updateKindBranch`) handles all three cases (deletion, leaf propagation, branch formation) cleanly.
- **`needFolding()`**: A simple prefix check (`hex_patricia_hashed.go:1850-1851`) that determines when the current key diverges from the traversal path, triggering fold operations.

This design means only the "frontier" of the trie — typically 1-2 active rows — is ever in memory during processing. The full 128x16 grid is allocated but most of it is inactive.

### 3. State Hash Memoization

The `stateHash` field on each cell (`hex_patricia_hashed.go:156,162`) allows skipping expensive keccak256 recomputation for unchanged subtrees:

- When a cell's subtree hasn't changed, its `stateHash` is reused directly
- The `stateHashLen > 0` check gates the fast path (`hex_patricia_hashed.go:2114`)
- Touched cells have their `stateHash` cleared, forcing recomputation
- Metrics (`skippedLoad`, `hadToLoad`, `hadToReset`) track memoization effectiveness

This provides significant speedups during block processing where only a fraction of the trie changes per block.

### 4. Sorted Update Processing via ETL

The `Updates` structure (`commitment.go`) uses an ETL (Extract-Transform-Load) pattern:

1. State changes are collected via `TouchPlainKey()`, `TouchAccount()`, `TouchStorage()`
2. Keys are hashed and sorted by their keccak256 hash (nibble-first ordering)
3. `HashSort()` iterates updates in sorted order, which guarantees that the fold/unfold mechanism processes keys in a trie-traversal order

This sorted iteration is critical — it ensures the grid's `currentKey` advances monotonically, making fold/unfold operations efficient (each key only triggers folds for divergent prefixes and unfolds for new depth).

### 5. Concurrent Commitment Processing

`ConcurrentPatriciaHashed` (`hex_concurrent_patricia_hashed.go:45`) enables 16-way parallelism:

- The root is unfolded to expose 16 subtries (one per first nibble)
- Each subtrie is mounted to a separate `HexPatriciaHashed` instance via `mountTo()`
- `ParallelHashSort()` processes each nibble's updates independently using `errgroup` with limit 16
- After parallel processing, `foldNibble()` merges each subtrie's result back into the root under a mutex
- `CanDoConcurrentNext()` (`hex_concurrent_patricia_hashed.go:318`) dynamically decides whether parallel processing is safe for the next block

This design respects the constraint that hash computation is inherently serial within a subtree while maximizing parallelism across independent subtrees.

### 6. Deferred Branch Update Encoding

The `DeferredBranchUpdate` system (`commitment.go:221-237`) separates concerns:

- During `fold()`, cell hashes are computed (serial, deterministic)
- Branch encoding and merging with previous data is deferred
- `ApplyDeferredBranchUpdates()` (`commitment.go:439`) runs encoding in parallel across `numWorkers`
- `sync.Pool` recycling via `deferredUpdatePool` reduces GC pressure
- The `cellEncodeData` struct (`commitment.go:205-217`) is a minimal copy of cell data (200 bytes vs. the full cell), reducing copy overhead

### 7. DuoNode Optimization

The `DuoNode` (`trie/node.go:51-56`) is an Erigon-specific optimization for branch nodes with exactly 2 children:

- Uses a bitmask + 2 pointers instead of a 17-element array
- Reduces memory from `17 * pointer_size` to `uint32 + 2 * pointer_size`
- Significant in practice since many branch nodes in Ethereum's trie have sparse children
- Transparently encodes as a standard 17-element RLP list for compatibility

### 8. Warmup Cache System

The `WarmupCache` (`warmup_cache.go:45`) pre-fetches data to avoid cold reads during commitment:

- Caches branches, accounts, and storage entries using concurrent hash maps
- A `Warmuper` goroutine reads ahead based on sorted update keys
- Can be enabled/disabled per-commitment via `WarmupConfig`
- `accountFromCacheOrDB()` / `storageFromCacheOrDB()` check cache first, falling back to DB

### 9. Comprehensive Metrics and Observability

The codebase has extensive metrics infrastructure:

- `mxTrieProcessedKeys`, `mxTrieBranchesUpdated` — high-level counters
- Per-level skip/load rates for both accounts and storage (`mxTrieStateLevelledSkipRatesAccount`)
- CSV export for detailed analysis (`EnableCsvMetrics`)
- Fold/unfold timing via `StartFolding()` / `StartUnfolding()`
- Progress reporting through the `CommitProgress` channel

### 10. Compact Key Encoding

The `HexNibblesToCompactBytes()` function efficiently packs nibble-based trie paths into byte arrays for database storage, and `CompactBytesToHexNibbles()` reverses this. This halves the storage overhead for trie path prefixes in the `CommitmentDomain`.

---

## Weaknesses

### 1. Fixed Grid Size Limits Flexibility

The grid is statically allocated as `[128][16]cell` where each `cell` is a large struct (~400+ bytes due to `hashedExtension[128]`, `extension[64]`, etc.). This means:

- **Memory**: Each `HexPatriciaHashed` instance allocates ~128 * 16 * ~400 bytes = ~800KB of grid space, most of which is inactive at any time. The concurrent variant multiplies this by 17 (root + 16 mounts).
- **Depth limit**: The 128-row limit (64 account + 64 storage nibbles) is exactly right for Ethereum's keccak256-keyed trie, but makes the implementation rigid. Any future key scheme change would require structural modifications.
- **No dynamic sizing**: Unlike a pointer-based trie that grows organically, the grid is always fully allocated regardless of the actual trie depth being traversed.

### 2. Abandoned Binary Trie Variant

`bin_patricia_hashed.go` (1,820 lines) is entirely commented out. This represents a significant amount of dead code that:

- Increases maintenance burden and cognitive overhead
- Suggests an abandoned experiment with binary (radix-2) key representation
- The initialization code (`commitment.go:157-162`) explicitly panics on `VariantBinPatriciaTrie`
- Should either be completed or removed to reduce codebase noise

### 3. Dual Trie Implementation Creates Consistency Risk

The coexistence of `HexPatriciaHashed` (for commitment) and `trie.Trie` (for proofs) creates a maintenance and correctness burden:

- Two completely different codebases must produce identical hash results
- The `trie.Trie` package is marked "Deprecated" but is actively used for `eth_getProof`
- `toWitnessTrie()` (`hex_patricia_hashed.go:1461`) bridges the two by converting grid state to a traditional trie, which is complex and error-prone (note the large amount of commented-out code at lines 1359-1458)
- Any bug in either implementation could cause state root mismatches that are extremely difficult to diagnose

### 4. Complex State Machine with Implicit Invariants

The fold/unfold mechanism relies on numerous implicit invariants:

- `currentKey`, `currentKeyLen`, `activeRows`, `depths[]`, `touchMap[]`, `afterMap[]`, `branchBefore[]` must all be consistent
- The `needFolding()` check is a simple prefix comparison, but the fold operation itself spans ~200 lines with three distinct update kinds
- `updateKindPropagate` (leaf/extension propagation) is particularly subtle — it must correctly handle the special case where all storage items are deleted (depth == 64) without deleting the account itself (`hex_patricia_hashed.go:2015-2018`)
- Debug traces (`hph.trace`) are scattered throughout with manual `fmt.Printf` calls, suggesting the system is difficult to debug without them

### 5. Unsafe Memory Operations

`getDeferredUpdate()` (`commitment.go:296-300`) uses `unsafe.Slice` and `unsafe.Pointer` for bulk memory copying:

```go
copy(
    unsafe.Slice((*byte)(unsafe.Pointer(&dst.extension[0])), cellArraysSize),
    unsafe.Slice((*byte)(unsafe.Pointer(&src.extension[0])), cellArraysSize),
)
```

While this is a performance optimization (single 200-byte memcpy vs. 5 individual array copies), it:

- Depends on struct field ordering remaining contiguous (`extension` through `stateHash` = 200 bytes)
- Will silently produce incorrect results if fields are reordered or padding changes
- Bypasses Go's memory safety guarantees
- The `cellArraysSize` constant must be manually kept in sync with the struct layout

### 6. Concurrent Variant Limitations

The `ConcurrentPatriciaHashed` has several constraints:

- Only supports `ModeDirect` — indirect mode (ETL-based) cannot be parallelized (`hex_concurrent_patricia_hashed.go:201-203`)
- `foldNibble()` acquires a mutex for merging into root (`hex_concurrent_patricia_hashed.go:73`), creating a serialization point
- `CanDoConcurrentNext()` uses a heuristic (checking zero-prefix branch length > 4) that may not always be correct
- The `mountTo()` function copies the entire grid state (lines 38-42), which is expensive for large active row counts
- Comment `// TODO clean up` at line 36 suggests the mount lifecycle management is incomplete

### 7. Proof Generation Requires Full Trie Reconstruction

For `eth_getProof`, the implementation must:

1. Touch relevant keys via the commitment system
2. Build a witness using `ProcessWitness()`
3. Convert grid state to a `trie.Trie` via `toWitnessTrie()`
4. Call `Prove()` on the traditional trie to generate proof paths

This multi-step conversion is:

- Significantly more expensive than proof generation in a trie-native implementation
- A potential source of inconsistency between computed state roots and generated proofs
- Not optimized for batch proof generation (each proof requires a separate witness building pass)

### 8. Large Cell Struct Size

The `cell` struct has fixed-size arrays that are oversized for most use cases:

- `hashedExtension [128]byte` — maximum possible extension length, but most extensions are short
- `extension [64]byte` — similarly oversized
- `storageAddr [length.Addr + length.Hash]byte` — 52 bytes always allocated
- `hashBuf common.Hash` — temporary buffer embedded in the struct

This means each cell is ~500+ bytes. With 2048 cells in the grid (128 * 16), each `HexPatriciaHashed` instance uses ~1MB just for the grid, even when processing a single key.

### 9. Lack of Formal Specification

The fold/unfold algorithm, while correct in practice (proven by running against mainnet), lacks formal documentation:

- No specification document describing the state machine transitions
- The `updateKind` system and its interaction with `touchMap`/`afterMap` bitmaps requires careful study of ~500 lines of code to understand
- The `depths[]` array semantics (what exactly does "depth" mean relative to row?) are not explicitly documented
- The special treatment of depth 64 (account/storage boundary) appears in multiple places without a centralized explanation

### 10. Branch Data Format Fragility

The `BranchData` binary encoding format (`commitment.go`) uses a custom encoding:

- Touch map, bitmap, and per-cell field flags are packed into a compact binary format
- The `BranchMerger` must correctly merge old and new branch data during updates
- No versioning in the format — changes would require migration
- The encoding/decoding logic is duplicated between `EncodeBranch()` and `encodeBranchFromCellData()` (for deferred updates), creating a consistency risk

### 11. Commented-Out Code and Debug Artifacts

Several files contain significant blocks of commented-out code:

- `hex_patricia_hashed.go:1359-1458` — Multiple commented-out functions (`readBranchData`, `nCells`, `firstNonEmptyIdx`, `terminalRowToFullNode`, `terminalRowToNode`)
- `bin_patricia_hashed.go` — Entirely commented out (~1,800 lines)
- Scattered `fmt.Printf` debug statements behind trace flags
- `//hph.trace = true` commented toggle at line 2575

This suggests ongoing development and refactoring that hasn't been cleaned up.

---

## Design Trade-offs

### Memory vs. I/O

Erigon deliberately trades a fixed ~1MB per `HexPatriciaHashed` instance (grid allocation) for massive I/O reduction. This is the right trade-off for a node that processes millions of blocks — the grid is a constant cost while I/O amplification compounds with every state update.

### Complexity vs. Performance

The fold/unfold state machine is significantly more complex than a straightforward recursive trie traversal. This complexity buys:
- Incremental processing (only touched paths are visited)
- Flat storage compatibility (no pointer chasing in the DB)
- Hash memoization (unchanged subtrees skip hashing)

### Compatibility vs. Optimization

Maintaining a separate `trie.Trie` implementation for proofs ensures compatibility with the Ethereum spec and existing tooling, at the cost of maintaining two codebases and bridging between them.

---

## Comparison with go-ethereum

| Aspect | go-ethereum | Erigon |
|--------|------------|--------|
| **Trie storage** | Each node stored by hash in LevelDB/Pebble | Flat state + compact branch data in MDBX |
| **Write amplification** | O(depth) per leaf update (~7-10 writes) | O(1) state write + incremental branch update |
| **Memory model** | Pointer-based tree, GC-managed | Fixed grid, stack-allocated cells |
| **Proof generation** | Direct from trie structure | Grid -> witness trie -> proof (multi-step) |
| **Parallelism** | Limited | 16-way concurrent via subtrie mounting |
| **State pruning** | Complex trie pruning | Naturally handled by domain snapshots |
| **Node types** | FullNode, ShortNode, HashNode, ValueNode | Same for proofs; grid cells for commitment |
| **Hash caching** | Node-level dirty flags | Cell-level stateHash memoization |

---

## Recommendations

1. **Remove or gate dead code**: The binary trie variant and commented-out functions should be removed or moved behind build tags.
2. **Formalize the fold/unfold specification**: A state machine diagram and invariant documentation would significantly reduce onboarding time and bug risk.
3. **Consider unifying proof generation**: Generating proofs directly from the grid/branch-data representation, without the intermediate `trie.Trie` conversion, would reduce complexity and improve proof generation performance.
4. **Add struct layout tests**: Given the `unsafe` memory operations in `getDeferredUpdate()`, automated tests verifying `cellEncodeData` field offsets would prevent silent corruption from struct layout changes.
5. **Profile grid utilization**: Instrumenting active row counts during mainnet processing could inform whether dynamic grid sizing or smaller fixed sizes would reduce memory overhead.

---

## Key Source Files

| File | Lines | Purpose |
|------|-------|---------|
| `execution/commitment/hex_patricia_hashed.go` | ~3,258 | Core grid-based commitment engine |
| `execution/commitment/commitment.go` | ~2,048 | Types, Update, BranchEncoder, ETL Updates |
| `execution/commitment/hex_concurrent_patricia_hashed.go` | ~360 | 16-way parallel commitment |
| `execution/commitment/commitmentdb/commitment_context.go` | ~886 | DB integration, ComputeCommitment entry point |
| `execution/commitment/warmup_cache.go` | ~200 | Pre-fetch cache for commitment |
| `execution/commitment/trie/trie.go` | ~1,440 | Traditional in-memory MPT |
| `execution/commitment/trie/node.go` | ~200 | Node type definitions (FullNode, ShortNode, etc.) |
| `execution/commitment/trie/proof.go` | ~426 | Merkle proof generation and verification |
| `execution/commitment/trie/hasher.go` | ~400 | RLP encoding and keccak256 hashing |
| `execution/commitment/trie/encoding.go` | ~200 | Key encoding (KEYBYTES/HEX/COMPACT) |
