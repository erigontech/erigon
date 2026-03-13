# Erigon MPT: Expanded Pain Point Analysis

This document expands on specific design issues in Erigon's `HexPatriciaHashed` implementation that create maintenance burden, increase bug surface area, and make the codebase harder to reason about.

---

## 1. Not Clear What Type a Cell Is

**Location**: `hex_patricia_hashed.go:150-168`

A `cell` is a union type disguised as a struct. It represents all of these simultaneously:

```go
type cell struct {
    hashedExtension [128]byte                       // extension node data
    extension       [64]byte                        // plain extension key
    accountAddr     common.Address                  // account leaf data
    storageAddr     [length.Addr + length.Hash]byte // storage leaf data
    hash            common.Hash                     // branch node hash
    stateHash       common.Hash                     // memoized state hash
    // ... length fields for each ...
    loaded          loadFlags
    Update                                          // embedded state update
}
```

The cell's "type" is inferred at runtime by checking which length fields are non-zero:

- `accountAddrLen > 0` → it's an account leaf
- `storageAddrLen > 0` → it's a storage leaf
- `hashLen > 0 && extLen > 0` → it's an extension node pointing to a branch
- `hashLen > 0 && extLen == 0` → it's a branch hash reference
- `hashedExtLen > 0` → it has a path extension (could be combined with any of the above)

This leads to combinatorial explosion. A single cell can simultaneously hold an account address, a storage address, a hash, an extension, AND a hashed extension — all at once. The code must reason about all possible combinations:

```go
// From computeCellHash (line 1060-1207) — 150 lines of nested if/else:
if cell.storageAddrLen > 0 {
    if cell.stateHashLen > 0 {
        // reuse cached...
        if !singleton {
            // ...
        }
        // ...
    } else {
        // hash from scratch...
    }
}
if cell.accountAddrLen > 0 {
    // ...
    if !storageRootHashIsSet {
        if cell.extLen > 0 {  // Extension
            if cell.hashLen == 0 {
                return nil, errors.New("computeCellHash extension without hash")
            }
            // ...
        } else if cell.hashLen > 0 {
            // ...
        } else {
            // ...
        }
    }
    if !cell.loaded.account() {
        if cell.stateHashLen > 0 {
            // ...
        }
        // reload...
    }
    // ...
}
// ELSE: neither account nor storage
buf = append(buf, 0x80+32)
if cell.extLen > 0 {
    // ...
} else if cell.hashLen > 0 {
    // ...
} else if storageRootHashIsSet {
    // ...
} else {
    // empty root...
}
```

**Impact**: There is no compile-time guarantee that a cell's fields are consistent. Any code touching a cell must defensively check all length fields. A tagged-union or sum-type approach would make the possible states explicit and prevent impossible combinations.

---

## 2. Tight Coupling Between Data and Algorithm

**Location**: `hex_patricia_hashed.go` (entire file, ~3,258 lines)

The `HexPatriciaHashed` struct is a monolithic god-object that conflates:

1. **Trie data** (grid, root, currentKey, depths, touchMap, afterMap)
2. **Traversal state** (activeRows, currentKeyLen, branchBefore)
3. **Hash computation** (keccak, keccak2, hashAuxBuffer, accValBuf)
4. **I/O context** (ctx PatriciaContext, cache)
5. **Branch encoding** (branchEncoder, auxBuffer)
6. **Concurrency support** (mounted, mountedNib, mountedTries)
7. **Debug/metrics** (trace, traceDomain, capture, metrics, hadToLoadL)
8. **Warmup** (cache, enableWarmupCache)

All of these are fields on a single struct, and all methods operate on the shared mutable state. For example, `computeCellHash()` is conceptually a pure function (compute hash from cell data), but it:

- Reads and writes `hph.keccak` (shared hasher state)
- Reads `hph.accountKeyLen` (configuration)
- Reads `hph.memoizationOff` (feature flag)
- Calls `hph.accountFromCacheOrDB()` (performs I/O)
- Writes to `cell.stateHash` (mutates input)
- Writes to `cell.hashedExtension` (mutates input)
- Increments global atomics (`hadToLoad`, `skippedLoad`, `hadToReset`)

There is no separation between the "what to compute" (algorithm) and "where to get data" (I/O) and "how to encode" (serialization). Refactoring any one concern requires understanding the other seven.

---

## 3. No Clear DB Schema

**Location**: `commitment.go:423-468` (cell serialization), `commitmentdb/commitment_context.go:833-837` (commitment state)

The commitment domain stores two kinds of data using the same `PutBranch` interface:

1. **Branch data**: Encoded as `touchMap (2 bytes) + bitmap (2 bytes) + per-cell [fieldBits (1 byte) + varint-length-prefixed fields...]`
2. **Commitment state**: Encoded as `txNum (8 bytes) + blockNum (2 bytes) + trie state blob`, stored under the magic key `KeyCommitmentState`

Neither format has:
- A version byte (no forward/backward compatibility)
- A formal schema definition (encoding is inline in `EncodeBranch()`, `fillFromFields()`, `extractBranchCellAddresses()`)
- Consistent encoding patterns (branch data uses bitfields + varints; commitment state uses fixed-width integers)

The `fieldBits` byte uses magic numbers:
```go
// From extractBranchCellAddresses (line 489):
// Field flags: extension=1, accountAddr=2, storageAddr=4, hash=8, stateHash=16
```

These are defined as typed constants (`fieldExtension`, `fieldAccountAddr`, etc.) in one place but decoded via raw bitmask checks (`fieldBits&1`, `fieldBits&2`, `fieldBits&16`) in another. The `skipCellFields()` function (`line 488-503`) and `extractBranchCellAddresses()` (`line 510-614`) each manually parse the same format independently.

**Impact**: Any change to the branch data format requires updating 3-4 separate serialization/deserialization paths in lockstep, with no automated validation that they agree.

---

## 4. Read-Only Operations with Side Effects

**Location**: `hex_patricia_hashed.go:1060-1207` (`computeCellHash`), `hex_patricia_hashed.go:876-1058` (`witnessComputeCellHashWithStorage`)

`computeCellHash()` is named as a hash computation function, but it silently mutates its input:

```go
func (hph *HexPatriciaHashed) computeCellHash(cell *cell, depth int16, buf []byte) ([]byte, error) {
    // ...
    cell.hashedExtension[64-hashedKeyOffset] = terminatorHexByte  // MUTATES cell
    // ...
    cell.setFromUpdate(update)                                     // MUTATES cell (loads from DB)
    // ...
    copy(cell.stateHash[:], buf[1:])                               // MUTATES cell (memoizes hash)
    cell.stateHashLen = int16(len(buf)) - 1                        // MUTATES cell
    // ...
    copy(cell.hash[:], storageRootHash[:])                         // MUTATES cell
    cell.hashLen = int16(len(storageRootHash))                     // MUTATES cell
}
```

Side effects of "computing" a hash:
1. **Overwrites `cell.hashedExtension`** with a terminator byte at a computed offset
2. **Loads account/storage data from DB** via `accountFromCacheOrDB()` / `storageFromCacheOrDB()` if the cell isn't loaded
3. **Writes back memoized hash** into `cell.stateHash`
4. **Overwrites `cell.hash`** and `cell.hashLen` in certain code paths
5. **Resets `hph.keccak`** (shared hasher state)

This means:
- Calling `computeCellHash()` twice on the same cell may produce different results (second call may hit the memoization path)
- The function cannot be safely called concurrently on cells sharing the same `hph` instance
- `witnessComputeCellHashWithStorage()` was created as a separate function specifically because `computeCellHash()` corrupts `cell.hashedExtension` — the comment at line 884 says: *"Use a temporary buffer for hashed key computation to avoid corrupting cell.hashedExtension which may be needed for subsequent witness operations"*

The existence of `witnessComputeCellHashWithStorage()` as a near-clone of `computeCellHash()` (lines 876-1058 vs 1060-1207, ~180 lines each) is direct evidence of how the side-effect design forces code duplication.

---

## 5. Nested If Cases for Cell Field Permutations

**Location**: `hex_patricia_hashed.go:1060-1207`, `hex_patricia_hashed.go:336-378`

The core hash computation must handle every permutation of cell field presence. `computeCellHash()` alone has this decision tree:

```
storageAddrLen > 0?
├── YES → stateHashLen > 0?
│   ├── YES (cached) → singleton?
│   │   ├── YES → set storageRootHash from cache
│   │   └── NO → return cached hash directly
│   └── NO (compute) → loaded.storage()?
│       ├── YES → leafHashWithKeyVal()
│       │   → singleton?
│       │   ├── YES → set storageRootHash, clear stateHash
│       │   └── NO → memoize, return
│       └── NO → ERROR (storage not loaded)
└── NO → (skip storage)

accountAddrLen > 0?
├── YES → storageRootHashIsSet?
│   ├── YES → (use it)
│   └── NO → extLen > 0?
│       ├── YES → hashLen == 0? → ERROR
│       │         extensionHash() → set storageRootHash
│       ├── hashLen > 0 → storageRootHash = cell.hash
│       └── else → storageRootHash = emptyRoot
│   loaded.account()?
│   ├── NO → stateHashLen > 0? → return cached
│   │         else → load from DB, setFromUpdate
│   └── YES → (already loaded)
│   accountForHashing() → accountLeafHashWithKey() → memoize
└── NO → (pure branch/extension)
    extLen > 0?
    ├── YES → hashLen > 0? → extensionHash()
    │         else → ERROR
    ├── hashLen > 0 → use hash directly
    ├── storageRootHashIsSet → use storageRootHash
    └── else → emptyRootHash
```

This is roughly 15 distinct code paths in a single function. `fillFromLowerCell()` (line 337-378) has a similarly complex conditional structure:

```go
func (cell *cell) fillFromLowerCell(lowCell *cell, lowDepth int16, preExtension []byte, nibble int) {
    if lowCell.accountAddrLen > 0 || lowDepth < 64 {  // guard 1
        cell.accountAddrLen = lowCell.accountAddrLen
    }
    if lowCell.accountAddrLen > 0 {                     // guard 2 (overlaps guard 1)
        // copy account fields...
    }
    // ...
    if lowCell.hashLen > 0 {
        if (lowCell.accountAddrLen == 0 && lowDepth < 64) || (lowCell.storageAddrLen == 0 && lowDepth > 64) {
            // prepend extension with preExtension | nibble
        } else {
            // copy extension as-is
        }
    }
}
```

The condition `(lowCell.accountAddrLen == 0 && lowDepth < 64) || (lowCell.storageAddrLen == 0 && lowDepth > 64)` distinguishes between an account-level branch hash propagation and a storage-level one, but this isn't expressed in types — it's a runtime check on two unrelated fields that happens to encode domain knowledge about the trie structure.

---

## 6. RLP Encoding Mixed with Business Logic

**Location**: `hex_patricia_hashed.go:617-682`, `hex_patricia_hashed.go:685-741`

Account RLP encoding is manually inlined in `accountForHashing()`:

```go
func (cell *cell) accountForHashing(buffer []byte, storageRootHash common.Hash) int {
    // Manual RLP length calculation
    balanceBytes := 0
    if !cell.Balance.LtUint64(128) {
        balanceBytes = cell.Balance.ByteLen()
    }
    // Manual RLP nonce encoding
    if cell.Nonce < 128 && cell.Nonce != 0 {
        buffer[pos] = byte(cell.Nonce)
    } else {
        buffer[pos] = byte(128 + nonceBytes)
        var nonce = cell.Nonce
        for i := nonceBytes; i > 0; i-- {
            buffer[pos+i] = byte(nonce)
            nonce >>= 8
        }
    }
    // ... 60+ lines of manual byte manipulation ...
}
```

Similarly, `completeLeafHash()` (`line 685-741`) mixes RLP encoding with hash-or-embed logic:

```go
func (hph *HexPatriciaHashed) completeLeafHash(...) ([]byte, error) {
    // Compute compact encoding (trie concern)
    // Compute RLP struct length (serialization concern)
    // Decide embed vs hash (optimization concern)
    canEmbed := !singleton && totalLen+pl < length.Hash
    // Choose writer based on embed decision
    var writer io.Writer
    if canEmbed {
        hph.auxBuffer.Reset()
        writer = hph.auxBuffer
    } else {
        hph.keccak.Reset()
        writer = hph.keccak
    }
    // Write RLP content to chosen writer (serialization + I/O)
    // Extract result differently based on embed decision
}
```

The RLP encoding rules (prefix bytes `0x80`, `0xC0`, length encoding) are hardcoded as magic numbers (`192 + structLength`, `247 + lengthBytes`, `128 + 32`) throughout the business logic. The `rlp` package exists and is imported, but the hot path bypasses it for performance, scattering encoding logic across multiple methods.

**Impact**: Changing the RLP encoding (e.g., for a different serialization format in a future fork) requires touching business logic functions. The manual byte manipulation is also a source of off-by-one bugs.

---

## 7. Singleton Storage Cells Need Special Handling

**Location**: `hex_patricia_hashed.go:933-964`, `hex_patricia_hashed.go:1072-1122`

A "singleton" storage cell is one where a storage leaf exists at depth <= 64 (within the account trie region, not the storage trie region). This happens when an account has exactly one storage slot and gets folded up to the account level.

The code forks on `singleton` at every point where storage is processed:

```go
singleton := depth <= 64  // line 1072

if cell.stateHashLen > 0 {
    if !singleton {
        return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil  // non-singleton: return directly
    }
    storageRootHashIsSet = true                                                                 // singleton: continue to account hashing
    storageRootHash = *(*common.Hash)(cell.stateHash[:cell.stateHashLen])
}
```

And further down:

```go
leafHash, err := hph.leafHashWithKeyVal(buf, ..., cell.Storage[:cell.StorageLen], singleton)
if !singleton {
    copy(cell.stateHash[:], leafHash[1:])                   // non-singleton: memoize
    cell.stateHashLen = int16(len(leafHash) - 1)
    return leafHash, nil
}
storageRootHash = *(*common.Hash)(leafHash[1:])             // singleton: fold into account
storageRootHashIsSet = true
cell.stateHashLen = 0                                       // singleton: CLEAR memoized hash
hadToReset.Add(1)
```

The singleton path:
- Cannot memoize its `stateHash` (because the hash depends on the account data too)
- Must compute the storage root hash and pass it forward to account hashing
- Has different embedding behavior in `completeLeafHash()` (`canEmbed := !singleton && ...`)
- Exists in both `computeCellHash()` AND `witnessComputeCellHashWithStorage()` (duplicated handling)

This creates a persistent source of bugs: any change to the non-singleton path must be evaluated for singleton impact, and vice versa. The two paths share enough structure to look similar but differ in subtle ways (memoization, return points, hash storage).

---

## 8. Cell Type Embeds Update with Conflicting Field Names

**Location**: `hex_patricia_hashed.go:150-168`, `commitment.go:1885-1892`

```go
type cell struct {
    // ... cell's own fields ...
    hash        common.Hash     // cell.hash — the node hash in the trie
    // ...
    Update                      // EMBEDDED — brings in Update's fields directly
}

type Update struct {
    CodeHash   common.Hash      // cell.CodeHash — conflicts conceptually with cell.hash
    Storage    common.Hash      // cell.Storage — conflicts with cell.storageAddr
    StorageLen int8
    Flags      UpdateFlags
    Balance    uint256.Int
    Nonce      uint64
}
```

Because `Update` is embedded (not a named field), all its fields are promoted to the `cell` namespace:

- `cell.hash` = the trie node hash (from `cell`)
- `cell.CodeHash` = the account's code hash (from `Update`)
- `cell.Storage` = the storage value (from `Update`, type `common.Hash`)
- `cell.storageAddr` = the storage key (from `cell`, type `[52]byte`)
- `cell.StorageLen` = length of storage value (from `Update`, type `int8`)
- `cell.storageAddrLen` = length of storage key (from `cell`, type `int16`)

The naming collision between `Storage`/`storageAddr` and `StorageLen`/`storageAddrLen` is confusing:
- `cell.Storage[:cell.StorageLen]` — the storage **value** (what's stored)
- `cell.storageAddr[:cell.storageAddrLen]` — the storage **key** (where it's stored)

The `reset()` method must clear both sets of fields:
```go
func (cell *cell) reset() {
    cell.accountAddrLen = 0      // cell's own
    cell.storageAddrLen = 0      // cell's own
    // ...
    cell.Update.Reset()          // must explicitly call Update.Reset() for embedded fields
}
```

And `setFromUpdate()` merges an `Update` into the cell via `cell.Update.Merge(update)`, which means `cell.Balance`, `cell.Nonce`, `cell.CodeHash`, `cell.Storage`, and `cell.Flags` are all silently shared between the cell's identity as a trie node and its role as a state carrier.

---

## 9. Grid Is Fixed Memory; Cannot Hold Flexible Paths

**Location**: `hex_patricia_hashed.go:79-83`

```go
grid          [128][16]cell  // ~1MB fixed allocation
currentKey    [128]byte
depths        [128]int16
branchBefore  [128]bool
touchMap      [128]uint16
afterMap      [128]uint16
```

The grid processes keys one at a time, sequentially. At any moment, only the cells along the **current active path** (from root to the frontier) are meaningful — all other cells are stale or empty. The `activeRows` field tracks how deep the current traversal is.

Consequences:

1. **Cannot hold two paths simultaneously**: When processing key A and then key B, all of A's grid state is folded away before B's is unfolded. This forces strictly sequential, sorted-key processing.

2. **Concurrent variant must copy the entire grid**: `mountTo()` (line 38-42 of `hex_concurrent_patricia_hashed.go`) copies all active rows:
   ```go
   for row := 0; row <= hph.activeRows; row++ {
       for nib := 0; nib < len(hph.grid[row]); nib++ {
           hph.grid[row][nib] = root.grid[row][nib]  // copy ~500 bytes per cell
       }
   }
   ```

3. **128-row limit is hardcoded**: 64 nibbles for account + 64 for storage. Any key scheme change (e.g., different hash output length) would require changing the constant AND all code that uses depth == 64 as the account/storage boundary.

4. **Wasted memory**: Only `activeRows * 16` cells are ever meaningful at once (typically 1-3 rows = 16-48 cells), but all 2,048 cells are allocated. Each cell is ~500+ bytes, so ~975KB is permanently reserved but mostly unused.

---

## 10. Different Semantics of `cell.hash` Based on Cell Type

**Location**: `hex_patricia_hashed.go:155`, used throughout

The `hash` field means different things depending on the implicit cell type:

| Cell state | `cell.hash` meaning |
|---|---|
| `accountAddrLen > 0, extLen > 0` | Hash of the **storage trie root** behind an extension node |
| `accountAddrLen > 0, extLen == 0, hashLen > 0` | Direct **storage trie root hash** (no extension) |
| `storageAddrLen > 0` | Not typically used (storage leaf hash goes to `stateHash`) |
| `accountAddrLen == 0, storageAddrLen == 0, extLen > 0` | Hash of a **child branch node** below an extension |
| `accountAddrLen == 0, storageAddrLen == 0, extLen == 0` | Direct **child branch hash** reference |
| After `computeCellHash()` | May be **overwritten** with `storageRootHash` (line 1201-1202) |

The semantic overloading is most visible in `computeCellHash()` at line 1198-1202:
```go
} else if cell.hashLen > 0 {
    buf = append(buf, cell.hash[:cell.hashLen]...)      // use as branch hash
} else if storageRootHashIsSet {
    buf = append(buf, storageRootHash[:]...)
    copy(cell.hash[:], storageRootHash[:])              // OVERWRITE hash with storage root
    cell.hashLen = int16(len(storageRootHash))
}
```

Here, `cell.hash` is repurposed from "branch child hash" to "storage root hash" — an in-place semantic change with no type-system enforcement.

Similarly, `stateHash` means:
- For storage cells: memoized RLP hash of the storage leaf node
- For account cells: memoized RLP hash of the account leaf node (including storage root)
- For branch references: not used (`stateHashLen == 0`)

The `stateHashLen` field serves double duty as both a "is memoized" flag and a length, which is why the code is littered with `cell.stateHashLen = 0` to "invalidate" the cache (lines 946, 986, 1120, 1143).

---

## 11. Redundant Boolean Flags

**Location**: `hex_patricia_hashed.go:87-89`

```go
rootChecked   bool  // Set to false if it is not known whether the root is empty
rootTouched   bool  // Whether the root has been modified
rootPresent   bool  // Whether the root is non-empty after modification
```

These three booleans encode 8 possible states, but only ~4 are meaningful:

| `rootChecked` | `rootTouched` | `rootPresent` | Meaning |
|---|---|---|---|
| false | false | true | Initial state after `Reset()` (line 2801-2803). Untested root. |
| true | false | * | Root was checked, found empty, no modifications |
| * | true | true | Root was modified and exists |
| * | true | false | Root was modified and deleted (entire trie empty) |

The flag `rootChecked` could be eliminated if `rootPresent` had a tri-state (unknown/present/absent). The check in `needUnfolding()` (line 1226-1230):
```go
if hph.root.hashedExtLen == 0 && hph.root.hashLen == 0 {
    if hph.rootChecked {
        return 0  // Previously checked, empty root
    }
    return 1  // Need to attempt to unfold the root
}
```

could instead be encoded via the root cell's own state. The current approach means the same information (root emptiness) is tracked in two places: the root cell's fields AND the boolean flags. When they disagree, the behavior is undefined.

The flags are serialized into the commitment state (`EncodeCurrentState()`, line 3082-3084) and restored on startup, meaning any flag inconsistency persists across restarts.

---

## 12. Redundant Data in Commitment Domain

**Location**: `commitmentdb/commitment_context.go:833-837`, `commitmentdb/commitment_context.go:621-644`

The commitment state stored under `KeyCommitmentState` contains:

```go
type commitmentState struct {
    txNum     uint64   // 8 bytes
    blockNum  uint64   // 8 bytes (encoded as 2 bytes in practice)
    trieState []byte   // serialized HexPatriciaHashed state
}
```

This creates redundancy because:

1. **`txNum` and `blockNum`** are already tracked by the domain system itself. Every entry in a domain has an associated transaction number. Storing `txNum` inside the commitment state blob means the "current position" is recorded in two places. The comment at line 636 acknowledges this: *"state could be equal but txnum/blocknum could be different"*.

2. **The state root hash** is derivable from the trie state, yet `encodeAndStoreCommitmentState()` is called separately from `Process()` which already computed the root hash. The root hash is not stored in `commitmentState`, but it's computed again during restore (line 711) for logging.

3. **The full trie state** (root cell + depths + touchMap + afterMap + branchBefore for all 128 rows) is serialized even though most of it is zeros. After `Process()` completes, `activeRows == 0` and `currentKeyLen == 0` (enforced by the panic at line 3087), meaning depths/touchMap/afterMap/branchBefore arrays are all zero — yet they're still serialized and stored.

The `encodeAndStoreCommitmentState()` function also does a full byte comparison with the previous state before writing (line 638), which means it reads the old state, serializes the new state, compares them, and only then decides to write. This round-trip exists because there's no cheaper way to detect "nothing changed" given the current schema.

---

## 13. Duplicated Data from Parent Row (fillFromUpperCell / fillFromLowerCell)

**Location**: `hex_patricia_hashed.go:289-334` (`fillFromUpperCell`), `hex_patricia_hashed.go:337-378` (`fillFromLowerCell`)

When unfolding (expanding a deeper level), `fillFromUpperCell()` copies data **down** from parent to child:

```go
func (cell *cell) fillFromUpperCell(upCell *cell, depth, depthIncrement int16) {
    // Copy hashed extension (minus consumed nibbles)
    cell.hashedExtLen = upCell.hashedExtLen - depthIncrement
    copy(cell.hashedExtension[:], upCell.hashedExtension[depthIncrement:upCell.hashedExtLen])
    // Copy plain extension
    cell.extLen = upCell.extLen - depthIncrement
    copy(cell.extension[:], upCell.extension[depthIncrement:upCell.extLen])
    // Copy account data (if in account region)
    if depth <= 64 {
        cell.accountAddrLen = upCell.accountAddrLen
        copy(cell.accountAddr[:], upCell.accountAddr[:cell.accountAddrLen])
        cell.Balance.Set(&upCell.Balance)
        cell.Nonce = upCell.Nonce
        cell.CodeHash = upCell.CodeHash
        cell.extLen = upCell.extLen                        // overwrites the value set 5 lines above!
        copy(cell.extension[:], upCell.extension[:upCell.extLen])  // overwrites previous copy!
    }
    // Copy storage data
    cell.storageAddrLen = upCell.storageAddrLen
    copy(cell.storageAddr[:], upCell.storageAddr[:upCell.storageAddrLen])
    cell.StorageLen = upCell.StorageLen
    copy(cell.Storage[:], upCell.Storage[:upCell.StorageLen])
    // Copy hash
    cell.hashLen = upCell.hashLen
    copy(cell.hash[:], upCell.hash[:upCell.hashLen])
    cell.loaded = upCell.loaded
}
```

When folding (collapsing back up), `fillFromLowerCell()` copies data **up** from child to parent. This is the inverse operation but not symmetric — it prepends extension bytes and has different conditional logic.

Issues with this approach:

1. **Data is physically duplicated**: After unfold, the same account address, balance, nonce, code hash, and storage data exist in both the parent cell AND the child cell. The parent cell retains this data until it's `reset()` during the next unfold at that row.

2. **Overwrite within fillFromUpperCell**: When `depth <= 64 && accountAddrLen > 0`, the extension data is copied once from `upCell` with adjustment for depth, then **immediately overwritten** with the raw copy from `upCell` (lines 313-316). This means the depth-adjusted extension is lost in favor of the full extension — the first copy was wasted work.

3. **Asymmetric operations**: `fillFromUpperCell` does `cell.hashedExtLen = upCell.hashedExtLen - depthIncrement` (subtraction), while `fillFromLowerCell` does `cell.extLen = lowCell.extLen + 1 + int16(len(preExtension))` (addition with prepend). These are inverse operations but look nothing alike in code, making it hard to verify correctness by inspection.

4. **Balance.Set() is a deep copy**: The `uint256.Int` balance requires `Set()` for a proper copy (line 310), but all other fields use simple assignment or `copy()`. Missing a `Set()` would cause aliased mutation bugs — a latent risk when modifying these functions.

---

## Summary

These pain points share a common root cause: the `cell` struct was designed as a performance-optimized union type (fixed-size, stack-friendly, no allocations) at the cost of type safety and separation of concerns. The grid-based architecture trades clarity for I/O efficiency. Over time, features were added (memoization, singleton handling, concurrency, warmup caching, deferred encoding) on top of this foundation, each adding more conditional branches to the already-complex state machine.

A potential path forward would be:
1. **Typed cell variants** (account cell, storage cell, branch cell, extension cell) that make impossible states unrepresentable
2. **Separate hash computation from state mutation** by having `computeCellHash` return the hash without modifying the cell
3. **Extract RLP encoding** into a dedicated layer below the trie logic
4. **Schema versioning** for the branch data format
5. **Dynamic grid or sparse representation** that only allocates cells for active paths
