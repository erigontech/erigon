# Plan: Redesigned MPT Implementation (`execution/commitment/mpt2/`)

## Context

The current `HexPatriciaHashed` in `execution/commitment/` has accumulated severe design debt: an untyped `cell` union struct, a fixed `[128][16]cell` grid that can't hold multiple paths, tight coupling between hashing/I/O/encoding, and `computeCellHash()` with hidden side effects. See `MPT_ANALYSIS_ERIGON_EXPANDED.md` for full details.

This plan implements a clean replacement as a new package `execution/commitment/mpt2/` that:
- Uses typed pointer-based nodes (no untyped cells)
- Separates trie logic, hashing, encoding, and I/O into distinct layers
- Stores `path -> (NodeData, txnum)` instead of ad-hoc branch data
- Processes one block at a time (no batched ETL-based commitment)
- Produces identical Ethereum state roots

The existing implementation is left untouched. The new package lives alongside it.

---

## Step 1: Node Types and Path Utilities

**Files**: `mpt2/node.go`, `mpt2/path.go`

### `node.go` — Typed node definitions

```go
type Node interface {
    nodeType() NodeType
    dirty() bool
    cachedHash() []byte
}

type BranchNode struct {
    Children [16]Node   // nil = empty slot
    flags    nodeFlags
}

type ExtensionNode struct {
    Key   []byte  // HEX nibbles, no terminator
    Child Node
    flags nodeFlags
}

type AccountLeafNode struct {
    Key         []byte          // remaining HEX nibbles + terminator
    Address     common.Address
    Nonce       uint64
    Balance     uint256.Int
    CodeHash    common.Hash
    StorageRoot common.Hash     // persisted in account domain
    StorageTrie Node            // in-memory sub-trie (nil or HashNode when not expanded)
    flags       nodeFlags
}

type StorageLeafNode struct {
    Key   []byte  // remaining HEX nibbles + terminator
    Value []byte  // raw value, max 32 bytes
    flags nodeFlags
}

type HashNode struct {
    Hash common.Hash
}

type nodeFlags struct {
    hash  common.Hash
    dirty bool
}
```

**Design decisions**:
- No `DuoNode`. A `BranchNode` with 2 non-nil children costs 16 pointers (128 bytes) — acceptable.
- No embedded `Update`. Account fields live directly on `AccountLeafNode`.
- `StorageTrie` is a pointer to the in-memory storage sub-trie. When nil, `StorageRoot` is used as-is.

### `path.go` — Nibble/path utilities

Copy pure functions from `execution/commitment/trie/encoding.go`:
- `hexToCompact(hex []byte) []byte`
- `compactToHex(compact []byte) []byte`
- `keybytesToHex(key []byte) []byte`
- `hasTerm(s []byte) bool`
- `commonPrefixLen(a, b []byte) int`
- `nibblesToCompactPath(nibbles []byte) []byte` — for DB key encoding

---

## Step 2: NodeData Schema — Encoding/Decoding

**File**: `mpt2/nodedata.go`

### Domain key: `hexToCompact(nibblePath)`

Every trie node is stored in the commitment domain keyed by the compact-encoded (Hex-Prefix) nibble path from the root to that node.

Compact encoding recap (Yellow Paper Appendix C):
- First byte high nibble: `0x0` = even non-term, `0x1` = odd non-term, `0x2` = even term, `0x3` = odd term
- If odd, the first byte's low nibble holds the first data nibble
- Remaining nibbles packed two per byte

```
Nibble path        Compact key bytes     Breakdown
─────────────────  ────────────────────  ──────────────────────────────
[]                 [00]                  flag=0x00 (even,0 nibs) | no data bytes → 1 byte total
[0]                [10]                  flag=0x10 (odd,1 nib)   | nib 0 in low nibble → 1 byte total
[0,0]              [00, 00]              flag=0x00 (even,2 nibs) | pack(0,0)=0x00 → 2 bytes total
[a]                [1a]                  flag=0x10 (odd,1 nib)   | nib a in low nibble → 1 byte total
[a,b]              [00, ab]              flag=0x00 (even,2 nibs) | pack(a,b)=0xab → 2 bytes total
[a,b,c]            [1a, bc]              flag=0x1a (odd,3 nibs)  | pack(b,c)=0xbc → 2 bytes total
[a,b,…,term]       [2a, b…]              flag=0x2a (odd+term)    | … → N bytes total
```

**No collisions**: compact encoding is injective. The key insight is that **the byte length differs**: the root `[]` produces 1 byte (`[0x00]`), while path `[0,0]` produces 2 bytes (`[0x00, 0x00]`). Even though both start with `0x00`, they differ in length. Paths `[]` and `[0]` both produce 1 byte, but differ in content (`0x00` vs `0x10`). In general, for N nibbles the compact encoding is `ceil((N+1)/2)` bytes, and the flag byte disambiguates even/odd counts at the same byte length.

### Domain value: `NodeData` (compact binary)

```
byte 0: type tag (0=branch, 1=extension, 2=account_leaf, 3=storage_leaf, 4=hash)

Branch:     [childBitmap:2][per set bit: 32-byte child hash]
Extension:  [keyLen:varint][key:bytes][32-byte child hash]
AccountLeaf:[keyLen:varint][key:bytes][20-byte address]
StorageLeaf:[keyLen:varint][key:bytes][valueLen:varint][value:bytes]
Hash:       [32-byte hash]
```

The `txnum` is stored by the domain system alongside the value (not inside NodeData).

Functions:
- `EncodeNodeData(n Node) ([]byte, error)`
- `DecodeNodeData(data []byte) (Node, error)`

**Key decision**: `AccountLeafNode` stores only the 20-byte address reference, not the full account RLP. The reader re-fetches from the account domain when needed for hashing. This avoids duplicating data across domains.

---

## Step 3: Reader/Writer Interfaces — DB Layer Separation

**Files**: `mpt2/reader.go`, `mpt2/db_reader.go`

### `reader.go` — Interfaces

```go
type TrieReader interface {
    ReadNode(path []byte) (data []byte, step uint64, err error)
    ReadAccount(address common.Address) (*AccountData, error)
    ReadStorage(address common.Address, storageKey common.Hash) ([]byte, error)
}

type TrieWriter interface {
    WriteNode(path []byte, data []byte, prevData []byte, prevStep uint64) error
    DeleteNode(path []byte, prevData []byte, prevStep uint64) error
}

type AccountData struct {
    Nonce       uint64
    Balance     uint256.Int
    CodeHash    common.Hash
    StorageRoot common.Hash
}
```

### `db_reader.go` — Concrete implementation

Wraps `PatriciaContext` to implement `TrieReader`/`TrieWriter`:
- `ReadNode(path)` → `ctx.Branch(compactPath)` (reads from commitment domain)
- `ReadAccount(addr)` → `ctx.Account(addr[:])` then decodes `Update` fields
- `ReadStorage(addr, key)` → `ctx.Storage(append(addr[:], key[:]...))` then extracts value
- `WriteNode(path, data, prev, step)` → `ctx.PutBranch(compactPath, data, prev)`

---

## Step 4: Hasher — Pure, Side-Effect-Free

**File**: `mpt2/hasher.go`

```go
type Hasher struct {
    sha keccak.KeccakState
    buf []byte // pre-allocated work buffer
}

func (h *Hasher) Hash(n Node) ([]byte, error)           // dispatch by type
func (h *Hasher) hashBranch(n *BranchNode) ([]byte, error)
func (h *Hasher) hashExtension(n *ExtensionNode) ([]byte, error)
func (h *Hasher) hashAccountLeaf(n *AccountLeafNode) ([]byte, error)
func (h *Hasher) hashStorageLeaf(n *StorageLeafNode) ([]byte, error)
```

Follows the exact rules from `execution/commitment/trie/hasher.go:hashChildren()`:
- **Branch**: 17-element RLP list `[child0..child15, value]`. Empty child = `0x80`.
- **Extension**: 2-element RLP list `[hexToCompact(key), childRef]`.
- **Account leaf**: 2-element RLP list `[hexToCompact(key), accountRLP]` where accountRLP = `[nonce, balance, storageRoot, codeHash]`.
- **Storage leaf**: 2-element RLP list `[hexToCompact(key), RLP(value)]`.
- **Embed vs hash**: RLP < 32 bytes → embed inline. RLP >= 32 bytes → keccak256(RLP).

Reuses: `execution/rlp/commitment.go` (`GenerateStructLen`, `RlpSerializableBytes`), `common/crypto/crypto.go` (keccak pool).

**Critical difference from current code**: `Hash()` is a **pure read** — no cell mutations, no DB loads, no keccak state leaking between calls.

---

## Step 5: Visitor Pattern

**File**: `mpt2/visitor.go`

```go
type Visitor interface {
    OnResolve(path []byte, hash common.Hash, resolved Node)
    OnInsert(path []byte, node Node)
    OnDelete(path []byte)
    OnHash(path []byte, hash []byte)
}

type NoopVisitor struct{}
```

All trie operations call visitor hooks at appropriate points. Replaces the scattered `fmt.Printf` debug traces.

---

## Step 6: In-Memory Trie — Insert/Delete/Resolve

**Files**: `mpt2/trie.go`, `mpt2/insert.go`, `mpt2/delete.go`

### `trie.go` — Core structure

```go
type Trie struct {
    root    Node
    reader  TrieReader
    visitor Visitor
}

func New(reader TrieReader) *Trie
func (t *Trie) Root() Node
func (t *Trie) Resolve(n Node, path []byte) (Node, error) // lazy-load HashNode from DB
func (t *Trie) Get(hashedKey []byte) (Node, error)
```

### `insert.go` — Standard MPT insertion

Walk trie following nibbles of hashed key:
- At `HashNode`: `Resolve()` from DB first, then continue
- At `BranchNode`: recurse into `Children[nibble]`
- At `ExtensionNode`: compute common prefix; split if keys diverge
- At leaf: update if same key, split into branch + two leaves if different
- Mark all modified nodes as dirty

Key length determines type: 64 nibbles = account, 128 nibbles = storage.

### `delete.go` — Standard MPT deletion

Walk to leaf, remove it. On the way back up:
- Branch with 1 remaining child → collapse into extension
- At depth 64 (account/storage boundary): deleting all storage doesn't delete the account, just sets `StorageRoot = EmptyRoot`

---

## Step 7: Committer — Persist Dirty Nodes

**File**: `mpt2/committer.go`

```go
type Committer struct {
    writer TrieWriter
    hasher *Hasher
}

func (c *Committer) Commit(root Node) (common.Hash, error)
```

Depth-first traversal:
1. Skip clean nodes (return cached hash).
2. Recursively commit children.
3. Hash this node via `Hasher.Hash()`.
4. Encode as `NodeData`, call `writer.WriteNode(path, data, prevData, prevStep)`.
5. Mark node clean, cache hash.

For deleted nodes, call `writer.DeleteNode`.

After commit, subtrees that are no longer needed can be replaced with `HashNode` to reduce memory.

---

## Step 8: Process() — Main Entry Point

**File**: `mpt2/process.go`

```go
type MPT2 struct {
    trie      *Trie
    hasher    *Hasher
    committer *Committer
    ctx       PatriciaContext
    rootHash  common.Hash
    trace     bool
    visitor   Visitor
}

func (m *MPT2) Process(ctx context.Context, updates *Updates, logPrefix string,
    progress chan *CommitProgress, warmup WarmupConfig) ([]byte, error)
```

Algorithm:
1. Build `TrieReader` from `m.ctx` (wraps `PatriciaContext`).
2. Initialize trie: if root hash exists, set `root = HashNode{rootHash}`; else empty.
3. Call `updates.HashSort()` — yields `(hashedKey, plainKey, update)` in lex order.
4. For each update:
   - Convert to nibbles
   - If delete: `trie.Delete(nibbles)`
   - Else: build `AccountLeafNode` or `StorageLeafNode` from update, call `trie.Insert(nibbles, node)`
   - `Resolve()` handles lazy-loading of `HashNode`s along the path
5. Call `committer.Commit(trie.root)` — hashes bottom-up, writes dirty nodes to DB.
6. Return root hash.

**No fold/unfold. No grid. No singleton special case.** The standard trie insertion handles all node shapes naturally.

---

## Step 9: SeekCommitment Replacement & Root Node Encoding

**File**: `mpt2/state.go`

Eliminate `KeyCommitmentState`. Instead, every trie node — including the root — is stored under its nibble path in the commitment domain using the same `path -> (NodeData, txnum)` schema.

### Root Node Lookup Key

The root node's lookup key is the **empty compact-encoded path**: `[]byte{0x00}`.

This is what `hexToCompact([]byte{})` produces — a single byte `0x00` meaning "even length, no terminator, zero nibbles". It is a valid, unambiguous key that cannot collide with any non-root node (which always has at least one nibble in its path).

All other trie nodes are keyed by `hexToCompact(nibblePath)`:
- A branch at nibble `[0xa]` → compact key `[0x1a]` (odd flag + nibble a)
- A branch at nibble `[0xa, 0xb]` → compact key `[0x00, 0xab]` (even, two nibbles packed)
- A leaf at depth 64 → compact key is the 33-byte compact encoding of the full 64-nibble path with terminator

### Root Node Value

The root is encoded as regular `NodeData` — it could be a `BranchNode`, `ExtensionNode`, or even a leaf (single-account trie). No special wrapper. The `txnum` comes from the domain value envelope, not from the `NodeData` payload.

For an **empty trie**, no root record exists. `SeekCommitment` returns `EmptyRoot` in that case.

### SeekCommitment Flow

```go
func (m *MPT2) SeekCommitment() (txnum uint64, rootHash common.Hash, err error)
```

1. Read root node at empty compact path: `reader.ReadNode([]byte{0x00})`
2. If not found → return `(0, EmptyRoot, nil)` (fresh/empty state)
3. If found → decode `NodeData`, compute its hash via `Hasher.Hash()`, extract `txnum` from the domain step
4. Return `(txnum, rootHash, nil)`

For `blockNum`: derive from txnum via `rawdbv3.TxNums` (same as the current fallback path).

### Why This Works

The domain system already stores `(value, step)` for every key. The `step` encodes the transaction number at which the value was written. So `SeekCommitment` simply reads the root's step to know the last committed position — no separate `KeyCommitmentState` sentinel needed.

---

## Step 10: Proof Generation

**File**: `mpt2/proof.go`

```go
func (t *Trie) Prove(key []byte) ([][]byte, error)
func VerifyProof(rootHash common.Hash, key []byte, proof [][]byte) ([]byte, error)
```

Proof generation is straightforward with a pointer-based trie:
1. Walk from root following nibbles, resolving `HashNode`s as needed.
2. At each node, RLP-encode it (via `Hasher`) and append to proof list.
3. Return proof when leaf found or absence proven.

Eliminates the current multi-step: grid → witness trie → proof.

---

## Step 11: Interface Compliance

**File**: `mpt2/interface.go`

Implement `commitment.Trie` interface:

```go
var _ commitment.Trie = (*MPT2)(nil)
```

Methods: `RootHash()`, `SetTrace()`, `SetTraceDomain()`, `SetCapture()`/`GetCapture()`, `EnableCsvMetrics()`, `EnableWarmupCache()`, `Variant()` (returns `"mpt2"`), `Reset()`, `ResetContext()`, `Process()`, `Release()`.

Add to `execution/commitment/commitment.go`:
- New constant: `VariantMPT2 TrieVariant = "mpt2"`
- New case in `InitializeTrieAndUpdates()` that creates an `MPT2` instance

This is the only change to existing code — a ~5-line addition.

---

## Step 12: Tests

**Files**: `mpt2/*_test.go`

### `mock_test.go` — Adapted test infrastructure
- Copy `MockState` and `UpdateBuilder` from `execution/commitment/patricia_state_mock_test.go`
- Adapt `MockState` to implement `TrieReader`/`TrieWriter` (store nodes in `map[string][]byte` keyed by path)

### `node_test.go` — NodeData roundtrip tests
- Encode/decode each node type, verify roundtrip fidelity

### `hasher_test.go` — Hash correctness
- Port tests from `execution/commitment/trie/hasher_test.go`
- Test embed vs hash threshold
- Test all node types produce correct RLP

### `trie_test.go` — Core trie operations
- Copy and adapt key tests from `execution/commitment/hex_patricia_hashed_test.go`:
  - `Test_ResetThenSingularUpdates`
  - `Test_EmptyUpdate`
  - `Test_UniqueRepresentation`
  - `Test_Deletions`
  - Random update sequences with known roots

### `cross_validate_test.go` — **Most critical test**
- For the same set of state updates, compute root with both `HexPatriciaHashed` and `MPT2`
- Assert root hashes are byte-identical
- Test with: single accounts, multiple accounts, storage, deletions, singleton storage, empty trie

### `proof_test.go` — Proof generation/verification
- Port from `execution/commitment/trie/proof_test.go`

---

## Step 13: Account Format Change (StorageRoot in Account Domain)

**File to modify**: `execution/types/accounts/account.go`

Currently `SerialiseV3` stores: `[nonceLen][nonce][balLen][balance][codeHashLen][codeHash][incLen][inc]`. No storage root.

Add `SerialiseV4` / `DeserialiseV4`:
```
[nonceLen][nonce][balLen][balance][codeHashLen][codeHash][rootLen][root][incLen][inc]
```

- `rootLen` = 0 if `Root == EmptyRoot` (saves 32 bytes for most accounts)
- `rootLen` = 32 otherwise
- `DeserialiseV4` is backward-compatible: detect format by checking if there are enough bytes remaining after codeHash

During `MPT2.Commit()`, after computing a storage sub-trie root hash, store it in `AccountLeafNode.StorageRoot`. The account domain writer includes this in the V4 encoding.

**Implement last** — after trie correctness is validated via cross-validation tests without this change.

---

## Implementation Order

```
Phase 1 (foundation, parallel):
  Step 1:  node.go, path.go
  Step 2:  nodedata.go
  Step 4:  hasher.go
  Step 5:  visitor.go

Phase 2 (trie core):
  Step 3:  reader.go, db_reader.go
  Step 6:  trie.go, insert.go, delete.go

Phase 3 (integration):
  Step 7:  committer.go
  Step 8:  process.go
  Step 9:  state.go
  Step 10: proof.go
  Step 11: interface.go

Phase 4 (validation):
  Step 12: all test files, especially cross_validate_test.go

Phase 5 (schema evolution):
  Step 13: account format change (SerialiseV4)
```

---

## Key Files Referenced

| Purpose | File |
|---------|------|
| `commitment.Trie` interface | `execution/commitment/commitment.go:89-119` |
| `PatriciaContext` interface | `execution/commitment/commitment.go:127-140` |
| `Update` type | `execution/commitment/commitment.go:1885-1892` |
| `Updates` / `HashSort` | `execution/commitment/commitment.go:1510-1523` |
| Reference hasher | `execution/commitment/trie/hasher.go` (especially `hashChildren()`) |
| Key encoding | `execution/commitment/trie/encoding.go` |
| RLP utilities | `execution/rlp/commitment.go`, `execution/rlp/encode2.go` |
| Keccak256 pool | `common/crypto/crypto.go` |
| Account serialization | `execution/types/accounts/account.go:607-710` |
| Mock state + UpdateBuilder | `execution/commitment/patricia_state_mock_test.go` |
| Example tests to port | `execution/commitment/hex_patricia_hashed_test.go` |
| DB integration | `execution/commitment/commitmentdb/commitment_context.go` |
| StateReader | `execution/commitment/commitmentdb/reader.go` |

## Verification

1. **Unit tests**: `go test ./execution/commitment/mpt2/...`
2. **Cross-validation**: `go test -run TestCrossValidate ./execution/commitment/mpt2/...` — computes roots with both old and new implementations; must be identical
3. **Lint**: `make lint`
4. **Build**: `make erigon`
5. **Integration**: Run erigon with `--no-downloader --chain=dev` and verify it produces blocks with correct state roots using the new trie variant
