# PBinPatriciaHashed — binary commitment engine (EIP-8297)

## Overview

Add `PBinPatriciaHashed`: a **binary** commitment engine implementing EIP-8297 (Partitioned Binary Tree), as a sibling of `HexPatriciaHashed` reusing the same grid/fold/unfold idea with a different node model.

**Problem it solves.** EIP-8297 is the EF's Standards Track successor to Verkle for the state tree: arity 2, hash-only (post-quantum), no `storage_root`, code chunked into the tree. Erigon has no binary trie. This lands one as a self-contained engine so the design can be evaluated — in particular its single-pass root computation, which has no account→storage sequential dependency and is therefore a clean testbed for parallel fold work.

**Key constraint: new engine, no external API changes.** `Trie`, `PatriciaContext`, `Updates`/`Update`, `keyHasher`, `cellEncodeData`, `BranchData` and the `nibbles` package are **not modified**. PBin is additive: new files plus one variant registration. `PatriciaContext.Branch` returns opaque bytes, so PBin uses its own branch record codec without touching the shared one.

**Scope: M0 only.** In-memory over `MockState`, `ModeDirect`, account + storage zones. Correctness against a reference oracle is the deliverable — not production wiring.

## Context (from discovery)

- Repo `/Users/awskii/org/wrk/erigon`, branch `main` @ `1e078ffb04`. Package `execution/commitment` (29,422 lines incl. tests; `hex_patricia_hashed.go` 3,164; `commitment.go` 2,345).
- Spec: `/Users/awskii/org/wrk/EIPs/EIPS/eip-8297.md`. Anchors: tags/merkelization `:187-222`, `insert`/split `:137-183`, constants `:271-278`, header values `:311-347`, storage `:399-437`, no-deletion `:441-447`, test cases `:583-630`.
- Reference points in HPH: grid `:129`, existing `cell` `:300-315`, `needUnfolding` `:1263-1319` (reads `cell.hashedExtension` at `:1310` — the mechanism that makes the cell prefix load-bearing for navigation), `fold` dispatch `:2031-2038`, `foldBranch` `:1660-1725`, `foldPropagate` `:1915-1953`, `RootHash` `:362`/`:1249`, branch DB key `:1443`, `updateKey` `:2023`.
- `BranchEncoder.CollectUpdate` `commitment.go:501-546` merges with `prev` before `PutBranch`. `keyHasher` `:1478`, `hasherReusesAddrPrefix` `:1485`.
- `MockState` test driver `patricia_state_mock_test.go:39-202`; note `Account`/`Storage` return `Flags = DeleteUpdate` for a **missing** key (`:92-95`, `:129-134`).
- Invariant tests worth porting: `hex_patricia_hashed_test.go:157-249`.

## Development Approach

- **testing approach**: TDD — in every task the failing test is written **before** the implementation it covers. The reference oracle (Task 4) exists before the engine it validates.
- **CRITICAL naming rule**: `package commitment` already declares `cell`, `computeCellHash`, `fold`, `unfold` and more. **Every new package-level identifier MUST carry a `pbin` prefix** — `pbinCell`, `pbinFold`, `pbinLeafHash`, `pbinTreeKeyAccount`, `pbinEmptyTreeHash`. Methods on new types need no prefix. A collision is a compile error, so this applies to every task.
- **CRITICAL no-API-change rule**: do not modify `Trie`, `PatriciaContext`, `Updates`/`Update`, `keyHasher`'s signature, `cellEncodeData`, `BranchData`, or the `nibbles` package. If a task appears to require it, stop and record it with ⚠️ rather than proceeding.
- complete each task fully before moving to the next
- **every task MUST include new/updated tests**, listed as separate checklist items
- **all tests must pass before starting the next task**
- **update this plan file when scope changes during implementation**
- plan is self-contained from a clean git state; no task depends on transient working-tree state

## Testing Strategy

- **unit tests**: required per task, table-driven where the input space is enumerable
- **differential tests**: root equality against the EIP reference oracle (Task 4). Note its blind spot: the oracle consumes the same value encoder as the engine, so it can **not** catch value-encoding bugs — those are pinned against hand-written hex in Task 3.
- **property tests**: permutation independence, fold/unfold round-trip, branch-record recompute across batches
- **fuzz**: codec round-trip across all bit lengths; process fuzzers with a low-entropy slot generator
- no e2e tests — library-internal engine, no UI surface

## Hazard Register

Each hazard has exactly one detecting guard. Guards are acceptance criteria, not nice-to-haves.

| ID | Hazard | Detecting guard | Task |
|----|--------|-----------------|------|
| H1 | Stale branch-cell hash after a prefix split (prefix is inside the branch hash, so shrinking it invalidates a cached hash) | Oracle diff on a mined deep-shared-prefix corpus; debug assert that a cell whose prefix bit length changed has `hashLen == 0` | 8, 11 |
| H2 | Untouched sibling dropped across `Process` batches (at arity 2 the sibling is the entire other half of the subtree) | Two-phase test: batch A writes both children, batch B touches one, assert root equals oracle over A∪B | 11 |
| H3 | Implicit prefix bit length — byte length silently carries up to 7 spurious bits into `encode_bit_prefix` | Explicit uvarint bit count; decode asserts `byteLen == ceil(bitLen/8)` and zero pad bits | 5 |
| H4 | Prefix buffer truncation (a 66-byte prefix into a smaller field; Go `copy` is min-length and silent) | Cell encode/decode round-trip with prefix bit length drawn from `[0, 529)` | 5 |
| H5 | DB branch-key aliasing — two bit paths encoding to one key means one read, one stale record | Codec round-trip fuzz over every bit length 0..528 + explicit non-canonical-pad rejection | 1 |
| H6 | State-blob depth truncation (`byte(depth)` maps bit-depth 300 → 44) | **N/A in M0** — no state blob. Re-arm when save/restore lands. | — |
| H7 | Zero-length prefix overloaded to mean "not a stored branch" (EIP-8297 permits an empty branch prefix) | Unfold a stored branch record with `prefixBitLen == 0`, assert it is descended into, not treated as leaf/empty | 7 |
| H8 | Zone mis-routing of slots 0..63 (hottest slots land in the wrong zone; tree stays internally consistent) | Zone-boundary tests at slots 63/64/255/256 + plain-key validator over every written record | 2, 11 |
| H9 | Terminator arithmetic carried over from hex (`hashedExtLen-1` at `:1310` exists only to strip the hex terminator) | Table of `(cellPrefix, probeKey) → expected needUnfolding result` covering `cpl==0`, `cpl==len(prefix)`, `cpl<len(prefix)` | 7 |
| H10 | Unclamped or unmasked common prefix — both operands running out of real bits yields 64 and over-unfolds | Seed words with `0xFF` beyond `bitLen`; plus a 272-bit path that is a bitwise prefix of a 528-bit path, asserting `commonPrefixBits == 272` | 1 |
| H11 | Empty-subtree constant substitution (`empty.RootHash` instead of `[0x00]*32`) | Explicit node-level test **and** `RootHash()` on a fresh engine asserting 32 zero bytes | 6, 9 |
| H12 | Arity-1 propagate dropping a prefix bit (still hashes fine, wrong root, violates canonical form `eip:100-104`) | Assert `popcount(afterMap) == 2` at every branch fold and `prefixBits == depth - upDepth - 1` at every propagate | 8 |
| H13 | Delete semantics carried over (EIP-8297 never removes entries) | M0 rejects deletes originating from the update stream; missing-key context reads treated as absent | 9 |
| H14 | Two hashers drifting (the "witness passes locally, reth rejects" failure mode) | Exactly one cell hasher exists; enforced by review of Task 6 | 6 |

## Solution Overview

EIP-8297's tree has **two node types** and no extension node:

```
leaf_hash   = H(0x00 || key || value)
branch_hash = H(0x01 || encode_bit_prefix(prefix) || left_hash || right_hash)
empty tree  = [0x00] * 32
```

`encode_bit_prefix` = 2-byte big-endian bit count, then bits MSB-first, zero-padded to a byte boundary (`eip:196-201`). A `BranchNode` carries the run of bits shared by every key below it; a `LeafNode` commits its **complete** key, so its hash never depends on where it sits. A one-key tree's root **is** a leaf (`eip:133-135`).

**Settled design decisions** (from brainstorm — do not revisit during implementation):

1. **Keys** are EIP-8297 tree keys from day one. Account/code 34 B (272 bits) = `zone(1) || H(addr32)(32) || sub_index(1)`. Storage 66 B (528 bits) = `0xFF || H(addr32)(32) || H(addr32||tree_index)(32) || sub_index(1)`, where `tree_index` is encoded as a **32-byte big-endian** integer (`eip:420`, `:629-630`). `addr32` = 12 zero bytes ‖ addr (`eip:291-296`). Slots `<64` live in `ACCOUNT_ZONE` under the account stem at `HEADER_STORAGE_OFFSET+slot` (`eip:424-425`); slots `>=64` in `STORAGE_ZONE` with `tree_index = slot/256`, `sub_index = slot%256` — the sub-index is the **raw** low byte, not hashed, so adjacent slots co-locate.
2. **Key representation** `[9]uint64` big-endian words + `bitLen int16`. Divergence = XOR + `bits.LeadingZeros64`, **clamped** by `min(aLen,bLen)`. Both 272 and 528 are `8k+2` bytes, so both end in a 16-bit tail word — one mask constant. The tail **must** be masked or XOR reads garbage (H10).
3. **Hash = Keccak-256** via erigon's `keccak.KeccakState`, used for both `H` and `key_hash`. EIP-8297 defines `H` abstractly (`:187-189`) and names Keccak as a candidate (`:513`), so this is spec-conformant. Reached through one interface so it can be swapped.
4. **Grid** `[528][2]pbinCell`. Row-indexed arrays are `[528]`; depth-indexed arrays are `[529]` because depth is inclusive of 528. HPH measured: cell 456 B, grid 933,888 B. PBin ≈416 B/cell → ≈439 KB.
5. **touchMap/afterMap** stay `uint16` using bits 0-1 only, so `OnesCount16`/`TrailingZeros16` logic ports unchanged. Assert `(touch|after) &^ 0b11 == 0` at fold entry.
6. **Prefix lives in two places**: the branch record's DB **key** (full path from root, as HPH does at `:1443`/`:2023`) *and* in the parent's stored cell. The cell copy is **navigation** — `unfold` cannot reconstruct the descent key without it (verified at `hex_patricia_hashed.go:1310`). Representation changes nibbles→bits only.
7. **Branch DB key codec**: `packBitsMSBFirst(path) || byte(bitLen mod 8)`, zero-padded, **non-canonical pad rejected on read**. Max 67 B, `MaxPathBits = 528`. `bitLen == 0` → single `0x00`. A *leading* length field is forbidden because it would scatter a subtree's records across the keyspace; the property relied on is **subtree-range contiguity**, not ancestor-before-descendant ordering (a 7-bit path encodes `[0x00,0x07]` while its 8-bit descendant encodes `[0x00,0x00]`, so descendants can sort before ancestors — that is acceptable and must not be assumed away).
8. **Split rehash = materialize-on-split.** Because the prefix is inside the branch hash and `_insert` shrinks a split survivor's prefix to `node.prefix[matched+1:]` (`eip:174-176`), a split invalidates the cached child hash — a problem HPH never has, since `extensionHash` hashes at the parent over the child's hash. Resolution: when `needUnfolding` reports divergence **inside** a cell's prefix, unfold the survivor at its own path and recompute from its two children. **If the survivor is a leaf it has no record and needs no read** — its hash commits the complete key (`eip:106-109`). The survivor's DB key does not change, only its hash.
9. **Branch records are self-contained**: always encode **both** cells (`bitmap = afterMap`), so no merge-with-previous path exists. This diverges deliberately from `BranchEncoder.CollectUpdate`'s merge (`commitment.go:501-546`) and is what makes H2 tractable at arity 2.
10. **Oracle** = a naive Go transcription of the EIP's `BinaryTree`/`_insert`/`merkelize` (`eip:112-222`) in the test package. Root equality against it is the M0 gate, with the value-encoding blind spot noted in Testing Strategy.

**How the no-API-change constraint is satisfied:**

- `keyHasher` stays `func([]byte) []byte`, returning the **primary** leaf's tree key. PBin writes the `CODE_HASH` sibling leaf at `sub_index+1` during the same stem visit. Ordering holds because sub-indices ascend `0 → 1 → 64..`, so `Updates`, `HashSort`, `TouchPlainKey` are untouched. `hasherReusesAddrPrefix` (`:1485`) pointer-compares against `KeyToHexNibbleHash`, so a PBin hasher yields `addrCacheReuse=false` with no edit.
- PBin uses its **own** branch record codec; a 66-byte prefix does not fit the shared `cellEncodeData.extension [64]byte`. `PatriciaContext.Branch` returns opaque bytes, so nothing shared changes.
- The only edit to a pre-existing non-test file is additive: a variant constant plus a switch case in `commitment.go`.

## Technical Details

**bitpath** (`pbin_bitpath.go`) — named `pbinBitpath` per the `pbin` prefix rule, which wins over this sketch.
```go
type pbinBitpath struct {
	w      [9]uint64 // big-endian words; byte order == descent order
	bitLen int16     // 0..528
}
func (p *pbinBitpath) bit(d int16) uint64
func (p *pbinBitpath) maskTail()
func pbinCommonPrefixBits(a, b *pbinBitpath) int16 // XOR + LeadingZeros64, clamped by min(aLen,bLen)
```

**Values** (`pbin_values.go`) — BASIC_DATA is 32 bytes (`eip:332-339`): `version(1) || reserved(3) || code_size(4) || nonce(8) || balance(16)`, big-endian. `Update.Balance` is a `uint256.Int` (`commitment.go:2187`) but the EIP field is 16 bytes, so balances `>= 2^128` **error** rather than truncate. Storage values are left-padded to exactly 32 bytes (`eip:132`).

**Branch record** (`pbin_branch.go`, PBin-local): `touchMap(2) || afterMap(2) || per-cell{ fields(1), prefixBitLen(uvarint), prefixBytes, hash|leafKey }`, both cells always present.

## What Goes Where

- **Implementation Steps** (`[ ]`): all code, tests, and the additive variant registration inside this repo
- **Post-Completion** (no checkboxes): measurements, deferred decisions, follow-on milestones

## Implementation Steps

### Task 1: bitpath type and bit-path DB key codec

**Files:**
- Create: `execution/commitment/pbin_bitpath.go`
- Create: `execution/commitment/pbin_bitpath_test.go`

- [x] write failing tests for `pbinCommonPrefixBits` at bit lengths 271, 272, 273, 527, 528; a case seeding `w[]` with `0xFF` beyond `bitLen`; and a 272-bit path that is a bitwise prefix of a 528-bit path asserting the result is 272 (guards H10)
- [x] write a failing fuzz test for codec round-trip across every bit length 0..528, plus explicit non-canonical-padding rejection cases (guards H5)
- [x] write a failing unit test asserting no valid encoding equals the literal `"state"` (`0x7374617465`)
- [x] implement `bitpath` with `[9]uint64` words, `bitLen int16`, `MaxPathBits = 528`, and `bit`/`slice`/`append`/`hasPrefix`/`maskTail` — as `pbinBitpath`/`pbinMaxPathBits` per the naming rule
- [x] implement `pbinCommonPrefixBits` using XOR + `bits.LeadingZeros64`, clamped by `min(aLen, bLen)`
- [x] implement `pbinEncodeBitPath`/`pbinDecodeBitPath` as `packBitsMSBFirst(path) || byte(bitLen mod 8)`, rejecting non-canonical padding on read; `bitLen == 0` encodes to a single `0x00`
- [x] run tests - must pass before task 2

### Task 2: EIP-8297 tree key derivation and zone routing

**Files:**
- Create: `execution/commitment/pbin_keys.go`
- Create: `execution/commitment/pbin_keys_test.go`

- [x] write failing tests reproducing the EIP's vectors (`eip:583-630`), each asserting the **full** 34/66-byte key against a `keccak` computed inline in the test body rather than via the helper under test: BASIC_DATA key; slot 5 → sub-index `0x45`; slot 1000 → `tree_index 3`/`sub_index 0xE8` with `tree_index` as 32-byte big-endian
- [x] write failing zone-routing tests at slots 63/64/255/256 and for the 12-byte address padding (guards H8)
- [x] implement `pbinAddr32`, `pbinTreeKeyAccount(addr, subIdx)`, `pbinTreeKeyStorage(addr, slot)` with the `slot < 64` account-zone route
- [x] implement the two-level digest cache: `H(addr32)` per address, `H(addr32||tree_index)` per 256-slot group, with `tree_index` encoded as 32-byte big-endian
- [x] provide a `keyHasher`-compatible `func([]byte) []byte` returning the primary leaf's tree key, and assert `len` is 34 or 66 at every construction site
- [x] run tests - must pass before task 3

### Task 3: Leaf value encoding

**Files:**
- Create: `execution/commitment/pbin_values.go`
- Create: `execution/commitment/pbin_values_test.go`

- [x] write failing tests pinning BASIC_DATA byte offsets 0/4/8/16 against hand-written hex — **not** against the encoder, since the Task 4 oracle shares this encoder and cannot catch its bugs
- [x] write a failing test asserting a balance `>= 2^128` returns an error rather than truncating
- [x] write failing tests for the CODE_HASH leaf value and for storage values left-padded to exactly 32 bytes
- [x] implement `pbinEncodeBasicData` per `eip:332-339`: `version(1) || reserved(3) || code_size(4) || nonce(8) || balance(16)` big-endian
- [x] implement `pbinCodeHashValue` and `pbinEncodeStorageValue`
- [x] run tests - must pass before task 4

### Task 4: EIP reference oracle in the test package

**Files:**
- Create: `execution/commitment/pbin_oracle_test.go`

- [x] transcribe the spec's `LeafNode`, `BranchNode`, `_insert` and `merkelize` (`eip:112-222`) as a naive in-memory Go tree, Keccak-256, no optimisation
- [x] implement `encode_bit_prefix` exactly per `eip:196-201` and define the empty-tree hash as 32 zero bytes per `eip:208`
- [x] add corpus builders: empty; single key (root **is** a leaf, `eip:133-135`); two keys diverging at bit 0; two diverging at bit 527; a split-inside-prefix triple forcing `node.prefix[matched+1:]`; a mined deep-shared-prefix cluster
- [x] write tests asserting the oracle is self-consistent: permutation independence and prefix-freedom over every corpus
- [x] run tests - must pass before task 5

### Task 5: pbinCell, grid, and branch record codec

**Files:**
- Create: `execution/commitment/pbin_cell.go`
- Create: `execution/commitment/pbin_branch.go`
- Create: `execution/commitment/pbin_cell_test.go`

- [x] write failing cell encode/decode round-trip tests with prefix bit length drawn from `[0, 529)` (guards H4)
- [x] write failing tests for record decode rejecting inconsistent `prefixBitLen`/byte length and non-zero pad bits (guards H3)
- [x] define `pbinCell` with a tree-key-space `bitpath` prefix and plain-key fields; **use one prefix, not two** — HPH's `hashedExtension`/`extension` split exists to hold hashed and plain spaces separately, whereas PBin derives the tree key from the plain key on demand. No `stateHash` field: a leaf hash is `H(0x00||key||value)` with nothing to memoize
- [x] define the grid as `[528][2]pbinCell` with row-indexed arrays `[528]` and depth-indexed arrays `[529]`, plus `reset`/`resetForReuse` clearing `bitLen`
- [x] implement the PBin branch record codec with `prefixBitLen` as an explicit uvarint **bit** count, always encoding both cells (`bitmap = afterMap`, no merge path)
- [x] run tests - must pass before task 6

### Task 6: node merkelization

**Files:**
- Create: `execution/commitment/pbin_hash.go`
- Create: `execution/commitment/pbin_hash_test.go`

- [x] write failing tests asserting each node hash matches the Task 4 oracle for hand-built shapes: single leaf, one branch, nested branch with non-empty prefix, branch with **empty** prefix
- [x] write a failing node-level test asserting the empty subtree is 32 zero bytes, explicitly not `empty.RootHash` (guards H11)
- [x] implement `pbinLeafHash = H(0x00 || key || value)` over the complete 34/66-byte key
- [x] implement `pbinBranchHash = H(0x01 || encode_bit_prefix(prefix) || left || right)` with one scratch buffer sized 133 B (1 tag + 2 count + 66 prefix + 64 children)
- [x] implement exactly **one** cell hasher — do not port both `computeCellHash` and `witnessComputeCellHashWithStorage` (guards H14)
- [x] run tests - must pass before task 7

### Task 7: unfold and needUnfolding

**Files:**
- Create: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_unfold_test.go`

- [x] write a failing table test of `(cellPrefix, probeKey) → expected pbinNeedUnfolding result` covering `cpl == 0`, `cpl == len(prefix)` (full match, descend), and `cpl < len(prefix)` (split signal) (guards H9)
- [x] write a failing test unfolding a stored branch record whose `prefixBitLen == 0`, asserting it is descended into rather than treated as leaf or empty (guards H7)
- [x] write failing unfold tests for divergence at bits 0, 63, 64, 65, 271 and 527
- [x] create `PBinPatriciaHashed` with the grid, `currentKey bitpath`, context and Keccak state
- [x] implement `pbinNeedUnfolding` with bit reads and clamped common-prefix, dropping hex terminator arithmetic and `clampToAccountBoundary`; its return contract MUST distinguish "prefix fully matched" from "diverges inside prefix" — landed as the method `needUnfolding` returning `pbinUnfolding{action, matched}`; the `pbin` prefix rule covers package-level identifiers only, and methods on `PBinPatriciaHashed` cannot collide with the hex engine's
- [x] implement `pbinUnfold`/`pbinUnfoldBranchNode` reading the parent's stored cell prefix to reconstruct the descent key, with an explicit node-kind flag so a zero-length prefix is not overloaded — landed as the methods `unfold`/`unfoldBranchNode`
- [x] run tests - must pass before task 8

### Task 8: fold primitives

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_fold_test.go`

- [x] write failing grid-seeded unit tests: hand-build one row, fold it, assert the emitted hash equals the oracle's `merkelize` of that node and that the record bytes round-trip
- [x] write a failing test for a split whose survivor is a **leaf**, asserting no branch record is read
- [x] write failing tests forcing splits inside prefixes at several depths, asserting each rehashed node matches the oracle
- [x] implement `pbinFold` dispatching the three kinds — delete / propagate / branch — mirroring `hex_patricia_hashed.go:2031-2038` — landed as the methods `fold`/`foldBranch`/`foldPropagate`/`foldDelete`, same reasoning as Task 7's `unfold`
- [x] implement `pbinFoldBranch` writing records keyed by the encoded bit path, asserting `(touchMap|afterMap) &^ 0b11 == 0` at entry and `popcount(afterMap) == 2` (guards H12)
- [x] implement `pbinFoldPropagate` accumulating the child's prefix bits into the parent cell and writing **no** record, asserting `prefixBits == depth - upDepth - 1` (guards H12) — landed as the equivalent post-condition on the assembled prefix, which also catches a dropped branch bit
- [x] implement materialize-on-split with the leaf-survivor short circuit, plus a debug assert that a cell whose prefix bit length changed has `hashLen == 0` (guards H1) — landed as `rehashAfterPrefixChange`, which enforces the invariant rather than asserting it: a cell that knows its children re-derives, one that does not is marked stale and materializes on demand
- [x] add instrumentation counters for splits-inside-prefix and extra `ctx.Branch` reads
- [x] run tests - must pass before task 9

⚠️ **Scope note (discovered here, resolved here).** Decision 8 covered only `needUnfolding`-reported splits, but a cell's node prefix also changes on the *normal* descent: `unfold` consuming a branch cell's prefix leaves the cell holding none of it, and the propagate that follows hands it back. Both directions invalidate a hash the prefix sits inside, and the propagate direction cannot be fixed by a record read at fold time without re-reading every descended node. Resolved by carrying the two child hashes in memory on cells this run built (`pbinCell.children`/`childrenSet`, not serialised), so a prefix change re-derives instead of re-reading; materialize-on-split stays the fallback for cells that arrived from a record. One Task 7 assertion (`pbin_unfold_test.go`, descended cell keeps the parent's hash) encoded the wrong behaviour and now pins `hashLen == 0`.

### Task 9: drive loop, Process and RootHash

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_process_test.go`

- [x] write a failing test asserting `RootHash()` on a fresh engine is 32 zero bytes, not `empty.RootHash` (guards H11)
- [x] write a failing test for a one-key tree asserting the root **is** the leaf hash `H(0x00||key||value)` (`eip:133-135`), and for a two-key tree asserting it is the branch hash
- [x] write failing `Process` tests over `MockState` for account-only, storage-only and mixed corpora, asserting root equality with the oracle
- [x] implement `pbinUpdateCell`, the key-path descent, and the `Process` drive loop — landed as the methods `updateCell`/`followAndUpdate`/`processKey`, same reasoning as Task 7's `unfold`
- [x] implement `RootHash` including the root-as-leaf case
- [x] implement the account fan-out: write the `CODE_HASH` leaf at `sub_index+1` during the same stem visit, leaving `Updates`/`HashSort`/`TouchPlainKey` untouched
- [x] reject deletes originating from the **update stream** only; a missing-key `ctx.Account`/`ctx.Storage` read returns `DeleteUpdate` (`patricia_state_mock_test.go:92-95`, `:129-134`) and MUST be treated as absent, not as a delete (guards H13)
- [x] run tests - must pass before task 10

### Task 10: variant registration

**Files:**
- Modify: `execution/commitment/commitment.go`
- Create: `execution/commitment/pbin_variant_test.go`

- [x] write `TestInitializeTrieAndUpdates_BinVariant` first as the red test, asserting the constructed type, `Variant()`, and `Mode() == ModeDirect`
- [x] add `VariantBinPatriciaTrie` plus a case in `ParseTrieVariant`/`InitializeTrieAndUpdates` — **additive only**
- [x] implement the remaining `Trie` methods to satisfy the interface unchanged: `Reset`, `ResetContext`, `Release`, `Variant`, `SetTraceWriter`, `EnableCsvMetrics`
- [x] write a test asserting `Reset` then reuse produces the same root as a fresh engine
- [x] run tests - must pass before task 11

**Registration notes.** `InitializeTrieAndUpdates` pins `ModeDirect` for this variant whatever mode the caller passes, mirroring how the parallel variant pins `ModeParallel`: `ModeParallel` allocates a hex-nibble prefix trie that has no meaning at arity 2. `SetTraceWriter` traces one line per run — the Task 8 counters — which is also how Task 12 reads them. `EnableCsvMetrics` is a no-op: M0 collects no metrics. `Release` pools the engine as the hex one does, since the grid is ~439 KB.

### Task 11: hazard guards and differential fuzzing

**Files:**
- Create: `execution/commitment/pbin_verify_test.go`
- Create: `execution/commitment/pbin_hazard_test.go`
- Create: `execution/commitment/pbin_fuzz_test.go`

- [x] implement an independent branch-record recompute oracle: walk every written record, decode, recompute bottom-up, assert it reproduces the root — landed as `pbinVerifier`, which finds the root record as the one no other record is a bit-prefix of
- [x] implement a bit-space plain-key validator asserting `treeKey(plainKey) == branchPath || cellPrefix` for every written record (guards H8)
- [x] write the two-phase sibling test: `Process` batch A writing both children, then batch B touching one child, asserting the root equals the oracle over A∪B (guards H2)
- [x] write the mined deep-shared-prefix corpus test and assert oracle equality (guards H1)
- [x] write permutation-independence tests porting `Test_HexPatriciaHashed_UniqueRepresentation`/`2`/`BrokenUniqueRepr` (`hex_patricia_hashed_test.go:157-249`)
- [x] write a differential fuzzer over `Process` against the oracle with a **low-entropy slot generator** — random 32-byte slots essentially never share a stem, so a default corpus never exercises sub-index sharing
- [x] run tests - must pass before task 12

### Task 12: Verify acceptance criteria

- [x] verify all requirements from Overview are implemented and M0 scope boundaries were respected — `VariantBinPatriciaTrie`/`PBinPatriciaHashed`/`pbinKeyHasher` appear outside the `pbin_*` files only at the three additive `commitment.go` sites, so nothing reaches the domain layer
- [x] verify no shared type, interface or signature was modified: `git diff --stat` shows `commitment.go` as the only pre-existing non-test file, additive only — 22 files, 6,289 insertions, 0 deletions; `commitment.go` +11/-0
- [x] verify every hazard in the register except H6 has a named passing test
- [x] verify every new package-level identifier carries the `pbin` prefix and the package compiles without collision — 274 identifiers checked by AST walk; the only ones not starting at position 0 are `errPBin*` and `NewPBinPatriciaHashed`, where Go's `err`/`New` convention precedes the marker
- [x] run the package test suite: `go test ./execution/commitment/...`
- [x] run fuzzers briefly, one target per invocation — `-fuzz` refuses a regex matching several: `go test ./execution/commitment/ -run=Fuzz -fuzz=FuzzPBinBitPathCodec -fuzztime=60s` then the same for `FuzzPBinProcessMatchesOracle`
- [x] verify `go build ./...` and `go vet ./execution/commitment/...` are clean
- [x] record the Task 8 instrumentation counters under Post-Completion

**Verification results.**

| Check | Result |
|-------|--------|
| `go test ./execution/commitment/...` | ok, 9.6s |
| `go vet ./execution/commitment/...` | clean |
| `go build ./...` | clean |
| `FuzzPBinBitPathCodec -fuzztime=60s` | pass, 8.2M execs, 0 new interesting |
| `FuzzPBinProcessMatchesOracle -fuzztime=60s` | pass, 231k execs, 97 new interesting |

Hazard → guard, all passing (H6 is N/A in M0, H14 is a review item):

| Hazard | Guard |
|--------|-------|
| H1 | `TestPBinFoldSplitInsidePrefixMatchesOracle`, `TestPBinSplitInsideStoredPrefix`, `TestPBinDeepSharedPrefixCorpus` |
| H2 | `TestPBinUntouchedSiblingSurvivesBatch` |
| H3 | `TestPBinBranchDecodeRejects` (`pbin_cell_test.go:172`) |
| H4 | `TestPBinBranchCodecRoundTripPrefixBitLengths` (`pbin_cell_test.go:63`) |
| H5 | `FuzzPBinBitPathCodec`, `TestPBinBitPathNeverEncodesToStateKey` |
| H7 | `TestPBinUnfoldEmptyPrefixBranchRecord` |
| H8 | `TestPBinStorageZoneRouting`, `TestPBinAddr`, `pbinVerifier.checkPlainKeys` |
| H9 | `TestPBinNeedUnfolding` |
| H10 | `TestPBinCommonPrefixBits_IgnoresBitsBeyondBitLen`, `TestPBinCommonPrefixBits_ShorterPathIsPrefix` |
| H11 | `TestPBinEmptyTreeHash`, `TestPBinRootHashEmptyEngine`, `TestPBinOracleEmptyTreeHash` |
| H12 | `TestPBinFoldRejectsInconsistentGrid`, `TestPBinFoldBranchRejectsWrongArity` |
| H13 | `TestPBinProcessRejectsStreamDelete`, `TestPBinProcessMissingStateIsAbsent` |
| H14 | `pbinHasher.cellHash` is the only cell hasher; `PBinPatriciaHashed.cellHash` delegates to it and `leafCellHash` is reachable only through it |

⚠️ **Fuzz-harness note.** `FuzzPBinProcessMatchesOracle` at the documented invocation can end in `context deadline exceeded`. It is the harness, not the engine: Go's default `-fuzzminimizetime` is 60s, so a newly interesting input found late in a 60s run keeps minimizing past the coordinator's shutdown deadline. Symptom is `execs` falling to 0/sec near the end. Adding `-fuzzminimizetime=2s` holds ~4,900 exec/s throughout and exits clean. Separately checked that no input is slow: 20,000 generated corpora ran with a worst case of 23ms.

### Task 13: [Final] Update documentation

- [ ] add a package-level doc comment on `pbin_patricia_hashed.go` naming the EIP, the Keccak suite choice and the M0 scope boundaries
- [ ] update `CLAUDE.md` if new patterns were discovered
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention, measurement, or follow-on milestones — no checkboxes*

**Task 8 instrumentation counters (measured in Task 12).**

`splitsInsidePrefix` counts probes diverging inside a cell's prefix; `materializeReads` counts the `ctx.Branch` reads that follow, i.e. the ones the descent alone would not have made.

| Corpus | keys | leaves | splitsInsidePrefix | materializeReads |
|--------|-----:|-------:|-------------------:|-----------------:|
| mixed, one batch | 54 | 60 | 59 | 0 |
| deep shared prefix, one batch | 4 | 8 | 7 | 0 |
| mixed, two batches | 54 | 60 | 59 | 6 |
| deep shared prefix, two batches | 4 | 8 | 7 | 0 |
| mixed, one key per batch | 54 | 60 | 59 | 33 |
| fuzz generator space, 2,000 runs | 200,329 | — | 79,437 | 751 |

Splits inside a prefix are the common case, not the exception — roughly one per key. What makes them cheap is Task 8's in-memory child hashes: **within a single `Process` call `materializeReads` is 0**, because every cell that splits was built by that same run and re-derives. A read costs only when a cell arrives from a record an earlier batch wrote, so the counter tracks batch granularity rather than tree shape — 6 reads at two batches, 33 at one key per batch (the drive loop's worst case), and 751 over 200,329 keys in the fuzz space (0.37% of keys, 0.95% of splits).

**Decisions deferred to data:**
- Split-rehash strategy. M0 ships materialize-on-split, narrowed by Task 8's in-memory child hashes to cells that arrived from a record — a node this run folded re-derives for free. The numbers above say the residual is small and, crucially, driven by how work is batched rather than by the corpus. Promoting the two child hashes into the record (32 B per branch cell, plus a migration story) would remove the hazard outright, but on M0 evidence it buys under 1% of splits; revisit against production batch sizes, where a batch spans one block and the cross-batch fraction will be higher than these tests show.
- Record the one-prefix-per-cell rationale (Task 5) here and in the commit body rather than as a source comment.

**Out of scope, in rough dependency order:**
- Code chunks (`chunkify_code`, `eip:374-397`), including the stateful PUSHDATA boundary byte and content-addressed overflow chunks shared between contracts. Discovered in Task 6: `Update` carries **no code size**, and adding one is an external API change, so M0 encodes BASIC_DATA `code_size` as 0. Both the engine and the oracle see the same value, so the M0 gate still holds, but a conformance claim needs a real code size sourced alongside code chunking.
- Deletion semantics. EIP-8297 never removes entries, but erigon's `StorageDomain` represents never-written and explicitly-zeroed identically (`execution/state/rw_v3.go:965` calls `DomainDel` on an empty value). Production needs a tombstone-capable encoding or a documented deviation. Under EIP-8297 SELFDESTRUCT must **not** remove storage leaves, which removes the rationale for erigon's storage-subtree collapse.
- Commitment state save/restore (re-arms H6). `SetState`/`EncodeCurrentState` are concrete `*HexPatriciaHashed` methods and `commitmentdb` type-switches on them (`commitment_context.go:895-901`, `:935-949`, panics at `:103`, silently no-ops `SetCollapseTracer` at `:411`). Promoting a `StatefulTrie` interface is an external API change, deliberately excluded from M0; `:411` should error rather than no-op before any variant ships.
- Parallel mounting. `mountedNib 0..15` plus a depth-63 fold wall does not translate to arity 2; a 2-way root split silently serialises rather than failing.
- Domain-layer wiring and branch-cache tuning (dense tiers land on bit depths 4/8/12/16; a literal port covers ~1 in 8 bit depths).

**Upstream:**
- The EIP is a Draft with an unfixed hash function, unfixed witness gas constants, and an unresolved header code-chunk count (EIP-7864 sets `CODE_CHUNKS_IN_HEADER = 16` at `eip-7864.md:219`; EIP-8297 puts 128 chunks in the header via `CODE_OFFSET = 128` at `eip-8297.md:271`; neither cites data). Any conformance claim should name the spec commit it was built against.
