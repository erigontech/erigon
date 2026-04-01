# Plan: TrieReader — stateless read-only Patricia trie navigation

## Overview

Implement a `TrieReader` in `execution/commitment/` that navigates the Patricia
trie by hashedKey without any mutable grid state. Each `Lookup` call starts from
the root (compact prefix `[]byte{}`) and descends independently.

## Background

`HexPatriciaHashed` carries ~200KB of mutable grid state (`grid[64][16]cell`,
`touchMap`, `afterMap`, etc.) designed for update processing. For read-only
path queries this is all unnecessary.

`TrieReader` reuses:
- `cell` struct and `cell.fillFromFields()`
- `HexNibblesToCompactBytes()`
- `CellType` enum + `cell.Type()` (just added in cell-type-refactor)
- `PatriciaContext.Branch()` — single external dependency

## Key Design

**Stateless**: no grid, no activeRows, no touchMap. Each `Lookup` allocates only
a small nibble prefix buffer on the stack (max 128 bytes).

**Descent algorithm**:
```
prefix = []
depth = 0
loop:
    branchData = ctx.Branch(compact(prefix))
    bitmap = BigEndian.Uint16(branchData[0:2])
    nibble = hashedKey[depth]
    if nibble not in bitmap → return NotFound
    cell = parseCellAt(branchData[2:], bitmap, nibble)
    switch cell.Type():
        Extension → verify extKey matches hashedKey, advance depth+1+extLen
        Account/Storage → return Found
        Hash/Empty → advance depth+1, continue (Hash = unloaded branch, recurse via Branch())
```

`CellTypeHash` is always a branch node that can be expanded via `ctx.Branch()` —
no special handling needed.

## Tasks

- [x] Add `parseCellAt(data []byte, bitmap uint16, nibble int) (cell, error)` in
  `execution/commitment/trie_reader.go`. Parses exactly one cell from branch data
  at position `nibble` by scanning bitmap entries with `fillFromFields`.

- [x] Implement `TrieReader` struct and `NewTrieReader(ctx PatriciaContext, accountKeyLen int16) *TrieReader`
  in the same file.

- [x] Implement `Lookup(hashedKey []byte) (cell, bool, error)` on `TrieReader`:
  - Start with empty prefix (root = compact of `[]`)
  - Loop: `ctx.Branch(compact(prefix))` → parse bitmap → find nibble → inspect cell
  - `CellTypeExtension`: validate extKey matches hashedKey slice, advance depth
  - `CellTypeAccount` or `CellTypeStorage`: return cell, true, nil
  - `CellTypeHash` or `CellTypeEmpty` with no data: advance one nibble and continue
  - Empty branchData or nibble absent from bitmap: return zero cell, false, nil

- [x] Add unit tests in `execution/commitment/trie_reader_test.go`:
  - Test account lookup hit (exact prefix match)
  - Test miss (key not in trie)
  - Test extension node traversal
  - Test multi-level descent (depth > 9)
  - Use `commitmentContext` test helper already present in test files

- [x] Run `go test ./execution/commitment/... -run TestTrieReader -count=1`
- [x] Run `go test ./execution/commitment/... -count=1` (full suite)
- [x] Run `go vet ./execution/commitment/...`

## Constraints

- New file `trie_reader.go` only — no changes to existing files except adding
  `parseCellAt` as an unexported function (or inline it in trie_reader.go)
- Do NOT touch `HexPatriciaHashed`, `unfoldBranchNode`, or any existing types
- `TrieReader` must work with any `PatriciaContext` implementation
- No keccak dependency: caller provides already-hashed keys
- No pool, no sync — caller manages concurrency
