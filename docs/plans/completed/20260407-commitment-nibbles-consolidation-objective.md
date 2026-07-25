# Objective: Consolidate nibble compact/uncompact helpers in commitment package

Addresses erigontech/erigon#17797 (parent: erigontech/erigon#13884 "Commitment evolving"). Label: `tech debt reduction`.

## Problem

The `execution/commitment/` package has **three independent implementations** of spec-compliant hex↔compact nibble encoding plus a **fourth custom CBOR-friendly variant** and a **third-copy drift** in debug tooling:

| File | Symbol | Role |
|------|--------|------|
| `execution/commitment/keys_nibbles.go:55` | `HexNibblesToCompactBytes` | hex → compact (yellow-paper) |
| `execution/commitment/keys_nibbles.go:91` | `uncompactNibbles` | compact → hex |
| `execution/commitment/trie/encoding.go:47` | `hexToCompact` | functional duplicate (has benchmarks) |
| `execution/commitment/trie/encoding.go:64` | `compactToHex` | functional duplicate |
| `execution/commitment/trie/encoding.go:161` | `keybytesToHex` | bytes → hex |
| `execution/commitment/trie/encoding.go:174` | `hexToKeybytes` | hex → bytes |
| `execution/commitment/trie/witness_marshalling.go:164` | `keyNibblesToBytes` | **CUSTOM** CBOR codec (NOT yellow-paper) |
| `execution/commitment/trie/witness_marshalling.go:197` | `keyBytesToNibbles` | inverse |
| `cmd/hack/hack.go:675` | `keybytesToHex` | third copy |

`HexNibblesToCompactBytes` ≡ `hexToCompact` and `uncompactNibbles` ≡ `compactToHex` — functionally equivalent but drift-prone. The witness marshalling pair is **intentionally a different wire format** (bit 0 = parity, bit 1 = terminator, no inline first nibble for odd lengths) — not a bug, but looks like compact encoding and is a false-friend trap for future contributors.

The duplication cannot be resolved by consolidating into either existing package: `execution/commitment/` already imports `execution/commitment/trie/` (`hex_patricia_hashed.go:44` for witness RLP), so a canonical version in either package creates an import cycle for its sibling.

## Goal

Create a single leaf package `execution/commitment/nibbles/` that owns the canonical spec-compliant hex/compact helpers. Repoint every production and test caller. Delete the duplicated definitions. Preserve the witness marshalling custom format unchanged — but rename it to make the divergence explicit and document the bit layout so no one "fixes" it in the future.

## Success Criteria

1. Exactly **one** implementation of each of: `HexToCompact`, `CompactToHex`, `KeybytesToHex`, `HexToKeybytes`, `HasTerm`, `CommonPrefixLen`. All live in `execution/commitment/nibbles/`.
2. `execution/commitment/trie/encoding.go` is **deleted**. The `Keybytes` struct and its RLP methods move to `execution/commitment/trie/keybytes.go` and call `nibbles.*` for conversion.
3. `execution/commitment/keys_nibbles.go` no longer defines the compact/hex helpers. It retains only the hashing helpers (`KeyToHexNibbleHash`, `KeyToNibblizedHash`, `hashKey`), the bit-packing helper (`CompactKey`), and debug helpers (`NibblesToString`, `updatedNibs`, `PrefixStringToNibbles`).
4. `execution/commitment/trie/witness_marshalling.go` keeps the custom CBOR codec **unchanged byte-for-byte**, but the functions are renamed `witnessKeyNibblesToBytes` / `witnessKeyBytesToNibbles` with a doc block describing the bit layout and warning against spec-conformance "fixes".
5. `cmd/hack/hack.go:675` no longer carries a private `keybytesToHex` copy; it imports `commitment/nibbles`.
6. All existing tests pass unchanged: `go test ./execution/commitment/... ./cmd/integration/... ./cmd/hack/... -count=1`
7. Benchmarks are preserved (lifted from `trie/encoding_test.go` to `nibbles/nibbles_test.go`). No measurable perf regression: `benchstat` shows no statistically significant change in ns/op or allocs/op for `BenchmarkHexToCompact`, `BenchmarkCompactToHex`, `BenchmarkKeybytesToHex`, `BenchmarkHexToKeybytes`.
8. `make lint` passes (run 2–3× per CLAUDE.md — linter is non-deterministic).
9. New fuzz target `FuzzHexCompactRoundtrip` added covering `CompactToHex(HexToCompact(x)) == x`.
10. The witness wire format test roundtrip at `witness_operators_test.go:254-256` passes unchanged (modulo the rename).

## Non-Goals

- **Rewriting** the witness CBOR codec to use spec-compliant compact encoding. This would change the witness wire format and break any existing consumer (including polygon zkevm). Belongs in a separate issue with explicit compat review and version bump.
- Consolidating the hashing helpers (`KeyToHexNibbleHash`, `KeyToNibblizedHash`, `hashKey`). These are a different concern and stay in `keys_nibbles.go`.
- Consolidating `prefixLen`/`CommonPrefixLen` copies elsewhere in the repo. Scope is limited to `commitment/`, `commitment/trie/`, and the `cmd/hack`+`cmd/integration` call sites.
- Changing RLP serialization of the `Keybytes` struct. Its bytes-on-wire must be byte-identical before and after.
- Any "while we're here" cleanups in the touched files. Every change must be mechanically verifiable as either (a) a pure extraction/move, (b) a rename, or (c) a deletion of a now-unreferenced symbol.
