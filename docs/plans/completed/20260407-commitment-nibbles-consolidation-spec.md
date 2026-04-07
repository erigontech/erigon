# Spec: Consolidate nibble compact/uncompact helpers in commitment package

Implementation plan for erigontech/erigon#17797. See companion `20260407-commitment-nibbles-consolidation-objective.md`.

## Overview

Introduce a new leaf package `execution/commitment/nibbles/` holding canonical spec-compliant hex↔compact encoding helpers. Both `commitment/` and `commitment/trie/` import from it, breaking the implicit cycle that caused the duplication in the first place. Old definitions are deleted after every caller has been repointed and a temporary equivalence test has proven behavioral parity.

**Guiding principle**: every task is a mechanical verifiable change — new code is literally lifted from existing sources, callers are mechanically repointed, deletions are last and leave the build green. No algorithmic changes, no cleanups, no scope creep.

## Context (key code locations)

### Current implementations

| What | Where | Lines |
|------|-------|-------|
| `HexNibblesToCompactBytes` | `execution/commitment/keys_nibbles.go` | 55–88 |
| `uncompactNibbles` | `execution/commitment/keys_nibbles.go` | 91–109 |
| `HasTerm` | `execution/commitment/keys_nibbles.go` | 112–114 |
| `commonPrefixLen` | `execution/commitment/keys_nibbles.go` | 117–125 |
| `hexToCompact` | `execution/commitment/trie/encoding.go` | 47–62 |
| `compactToHex` | `execution/commitment/trie/encoding.go` | 64–76 |
| `Keybytes` struct + methods | `execution/commitment/trie/encoding.go` | 81–125 |
| `CompactToKeybytes` | `execution/commitment/trie/encoding.go` | 128–144 |
| `EncodeRLP`/`DecodeRLP` | `execution/commitment/trie/encoding.go` | 147–159 |
| `keybytesToHex` | `execution/commitment/trie/encoding.go` | 161–170 |
| `hexToKeybytes` | `execution/commitment/trie/encoding.go` | 172–184 |
| `decodeNibbles` | `execution/commitment/trie/encoding.go` | 186–199 |
| `prefixLen` | `execution/commitment/trie/encoding.go` | 202–213 |
| `hasTerm` (lower-case) | `execution/commitment/trie/encoding.go` | 216–218 |
| `keyNibblesToBytes` (custom) | `execution/commitment/trie/witness_marshalling.go` | 164–195 |
| `keyBytesToNibbles` (custom) | `execution/commitment/trie/witness_marshalling.go` | 197–224 |
| Third-copy `keybytesToHex` | `cmd/hack/hack.go` | 675+ |

### Caller sites to repoint

**`commitment/` package:**
- `commitment.go:1175` — `uncompactNibbles` → `nibbles.CompactToHex`
- `verify.go:52` — `uncompactNibbles` → `nibbles.CompactToHex`
- `verify_test.go:74,181,266,342` — `HexNibblesToCompactBytes` → `nibbles.HexToCompact`
- `warmuper.go:197` — `HexNibblesToCompactBytes` → `nibbles.HexToCompact`
- `hex_patricia_hashed.go:1719,2254` — `HexNibblesToCompactBytes` → `nibbles.HexToCompact`
- `hex_concurrent_patricia_hashed.go:338` — `HexNibblesToCompactBytes` → `nibbles.HexToCompact`

**`commitment/trie/` package:**
- `hasher.go:165` — `hexToCompact` → `nibbles.HexToCompact`
- `proof.go:44,242` — `keybytesToHex` → `nibbles.KeybytesToHex`
- `proof.go:172` — `CompactToKeybytes` → stays (moves with `Keybytes`)
- `stream.go:815,820` — `keybytesToHex` → `nibbles.KeybytesToHex`
- `trie.go:301,310,319,335,357,501,517,539,565,1046,1273,1322` — `keybytesToHex` → `nibbles.KeybytesToHex` (13 sites)
- `trie.go:1594` — `CompactToKeybytes` → stays
- `flatdb_sub_trie_loader_test.go:56,57` — `keybytesToHex` → `nibbles.KeybytesToHex`
- `structural_test.go:173` — `hexToKeybytes` → `nibbles.HexToKeybytes`
- `trie_test.go:401` — `keybytesToHex` → `nibbles.KeybytesToHex`

**External:**
- `cmd/integration/commands/commitment.go:233,677` — `commitment.HexNibblesToCompactBytes` → `nibbles.HexToCompact`
- `cmd/hack/hack.go:675+` — delete local copy, import `nibbles`

**Docs:**
- `docs/plans/20260325-fold-refactor-spec.md:295,339`
- `docs/plans/completed/20260325-fold-refactor-spec.md:295,339`

## Development Approach

- **Testing approach**: new package first, then equivalence tests, then repoint, then delete. Tests added alongside code at each step. Equivalence test is a temporary tripwire that catches behavioral drift before the delete commit removes its target.
- Each task is one commit. Build must be green at every commit boundary.
- Run `make lint` after every task (2–3× per CLAUDE.md — non-deterministic).
- Use `benchstat` to compare benchmarks before/after the move. No perf regression tolerated.
- Do not combine tasks. Each is mechanically verifiable in isolation.

## Testing Strategy

- **Unit tests**: mandatory per task. New tests live alongside new code in `execution/commitment/nibbles/nibbles_test.go`. Existing tests for `Keybytes` move to `execution/commitment/trie/keybytes_test.go`.
- **Equivalence tests**: temporary tripwire added in Task 2. Compares `nibbles.HexToCompact` output to `HexNibblesToCompactBytes` output across a fuzz corpus of nibble slices (lengths 0..128, with/without terminator). Similar for `CompactToHex` vs `uncompactNibbles`. Removed in Task 7 when the old definitions go away.
- **Roundtrip property test**: `TestHexCompactRoundtrip` — for random hex inputs, `CompactToHex(HexToCompact(x)) == x`. Lives in the new package permanently.
- **Fuzz target**: `FuzzHexCompactRoundtrip` — cheap insurance for the new package boundary.
- **Benchmark preservation**: `BenchmarkHexToCompact`, `BenchmarkCompactToHex`, `BenchmarkKeybytesToHex`, `BenchmarkHexToKeybytes` move verbatim from `trie/encoding_test.go` to `nibbles/nibbles_test.go`.
- **Wire format regression**: witness roundtrip test at `witness_operators_test.go:254-256` exercises `witnessKeyNibblesToBytes` ∘ `witnessKeyBytesToNibbles`. Passes unchanged (only function names change).

## Implementation Steps

### Task 1: Introduce `commitment/nibbles` sub-package with canonical helpers

**Files:**
- Create: `execution/commitment/nibbles/nibbles.go`
- Create: `execution/commitment/nibbles/nibbles_test.go`

- [ ] create `execution/commitment/nibbles/nibbles.go`, package `nibbles`, with exported `HexToCompact`, `CompactToHex`, `KeybytesToHex`, `HexToKeybytes`, `HasTerm`, `CommonPrefixLen`, and const `Terminator byte = 0x10`
- [ ] copy the function bodies verbatim from `trie/encoding.go:47-199` (canonical choice — these have benchmarks and are the go-ethereum-derived originals), renaming the lowercase names to their exported forms and adjusting receivers to use the `Terminator` constant instead of the magic number 16
- [ ] create `execution/commitment/nibbles/nibbles_test.go` containing `TestHexToCompact` + `TestKeybytesHex` (lifted from `trie/encoding_test.go:35-87`) + `BenchmarkHexToCompact`/`BenchmarkCompactToHex`/`BenchmarkKeybytesToHex`/`BenchmarkHexToKeybytes` (lifted verbatim)
- [ ] add `TestHexCompactRoundtrip` — random hex inputs (with/without terminator, odd/even lengths), assert `CompactToHex(HexToCompact(x)) == x`
- [ ] add `FuzzHexCompactRoundtrip` with 5 seed cases and the same roundtrip property
- [ ] run tests — must pass before Task 2: `go test ./execution/commitment/nibbles/... -count=1`
- [ ] run `make lint` until clean (old code still present at this point; this task only adds)

### Task 2: Add temporary equivalence tripwire between old and new helpers

**Files:**
- Create: `execution/commitment/nibbles/equivalence_test.go`

- [ ] create `execution/commitment/nibbles/equivalence_test.go` with `// Package nibbles_test` — separate package to avoid circular-import issues (can import both `commitment` and `commitment/trie` freely)
- [ ] add `TestEquivalence_HexToCompact_vs_HexNibblesToCompactBytes` — for 500 random nibble slices of length 0..128 (half with terminator), assert `nibbles.HexToCompact(x) == commitment.HexNibblesToCompactBytes(x)` byte-for-byte
- [ ] add `TestEquivalence_CompactToHex_vs_uncompactNibbles` — symmetric. Note: `uncompactNibbles` is unexported in `commitment/` — this test requires temporarily exporting it OR using a test helper defined in a `commitment`-package test file. **Use a test helper**: add `export_test.go` in `commitment/` exposing `UncompactNibblesForEquivalenceTest = uncompactNibbles`. Remove in Task 7.
- [ ] add `TestEquivalence_KeybytesToHex_vs_trie_keybytesToHex` — the `trie/` version is unexported. Same trick: `export_test.go` in `trie/` exposing `KeybytesToHexForEquivalenceTest = keybytesToHex`. Remove in Task 7.
- [ ] add symmetric `TestEquivalence_HexToKeybytes_*`
- [ ] run tests — must pass before Task 3: `go test ./execution/commitment/... -count=1 -run Equivalence`
- [ ] run `make lint`

### Task 3: Repoint `commitment/` package callers to `nibbles`

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/verify.go`
- Modify: `execution/commitment/verify_test.go`
- Modify: `execution/commitment/warmuper.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/hex_concurrent_patricia_hashed.go`

- [ ] add `"github.com/erigontech/erigon/execution/commitment/nibbles"` import and repoint every caller listed in the Context section (commitment package subset)
- [ ] leave the old definitions in `keys_nibbles.go` in place — they're still referenced by their own test file and by the equivalence test helper until Task 7
- [ ] run tests — must pass: `go test ./execution/commitment/... -count=1`
- [ ] run `make lint` — fix any import ordering issues
- [ ] verify no build regression in dependents: `go build ./...`

### Task 4: Repoint `commitment/trie/` internal callers to `nibbles`

**Files:**
- Modify: `execution/commitment/trie/hasher.go`
- Modify: `execution/commitment/trie/proof.go`
- Modify: `execution/commitment/trie/stream.go`
- Modify: `execution/commitment/trie/trie.go`
- Modify: `execution/commitment/trie/flatdb_sub_trie_loader_test.go`
- Modify: `execution/commitment/trie/structural_test.go`
- Modify: `execution/commitment/trie/trie_test.go`

- [ ] add `"github.com/erigontech/erigon/execution/commitment/nibbles"` import in every file that needs it
- [ ] repoint every `hexToCompact` / `compactToHex` / `keybytesToHex` / `hexToKeybytes` / `hasTerm` / `prefixLen` call to its `nibbles.*` equivalent
- [ ] leave `CompactToKeybytes` calls alone — that symbol moves in Task 5 to `trie/keybytes.go` and stays in package `trie`
- [ ] leave the old definitions in `trie/encoding.go` in place — still referenced by the equivalence test helper and `Keybytes` methods; they go away in Task 7
- [ ] run tests — must pass: `go test ./execution/commitment/trie/... -count=1` and `go test ./execution/commitment/... -count=1`
- [ ] run `make lint`

### Task 5: Extract `Keybytes` struct into its own file

**Files:**
- Create: `execution/commitment/trie/keybytes.go`
- Create: `execution/commitment/trie/keybytes_test.go`
- Modify: `execution/commitment/trie/encoding.go` (remove Keybytes-related code)

- [ ] create `execution/commitment/trie/keybytes.go`, package `trie`, containing: `Keybytes` struct (lifted from `encoding.go:81-85`), `Nibbles()` method (88-94), `ToHex()` (97-99), `ToCompact()` (102-125), `CompactToKeybytes` (128-144), `EncodeRLP` (147-149), `DecodeRLP` (152-159)
- [ ] inside `Keybytes.ToHex()`, replace the call to the (about-to-be-deleted) local `compactToHex` with `nibbles.CompactToHex`
- [ ] inside `Keybytes.ToCompact()`, no conversion calls — body stays identical
- [ ] add `"github.com/erigontech/erigon/execution/commitment/nibbles"` import to `keybytes.go`
- [ ] create `execution/commitment/trie/keybytes_test.go` and move `TestKeybytesToCompact` + `TestCompactToKeybytes` there (from `encoding_test.go:89-123`)
- [ ] delete the moved content from `encoding.go` and `encoding_test.go` (they shrink but do not disappear yet — the non-Keybytes helpers still live there until Task 7)
- [ ] run tests — must pass: `go test ./execution/commitment/trie/... -count=1`
- [ ] run `make lint`

### Task 6: Rename witness marshalling helpers and document non-spec format

**Files:**
- Modify: `execution/commitment/trie/witness_marshalling.go`
- Modify: `execution/commitment/trie/witness_operators_test.go`

- [ ] rename `keyNibblesToBytes` → `witnessKeyNibblesToBytes` in `witness_marshalling.go:164`
- [ ] rename `keyBytesToNibbles` → `witnessKeyBytesToNibbles` in `witness_marshalling.go:197`
- [ ] update callers in the same file: line 70 (`ReadKey`) and line 109 (`WriteKey`)
- [ ] update callers in `witness_operators_test.go:254,256`
- [ ] add doc block immediately above `witnessKeyNibblesToBytes` explicitly stating:
  ```
  // witnessKeyNibblesToBytes / witnessKeyBytesToNibbles implement a CUSTOM
  // wire format for the OperatorMarshaller CBOR stream, originally derived
  // from polygon zkevm. It is NOT the Ethereum yellow-paper compact encoding.
  //
  // Bit layout of the first byte:
  //   bit 0: parity (1 if the nibble count is odd)
  //   bit 1: terminator flag
  //   (no inline first nibble for odd lengths — unlike spec compact encoding)
  //
  // DO NOT "fix" this to match the spec: that would change the witness wire
  // format and break any consumer reading existing witnesses.
  // For spec-compliant compact encoding, use nibbles.HexToCompact /
  // nibbles.CompactToHex in execution/commitment/nibbles.
  ```
- [ ] run tests — must pass, wire format byte-identical: `go test ./execution/commitment/trie/... -count=1 -run Witness`
- [ ] run full trie test suite: `go test ./execution/commitment/trie/... -count=1`
- [ ] run `make lint`

### Task 7: Delete duplicated helpers from keys_nibbles.go and trie/encoding.go

**Files:**
- Modify: `execution/commitment/keys_nibbles.go` (delete duplicates)
- Modify: `execution/commitment/keys_nibbles_test.go` (delete corresponding tests)
- Delete: `execution/commitment/trie/encoding.go`
- Delete: `execution/commitment/trie/encoding_test.go` (already emptied in Task 5)
- Delete: `execution/commitment/nibbles/equivalence_test.go` (tripwire retired)
- Delete: `execution/commitment/export_test.go` (test helper retired — if it existed)
- Delete: `execution/commitment/trie/export_test.go` (same)

- [ ] in `keys_nibbles.go`, delete: `HexNibblesToCompactBytes` (55-88), `uncompactNibbles` (91-109), `HasTerm` (111-114), `commonPrefixLen` (117-125), and the `terminatorHexByte` const (wherever defined). Keep `KeyToHexNibbleHash`, `KeyToNibblizedHash`, `NibblesToString`, `CompactKey`, `updatedNibs`, `hashKey`, `PrefixStringToNibbles`.
- [ ] delete corresponding tests from `keys_nibbles_test.go` (only those targeting the deleted symbols — `KeyToHexNibbleHash`/etc tests stay)
- [ ] delete `execution/commitment/trie/encoding.go` entirely
- [ ] delete `execution/commitment/trie/encoding_test.go` entirely (Keybytes tests already moved in Task 5; the compact/keybytes tests already moved to `nibbles/nibbles_test.go` in Task 1)
- [ ] delete `execution/commitment/nibbles/equivalence_test.go`
- [ ] delete `execution/commitment/export_test.go` and `execution/commitment/trie/export_test.go` (the test helpers from Task 2)
- [ ] run all affected tests: `go test ./execution/commitment/... ./cmd/integration/... -count=1`
- [ ] run `make lint` 2–3×

### Task 8: Replace `cmd/hack/hack.go` local copy with import

**Files:**
- Modify: `cmd/hack/hack.go`

- [ ] delete the local `keybytesToHex` definition at `hack.go:675+`
- [ ] add `"github.com/erigontech/erigon/execution/commitment/nibbles"` import
- [ ] repoint all call sites in `hack.go` to `nibbles.KeybytesToHex`
- [ ] `go build ./cmd/hack/...` must succeed
- [ ] `go vet ./cmd/hack/...` must pass
- [ ] run `make lint`

### Task 9: Update docs/plans references

**Files:**
- Modify: `docs/plans/20260325-fold-refactor-spec.md`
- Modify: `docs/plans/completed/20260325-fold-refactor-spec.md`

- [ ] replace `HexNibblesToCompactBytes` with `nibbles.HexToCompact` at `docs/plans/20260325-fold-refactor-spec.md:295,339`
- [ ] same at `docs/plans/completed/20260325-fold-refactor-spec.md:295,339`
- [ ] no test step — these are docs

### Task 10: Verify acceptance criteria

- [ ] verify goal #1 — exactly one implementation of each spec-compliant helper: `rg -n 'func HexToCompact|func CompactToHex|func KeybytesToHex|func HexToKeybytes|func HasTerm' execution/commitment/ cmd/` should list each exactly once, all under `execution/commitment/nibbles/`
- [ ] verify goal #2 — `trie/encoding.go` does not exist: `test ! -f execution/commitment/trie/encoding.go`
- [ ] verify goal #3 — `keys_nibbles.go` no longer contains the deleted symbols: `rg -n 'HexNibblesToCompactBytes|uncompactNibbles|commonPrefixLen' execution/commitment/keys_nibbles.go` returns nothing
- [ ] verify goal #4 — witness funcs renamed and doc block present: `rg -n 'witnessKeyNibblesToBytes' execution/commitment/trie/witness_marshalling.go` returns the rename + doc
- [ ] verify goal #5 — `cmd/hack/hack.go` no longer defines `keybytesToHex`: `rg -n 'func keybytesToHex' cmd/hack/` returns nothing
- [ ] run full relevant test sets: `go test ./execution/commitment/... ./cmd/integration/... ./cmd/hack/... -count=1`
- [ ] run full erigon build: `make erigon integration`
- [ ] run benchmarks before (from a commit before Task 1) and after (on current branch) and compare with benchstat — no significant regression on `BenchmarkHexToCompact|BenchmarkCompactToHex|BenchmarkKeybytesToHex|BenchmarkHexToKeybytes`
- [ ] run `make lint` 2–3× until clean

### Task 11: [Final] Update documentation and move plan

- [ ] no README changes expected — internal refactor only
- [ ] no CLAUDE.md changes expected — no new patterns introduced
- [ ] move this plan and its objective to `docs/plans/completed/`
- [ ] create that directory if missing: `mkdir -p docs/plans/completed`

## Technical Details

### Canonical function bodies (Task 1)

Lifted from `trie/encoding.go`. Minimal renames to exported names. The magic number `16` for the terminator is replaced with the new `Terminator` const.

```go
package nibbles

const Terminator byte = 0x10

func HexToCompact(hex []byte) []byte {
    terminator := byte(0)
    if HasTerm(hex) {
        terminator = 1
        hex = hex[:len(hex)-1]
    }
    buf := make([]byte, len(hex)/2+1)
    buf[0] = terminator << 5 // the flag byte
    if len(hex)&1 == 1 {
        buf[0] |= 1 << 4 // odd flag
        buf[0] |= hex[0] // first nibble is contained in the first byte
        hex = hex[1:]
    }
    decodeNibbles(hex, buf[1:])
    return buf
}

// CompactToHex, KeybytesToHex, HexToKeybytes, HasTerm, CommonPrefixLen, decodeNibbles
// lifted verbatim (decodeNibbles is unexported within the new package).
```

### Why the canonical bodies come from `trie/encoding.go`, not `keys_nibbles.go`

1. Benchmarks already exist for the `trie/encoding.go` versions (`BenchmarkHexToCompact` et al). Preserving them means the perf baseline is continuous.
2. Implementation style is cleaner: `terminator<<5` + `1<<4` is more direct than the `compactZeroByte` accumulator pattern in `keys_nibbles.go`.
3. The `trie/encoding.go` code is the older go-ethereum-derived original (copyright header from 2014). It has been battle-tested the longest.
4. `keys_nibbles.go` added its versions recently (PR #17764 per the issue) without benchmarks. Younger, less exercised.

Equivalence is proven in Task 2 before either version is deleted.

### Keybytes struct placement

Only two callers of `Keybytes` exist, both in `trie/` (`proof.go:172`, `trie.go:1594`). It implements `rlp.Encoder`/`rlp.Decoder`, so its package matters for any RLP type registry. Moving it to `nibbles/` would impose an `execution/rlp` dep on a leaf package that otherwise has zero deps. Keeping it in `trie/` preserves leaf-package purity and matches its actual usage locality.

### Witness format bit layout (for the Task 6 doc block)

Input: `[]byte{a, b, c, d, e}` (5 hex nibbles, odd length, no terminator).

`witnessKeyNibblesToBytes` produces:
```
result[0] = 0x01     // parity bit (bit 0) set, terminator (bit 1) clear
result[1] = a<<4 | b
result[2] = c<<4 | d
result[3] = e<<4 | 0  // trailing padding for odd length
```

With terminator (same 5 nibbles):
```
result[0] = 0x03     // parity bit set, terminator bit set
result[1..3] = same as above
```

Compare to spec `HexToCompact` on the same input (odd, no terminator):
```
buf[0] = (0<<5) | (1<<4) | a  // 0x10 | a — terminator<<5 + odd_flag + inline first nibble
buf[1] = b<<4 | c
buf[2] = d<<4 | e
```

The wire bytes are demonstrably different. Any consumer of the witness CBOR stream decodes using the witness format; swapping in the spec format silently corrupts output.

## Risks

| Risk | Mitigation |
|---|---|
| Subtle output diff between old impls (Task 1) | Task 2 tripwire runs before Task 7 delete |
| Witness wire format accidentally changed | Witness pair only renamed, never touched; `witness_operators_test.go:254-256` roundtrip catches any regression |
| `Keybytes` RLP serialization changes | Struct stays in `trie/`, only `ToHex` method body repoints to `nibbles.CompactToHex` (same bytes out) |
| Caller import updates miss a site | Full test + lint after each commit; Task 10 verifies with ripgrep |
| `make lint` flakiness | Run 2–3× per CLAUDE.md note |
| `cmd/hack` orphan copy missed | Explicit Task 8 with ripgrep check in Task 10 |
| Unexported symbol name collisions (e.g. `decodeNibbles`, `hasTerm`) between `trie/` local remnants and `nibbles/` | Task 4 deletes the `trie/` local helpers after repointing; no collision window |

## Post-Completion

**Manual verification** (if applicable):
- none required beyond CI — this is a pure refactor with full test coverage

**External system updates** (if applicable):
- PR description should explicitly note: "The witness CBOR wire format (`witnessKeyNibblesToBytes`) is **preserved byte-for-byte**. Consolidation of the witness format into spec compact encoding is deliberately out of scope — belongs in a separate issue with consumer compat review."
- Tag @taratorio on the PR for review (issue reporter)
- Link erigontech/erigon#17797 (fixes) and erigontech/erigon#13884 (parent tracking issue, do not close)
