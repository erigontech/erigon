# Nibbles V2 Key Encoding

## Overview

Add a V2 byte encoding for commitment-trie nibble paths, alongside the existing V1 (`HexToCompact`/`CompactToHex`). V2 packs nibbles 2-per-byte high-first and appends a single parity byte (`0x00` even, `0x01` odd) at the end of the key instead of at the front.

Motivation is GitHub issue [erigontech/erigon#17838](https://github.com/erigontech/erigon/issues/17838): the current HP-prefix-front encoding shards each logical subtree across two unrelated DB regions (one for even-parity keys, one for odd), destroying data locality during commitment fold/unfold operations. Moving the parity flag to a suffix makes the DB layout mirror the trie shape — keys sharing a path prefix cluster together regardless of parity.

This plan delivers **only the library primitives** (encoder, decoder, tests). Wiring V2 into `PutBranch` / `Branch`, snapshot v2, and migration are explicitly out of scope and will come in follow-up plans. The change is purely additive — V1 is untouched.

## Context (from discovery)

- **Files involved**:
  - Existing V1: `execution/commitment/nibbles/nibbles.go` (`HexToCompact`, `CompactToHex`, `KeybytesToHex`, …) — **untouched** by this plan.
  - V1 callers (untouched): `execution/commitment/trie/hasher.go:166`, `execution/commitment/trie/witness_marshalling.go`, `execution/commitment/trie/keybytes.go:49`. These are RLP-node-hash paths, not DB-key paths, and must keep using V1 for hash compatibility.
  - DB-key call site (future wiring, **not in this plan**): `execution/commitment/commitmentdb/commitment_context.go:808` (`TrieContext.PutBranch`).
- **Patterns observed**: package `nibbles` is a leaf utility package; V1 functions use plain `[]byte` for nibble slices (each byte holds one nibble in `[0x00, 0x0F]`); errors are not currently returned from V1 (it panics on odd input via `HexToKeybytes`).
- **Testing convention**: Erigon uses standard `go test`; Go 1.25+, so `testing.F` fuzzing is available.
- **Build verification**: `make lint && make erigon integration` per CLAUDE.md.

## Development Approach

- **testing approach**: Regular — encoder and decoder go in together as Task 1, immediately followed by golden-vector + round-trip tests in Task 2 before any further test work. Tasks 1 and 2 form a tight loop: implementation → tests → must pass → continue.
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change
- maintain backward compatibility (V1 untouched)

## Testing Strategy

- **unit tests**: required for every task (golden vectors, decoder error matrix, encoder panic cases).
- **fuzz**: `testing.F` round-trip fuzz target for `Encode → Decode == identity`.
- **property tests**: subtree-locality assertion over 10K random path pairs.
- **benchmarks**: locality measurement comparing V1 vs V2 sorted-key neighbor distance — evidence-only, not a CI assertion.
- no e2e tests apply (pure library code, no UI surface).

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

A new file `nibbles_v2.go` exposes:

```go
const MaxPathNibbles = 128

var (
    ErrV2KeyLength       = errors.New("nibbles v2: key length out of range")
    ErrV2KeyParity       = errors.New("nibbles v2: parity byte must be 0x00 or 0x01")
    ErrV2KeyShape        = errors.New("nibbles v2: parity=1 with no packed byte")
    ErrV2NonCanonicalPad = errors.New("nibbles v2: non-zero pad nibble in odd encoding")
)

func EncodeKeyV2(nibbles []byte) []byte
func DecodeKeyV2(key []byte) ([]byte, error)
```

V2 lives in the same package as V1 (sibling file). Both are importable side-by-side as `nibbles.HexToCompact` and `nibbles.EncodeKeyV2`. Choosing same-package over a `nibbles/v2/` sub-package because there's no precedent for per-version sub-packages in this codebase, and side-by-side file diffs are easier to review.

V2 is a pure function pair — no global state, no allocations beyond the output slice.

## Technical Details

### Encoding algorithm

For path `P` of length `N` (each element a nibble in `[0x00, 0x0F]`):

1. Allocate `out := make([]byte, N/2 + N&1 + 1)`.
2. For `i ∈ [0, N/2)`: `out[i] = (P[2i] << 4) | (P[2i+1] & 0x0F)`.
3. If `N` is odd: `out[N/2] = P[N-1] << 4` (low nibble = `0` pad).
4. `out[len(out)-1] = byte(N & 1)` (parity: `0x00` even, `0x01` odd).

Empty path `N=0` → single byte `{0x00}`. Maximum `N=128` → 65-byte key.

Encoder **panics** on programmer errors (nibble > `0x0F` or `N > MaxPathNibbles`). This matches V1's `HexToKeybytes` panic-on-odd convention.

### Decoding algorithm

1. Validate `len(key) ∈ [1, 65]` else `ErrV2KeyLength`.
2. Read parity byte `key[len(key)-1]`. If `> 1` → `ErrV2KeyParity`.
3. Let `packed := key[:len(key)-1]`.
4. If parity == 0 (even): every byte holds 2 real nibbles. `N = len(packed) * 2`.
5. If parity == 1 (odd):
   - If `len(packed) == 0` → `ErrV2KeyShape`.
   - If `packed[len-1] & 0x0F != 0` → `ErrV2NonCanonicalPad`.
   - `N = len(packed)*2 - 1`.
6. Unpack high-first into a `[]byte` of length `N`.

Strict canonicality (rejecting non-zero pad nibbles) ensures path ↔ key is a clean bijection: only one valid byte sequence per logical path. This catches corruption from buggy upstream encoders.

### Required golden vectors

| Path nibbles            | N   | V2 key bytes                          |
|-------------------------|-----|---------------------------------------|
| `[]`                    | 0   | `{0x00}`                              |
| `[a]`                   | 1   | `{0xa0, 0x01}`                        |
| `[2, f]`                | 2   | `{0x2f, 0x00}`                        |
| `[2, f, b]`             | 3   | `{0x2f, 0xb0, 0x01}` ★ required       |
| `[2, f, b, 3]`          | 4   | `{0x2f, 0xb3, 0x00}` ★ required       |
| `[2, f, b, 0]`          | 4   | `{0x2f, 0xb0, 0x00}` (disambiguates from N=3 case) |
| `[0, 0, 0, 0]`          | 4   | `{0x00, 0x00, 0x00}`                  |
| `[0, 0, 0]`             | 3   | `{0x00, 0x00, 0x01}`                  |
| 128 × `0xa`             | 128 | 64 × `0xaa` + `0x00`                  |
| 127 × `0xa`             | 127 | 63 × `0xaa` + `{0xa0, 0x01}`          |

### Sort-order locality property

For any two paths `P1`, `P2` with common nibble prefix length `k`, `EncodeKeyV2(P1)` and `EncodeKeyV2(P2)` share at least `⌊k/2⌋` leading bytes. This is the load-bearing claim from issue #17838.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): everything below — new code, tests, benchmarks, lint pass.
- **Post-Completion** (no checkboxes): linking the PR back to issue #17838, follow-up plans for wiring V2 into `PutBranch`, snapshot v2, migration.

## Implementation Steps

### Task 1: Implement V2 encoder and decoder

**Files:**
- Create: `execution/commitment/nibbles/nibbles_v2.go`

- [x] include the standard Erigon LGPL header matching `nibbles.go` (lines 1–18)
- [x] add `const MaxPathNibbles = 128` and the four sentinel errors (`ErrV2KeyLength`, `ErrV2KeyParity`, `ErrV2KeyShape`, `ErrV2NonCanonicalPad`)
- [x] implement `EncodeKeyV2(nibbles []byte) []byte` per the algorithm in Technical Details
- [x] panic with a descriptive message on `len(nibbles) > MaxPathNibbles` or any nibble byte `> 0x0F`
- [x] implement `DecodeKeyV2(key []byte) ([]byte, error)` per the algorithm in Technical Details
- [x] return the four sentinel errors directly (no `fmt.Errorf` wrapping at this layer)
- [x] no comments restating what the code does — only `// why` comments where the reasoning isn't obvious (project preference)
- [x] verify build: `go build ./execution/commitment/nibbles/`

### Task 2: Golden-vector and round-trip tests

**Files:**
- Create: `execution/commitment/nibbles/nibbles_v2_test.go`

- [ ] add `v2Vectors` table covering all rows in Technical Details (including the two starred rows `[2,f,b]→2f b0 01` and `[2,f,b,3]→2f b3 00`)
- [ ] write `TestEncodeKeyV2_Vectors` asserting `EncodeKeyV2(nibbles) == key` for each vector
- [ ] write `TestDecodeKeyV2_Vectors` asserting `DecodeKeyV2(key) == nibbles` for each vector
- [ ] write `TestEncodeKeyV2_RoundTrip` over the same vectors (`Decode(Encode(P)) == P`)
- [ ] write `TestEncodeKeyV2_MaxLen` asserting `len(EncodeKeyV2(make([]byte, 128))) == 65`
- [ ] run tests: `go test ./execution/commitment/nibbles/ -run V2 -v` — must pass before next task

### Task 3: Decoder error tests

**Files:**
- Modify: `execution/commitment/nibbles/nibbles_v2_test.go`

- [ ] write `TestDecodeKeyV2_Errors` table-driven with one row per sentinel error
- [ ] cover: empty input, 67-byte over-long input, parity byte `0x02`, parity byte `0xff`, `{0x01}` (shape: parity=1 no packed byte), `{0x2f, 0xb3, 0x01}` (non-canonical pad), `{0x00, 0xa1, 0x01}` (non-canonical pad mid-key)
- [ ] use `errors.Is` for sentinel comparison
- [ ] run tests: `go test ./execution/commitment/nibbles/ -run V2 -v` — must pass before next task

### Task 4: Encoder panic tests

**Files:**
- Modify: `execution/commitment/nibbles/nibbles_v2_test.go`

- [ ] write `TestEncodeKeyV2_Panics` covering: nibble byte `0x10`, nibble byte `0xff`, length `129` (over `MaxPathNibbles`)
- [ ] use `defer/recover` to assert each case panics
- [ ] run tests: `go test ./execution/commitment/nibbles/ -run V2 -v` — must pass before next task

### Task 5: Subtree-locality property test

**Files:**
- Modify: `execution/commitment/nibbles/nibbles_v2_test.go`

- [ ] write `TestEncodeKeyV2_SubtreeLocality`: over 10K iterations with a fixed seed, generate two random paths of random length `[0, 128]`, compute common nibble prefix `k`, assert encoded keys share at least `k/2` leading bytes
- [ ] also include adversarial deterministic cases: odd parent + grandchild whose continuation begins with nibble `0` (parity-byte ordering edge case from brainstorm)
- [ ] run tests: `go test ./execution/commitment/nibbles/ -run V2 -v` — must pass before next task

### Task 6: Round-trip fuzz target

**Files:**
- Modify: `execution/commitment/nibbles/nibbles_v2_test.go`

- [ ] add `FuzzEncodeDecodeKeyV2` using `testing.F`: input is `(uint, uint64)` → length and seed; generate path, assert `Decode(Encode(P)) == P`
- [ ] seed corpus with the lengths `0, 1, 2, 3, 4, 8, 9, 64, 127, 128`
- [ ] run fuzz briefly: `go test ./execution/commitment/nibbles/ -run='^$' -fuzz=FuzzEncodeDecodeKeyV2 -fuzztime=10s` — must complete clean
- [ ] run normal tests: `go test ./execution/commitment/nibbles/ -v` — all pass before next task

### Task 7: Locality benchmark vs V1

**Files:**
- Modify: `execution/commitment/nibbles/nibbles_v2_test.go`

- [ ] write `BenchmarkLocalityV1VsV2`: generate 100K seeded random paths with realistic length distribution
- [ ] V1 key form for comparison = `nibbles.HexToCompact(path)` with no terminator; V2 key form = `EncodeKeyV2(path)`
- [ ] build sorted V1 key set and sorted V2 key set; report mean nibble-prefix-length between consecutive sorted keys for each scheme
- [ ] use `b.ReportMetric` to surface `v1_neighbor_prefix` and `v2_neighbor_prefix` as named metrics
- [ ] **NOT a pass/fail assertion** — evidence-only for the issue thread
- [ ] verify it runs: `go test ./execution/commitment/nibbles/ -run='^$' -bench=Locality -benchtime=1x`

### Task 8: Verify acceptance criteria

- [ ] verify all requirements from Overview are implemented
- [ ] verify all golden vectors round-trip
- [ ] verify all four sentinel errors are reachable from a malformed-input case
- [ ] verify V1 source files have no diff vs HEAD: `git diff -- execution/commitment/nibbles/nibbles.go execution/commitment/nibbles/nibbles_test.go` must be empty
- [ ] run package tests: `go test ./execution/commitment/nibbles/ -v -count=1`
- [ ] run `make lint` until clean (lint is non-deterministic — re-run if needed)
- [ ] run broader build: `make erigon integration`

### Task 9: [Final] Update documentation and finalize

- [ ] no README/CLAUDE.md updates needed (additive library change, no new pattern to document)
- [ ] check the dominant commit prefix style for this package via `git log --oneline -- execution/commitment/nibbles/ | head` and match it; default suggestion: `commitment/nibbles: add V2 key encoding (encode/decode + tests)`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only*

**Issue / PR linkage:**
- Reference issue #17838 in the PR description; note that this PR delivers only the library primitives and is the first step of the multi-PR plan implied by the issue's "rollout must be planned very carefully" caveat.
- This PR alone does **not** change any DB-stored data, snapshot format, or runtime behavior. It can land independently.

**Follow-up plans (separate PRs, not in scope here):**
- Plan B: audit and confirm wmitsuda's load-bearing assumption (no caller outside the V1 transformation function depends on prefix being at the front). Targets: `execution/commitment/`, `execution/commitment/commitmentdb/`, `db/state/` callers of `PutBranch`/`Branch`.
- Plan C: wire `EncodeKeyV2`/`DecodeKeyV2` into `commitmentdb/commitment_context.go`'s `TrieContext.PutBranch` and the matching read path, behind a feature flag for A/B benching.
- Plan D: snapshot V2 file format / commitment v2 migration story.
- Plan E: A/B benchmark on `arb-dev1` (or equivalent) measuring fold/unfold throughput, page faults, and DB cold-cache reads — the empirical case for the change.
