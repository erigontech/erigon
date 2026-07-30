# PBin M1 — binary trie as a local EL state trie

## Overview

M0 landed `PBinPatriciaHashed`, an EIP-8297 binary commitment engine that computes correct roots in memory and reproduces 6 of 7 root vectors from the reference implementation. It is not wired to anything: the domain path panics, code never enters the tree, and the hash is Keccak-256 while every other client uses BLAKE3.

M1 makes it run. **Target: a dev-chain container started with `--experimental.bin-commitment`, booting and producing blocks on the binary trie.**

Two deliberate scope choices define what that means:

- **Keccak-256 stays the production hash.** BLAKE3 is a **test-only** override, used to replay the reference vectors. This is not cross-client compatibility and must not be described as such — no other client would agree with our roots.
- **The header state-root check becomes independently togglable, defaulting to ON.** It is *not* gated on the variant. On a chain we produce ourselves the check is worth keeping — it cross-checks the builder's root against the executor's, a real if weak oracle — and a dev chain therefore keeps a root oracle. It must be switchable off for a chain whose headers we cannot reproduce, which is the mainnet case below.

Defaulting to ON is the safety property: a bin run against foreign headers fails loudly at block 1 rather than silently building a wrong chain, and hex behaviour is untouched.

This needs no overlay or migration mechanism — verified: dev pins no genesis hash (`execution/chain/spec/genesis.go:141-171`), the dev beacon takes `Eth1Data` from the runtime-computed EL genesis hash (`cmd/utils/flags.go:2243-2250`), and the header root and block-0 exec root come from the same function (`genesiswrite.ComputeGenesisCommitment` → `sd.ComputeCommitment`, `genesis_write.go:468`), so they flip together.

But dev is not a cheap target. Its alloc (`execution/chain/spec/allocs/dev.json`) has 18 entries, 7 code-bearing, and the deposit contract `0x00000000219ab540...705Fa` is 6358 bytes = 206 chunks = 128 header + **78 CODE_ZONE overflow chunks**. Overflow keys are `key_hash(code_hash ‖ tree_index)`, which cannot be derived from a 20-byte plain key — so the one unavoidable API break lands on day one. The contract cannot be dropped: dev is PoS-from-genesis (`TerminalTotalDifficulty: 0`, `CancunTime: 0`, `DepositContract` set, `genesis.go:157-162`).

**M1a is a mandatory intermediate gate, not acceptance.** pbin over a real MDBX datadir with no consensus, via `RebuildCommitmentFiles` (`db/state/squeeze.go:876`) or `backtester` (`execution/commitment/backtester/backtester.go:199-215`). It is the only place collation, merge, restart and branch-record round-trip get exercised without consensus noise — but **it has no header-root oracle**. A wrong root there surfaces only as non-determinism between a forward run and a rebuild. Do not mistake a green M1a for a correct engine.

## Context (from discovery)

- Repo `/Users/awskii/org/wrk/wt/pbin`, branch `awskii/pbin-patricia`, base `1e078ffb04`. Prior plan: `docs/plans/completed/20260729-pbin-patricia-hashed.md` (M0, complete).
- Spec: `/Users/awskii/org/wrk/EIPs/EIPS/eip-8297.md`. Reference implementation: `ethereum/execution-specs` branch `projects/binary-trie`.
- Engine: `execution/commitment/pbin_*.go` (~6.3k lines incl. tests).
- External oracles already green and to be kept green: `pbin_specroots_test.go` (7 fixed + 600 sequence roots, via the oracle), `pbin_specengine_test.go` (6/7, via the engine), `pbin_specvectors_test.go` (BASIC_DATA + key routing).
- Integration surfaces: `execution/commitment/commitmentdb/commitment_context.go`, `db/state/execctx/{domain_shared,options}.go`, `db/state/{squeeze,erigondb_settings,domain_stream}.go`, `execution/commitment/branch_cache.go`.

## Development Approach

- **testing approach**: TDD — the failing test comes first in every task.
- **CRITICAL naming rule** (carried from M0): `package commitment` already declares `cell`, `fold`, `unfold`, `computeCellHash` and more. **Every new package-level identifier MUST carry a `pbin` prefix.** A collision is a compile error, so this applies to every task.
- **The M0 "no external API changes" rule is relaxed, but only for three sanctioned breaks** — Task 7 (option semantics), Task 6 (new persisted toml key), Task 13 (plain-key namespace). Everything else stays additive. If a task appears to need a fourth break, stop and record it with ⚠️ rather than proceeding.
- complete each task fully before the next; small focused changes
- **every task MUST include new/updated tests**, listed as separate checklist items
- **all tests must pass before starting the next task**
- **update this plan file when scope changes during implementation**
- self-contained from a clean git state; no task depends on transient working-tree state

## Testing Strategy

- **unit tests**: required per task, table-driven where the input space is enumerable
- **external conformance**: the three `pbin_spec*_test.go` files are the ground truth and must stay green. Task 1 makes the test path run under BLAKE3 — `pbin_specroots_test.go:55` already hard-asserts `meta.hasher == "blake3"`, so they only become meaningful after Task 1.
- **determinism as a proxy oracle** (M1a): forward-run root vs rebuild-from-domains root over the same datadir. Note this proxy is only valid if the answer to Q2 is "pure function of state".
- **structural asserts** where a test cannot cover the failure: variant/cache combinations, monotonic visit order.
- no e2e tests in the erigon sense; the M1b gate is a node smoke run.

## Progress Tracking

- mark completed items `[x]` immediately
- add newly discovered tasks with ➕
- document blockers with ⚠️
- keep the plan in sync with the work actually done

## Solution Overview

Settled decisions — do not revisit during implementation:

1. **Keccak-256 is the production hash; BLAKE3 is test-only.** Both injection seams (`pbinHasher.sum`, `pbinDigestCache.sum`) keep their Keccak nil-default. The test harness drives the engine under BLAKE3 through **both** seams so the reference vectors mean something. Never describe this as cross-client compatibility, and never as a speedup — BLAKE3 is slower than erigon's `fastkeccak` on arm64 at the 133-byte branch preimage.

   The vector conformance still transfers to the production path: the trie treats keys as opaque bytes, so an algorithm correct for BLAKE3-derived keys is correct for Keccak-derived ones. What does **not** transfer is any claim of agreement with another client. The residual risk is a hash call site that bypasses the injectable seam — caught because the vectors run under BLAKE3 and a hardcoded Keccak site would break them.

2. **The header state-root check is independently togglable, default ON**, at all five comparison sites: `exec3.go:810`, `exec3.go:730`, `exec3_serial.go:205`, `committer.go:557`, `:659`, `:764`. Follow the existing `common/dbg/experiments.go` `EnvBool` convention (as `DiscardCommitment` does) — one definition site, no CLI plumbing, easy to set in a container. Do **not** gate it on the variant: a self-produced chain keeps the check as an oracle, and only a foreign-header chain needs it off. Note `dbg.DiscardCommitment()` is a different thing — it skips computing the root at all (`exec3.go:788`) — and must not be reused for this.
3. **The CODE_HASH leaf value stays Keccak** (eip:344-347, :578-579) — already correct at `pbin_values.go:68-73`. The empty-tree hash is 32 zero bytes and hash-independent.
4. **Zero-vs-absent is fixed in the engine, not the domain.** The engine holds the presence bit the domain lacks. Domain encoding is untouched.
5. **`code_size` on `Update` is additive, not a break** — verified below.
6. **Overflow code chunks carry their value in the branch record**, and the new plain-key shape is **tag-discriminated, never length-discriminated**.
7. **pbin is a whole-datadir property**, resolved at first start and persisted. No mid-chain activation.
8. **pbin stays `ModeDirect`, sequential.** Parallel/streaming mounting is structurally excluded.

### Why `code_size` on `Update` is not an external break

`Update.Encode/Decode` (`commitment.go:2253-2335`) has exactly two production call sites, both in `RecordingContext` (`recording_context.go:72,85`) feeding `BuildTrieTrace` into a debug TOML (`trie_trace.go:36-127`). `Update` is never in an MDBX table, never a domain value, never crosses gRPC. The `ModeDirect` ETL spill carries only `(hashedKey → plainKey)` (`commitment.go:1938-1942`), and the pbin branch record does not serialize `Update` at all. All 141 `Update` composite literals repo-wide are keyed, so a new field compiles everywhere unchanged. The comment at `pbin_hash.go:138-139` claiming otherwise is wrong and must be deleted.

### The push side is dead for pbin

The bin variant is hardwired to `ModeDirect` (`commitment.go:165-170`), whose `TouchPlainKey` ignores both `val` and `fn` (`:1668-1672`), and `HashSort` passes `update = nil` (`:1966`). So `Updates.TouchCode` (`:1834-1844`) can **never** deliver code to pbin. Everything comes from the read side, at `TrieContext.Account`. Do not patch `calc_state.go:353-360` expecting code to land.

## Technical Details

**Hash injection** — `pbinHasher.hash` (`pbin_hash.go:63-68`) and `pbinDigestCache.hash` (`pbin_keys.go:127-132`) both nil-fallback today. Flipping the fallback costs **0 edits** at the 43 `pbinTreeKeyAccount`/`pbinTreeKeyStorage` call sites and 12 `pbinKeyHasher()` sites; threading a parameter would cost ~45.

**Root record key** — bit-path keys always end in a byte ≤ 7 (`pbin_bitpath.go:191-193`), so a single-byte sentinel ≥ `0x08` cannot collide. It must also avoid `0x00`, which `pbinEncodeBitPath` produces for the empty path and which the row-0 fold already writes (`pbin_patricia_hashed.go:669`).

**State blob** — hex writes depths as one byte per row (`hex_patricia_hashed.go:2777-2779`); pbin depths are `[528]int16` (`pbin_cell.go:80`), so a naive port truncates ≥256. Preferred shape is root cell + 3 flags ≈ 160 B, resting on an unproven inference (see Thin/Unverified). The 16-byte `txNum‖blockNum` header stays byte-identical — it is read raw and variant-blind at `commitment_context.go:1140-1150`.

**Code chunking** (eip:374-397) — pad to a multiple of 31 **before** the pushdata scan; `bytes_to_exec_data` sized `len(padded)+32`; residual pushdata carries **across** chunk boundaries; `byte0 = min(bytes_to_exec_data[pos], 31)`. `MaxCodeSize` 24576 → 793 chunks → 128 header + 665 overflow across 3 CODE_ZONE stems. A 7702 designator is 23 bytes → 1 chunk.

## Hazard Register

Each hazard needs a named test or a structural assert. These are the plan's real acceptance criteria.

| ID | Hazard | Task | Guard |
|----|--------|------|-------|
| H1 | **BranchCache slot collision** — `trunkSlot` returns another node's *well-formed* record; `pbinDecodeBranch` accepts it, the subtree hashes, root is wrong, no error. Deterministic, concentrated at the top of the tree (8 slots for every ≤8-bit path) | 4 | structural assert in the ctor + test that a bin SharedDomains has no shared branch cache |
| H2 | **Root record lost to empty-key iteration truncation** — `loadRoot` treats absent as an empty tree (`pbin_patricia_hashed.go:349-351`); looks like a fresh datadir | 2 | round-trip a stored root through a real domain iteration |
| H3 | **A hash call site bypassing the injectable seam** — with Keccak in production and BLAKE3 only in tests, a site hardcoding either one drifts silently. Also a pooled engine inheriting a stale `hasher.sum` | 1 | full 32-byte key equality in `TestPBinSpecKeyRouting` under BLAKE3 — a hardcoded site breaks the vectors; `Release()` must clear `hasher.sum` |
| H4 | **Variant mismatch across processes** — genesis hex + exec pbin, flagless restart, rpcdaemon defaulting to hex, `integration commitment rebuild` overwriting pbin records | 6, 7 | persisted `trie_variant` + refusal on disagreement |
| H5 | **Backwards visit from the header-chunk fan-out** — `fold` writes with `prevData = nil` and the record replaces its predecessor outright; re-descending a folded row rewrites it with a `touchMap` that no longer names the previously-touched bit | 12 | assert monotonic visit order; test a batch touching a header slot *and* code on one account |
| H6 | **State-blob depth truncation** — `byte(depth)` truncates ≥256; paths reach 528 bits | 5 | restart round-trip with a >256-bit path |
| H7 | **Code key misread as storage** — a 52-byte length-discriminated code key read as `(addr, slot)` | 13 | tag-discriminated by construction + test that a code key never routes to the storage zone |
| H8 | **Stale high code chunks after a shortening redeploy** — header chunks overwrite in place and are never removed, so a forward run keeps residue while a rebuild emits only `ceil(code_size/31)`. Two internally-consistent, different roots. **Breaks recompute-from-domains as an oracle** | 12 | shortening-redeploy test comparing forward-run vs rebuild. See Q2 |
| H9 | **Unconditional `CodeDomain` read promotes tolerated inconsistency to root divergence** — cleared 7702 residue, `eth_simulateV1` overlays. The existing code documents the residue as benign (`commitment_context.go:1054-1057`); PBT removes that license | 11 | decide and test the residue case explicitly |
| H10 | **`ReplacePlainKeys` over pbin records** if references are ever enabled — rewrites bytes at hex cell offsets during background merge. Inert by default, one flag away, no variant check in that path | 6 | refuse the combination |
| H11 | **Overflow-chunk sibling rehash via `CodeStore` by-hash** → cache *miss* (not error) → zero-valued chunk leaf | 13 | avoided entirely by value-in-record |
| H13 | **Root verification switched off leaves nothing validating the node path** — a silently wrong chain looks healthy. Only relevant when the toggle is used, i.e. against foreign headers; a self-produced chain keeps the check | 6 | default ON so it is opt-out not opt-in; loud startup log when off; a bin run against foreign headers without the toggle must fail at block 1, not degrade |
| H12 | **`foldDelete` "enabled" to make a test pass** — collapses nodes the reference leaves in place | 10 | guarded by plan text + a test asserting it stays unreachable from `Process` |

## Open Questions

Blocking items needing a human or upstream answer. Do not proceed past the task that depends on one without recording the answer here.

- **Q1 (highest value) — what does the reference produce for a removed account?** EIP-161 empty-removal and EIP-6780 destruct both hand pbin a `DeleteUpdate` on a committed leaf. Task 9 would turn that into BASIC_DATA `{version 0, code_size 0, nonce 0, balance 0}` + CODE_HASH `keccak(empty)`. Consistent with eip:345-347 but **not verified against the reference**, and `testdata/eip8297_vectors.json` does not cover it. It silently changes the root. ⚠️ **Deferred at Task 9, still unanswered.** Account removal keeps erroring at both sites (`updateCell`, the `loadCellState` account arm); only storage was reinterpreted. Note the `zero_value_present` vector *is* an account-zone BASIC_DATA leaf of 32 zero bytes, so the reference at least admits that leaf shape — it does not say a removal produces it. Unblocks nothing in M1: a dev chain reaches neither removal path.
- **Q2 — is the tree a pure function of current state, or of history?** (H8.) eip-8297 is silent. Determines whether recompute-from-domains is a valid oracle at all, hence whether the M1a gate means anything. Possibly an upstream spec question.
- **Q3 — does the reference create a present-zero leaf on `SSTORE 0` to a virgin slot?** Erigon drops it at `execution/state/state_object.go:245-246` before any writer sees it. If yes, that guard must be variant-gated and every zero-SSTORE becomes a state write. Still open after Task 9, which deliberately kept the virgin case a no-op: a delete only zeroes a cell that already holds a leaf.
- **Q4 — confirm `github.com/zeebo/blake3`** as a **test-only** dependency (lower stakes now that production stays Keccak; the in-graph `lukechampine.com/blake3` may simply be enough). Its measured advantage was 13–18% arm64 / 58–78% amd64, not re-verified. **Answered (Task 1):** `zeebo` not added — production stays Keccak so BLAKE3 speed is irrelevant, and `lukechampine.com/blake3` is already a direct dependency used by `pbin_specroots_test.go`; it now backs `pbinBlake3Hash` for all three seam tests.
- **Q5 — forward/backward compatibility of a new `erigondb.toml` key** across erigon binaries, given the file is downloader-delivered and **wins over the CLI** (`erigondb_settings.go:74-89`). **Answered (Task 6):** `readErigonDBSettings` uses `go-toml/v2` `Unmarshal`, which ignores unknown keys — older binaries parse a `trie_variant` toml fine. The key is written only when bin, so published/downloader tomls stay byte-identical, and a downloader-delivered hex toml under a bin process is refused at resolve. Residual risk: a binary **predating the key** opens a bin datadir as hex with no guard — inherent to any new key; acceptable while bin is experimental and fresh-datadir-only.

## Thin / Unverified

Do not treat these as established:

- `pbin_branch.go` record field-bit layout beyond the leaf-kind rejection at `:156-165`. **Verify before designing Task 13's value-in-record field.**
- ~~The "grid arrays are restorable-as-zero" argument underpinning Task 5's ~160-byte blob~~ — **proven in Task 5** (see the Task 5 checklist for the read/write-site audit); the root-cell blob landed.
- ~~Whether pbin branch records are truly opaque to the pass-through merge path~~ — **exercised in Task 10** with references off: collation, prune and merge round-trip the records byte-for-byte, checked against a db snapshot with a positive count of records provably served from files.
- ~~Task 8's deferral mis-attribution~~ — **Task 8**: still no concrete failing sequence, and the exposure is bounded: deferral is only ever requested by `ExecV3` (fork validation / parallel apply), and the fork-validation writes it would mis-route land in a validation overlay that is never flushed. The guards are structural — bin cannot reach the deferred path at all now.

## What Goes Where

- **Implementation Steps** (`[ ]`): code, tests and asserts inside this repo
- **Post-Completion** (no checkboxes): the node smoke run, upstream questions, follow-on milestones

## Implementation Steps

### Task 1: BLAKE3 as a test-only hash, wired through both seams

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_specvectors_test.go`
- Modify: `execution/commitment/pbin_specengine_test.go`
- Modify: `go.mod`

Production keeps Keccak-256. This task only makes the **test** path run the whole engine — node hashing *and* key derivation — under BLAKE3, so the reference vectors become meaningful for the key-derivation surface too.

- [x] write a failing test asserting `TestPBinSpecKeyRouting` compares **full 32-byte tree keys** against `embedding_vectors`, not just zone/length/sub-index — this is what proves no hash site bypasses the seam (guards H3) — full-key equality passes: derivation matches the reference under BLAKE3
- [x] write a failing test asserting a pooled engine does not inherit a previous `hasher.sum` after `Release()` — `TestPBinReleaseClearsHashSuite`, red before the fix
- [x] add `github.com/zeebo/blake3` (test use only; confirm Q4 first) — Q4 answered: not added, in-graph `lukechampine.com/blake3` suffices (see Open Questions)
- [x] give the engine a way to set BLAKE3 on **both** seams together — `pbinHasher.sum` and the `pbinDigestCache` behind `pbinKeyHasher` — so a half-configured test is impossible — `setHashSuite(sum)` sets the node seam and returns a matching `pbinKeyHasherWith(sum)`
- [x] clear `hasher.sum` in `Release()` (`pbin_patricia_hashed.go:107-115`)
- [x] leave the production nil-defaults on Keccak, the CODE_HASH leaf value on Keccak, and the empty-tree hash at 32 zero bytes
- [x] run tests — the three `pbin_spec*_test.go` files must all pass under BLAKE3 before task 2

### Task 2: Root record key sentinel

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_rootkey_test.go`

- [x] write a failing test that stores a root record and reads it back through a real `TblCommitmentVals` iteration, asserting the iteration does not truncate (guards H2) — `TestPBinRootRecordRealTableIteration`, red before the fix: MDBX accepts the empty-key Put but hands the key back zero-length mid-iteration, which `domain_stream.go:343,577` reads as end-of-stream
- [x] replace `pbinRootKey = []byte{}` with a single-byte sentinel ≥ `0x08` — `0x08`
- [x] assert the sentinel cannot be produced by `pbinEncodeBitPath` for any bit length 0..528 — `TestPBinRootKeySentinelNotABitPath`, plus `pbinDecodeBitPath` rejecting the sentinel outright
- [x] write a test asserting `loadRoot` distinguishes "no record" from "empty tree" — `TestPBinLoadRootNoRecordVersusStoredTree`: no record reads as the empty tree with `rootPresent == false`; a stored record reproduces the stored root
- [x] run tests — must pass before task 3

### Task 3: No nil values into the domain

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_domainwrite_test.go`

- [x] write a failing test asserting neither `storeRoot` nor `foldDelete` ever hands a nil value to `PutBranch` — `TestPBinStoreRootEmptiedTreeWritesNonNil` + `TestPBinFoldDeleteWritesNonNilWithRealPrev` over `pbinStrictWriteContext`, which refuses nil the way `SharedDomains.DomainPut` does; both red before the fix
- [x] route the empty-root `storeRoot` path (`:328-337`) and `foldDelete` (`:725-727`) through `DomainDel` or a non-nil zero-length slice — non-nil zero-length: `PatriciaContext` has no `DomainDel`, and `TemporalMemBatch.putHistory` routes any `len(v) == 0` write to `DeleteWithPrev`, so `[]byte{}` IS the deletion encoding at the domain boundary
- [x] pass real `prevData` at both `PutBranch` sites to avoid the extra `GetLatest` per branch write — the grid retains each row's record bytes at unfold (`pbinGrid.prevRecord`), the engine retains the root record across load/store (`rootPrev`); all three write sites (`foldBranch`, `foldDelete`, `storeRoot`) now pass it. `TestPBinProcessPutBranchCarriesRealPrev` checks every write's prev equals the record it replaces, red before
- [x] write a test asserting a zero-length branch value round-trips as a deletion — `TestPBinZeroLengthBranchRoundTripsAsDeletion`: the engine empties a stored tree, the zero-length records stay in the store, a fresh engine reads them back as no tree
- [x] run tests — `go test ./execution/commitment/... -count=1` green, `make lint` clean

### Task 4: Disable the shared BranchCache for the bin variant

**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/commitmentdb/pbin_nocache_test.go`

- [x] write a failing test asserting a bin-variant `SharedDomains` has no shared branch cache — `TestPBinSharedDomainsHasNoSharedBranchCache`, red while the ctor assert saw the shared cache; written to survive Task 5 (tolerates the save/restore panic, asserts directly on the SD once construction succeeds)
- [x] write a failing test demonstrating the `trunkSlot` collision for two distinct ≤8-bit bit-path keys, so the reason is pinned in the suite (guards H1) — `TestPBinBranchCacheTrunkSlotCollision`: 3-bit paths 000 (`00 03`) and 001 (`20 03`) both index `d2[0x03]`; `Get` serves the other path's record as a well-formed hit. Pinning test — it passes against current `trunkSlot` by design and fails if the collision ever disappears
- [x] construct the bin-variant `SharedDomains` with `execctx.WithoutSharedBranchCache()` — `NewSharedDomains` applies it whenever `trieCfg.Variant` is bin; the co-located `AdaptivePinController` is gated on the same option and stays off too
- [x] add a structural assert in the commitment-context ctor that the bin variant never has a shared branch cache — enforce, do not document — the commitmentdb `sd` interface gained `HasSharedBranchCache()` (implemented by `execctx.SharedDomains`); `NewSharedDomainsCommitmentContext` panics on bin+shared-cache ahead of the save/restore panic Task 5 removes
- [x] run tests — `go test ./execution/commitment/... -count=1` and `./db/state/... -short` green, `make lint` clean twice

### Task 5: SetState / EncodeCurrentState for pbin, and remove the panic

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_state.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Create: `execution/commitment/pbin_state_test.go`

- [x] write a failing restart round-trip test covering a path deeper than 256 bits (guards H6) — `TestPBinRestartRoundTripDeepPath`: same-group slots 256/257 share the first 520 tree-key bits, so the root branch prefix is 527 bits; encode → restore → continue reproduces the oracle root
- [x] prove or refute that the grid arrays are restorable-as-zero — **proven**: every row-indexed array (`rows`, `depths`, `touchMap`, `afterMap`, `branchBefore`, `prevRecord`) is written only in `unfold`/`unfoldBranchNode` before `activeRows++` exposes the row, and read only at indexes < `activeRows` (`updateCell`, `needUnfolding`, `fold` and its three arms). At `activeRows == 0` — which both state calls enforce — the live state is exactly the root cell plus the three root flags. Chose the root-cell blob: `0xB1 marker ‖ flags ‖ uint16 len ‖ pbinAppendCell(root)`. `rootPrev` is deliberately not serialized: a post-restore `storeRoot` passes nil prev and `DomainPut` fetches the stored value itself
- [x] implement `pbin` `SetState`/`EncodeCurrentState` — the chosen blob serializes no depths at all, so no depth ever meets a one-byte encoding; the marker byte also rejects a hex blob outright (hex starts with a flags byte ≤ 0x07)
- [x] remove the `VariantBinPatriciaTrie` panic and fix the hardcoded `variant:` in the struct literal — the stale `Test_NewSharedDomainsCommitmentContext_RejectsBinVariant` that pinned the panic became `..._AcceptsBinVariant`
- [x] extend the three variant gates: `LatestCommitmentState`, `encodeCommitmentState`, `restorePatriciaState` — all three assert `commitment.StatefulTrie`; the trie-trace state capture in `ComputeCommitment` now uses the same seam instead of a hex/parallel type switch
- [x] promote `StatefulTrie` as an **optional** interface asserted at those 3 sites; do not widen `Trie` — declared beside `Trie`; hex satisfies it as-is, `ParallelPatriciaHashed` delegates to its template trie, pbin implements it in `pbin_state.go`
- [x] write a test asserting the 16-byte `txNum‖blockNum` header is byte-identical to hex's — `TestPBinCommitmentStateHeaderMatchesHex` (➕ white-box `commitmentdb/pbin_state_header_test.go`, not in the planned file list) also round-trips block/tx through `restorePatriciaState` under bin
- [x] run tests — `go test ./execution/commitment/... ./db/state/... -count=1` green, `make lint` clean

### Task 6: The --experimental.bin-commitment flag, persistence, and root-check gating

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `db/state/erigondb_settings.go`
- Modify: `db/state/squeeze.go`
- Modify: `cmd/utils/flags.go`
- Modify: `node/cli/default_flags.go`
- Modify: `node/ethconfig/config.go`
- Modify: `node/eth/backend.go`
- Modify: `cmd/integration/commands/flags.go`
- Create: `db/state/pbin_variant_persist_test.go`

- [x] write a failing test asserting a datadir created with the bin variant is **refused** when opened with a conflicting config (guards H4) — `db/state/pbin_variant_persist_test.go`: bin datadir + streaming/parallel flags refused; hex datadir (absent or explicit `trie_variant`) + bin flag refused; a downloader-delivered hex toml under an in-memory-bin process refused; legacy datadir (preverified.toml) + bin flag refused
- [x] write a failing test asserting `references_in_commitment_branches = true` is refused under the bin variant (guards H10) — `TestPBinVariantRefusesReferences`: both the persisted refs=true+bin toml and the first-start refs-override+bin combination error
- [x] add a `statecfg` global for the variant — `statecfg.ExperimentalBinCommitment` (`COMMITMENT_BIN` env, mirroring `COMMITMENT_PARALLEL`)
- [x] add the `--experimental.bin-commitment` flag across the 7-site experimental-commitment template — flag def + ctx→cfg (`cmd/utils/flags.go`), `node/cli/default_flags.go`, `ethconfig.Config` field, cfg→statecfg (`node/eth/backend.go`), `cmd/integration/commands/flags.go`, statecfg global
- [x] replace the duplicated inline switch at `squeeze.go:1023-1029` with `PickTrieVariant()` — the `EnableParaTrieDB` gate below it now derives from the returned variant instead of the raw flags
- [x] add `trie_variant` to `ErigonDBSettings`, resolved first-start exactly as `ReferencesInCommitmentBranches` is, and note in a comment that `erigondb.toml` wins over the CLI — `*string` ("hex"/"bin", absent = hex), written only when bin so published tomls stay unchanged; `reconcileTrieVariant` runs at every resolve: a persisted bin adopts bin process-wide (sets the statecfg global), all conflicts refuse rather than degrade
- [x] write a failing test asserting the header state-root comparison is enforced by default and skipped only when the new toggle is set — under **both** variants, since the toggle is variant-independent — `TestHeaderRootCheckDefaultOnAndTogglable` drives `headerRootMismatch` under both settings of the bin global
- [x] add the third case to `PickTrieVariant()` reachable via `--experimental.bin-commitment` — bin wins over streaming/parallel (the resolver refuses the combination anyway); `TestPickTrieVariant_BinFlag`
- [x] add a root-check toggle to `common/dbg/experiments.go` following the `DiscardCommitment` `EnvBool` pattern, **defaulting to check-enabled**, and honour it at all five sites: `exec3.go:810`, `exec3.go:730`, `exec3_serial.go:205`, `committer.go:557`, `:659`, `:764` — `dbg.CheckHeaderStateRoot` (`CHECK_HEADER_STATE_ROOT`, default true), applied via a shared `headerRootMismatch` helper at all five comparisons; `handleIncorrectRootHashError` (the `:730` arm) is only reachable from gated comparisons
- [x] log loudly once at startup when the check is disabled, so a running node says so out loud (guards H13) — `backend.go` Warn at node construction
- [x] write a test asserting a flagless restart of a bin datadir stays bin — `TestPBinVariantFlaglessRestartStaysBin`: persisted bin re-adopts with the global off, `PickTrieVariant()` returns bin
- [x] run tests — `go test ./execution/commitment/... ./db/state/... -count=1` and `./execution/stagedsync/... -short` green, `make lint` clean twice

### Task 7: Un-pin the genesis variant

**Files:**
- Modify: `db/state/execctx/options.go`
- Modify: `execution/state/genesiswrite/genesis_write.go`
- Modify: `rpc/rpchelper/commitment.go`
- Create: `db/state/execctx/pbin_options_test.go`

- [x] write a failing test asserting genesis under the bin variant computes a **binary** root, not a hex one — `TestPBinGenesisComputesBinaryRoot` (➕ `execution/state/genesiswrite/pbin_genesis_test.go`, not in the planned file list), red before: `GenesisToBlock` returned the hex root under bin. Asserts both `bin != hex` and `bin == ` the root of a SharedDomains explicitly running the bin trie over the same alloc. Code-free alloc — code chunking is Task 12/13
- [x] add `WithoutParallelCommitment()` that demotes streaming/parallel to hex and leaves bin as bin; keep `WithSequentialCommitment()` as a deprecated alias or migrate all 11 call sites — migrated all 11 and removed `WithSequentialCommitment`; no alias, so a new call site has to pick a variant policy deliberately
- [x] switch `genesis_write.go:381` to the new option
- [x] make the 10 RPC/integrity sites return an explicit unsupported-variant error rather than silently forcing hex over pbin records — `WithHexCommitmentOnly()` + `ErrBinCommitmentUnsupported`, refused in `NewSharedDomains` before any domain work. ➕ an 11th site (`commitment_integrity.go:1194`, the per-block SD inside `CheckCommitmentHistAtBlkRange`) carried no variant option at all and got the same gate
- [x] write a test asserting each of those paths errors under bin instead of returning a hex root — functional per-caller tests: `CheckCommitmentHistAtBlk` + `CheckCommitmentHistAtBlkRange` (`db/integrity/pbin_hex_only_test.go`), `ComputeCustomCommitmentFromStateHistory` (`rpc/rpchelper/pbin_commitment_test.go`), `eth_getProof` + `eth_simulateV1` (`rpc/jsonrpc/pbin_hex_only_test.go`). The remaining sites (`getWitness`, `buildWitnessResult`, both receipt-regeneration sites, `checkCommitmentRootViaSd`) reach their SharedDomains only after full block re-execution or over snapshot files, so they are covered by the shared refusal itself, tested directly in `TestPBinHexOnlyCommitmentRefusesBin`
- [x] run tests — `./db/state/... ./execution/commitment/... ./execution/state/genesiswrite ./db/integrity ./rpc/rpchelper ./rpc/jsonrpc/...` green, `make lint` clean twice

### Task 8: Make the silent degradations loud

**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go` (➕ `ErrPBinUnsupported`, the sentinel both packages wrap)
- Modify: `execution/stagedsync/exec3.go`
- Modify: `node/eth/backend.go` (➕ startup limitation log)
- Create: `execution/commitment/commitmentdb/pbin_unsupported_test.go`
- Create: `execution/stagedsync/pbin_defer_test.go` (➕)

- [x] write a failing test asserting `SetLeaveDeferredForCaller` and the deferred-update take reject the bin variant instead of silently no-opping — `TestPBinRefusesDeferredCommitmentUpdates` (enabling side) + `TestPBinComputeCommitmentRefusesDeferredTake` (taking side), both red before: the flag was accepted and the post-`Process` type switch matched no bin trie
- [x] reject the bin variant explicitly where `exec3.go:206-210` enables deferral for fork validation and the parallel apply path — `deferCommitmentUpdates(variant, isForkValidation, parallel, isApplyingBlocks)` excludes bin, so `ExecV3` never makes a request the context panics on. **Not an ExecV3 error**: `ValidateChain` runs fork validation on every `engine_newPayload` (`exec_module.go:589`), so erroring there would make the M1b dev-chain gate unreachable; deferral is a re-org-overhead optimisation and the inline path it falls back to is the default one, over a validation overlay that is never flushed (`exec_module.go:600-602`)
- [x] make `SetCollapseTracer` (`:415-420`), `BranchChildCount` (`:424-431`) and trace-state capture (`:501-506`) error under bin rather than degrade — `BranchChildCount` and the trace capture (in `ComputeCommitment`) return `commitment.ErrPBinUnsupported`; the two void setters (`SetDeferCommitmentUpdates`, `SetCollapseTracer`) panic with the same wrapped error, matching this file's existing misuse convention (`EnableParaTrieDB`, the Task 4 ctor assert) instead of taking a fourth API break for an error return. Both are unreachable under bin in production — their only callers reach a hex-only SharedDomains (Task 7)
- [x] write tests asserting each of the four paths errors under bin — `commitmentdb/pbin_unsupported_test.go`: deferral enable, deferral take, trie-trace capture, collapse tracer, branch child count; each also pins that hex still accepts. ➕ `stagedsync/pbin_defer_test.go` (not in the planned file list) table-tests the exec3 decision
- [x] ➕ log the bin variant's unsupported paths once at startup, after the erigondb resolve so a flagless bin restart says it too (`node/eth/backend.go`)
- [x] run tests — `./execution/commitment/... ./db/state/... ./execution/stagedsync/... ./rpc/jsonrpc/...` green, `make lint` clean twice

### Task 9: Zero-vs-absent in the engine

**Files:**
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_zerovalue_test.go`
- Modify: `execution/commitment/pbin_process_test.go` (➕ retire the two tests pinning the replaced behaviour)

- [x] write a failing test asserting a `DeleteUpdate` on an existing **storage** leaf writes 32 zero bytes and keeps the leaf, matching the reference `zero_value_present` root — `TestPBinStorageDeleteKeepsLeafAsPresentZero` (storage zone + account-header zone) and `TestPBinStorageDeleteOnUntouchedSiblingIsPresentZero`, both red before. ⚠️ correction: `zero_value_present`'s single entry is an **account-zone BASIC_DATA** key (`0x00 ‖ stem ‖ 0x00`), so it is Q1's shape, not a storage one, and the engine already reproduces it via `pbin_specengine_test.go`. The storage roots are anchored on the oracle instead — the same tree code that vector pins in `pbin_specroots_test.go`. Each test also asserts the zeroed leaf is *not* dropped: present-zero ≠ absent
- [x] reinterpret `DeleteUpdate` for storage at the three reject sites — `updateCell` and the `loadCellState` storage arm now route through `pbinZeroedLeafUpdate`, which returns a zero `StorageUpdate` for a 52-byte plain key and `errPBinDeleteUnsupported` for an account. The `loadCellState` **account** arm keeps its own explicit rejection. A delete landing on an empty cell stays a no-op (no leaf to zero) — Q3's virgin-slot case is untouched. `TestPBinLoadCellStateAbsentRead` pins the two arms apart directly. The fourth guard, `processKey` (`:184-186`), is deliberately left rejecting: it only sees a non-nil stream update, which `ModeDirect` — the mode bin is hardwired to — never passes
- [x] for the **account-removal** encoding only: record the answer to Q1 in this plan first; if unanswered, mark ⚠️, leave account removal rejecting, and continue — ⚠️ **Q1 remains unanswered**: no reference behaviour for a removed account, and no vector covers it. Account removal still errors; `TestPBinAccountRemovalStillRefused` pins that. A dev chain reaches neither EIP-161 clearing nor the EIP-6780 pre-funded-CREATE2 case, so M1b is not blocked
- [x] leave the domain encoding and the three zero-write `DomainDel` sites untouched — `git diff --stat` for this task touches only `pbin_patricia_hashed.go` and two test files
- [x] write a test asserting `foldDelete` remains unreachable from `Process` (guards H12) — `TestPBinFoldDeleteUnreachableFromProcess`: a run zeroing every stored leaf (both zones) plus an absent key, asserted through `pbinStrictWriteContext` to write no zero-length record. That is foldDelete's only observable — `storeRoot` is the sole other zero-length write and only at the root key
- [x] ➕ retired `TestPBinProcessRejectsDeletedLeaf` / `TestPBinProcessRejectsDeletedSibling` from `pbin_process_test.go`: they pinned the storage behaviour this task replaces, and the two present-zero tests are their successors
- [x] run tests — `go test ./execution/commitment/... -count=1` and `./db/state/... -short` green, `go build ./...` clean, `make lint` clean twice

### Task 10: M1a gate — pbin over a real datadir

**Files:**
- Create: `execution/commitment/backtester/pbin_m1a_test.go`
- Modify: `db/state/squeeze.go` (➕ phantom empty-key touch in `rebuildCommitmentShard`)

- [x] write a test driving pbin over a real MDBX datadir via `RebuildCommitmentFiles` or the backtester, with no consensus — `backtester_test` builds its own datadir (real MDBX under a temp dir + real `.kv` domain files) and drives it through `execctx.SharedDomains` with `statecfg.ExperimentalBinCommitment` on; every SD open asserts the trie really is `*PBinPatriciaHashed`, so a hex fallback cannot make the suite vacuous. The `Backtester` type itself is unusable here — it needs a synced datadir with canonical headers
- [x] assert the forward-run root equals the rebuild-from-domains root over the same input — `TestPBinM1AForwardRunMatchesRebuildFromDomains`. Two arms: a full-touch recompute over the same datadir, and `RebuildCommitmentFiles` after wiping every commitment record and file. ➕ **found a bug in shared code**: `rebuildCommitmentShard` touches the key from `next()` before testing `ok`, so at stream exhaustion it touches a 0-length plain key. Hex hashes it into a spurious absent update; pbin panics (a plain key is neither 20 nor 52 bytes). Fixed by skipping the touch for an empty key — `next()` signals exhaustion as `(false, nil)` but a shard boundary as `(false, key)`, so the key has to be checked separately from `ok`. ➕ the comparison point is the **last collated** step boundary, not the last forward root: collation always leaves the newest step in the db, so a files-only rebuild reproduces the root as of `TxNumsInFiles`
- [x] assert a restart mid-run resumes to the same root (exercises Task 5) — `TestPBinM1ARestartResumesToSameRoot`: two halves of one input across an aggregator reopen, second half touching only its own keys, must reach the uninterrupted root; plus a fresh SD restoring the saved root before folding anything. ⚠️ **correction**: this does not exercise Task 5's state blob. `RootHash()` calls `loadRoot()` whenever `rootChecked` is false, so a gutted `SetState` still returns the right root — for pbin the restart carrier is Task 3's root record in the commitment domain, and the blob is a cache. Verified by mutation: gutting `SetState` leaves this test and `TestPBinRestartRoundTripDeepPath` green, and only the blob's own unit tests (`TestPBinStateBlobRoundTripsFlags`, `TestPBinSetStateRejectsForeignBlob`) go red
- [x] assert collation and merge preserve branch records byte-for-byte — `TestPBinM1ABranchRecordsSurviveCollationAndMerge`: latest records snapshotted from the db before collation must read back identically after `BuildFiles` + prune + `MergeLoop`, and again after a folder reopen. Non-vacuity is asserted, not assumed: every record is db-resident before collation, and a positive number (12 of 36) are gone from `TblCommitmentVals` afterwards, so their latest read can only come from the files
- [x] record in this plan that M1a has **no header-root oracle** and is not acceptance — stated in the file's package doc and here: nothing outside the engine validates these roots. Both rebuild arms and the restart arm are self-consistency checks over the same engine, so a green M1a means deterministic, not correct. H8 (stale high code chunks) is also out of reach until Task 12 puts code in the tree, which is where the forward-vs-rebuild comparison first gets a chance to fail for a real reason
- [x] run tests — `go test ./execution/commitment/... ./db/state/... -count=1` and `./execution/stagedsync/... -short` green, `go build ./...` clean, `make lint` clean twice

### Task 11: code_size on Update

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `execution/commitment/pbin_hash.go`
- Create: `execution/commitment/pbin_codesize_test.go`
- Create: `execution/commitment/commitmentdb/pbin_codesize_test.go` (➕ the read side)

- [x] write a failing test asserting BASIC_DATA for a code-bearing account carries the real `code_size`, checked against `basic_data_vectors` — `TestPBinBasicDataLeafCarriesCodeSize` drives `pbinLeafValue` over every vector; `TestPBinEngineRootCarriesCodeSize` then takes the size through the whole engine (context read → cell merge → leaf hash) and asserts a size-less variant of the same account roots differently. Both red before
- [x] add the `code_size` field to `Update` plus handling in `Reset`/`Copy`/`Merge`/`Encode`/`Decode`/`String` — `CodeSize uint64`, carried under the existing `CodeUpdate` flag at every hook (a size and a hash describe the same code, so a merge can never take one from the old account and the other from the new). Encode appends a varint inside the `CodeUpdate` block; ➕ `TestUpdate_EncodeDecode`/`TestUpdate_Merge` in `hex_patricia_hashed_test.go` gained the field, not in the planned file list
- [x] populate it at `TrieContext.Account` (`:1026-1070`) by reading `kv.CodeDomain` unconditionally — "unconditionally" in the sense that matters: the read no longer hides behind `dbg.AssertEnabled`. It is gated on `TrieContext.readCodeSize`, set from the variant at the one construction site the bin trie can reach (`trieContext`), so hex takes no extra domain read per code-bearing account. The warmup/concurrent factories are deliberately left alone: they need `paraTrieDB` and only ever serve page-cache warmup or a `*ParallelPatriciaHashed` fold, neither of which bin can reach
- [x] delete the wrong comment at `pbin_hash.go:138-139` and pass the real size instead of `0`
- [x] decide and test the cleared-7702-residue case explicitly — the existing benign-residue license no longer holds (guards H9) — **decision: code_size follows the account's own code hash, never CodeDomain presence.** A code-less account keeps code_size 0 whatever residue a cleared delegation left behind, so the tolerated inconsistency stays out of the root (`TestPBinTrieContextIgnoresClearedDelegationResidue`). The mirror case cannot be tolerated: a code-bearing account with no code behind it would hash as code_size 0 and produce a silently wrong root, so it errors (`TestPBinTrieContextRefusesCodeBearingAccountWithoutCode`). That is only reachable under bin — the overlay callers it would otherwise break (`eth_simulateV1`) are already refused by Task 7
- [x] write a test asserting the push side is inert for pbin, so nobody patches `calc_state.go` expecting code to arrive — `TestPBinPushSideNeverDeliversCode`: the bin variant overrides the requested mode to `ModeDirect`, and a `TouchCode` touch reaches `HashSort` as a nil update. Pinning test — it passes against current behaviour by design and fails if the push side ever starts carrying values
- [x] run tests — `./execution/commitment/... ./db/state/... ./execution/state/genesiswrite ./db/integrity` and `./execution/stagedsync/... -short` green, `go build ./...` clean, `make lint` clean twice. ➕ the tests for the read side live in `commitmentdb/pbin_codesize_test.go` (the trie context is in that package), not in the planned `execution/commitment/pbin_codesize_test.go`; the wiring test `TestPBinSharedDomainsReadsCodeSizeUnderBin` pins variant → read and is non-vacuous by mutation

### Task 12: chunkify_code and header code chunks

**Files:**
- Create: `execution/commitment/pbin_code.go`
- Modify: `execution/commitment/pbin_keys.go`
- Modify: `execution/commitment/pbin_hash.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Create: `execution/commitment/pbin_code_test.go`

- [ ] write failing tests for `pbinChunkifyCode` against `chunkify_vectors`, covering pushdata straddling a chunk boundary and a 7702 designator
- [ ] write a failing test for a batch touching both a header storage slot and code on one account, asserting monotonic visit order (guards H5)
- [ ] write a failing shortening-redeploy test comparing forward-run and rebuild roots (guards H8); if they differ, record Q2's answer before proceeding
- [ ] implement `pbinChunkifyCode` per eip:374-397 exactly — pad to 31 before the scan, carry residual pushdata across boundaries
- [ ] add `pbinCodeZone` and make the zone explicit at the three places a code key currently passes by accident (`pbin_keys.go:62-66`, `pbin_hash.go:132-148`, `pbin_hash.go:117-119`)
- [ ] emit header chunks 0..127 with a stem-exit flush or as their own sorted stream keys, never mid-fan-out
- [ ] run tests — must pass before task 13

### Task 13: CODE_ZONE overflow chunks

**Files:**
- Modify: `execution/commitment/pbin_keys.go`
- Modify: `execution/commitment/pbin_branch.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/pbin_specengine_test.go`
- Create: `execution/commitment/pbin_overflow_test.go`

- [ ] verify the `pbin_branch.go` record field-bit layout before designing the new field; record the layout in this plan
- [ ] write a failing test asserting `full_header_stem` reproduces through the **engine**, and empty the asserted exclusion list in `pbin_specengine_test.go`
- [ ] write a failing test asserting a code key never routes to the storage zone (guards H7)
- [ ] add a tag-discriminated third plain-key shape recognised by `pbinKeyHasher` and `updateCell`; never discriminate by length
- [ ] add a `pbinCellFields` bit carrying the 32-byte chunk value in the branch record, so no reverse lookup is needed (guards H11)
- [ ] extend `pbinDecodeCell` and `loadCellState` for the new shape
- [ ] run tests — all 7 engine vectors must pass before task 14

### Task 14: M1b gate — --chain=dev from genesis

**Files:**
- Create: `docs/pbin-m1b-smoke.md`

- [ ] verify genesis block 0 computes a binary root and the dev beacon accepts it
- [ ] run a local `--chain=dev` node to a few blocks, deploying and calling a contract
- [ ] verify a restart resumes at the same root
- [ ] record the observed genesis root and block roots in `docs/pbin-m1b-smoke.md` with the exact command line
- [ ] verify `integration commitment rebuild` on the resulting datadir reproduces the same roots
- [ ] run the package suite — must pass before task 15

### Task 15: Verify acceptance criteria

- [ ] verify every hazard H1–H12 has a named passing test or a structural assert
- [ ] verify all five open questions are answered and recorded, or explicitly deferred with ⚠️ and a reason
- [ ] verify only the three sanctioned API breaks were taken; `git diff --stat` shows no fourth
- [ ] verify every new package-level identifier carries the `pbin` prefix
- [ ] run `go test ./execution/commitment/... ./db/state/... -count=1`
- [ ] run `go build ./...` and `make lint` until clean
- [ ] verify the three `pbin_spec*_test.go` oracles pass under BLAKE3 with 7/7 engine vectors

### Task 16: [Final] Update documentation

- [ ] update the package doc comment on `pbin_patricia_hashed.go` to state BLAKE3, the M1 scope, and the stated limitations (no witness, no getProof, no parallel)
- [ ] update `CLAUDE.md` if new patterns were discovered
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Manual, external, or follow-on — no checkboxes*

**Upstream questions to raise:**
- Q1 (removed-account encoding) and Q2 (pure function of state vs history) are plausibly spec questions for EIP-8297, not just implementation ones. Q2 in particular determines whether recompute-from-domains is a legitimate oracle for any client.
- A root vector with a code-bearing account is absent from the exported reference vectors (all 8 BASIC_DATA leaves have `code_size = 0`, no zone `0x01` keys). EELS may already have one in its own suite; check before offering.

**Deliberately out of M1:**
- Access events / witness gas (EIP-4762 recalibration). Zero repo hits, and the EIP says `WITNESS_BRANCH_COST` is not yet fixed. The state root is computable without it.
- Cross-client devnet and EEST fixtures **as acceptance**. BLAKE3 buys reference-vector comparability and a geth `--chain=dev` diff, not consensus parity, because the gas rules do not exist. Debugging tool only.
- State expiry; parallel/streaming mounting for pbin (structurally excluded — `ModeParallel`'s prefix trie is nibble-based); witness / `eth_getProof` / `eth_simulateV1` / receipt regeneration under pbin (Task 8 makes them error — a stated limitation); mid-chain fork activation (no precedent, no state-format field in `chain.Config`, and a mid-chain switch would straddle step `.kv` files with no discriminator); referenced/squeezed commitment branches; real node deletion.

**Publishing:**
- `ethpandaops/eth-client-docker-image-builder` issue #398 tracks binary-trie branches per client; Erigon is unchecked. Its convention is a branch named literally `binary-trie`. An image is only worth building once Task 14 passes — before that it would produce a node that cannot sync. Nothing to publish from M0.
