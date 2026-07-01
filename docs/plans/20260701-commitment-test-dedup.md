# Commitment test-package de-duplication

Branch: `awskii/commitment-test-refactor` (off #22099 `getwitness-blinded-storage`).

## Problem

~17k LOC of test code across ~50 files in `execution/commitment` (+ `trie`, `commitmentdb`, `nibbles`).
The duplication is lopsided: one setup pattern is hand-inlined ~50× despite a shared helper
already existing, and a second layer of already-existing helpers (parity oracles, key-wrap,
fixtures) is reimplemented inline in a handful of sites. Goal: shorten and de-duplicate the
tests **without losing any correctness check**.

## Decisions

- Delete dead/subsumed tests, but **per-candidate verify the specific case is still exercised by
  a surviving test that asserts it** — not merely that the same code path is called elsewhere.
- Include the legacy `execution/commitment/trie` subpackage, using **package-local** helpers only
  (different trie type; never reach for the commitment `MockState`/`HexPatriciaHashed` helpers).
- Defer medium-risk table-driving to a follow-up pass; land low-risk mechanical route-through first.
- Helpers live in the existing testkit (`testutil_test.go`, `parallel_testkit_test.go`,
  `patricia_state_mock_test.go`) — no new parallel test infrastructure.

## Correctness spine (must-keep, never weakened)

Every phase is a mechanical route-through of code the tests already execute; no oracle changes.
Load-bearing invariants that MUST survive verbatim:

- Golden roots: `Test_HexPatriciaHashed_Sepolia` (hardcoded roots); trie-subpkg golden hashes
  (`ShortNode` fa7297…, structural rootHash over the 100000-key set); `keys_nibbles` /
  `nibbles_v2` / `nibbles` golden vectors.
- Determinism: seq-per-key == single-batch root (`UniqueRepresentation`, `BrokenUniqueRepr`,
  `ProcessWithDozensOfStorageKeys`, `AfterStateRestore`, `InTheMiddle`).
- `DeferBranchUpdates` == eager, root **and** stored branch bytes (`DeferredBranchUpdates`,
  `requireDeferredMatchesEager`, fuzz).
- Fuzz: ModeDirect == ModeUpdate root; every root is `length.Hash`; ≤100k keys never error.
- Parallel/streaming/branch parity oracles (`requireRootParity`, `requireAllEnginesParity`,
  `requireBranchParity`) across worker sweeps; full-collapse → empty-trie root; deep-fold
  `DeepLocalFolds`/`RefoldCount` probes; copy-on-write lifetime + keyArena pointer stability.
- Witness: captured-set reconstructs root (memoizationOff); #21810 exclusion shape; byHash prune
  == RLPDecode prune node set; `VerifyBranchHashes` accept/reject.

Guard each phase: `go test ./execution/commitment/... ./execution/commitment/trie/...` green,
plus `-race` on the parallel/streaming files and the fuzz corpora, before merge. Golden + parity
roots are the tripwire for fixture/key-order drift.

**Tripwire blind spot:** the mid-stream-restore tests (`_AfterStateRestore`, `InTheMiddle`) assert
that an `EncodeCurrentState`/`SetState` restore does NOT change the final root — so losing their
restore hook keeps the root equal and passes silently. They are carved out of Phase 1; never
collapse a seq-loop that carries a mid-iteration restore or intermediate-root assert.

## Phases (each independently mergeable, suite stays green)

### Phase 1 — Adopt the Process helpers (Tier 1, low risk, ~260 LOC)
Route the ~50 reinlined `NewMockState → applyPlainUpdates → WrapKeyUpdates(ModeDirect) → Process →
Copy(root) → Close` sites through `processSeq`/`processBatch`/`engineRoot`. Keep every outer
`require.Equal(rSeq,rBatch)` and golden/parity assertion verbatim. Promote one canonical
trie-returning `processFreshTrie` (the `witness_parity_test.go:30` clone; it returns
`(*HexPatriciaHashed, root)` — a different shape than `processBatch`, so move it, don't fold it
into `processBatch`) to `testutil_test.go` and delete the co-located clone.

**Carve-out — do NOT route these:** any seq-loop that contains `EncodeCurrentState` / `SetState` /
`Reset` mid-iteration, or a per-iteration/intermediate root assertion. The restore hook is the
load-bearing part of the test and a plain `processSeq` still produces the same final
`rSeq==rBatch`, so the golden/parity tripwire **cannot** catch its silent loss. Concretely, leave
`Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentation_AfterStateRestore`
(`hex_patricia_hashed_test.go:967`, mid-loop restore) and
`..._InTheMiddle` (`:1045`, mid-loop restore + intermediate `somewhereRoot` assert) hand-written.

### Phase 2 — Close existing-helper usage gaps (Tier 2, low risk, ~350 LOC)
- Replace inline branch-map union/diff loops with `requireBranchParity` (`deep_storage`,
  `parallel_patricia`).
- Build parallel `Updates` via `WrapKeyUpdates(ModeParallel)` instead of hand-rolled `TouchPlainKey`.
- Route inline 18-key UpdateBuilder tables through `fixtureBaseAccounts`/`fixtureBaseWithCode`
  (give `BrokenUniqueRepr` its own named fixture); recompute golden/parity root before/after.
- Use `benchWorkerCounts()` **only** where the existing worker list is exactly `{1,4,8}` or
  `{1,4,8,NumCPU}`. Do NOT replace lists carrying other values: `TestVerifyParallel_RandomStorageIncremental`
  and `_StorageIncrementalDeletes` sweep `{1,2,4,8}` — leave those as-is so the `workers=2` parity
  case survives (`benchWorkerCounts` omits 2).
- Collapse only the `assertEquivalentRoot → assertEquivalentRootWorkers → requireRootParity` alias
  chain. **Keep** the 2-line `sequentialRoot`/`parallelRoot` aliases — inlining their 30+ call
  sites is churn that *increases* LOC.

### Phase 3 — New small shared helpers (Tier 3, low-med, ~600 LOC)
- `addRandomAccount(ub,rnd,slots)` + corpus-spec → collapse the 7 random-corpus builders;
  `buildWitnessCorpus`+`touchAccountsSlots` for the witness flavor (seeds unchanged → byte-identical corpora).
- `newStreamingFixture(t,keys,upds,workers)` + `touchAll(sc,keys)` (extract the `newCommitter`
  closure); callers keep their own scheduler/eager/lazy wiring.
- `encodeCellRow(tb,size)→(row,bm,enc)`; `testWarmuper(ctx,factory,workers)` (const MaxDepth/LogPrefix);
  promote `cellMustEqual` (+ decoder-only `requireDecodedCellEq`); `benchProcess` timing skeleton
  (preserve StopTimer/StartTimer boundaries — verify one before/after bench run).

### Phase 4 — trie subpkg, package-local (Tier 3, low-med, ~180-355 LOC)
Normalize `New(common.Hash{})` → `newEmpty()`. Add `newCodeTrie(t)→(trie,rand,addr)`,
`testAccount(nonce,balance,opts)`, `expectingCollector([]stepExpectation)` reproducing every
hasHash/hasTree bitmask byte-for-byte, `rebuildFromWitness(t,tr,rl)`. Package-local only.

### Phase 5 — Deletions, gated by coverage verification (Tier 5, ~120 LOC)
Gate for EACH candidate: (a) name the specific case it pins; (b) find a surviving test that
exercises that case **with a real assertion** (not merely calls the same code path); (c) delete
only if (b) holds. If (b) fails: **leave the test untouched** — do not delete, and never
fold/unskip/reactivate a pre-existing skip (folding a known-crash path is a test an autonomous
agent cannot legally recover from, since adding a skip is forbidden).

Only these three are cleared for deletion (each verified truly subsumed):

| candidate | case | surviving coverage (verified) |
|---|---|---|
| `TestParallelDeleteWithSurvivingSiblings` (`:574`) | 2-batch delete root parity | `_BranchInspection` (`:732`) runs the byte-identical corpus at `workers=4`, asserts per-batch `seqRoot==parRoot` + a root-branch survival bitmap → delete |
| `TestTrieDeleteSubtree_ValueNode_PartialMatch` (`:245`) | delete-subtree present/absent | byte-for-byte identical to `_DuoNode` (`:106`) → delete one |
| `TestEncodeKeyV2_RoundTrip` (`:121`) | encode∘decode == identity | `TestEncodeKeyV2_Vectors`+`_DecodeKeyV2_Vectors` (same `v2Vectors`) + `FuzzEncodeDecodeKeyV2` → delete |

Explicitly NOT deleted (failed the gate on review):
- `Test_HexPatriciaHashed_StateRestoreAndContinue` (skip, `:753`) — its unique case (restore into a
  **second, separately-backed** MockState → continue → compare roots) is NOT asserted by any live
  test; it is also the known-crash path it documents. Leave the pre-existing skip untouched.
- `TestPrintProof` (`trie/proof_test.go:17`) — NOT print-only: it asserts `require.NoError` on the
  JSON unmarshal and on `PrintProof` for the account + each storage proof, and is the ONLY test of
  production `PrintProof` (`trie/proof.go:650`). Keep it.

Keep (do not touch): `#20961` skip (`:1692`), `-short` guards (bloatnet `:652`, verify `:373`),
env-gated trie-trace harness (`:598`), and the **in-fuzz `t.Skip` input filters**
(`verify_test.go:405` oversized-batch, `hex_patricia_hashed_fuzz_test.go:45,178` degenerate-input)
— these are runtime input filters, not muted tests.

## Out of scope (this run — deferred to a follow-up)

- **Table-driving the knob-only families** (`ReplacePlainKeys{/WithEmpty/PartialChange}`,
  `CollectUpdate/CollectDeferredUpdate`, `VerifyBranchHashes`, the 9 `TrieDeleteSubtree_*`).
  Medium risk (easy to silently drop a per-row assertion); handled as a separate reviewed pass,
  not by this autonomous run. Do NOT attempt it here.
- No production (non-test) code changes.
- No new test infrastructure package; extend the existing testkit.
- No behavior/coverage reduction — LOC drops from de-duplication, not from testing less.
- No `git push` / PR creation from this run.
