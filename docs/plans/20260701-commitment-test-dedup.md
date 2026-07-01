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

## Phases (each independently mergeable, suite stays green)

### Phase 1 — Adopt the Process helpers (Tier 1, low risk, ~260 LOC)
Route the ~50 reinlined `NewMockState → applyPlainUpdates → WrapKeyUpdates(ModeDirect) → Process →
Copy(root) → Close` sites through `processSeq`/`processBatch`/`engineRoot`. Keep every outer
`require.Equal(rSeq,rBatch)` and golden/parity assertion verbatim. Promote one canonical
`processInto`/`processFreshTrie` to `testutil_test.go`; delete the `witness_parity` clone.

### Phase 2 — Close existing-helper usage gaps (Tier 2, low risk, ~350 LOC)
- Replace inline branch-map union/diff loops with `requireBranchParity` (`deep_storage`,
  `parallel_patricia`).
- Build parallel `Updates` via `WrapKeyUpdates(ModeParallel)` instead of hand-rolled `TouchPlainKey`.
- Route inline 18-key UpdateBuilder tables through `fixtureBaseAccounts`/`fixtureBaseWithCode`
  (give `BrokenUniqueRepr` its own named fixture); recompute golden/parity root before/after.
- Use `benchWorkerCounts()` at every worker-list; inline + delete the thin alias layer
  (`assertEquivalentRoot→…→requireRootParity`, `sequential/parallelRoot`).

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

### Phase 5 — Deletions, gated by coverage verification (Tier 5, ~200 LOC)
For EACH candidate: (a) name the specific case it pins; (b) find a surviving test that exercises
that case with a real assertion; (c) delete only if (b) holds — else keep or fold the unique case.

| candidate | case to prove still-covered | expected verdict |
|---|---|---|
| `Test_HexPatriciaHashed_StateRestoreAndContinue` (skip, l.753) | mid-stream restore into 2nd trie → continue → same root | check `RestoreAndContinue` (889) + `_AfterStateRestore` (967) actually exercise it |
| `TestParallelDeleteWithSurvivingSiblings` | 2-batch delete root parity | `_BranchInspection` runs same corpus + bitmap → likely delete |
| `TestTrieDeleteSubtree_ValueNode_PartialMatch` | == `_DuoNode` byte-for-byte | delete one |
| `TestPrintProof` | asserts nothing (print-only) | delete (no assertion lost) |
| `TestEncodeKeyV2_RoundTrip` | encode∘decode identity | covered by `FuzzEncodeDecodeKeyV2` + Vectors → delete |

Keep: `#20961` skip, `-short` bloatnet/verify guards, env-gated trie-trace harness (legit).

## Out of scope (this run — deferred to a follow-up)

- **Table-driving the knob-only families** (`ReplacePlainKeys{/WithEmpty/PartialChange}`,
  `CollectUpdate/CollectDeferredUpdate`, `VerifyBranchHashes`, the 9 `TrieDeleteSubtree_*`).
  Medium risk (easy to silently drop a per-row assertion); handled as a separate reviewed pass,
  not by this autonomous run. Do NOT attempt it here.
- No production (non-test) code changes.
- No new test infrastructure package; extend the existing testkit.
- No behavior/coverage reduction — LOC drops from de-duplication, not from testing less.
- No `git push` / PR creation from this run.
