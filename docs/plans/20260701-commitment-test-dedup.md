# Commitment test-package de-duplication

Branch: `awskii/commitment-test-dedup` (off #22099 `getwitness-blinded-storage`).

## Problem

~17k LOC of test code across ~50 files in `execution/commitment` (+ `trie`, `commitmentdb`,
`nibbles`). The duplication is lopsided: one setup pattern is hand-inlined ~50× despite a shared
helper already existing, and a second layer of already-existing helpers (parity oracles, key-wrap,
fixtures) is reimplemented inline in a handful of sites. Goal: shorten and de-duplicate the tests
**without losing any correctness check**. Test-only — no production code changes.

## Decisions (fixed — do not revisit)

- Delete dead/subsumed tests only when the specific case is still asserted by a surviving live
  test (not merely that the same code path is called). If not covered → leave the test untouched.
- Include the legacy `execution/commitment/trie` subpackage, **package-local** helpers only
  (different trie type; never reach for the commitment `MockState`/`HexPatriciaHashed` helpers).
- Helpers live in the existing testkit (`testutil_test.go`, `parallel_testkit_test.go`,
  `patricia_state_mock_test.go`) — no new test-infrastructure package.

## Correctness spine (must-keep, never weakened)

Every task is a mechanical route-through of code the tests already execute; no oracle changes.
Load-bearing invariants that MUST survive verbatim: golden roots (`Test_HexPatriciaHashed_Sepolia`;
trie-subpkg golden hashes; `keys_nibbles`/`nibbles`/`nibbles_v2` vectors); seq-per-key == single-batch
root; `DeferBranchUpdates`==eager (root **and** stored branch bytes); fuzz ModeDirect==ModeUpdate;
parallel/streaming/branch parity across worker sweeps; full-collapse→empty-trie root; deep-fold
`DeepLocalFolds`/`RefoldCount` probes; copy-on-write lifetime + keyArena pointer stability; witness
capture-reconstructs-root, #21810 exclusion shape, byHash-prune==RLPDecode-prune node set;
`VerifyBranchHashes` accept/reject.

**Tripwire blind spot:** the mid-stream-restore tests (`_AfterStateRestore`, `InTheMiddle`) assert
that an `EncodeCurrentState`/`SetState` restore does NOT change the final root — so losing their
restore hook keeps the root equal and passes silently. Never collapse a seq-loop that carries a
mid-iteration restore or intermediate-root assert (see Task 1 carve-out).

## Out of scope (do NOT do in this run)

- Table-driving the knob-only families (`ReplacePlainKeys*`, `CollectUpdate/CollectDeferredUpdate`,
  `VerifyBranchHashes`, the 9 `TrieDeleteSubtree_*`). Medium risk (easy to silently drop a per-row
  assertion) — separate reviewed pass. Do NOT attempt here.
- Any production (non-test) code change.
- Any new test-infrastructure package.
- `git push`, PR creation, or force operations.
- Adding any `t.Skip` (forbidden). Deleting a whole subsumed test is allowed; unskipping/folding a
  known-crash skip is not.

## Tasks

### Task 1: Promote the process helper and route the main file's seq-vs-batch tests
- [x] Move the trie-returning `processFreshTrie` from `witness_parity_test.go:30` to
      `testutil_test.go` (it returns `(*HexPatriciaHashed, root)` — a different shape than
      `processBatch`; move it, do NOT fold into `processBatch`); delete the co-located clone.
- [x] In `hex_patricia_hashed_test.go`, replace the hand-inlined
      `applyPlainUpdates → WrapKeyUpdates(ModeDirect,KeyToHexNibbleHash) → Process → Copy(root) → Close`
      seq/batch blocks with `processSeq`/`processBatch`, keeping every outer
      `require.Equal(rSeq,rBatch)` and golden assertion verbatim.
- [x] CARVE-OUT: leave hand-written any seq-loop containing `EncodeCurrentState`/`SetState`/`Reset`
      mid-iteration or a per-iteration/intermediate root assertion — specifically
      `Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentation_AfterStateRestore` (`:967`) and
      `..._InTheMiddle` (`:1045`).
- [x] Run `go test ./execution/commitment/...`; confirm `Test_HexPatriciaHashed_Sepolia` and all
      parity roots unchanged. Must pass before Task 2.

### Task 2: Route the remaining ModeDirect Process-ceremony sites
- [x] Route the same ceremony through `processSeq`/`processBatch`/`engineRoot` in
      `trie_reader_test.go`, `witness_prune_test.go`, `witness_tracer_test.go`,
      `witness_strict_test.go`, `additive_updates_test.go`, `deep_storage_test.go`
      (same carve-out as Task 1; keep every existing assertion verbatim).
      (`additive_updates_test.go` already routes its seq oracle through `sequentialRoot`→`engineRoot`;
      its parallel/streaming arms are additive double-touch assertions that cannot be collapsed.)
- [x] Run `go test ./execution/commitment/...` green. Must pass before Task 3.

### Task 3: Close existing-helper usage gaps (parity oracles, key-wrap, worker counts, aliases)
- [x] Replace the inline branch-map union/diff loops with `requireBranchParity` in
      `deep_storage_test.go` and `parallel_patricia_hashed_test.go`.
- [x] Build parallel `Updates` via `WrapKeyUpdates(ModeParallel, KeyToHexNibbleHash, …)` instead of
      hand-rolled `TouchPlainKey` closures at the parallel/streaming sites.
- [x] Use `benchWorkerCounts()` ONLY where the existing worker list is exactly `{1,4,8}` or
      `{1,4,8,NumCPU}`. Do NOT touch lists carrying other values — leave
      `TestVerifyParallel_RandomStorageIncremental` and `_StorageIncrementalDeletes` (`{1,2,4,8}`)
      as-is so the `workers=2` case survives.
- [x] Collapse only the `assertEquivalentRoot → assertEquivalentRootWorkers → requireRootParity`
      alias chain. KEEP the 2-line `sequentialRoot`/`parallelRoot` aliases (inlining 30+ sites is
      churn that increases LOC).
- [x] Run `go test ./execution/commitment/...` and `-race` on the parallel/streaming files green.
      Must pass before Task 4.

### Task 4: Route inline UpdateBuilder tables through the shared fixtures (root-sensitive)
- [x] Replace the copy-pasted ~18-key balance/storage tables with `fixtureBaseAccounts()` /
      `fixtureBaseWithCode()` in `UniqueRepresentation`, `DeferredBranchUpdates`, `AfterStateRestore`
      (chain small `.Balance`/`.Storage` deltas for follow-up rounds). Give `BrokenUniqueRepr` its
      own named fixture (dup-keys / no-codehash shape).
- [x] Verify byte-identical key set + order: `Test_HexPatriciaHashed_Sepolia` golden roots and all
      parity roots unchanged. If any root shifts, revert that substitution. Must pass before Task 5.

### Task 5: One random-corpus builder + shared witness corpus
- [x] Add `addRandomAccount(ub,rnd,slots)` (+ per-nibble variant) and a corpus-spec struct to
      `parallel_testkit_test.go`; refactor `buildWhaleCorpus`/`buildMixedCorpus`/`build100KAccountsCorpus`/
      `build500KStorageHeavyCorpus`/`buildClusteredStorageCorpus`/`buildWhaleStorageGroups`/`whaleByNibble`
      into thin count/seed specs. Seeds and `rnd.Read` draw order UNCHANGED → byte-identical corpora.
      (Added shared `addRandomSlot` primitive; `addRandomAccount`+/-1-balance nibble variant `addNibbleAccount`;
      `whaleOpts` remains the whale corpus-spec struct. `build100KAccountsCorpus` left minimal — no slot loop
      to share and its balance is `Uint64()` (no `+1`), so routing it would change the corpus.)
- [x] Add `buildWitnessCorpus(tb,ms,hph,accts,slots)`+`touchAccountsSlots` for the witness flavor;
      route the 5 witness accts×slots loops through it (`benchCapturedSuperset`, `BenchmarkBranchWitnessTotal`,
      `BenchmarkWitnessCapture`, `benchWitnessTrie`, `TestWitnessNodesForKeys_ByHashEquivalence`).
- [x] Run `go test ./execution/commitment/...` green (golden/parity roots are the drift tripwire).
      Must pass before Task 6.

### Task 6: newStreamingFixture + touchAll
- [ ] Promote `newStreamingFixture(t,keys,upds,workers[,scheduler])` (`*StreamingCommitter,*MockState`)
      and `touchAll(sc,keys)` to `parallel_testkit_test.go` (extract the `newCommitter` closure in
      `TestStreaming_FoldEagerPolicy`); adopt at the ~15 streaming parity sites. Callers keep their
      own scheduler / eager-lazy / `waitSchedulerIdle` wiring.
- [ ] Run `go test ./execution/commitment/...` and `-race` on `streaming_commitment_test.go` green.
      Must pass before Task 7.

### Task 7: Extract branch-encode / warmup / cell-equality helpers
- [ ] Add `encodeCellRow(tb,size)->(row,bm,enc)` to `testutil_test.go`; drop the
      `generateCellRow → NewBranchEncoder(1024) → generateCellEncodeDataRow → EncodeBranch → NoError`
      4-liner at its ~11 test + 3 bench sites.
- [ ] Add `testWarmuper(ctx,factory,workers)` filling the constant `MaxDepth:64`/`LogPrefix:"test"`/
      `Enabled:true`; promote `cellMustEqual` to `testutil_test.go` (+ a decoder-only
      `requireDecodedCellEq` variant that skips `hashedExtension`/`stateHash`).
- [ ] Run `go test ./execution/commitment/...` green. Must pass before Task 8.

### Task 8: trie subpackage cleanup (package-local helpers only)
- [ ] In package `trie` ONLY (never import the commitment helpers): normalize `New(common.Hash{})` →
      the existing `newEmpty()`; add `newCodeTrie(t)->(trie,rand,addr)` for the 11 `TestCodeNode*`
      prologues, `testAccount(nonce,balance,opts)` for the `accounts.Account{...}` literals, and
      `expectingCollector([]stepExpectation)` reproducing every hasHash/hasTree bitmask byte-for-byte.
- [ ] Run `go test ./execution/commitment/trie/...` green; trie golden hashes unchanged.
      Must pass before Task 9.

### Task 9: Gated deletions (only the three verified-subsumed)
- [ ] Delete `TestParallelDeleteWithSurvivingSiblings` (`parallel_patricia_hashed_test.go:574`) —
      subsumed by `_BranchInspection` (`:732`, byte-identical corpus at workers=4, asserts per-batch
      seq==par root + survival bitmap). Confirm `_BranchInspection` still passes.
- [ ] Delete one of the byte-identical pair `TestTrieDeleteSubtree_ValueNode_PartialMatch` (`:245`)
      / `TestTrieDeleteSubtree_DuoNode` (`:106`) — keep `_DuoNode`.
- [ ] Delete `TestEncodeKeyV2_RoundTrip` (`nibbles_v2_test.go:121`) — encode∘decode identity is
      covered by `TestEncodeKeyV2_Vectors`+`_DecodeKeyV2_Vectors` (same `v2Vectors`) + `FuzzEncodeDecodeKeyV2`.
- [ ] Do NOT touch: `Test_HexPatriciaHashed_StateRestoreAndContinue` skip (`:753`, case not covered),
      `TestPrintProof` (real assertions, sole `PrintProof` coverage), `#20961` skip, `-short` guards,
      env-gated trie-trace harness, and in-fuzz `t.Skip` input filters
      (`verify_test.go:405`, `hex_patricia_hashed_fuzz_test.go:45,178`).
- [ ] Run `go test ./execution/commitment/... ./execution/commitment/trie/...` green. Must pass before Task 10.

### Task 10: Acceptance verification
- [ ] `go test ./execution/commitment/... ./execution/commitment/trie/... ./execution/commitment/commitmentdb/... ./execution/commitment/nibbles/...` all pass.
- [ ] `-race` pass on the parallel/streaming/deep files; fuzz corpora (`-run '^Fuzz'`) pass.
- [ ] `make lint` clean (run repeatedly until stable).
- [ ] Confirm no production (non-test) file changed: `git diff --name-only <base> | grep -v '_test\.go$'` shows only `docs/plans/`.
