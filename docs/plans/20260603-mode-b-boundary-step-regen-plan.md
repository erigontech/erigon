# Mode-B boundary-step commitment file regeneration — plan

Author: live-verify session 2026-06-03
Status: planning (no code yet)

## Problem

Mode-B unwind to block N (=2,910,208 on the live cycle that surfaced this)
leaves the boundary-step commitment file (e.g. `v2.0-commitment.264-266.kv`)
on disk unchanged, but with `KeyCommitmentState` records that reflect chain
state at the file's max txNum (= block 2,912,403 in the live cycle). When
`SeekCommitment` reads `KeyCommitmentState` via `trieContext.Branch(...)`
it sees the file's record (txNum=103,906,249) winning over mode-B's
writable-shadow anchor (txNum=103,848,485) because Branch returns
"highest-txNum entry wins" and there's no `AsOf` cap applied to that key.

Result: post-mode-B SeekCommitment returns block 2,912,403 instead of
2,910,208. `TxNums.Last(tx)=2,910,208 < 2,912,403` → `ErrBehindCommitment`
→ engineapi catch-up downloader hits `ExecutionStatusTooFarAway` → blocks
get downloaded but never executed → chain wedges at the unwind target
indefinitely.

This is bug #5 from the 2026-06-03 live-verify session. Bugs #1-#4 are
landed or filed separately:
- #1 storage-flow orchestrator race (committed 9198875f91)
- #2 seedLeftoverBlocks system-tx-empty crash (committed 92a3d4c154)
- #3 newToBlock<=straddle.From skips trim+rebuild (committed 04c568a71d)
- #4 AllSnapshots.OpenFolder doesn't drop mmaps for deleted files (open)

## Why option (C) full regeneration is the right shape

Two cheaper options were considered and rejected as workarounds:

- **(A) Cap `Branch(KeyCommitmentState)` reads at `TxNums.Last(tx)`.** Hides
  the symptom but leaves the file on disk in a state that doesn't match
  the chain. Any future code path that reads the file without the cap
  (or computes a hash over file contents, or treats the file as
  authoritative for sync) sees a chain at block 2,912,403, not the actual
  head 2,910,208.

- **(B) Mode-B writes the anchor at an effective-txNum past the file's
  max.** Lies about txNum to win the read. Leaves the writable shadow
  with a `KeyCommitmentState` entry at a txNum past `TxNums.Last`, which
  is a per-key invariant violation that other code may or may not
  tolerate cleanly. Same "doesn't match the chain" problem.

Both leave the **on-disk state inconsistent with the unwind target**.
That's the test: after mode-B, the snapshot files should reflect the
chain at toBlock, period. Anything else is hiding a problem rather than
fixing it.

(C) makes the on-disk file actually match the chain. Same shape as block
straddle rebuild (3e7136fd02, 04c568a71d) but for state-domain files:
rewrite content + rebuild accessors + replace atomically.

## Connection to diffset replacement (user pin, 2026-06-03)

Diffsets today are the per-block state-change ledger that `Aggregator.Unwind`
replays to roll the writable shadow back N blocks. They're a per-block
inverted index — small, but they grow without bound and need their own
sync/replication path.

The proposed replacement: at unwind time, reconstruct the writable-shadow
state at the unwind target by replaying **history files** (.v) — which we
already have, distribute, and verify. The reconstruction primitive is the
same one this plan introduces: given a target txNum, regenerate a domain's
state file to reflect the values valid at that txNum.

Mode-B's boundary-step regeneration is the **forcing function** for that
primitive. Once the primitive exists for the boundary step, generalizing
to "any txNum, any depth" is straightforward and obsoletes diffsets.

This plan deliberately scopes to **only mode-B's needs** (one txNum, the
boundary step, the existing trim/rebuild lifecycle). The generalization
is left for the diffset-replacement work.

## Design

### Inputs and invariants

Pre-conditions when the regeneration runs (inside mode-B's
`Provider.Unwind`, after `unwindDBPastBlock` and before
`commitment-anchor applied`):

- `toBlock` — the unwind target.
- `lastTxNum = rawdbv3.TxNums.Max(tx, toBlock)` — already computed.
- `stepBoundary = (lastTxNum / stepSize) + 1` — already computed.
- For every domain (accounts / storage / code / commitment / receipt /
  rcache / logaddrs / logtopics / tracefrom / traceto / gas):
  - All snapshot files with `ToStep > stepBoundary` are already in the
    pendingTrim set (existing `collectFilesPastBlock` covers this).
  - The boundary-step file (`FromStep < stepBoundary && ToStep ==
    stepBoundary`) is on disk, indexed, advertisable. **NOT** yet in
    pendingTrim.
- `WipeWritableShadowPast(tx, lastTxNum)` has run — writable shadow has
  no entries past lastTxNum.

Post-condition the regeneration must establish:

- For each domain's boundary-step file: every entry (k, v) in the
  regenerated file is the value of k as-of lastTxNum, derived from
  history (.v / .ef) files.
- Specifically for the commitment domain's `KeyCommitmentState`: encoded
  as `(blockNum=toBlock, txNum=lastTxNum, trieState=encodedTrieAt(toBlock))`.
- All accessor files (.kvi, .bt, .kvei, .efi per domain) rebuilt against
  the regenerated .kv.
- Old boundary-step files + their accessors staged for FinalizeUnwind
  deletion.
- AllSnapshots view refreshed so reads see the regenerated files.

### The primitive

```go
// RegenerateBoundaryStepFile regenerates a single domain's boundary-step
// snapshot file to reflect the state of every key in the file as-of
// lastTxNum, using the domain's history files.
//
// The regenerated file has the same path (overwrites atomically via
// .tmp + rename) and the same step coordinates. Only the entries
// (and KeyCommitmentState for the commitment domain) change.
//
// Pre: writable shadow is wiped past lastTxNum.
// Pre: history files for this domain cover lastTxNum.
//
// Returns the new FileInfo and the rebuilt accessor paths (for
// AbortUnwind cleanup if mode-B's tx rolls back).
func RegenerateBoundaryStepFile(
    ctx context.Context,
    agg *Aggregator,
    domain kv.Domain,
    boundaryStepFile snaptype.FileInfo,
    toBlock, lastTxNum uint64,
    // Commitment-only: the encoded trie state for KeyCommitmentState.
    // nil for non-commitment domains.
    commitmentAnchorTrieState []byte,
    snapDir, tmpDir string,
    logger log.Logger,
) (newFI snaptype.FileInfo, accessorPaths []string, err error)
```

Implementation outline (per-domain):

1. Open the existing boundary-step .kv file via seg.Decompressor.
2. Walk every (k, v) entry. For each key k:
   - Resolve the value of k as-of lastTxNum:
     - Use `AggregatorRoTx.HistorySeek(domain, k, lastTxNum)` to find
       the latest value at or before lastTxNum (the standard erigon
       AsOf primitive — already wired for forward reads).
     - If the result is "no value at this txNum" (k was created post-
       lastTxNum), drop k from the output.
     - Otherwise emit (k, valueAtLastTxNum) to the output stream.
   - **Commitment-domain `KeyCommitmentState` special case**: replace
     with `commitmentdb.NewCommitmentState(lastTxNum, toBlock,
     trieState).Encode()`.
3. Sort the output stream by key (boundary-step files are key-ordered;
   reuse `seg.Compressor` with the same compression config as the
   original).
4. Write to `<originalPath>.tmp` first, then `os.Rename` to the original
   path. (Atomicity across the mode-B tx is best-effort; the rename is
   a single syscall, and AbortUnwind unwinds it by re-writing the
   original from a backup or by re-running regeneration from history.)
5. Call `SnapType.BuildIndexes(newFI, ...)` to rebuild accessors. Each
   domain has its own accessor set (`Domain.GetIndexesUsage()` enumerates
   them — kvi / bt / kvei).
6. Return new FileInfo + accessor paths.

The orchestrator calls this per-domain inside the storage component,
collects all new FileInfo + accessor paths, stages everything for
FinalizeUnwind (or AbortUnwind).

### Replacing the commitment-anchor write

The existing `commitment-anchor applied` step writes
`KeyCommitmentState` to the **writable shadow** at lastTxNum. Once
regeneration runs, the boundary-step **file** has the correct
`KeyCommitmentState` baked in. The writable-shadow anchor write becomes:

- Either kept (writable shadow's entry at lastTxNum is the same blob
  the regenerated file holds at the same txNum — redundant but harmless).
- Or dropped (the file's record is sufficient; SeekCommitment finds it).

Pick at implementation time based on which is simpler given the
domain-write API.

### In-DB unwind needs its own consistency check (user pin, 2026-06-03)

The file regeneration above is one half of mode-B's post-state. The
other half is the in-DB unwind — the writable-shadow wipe + per-table
truncations (`unwindDBPastBlock`, `WipeWritableShadowPast`,
`TxNums.Truncate`). These are LESS catastrophic if mis-done than a full
file delete (we're cleaning rows, not removing an entire backing store),
but they share the same contract: after mode-B, the DB image must
reflect the chain at toBlock, period. No rows for blocks > toBlock in
any table. No writable-shadow entries for txNum > lastTxNum. Stage
progress entries pointing at toBlock.

Today nothing actually VERIFIES this post-state. The unwind sub-ops are
trusted to be correct; if any one of them silently misses a table, the
inconsistency survives and surfaces hours later as a wrong-block-data
or wrong-state-root error. This is exactly the failure mode bug #1
(orphan-row wipe gap → `firstNonGenesisCheck` wedge) and bug #5
(writable-shadow anchor shadowed by file's record) both lived in
before live-rig caught them.

Add a post-unwind DB-image verifier that, after every mode-B
`Provider.Unwind` and before commit:

- `rawdbv3.TxNums.Last(tx)` returns toBlock.
- The "max blockNum" cursor walks of `kv.Headers`, `kv.BlockBody`,
  `kv.HeaderTD`, `kv.Senders`, `kv.HeaderNumber` all return toBlock
  (or empty, if the table has no entries below toBlock either).
- `kv.EthTx`'s last key decodes to a txnID ≤ lastTxNum.
- Stage progress for `Execution`, `Headers`, `Bodies`, `Senders`,
  `TxLookup`, `Finish` all = toBlock.
- Writable-shadow's per-domain "tip" cursor ≤ lastTxNum.

If any check fails, return an error from `Provider.Unwind` (rolls back
the tx via AbortUnwind). This is **same-tx, not best-effort** — DB
inconsistency post-unwind is unrecoverable without a restart-with-wipe
otherwise.

For the file side, the symmetric check is: after every regen + trim,
no snapshot file contains entries past lastTxNum. That's a Phase 4
spot-check today (cheap to verify out of band); promoting to a
same-call assertion would require reading the file content, which is
expensive. Defer until soak surfaces a need.

### All domains, unconditionally — clean state is the point

The setHead RPC exists to bring the chain to a known good point. Anything
the unwind leaves behind that doesn't reflect that target defeats the
contract — even if specific read paths happen to mask it. Any future
code path that:

- computes a content hash over a snapshot file (advertisement, integrity
  check, peer manifest verify),
- iterates the file directly (validators, integrity tooling, debug
  commands),
- treats the file as the authoritative tip of a domain (sync, federation,
  history-network),

will see the stale post-anchor entries and either disagree with the
chain's actual head, fail an integrity invariant, or propagate the stale
state to peers. The "AsOf hides it from forward exec" property is a
read-path-only accident, not a contract.

So: regenerate the boundary-step file for **every domain** that has one
past the boundary step's content frontier. Accounts, storage, code,
commitment, receipt, rcache, logaddrs, logtopics, tracefrom, traceto, gas.
Same shape per domain — the only domain-specific bit is commitment's
`KeyCommitmentState` injection.

### Crash safety

Mode-B's tx wraps the DB-side work (TxNums truncate, kv.Headers wipe,
etc.). The snapshot-file regeneration happens in the same tx window
but writes to disk outside the tx. Existing pendingTrim / pendingRebuild
+ AbortUnwind already handle this for block straddles.

For state-domain regeneration:
- New files are written to `<original>.tmp` paths. AbortUnwind deletes
  them.
- The `os.Rename` to the final path is the commit point. If the rename
  succeeds and then the mode-B tx commits, we're consistent. If the
  rename succeeds and the tx rolls back, AbortUnwind must rename the
  ORIGINAL back from a backup or re-execute regeneration.
- Simpler: keep the original file untouched until FinalizeUnwind. Write
  `<original>.new`. In FinalizeUnwind: rename `<original> →
  <original>.old`, rename `<original>.new → <original>`, then delete
  `<original>.old`. AbortUnwind: just delete `<original>.new`.

This is the same atomicity pattern existing block-straddle rebuild
already uses with `pendingRebuild`.

### Interaction with bug #4 (stale mmap)

When the boundary-step file is replaced atomically, the **inode changes**.
Existing process mmaps of the old inode survive (Linux semantics) and
continue serving the old content until `AllSnapshots.OpenFolder` releases
them.

This is exactly bug #4. Fixing #4 (force mmap close on file replacement)
benefits BOTH the existing block-straddle rebuild AND the new state-domain
regeneration. Treat #4 as a hard prerequisite — without it, post-regen
reads serve stale data until process restart.

The minimum #4 fix: when FinalizeUnwind detects a replaced/deleted file,
it must explicitly release the mmap before the rename/unlink, and re-open
post-rename. Erigon's existing `AllSnapshots.Reopen()` / `OpenFolder()` API
needs verification that it actually releases the old fd; if it doesn't,
that's a focused fix in the snapshot subsystem.

## Implementation phases

### Phase 0 — DB-image consistency check (independent, can run in parallel)

Hard same-tx assertion at the end of `Provider.Unwind` per the
"In-DB unwind needs its own consistency check" section. Catches today's
known DB-wipe gaps (like bug #1 was) and any future ones at the
moment they happen rather than via downstream wedge.

- New function `verifyPostUnwindDBImage(ctx, tx, toBlock, lastTxNum)`
  returning an error per failed check.
- Wired in `Provider.Unwind` after all sub-ops, before the tx commits.
- Unit test feeding intentionally-incomplete unwinds and verifying
  each check fires.

This phase is INDEPENDENT of the file regeneration work — it can land
first and immediately strengthen mode-B's existing path. The file
regen below benefits from it too (lets us detect drift between the
regenerated files and the DB state).

### Phase 1 — Bug #4 fix (hard prerequisite for file regen)

Atomic file replacement only matters if the running process actually
drops the old mmap. Today it doesn't (live-rig 2026-06-03: deleted
straddle files served stale blocks via `(deleted)` mmaps until process
restart). Without this fix, regeneration succeeds on disk but the
process keeps serving the pre-regen content until restart — every
mode-B becomes "restart required to take effect", which is not what
the user contract says.

- Trace `AllSnapshots.OpenFolder()` to find where mmaps are
  acquired/released.
- Add explicit `munmap` + fd close on file replacement (path: when a
  FileEntry transitions from Local → not-Local in the inventory, OR
  when a same-named file's inode changes).
- Verify with `/proc/<pid>/maps` post-replacement that no `(deleted)`
  entries remain for trimmed/regenerated files.

### Phase 2 — primitive

Implement `RegenerateBoundaryStepFile` per the design above.

- Unit test (per domain — at minimum commitment, accounts, storage):
  synthesize a boundary-step .kv with entries at varying txNums (some
  past lastTxNum), feed a fake history seek, assert the regenerated
  file contains only entries as-of lastTxNum, asserts
  `KeyCommitmentState` matches the injected anchor for the commitment
  domain.
- The primitive is per-file. The orchestrator (Phase 3) decides how
  many files to regenerate.

### Phase 3 — wire into mode-B

- Walk every domain. For each: identify the boundary-step file (the
  file whose `FromStep < stepBoundary && ToStep == stepBoundary`).
  Some domains may not have one (file already trimmed, or never had a
  file at that step yet).
- For each boundary-step file: call `RegenerateBoundaryStepFile` with
  the appropriate inputs (anchor trieState for commitment, nil for the
  rest).
- Stage new files + old files + accessor paths via pendingRebuild /
  pendingTrim.
- FinalizeUnwind handles the atomic rename + inventory refresh + mmap
  release (via Phase 1 fix).
- The existing `commitment-anchor applied` write to the writable
  shadow becomes redundant or wrong (the regenerated file holds it):
  drop it.

### Phase 4 — live verify

- Single-process mode-B + forward exec on hoodi (the scenario this
  plan exists to fix).
- Restart + forward exec.
- Hand-picked setHead targets covering: aligned cut, non-aligned,
  deep unwind (crosses step boundary), shallow unwind (same step),
  sequential unwinds without forward exec between.
- Verify on `/proc/<pid>/maps` that no `(deleted)` entries linger.
- Verify on disk that NO file contains entries past the unwind target's
  lastTxNum (random spot-check via `mdbx_dump`-equivalent on the .kv
  files).

### Phase 5 — soak (the final gate)

- 50-iteration randomized soak per user's spec.
- Mix of single-process and restart scenarios.
- Random targets in `[snapshot_tip - 5000, snapshot_tip]`, weighted to
  exercise: aligned cuts, non-aligned cuts, same-step unwinds,
  cross-step unwinds, near-genesis (unlikely but tests boundary), and
  near-tip.
- Pass criterion: every iteration produces clean post-state — head
  matches target, no stale files, forward exec resumes, no
  `ErrBehindCommitment`, no `(deleted)` mmaps in /proc, no
  inventory/disk drift.

## Open questions for implementation

1. Is there an existing per-file regeneration primitive in
   `db/state/squeeze.go` that can be reused? `SqueezeCommitmentFiles`
   regenerates the whole commitment-domain file set (too coarse) but
   may share helpers. `RebuildCommitmentFilesWithHistory` is the
   closer analog — reuse what we can.
2. What's the commitment-domain trieState at a specific txNum? Is
   there a primitive to materialize the patricia trie at an arbitrary
   txNum from history, or do we need to rerun forward from the
   previous commitment anchor? `RebuildCommitmentFilesWithHistory`
   does this; pull the same approach.
3. The `os.Rename` atomicity vs mode-B tx commit ordering — confirm
   the pendingTrim/pendingRebuild + FinalizeUnwind dance works for
   state-domain files too (it already works for block-snapshot files).
4. Per-domain regeneration cost. The boundary-step .kv can be hundreds
   of MB. Regenerating + rebuilding accessors for ~11 domains per
   mode-B may be slow. If the 50-iter soak shows it, parallelize
   per-domain (they're independent) or batch.
5. Does this need to extend to the snapshot **adjacent** to the
   boundary step (i.e., file whose ToStep == FromStep of the
   boundary-step file)? Probably not — its content is wholly below
   lastTxNum by construction. Spot-check in Phase 4.
6. Pruning interaction: under `--prune.mode=minimal`, history files
   below the prune horizon are absent. If mode-B's lastTxNum is below
   that horizon, history seek can't materialize the as-of value for
   some keys. Need to either reject the unwind (mode-B-can't-go-here),
   or treat such keys as "drop from file" (lossy). Decide.

## Not in scope

- Diffset replacement at large. This plan deliberately stops at "what
  mode-B needs". The generalization is a separate workstream.
- Caplin / consensus-layer state. Mode-B is EL-only; caplin lives on
  its own slot axis.

## Files likely touched

- `node/components/storage/provider_unwind_commitment.go` — call site
  for the new regen.
- `node/components/storage/provider_unwind_state_regen.go` — NEW file,
  the regeneration logic.
- `node/components/storage/provider_unwind_state_regen_test.go` — NEW
  test file.
- `db/state/squeeze.go` — possible helper extraction.
- `db/state/aggregator.go` — possibly expose a "regenerate single file"
  entry point if needed.
- `node/components/storage/snapshot/inventory.go` — if FileEntry needs
  new fields for the regen-staging dance.
- `node/components/storage/provider_unwind_finalize.go` — extend
  FinalizeUnwind / AbortUnwind for state-domain regen ops.
