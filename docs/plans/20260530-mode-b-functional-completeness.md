# Mode B — functional completeness

This plan closes G3.15 and the related boundary-step coverage gaps so
mode-B unwind can pass T3.A2 (single setHead cycle against a real
chain with the chain advancing past the new head). It supersedes the
incremental "fix-as-you-find-it" approach for the post-unwind state
surface; the issues are coupled and benefit from one coherent change.

## Background

The current `WipeWritableShadowPast` calls a per-domain diff-replay
that pulls as-of-`lastTxNum` values from history and writes them back
to the boundary step of the writable shadow. This works for
accounts/storage/code (history-tracked domains). It does NOT work for:

1. **Commitment branches** — `HistoryDisabled` means
   `HistoryKeyTxNumRange(CommitmentDomain)` returns zero keys;
   diff-replay is a no-op. Forward-exec branches in the boundary step
   stay in shadow, get read by post-unwind exec, produce wrong trie
   reads, cause consensus-divergent gas/state.
2. **Receipts** — `ReceiptDomain` isn't in the diff-replay's domain
   list at all. Post-target receipts persist in shadow, corrupting RPC
   reads and any consensus check that walks them.
3. **History tables within the boundary step** — `dt.ht.prune(lastTxNum+1, …)`
   is called per domain, but needs to be verified that it actually
   reaches boundary-step history records (vs. having a step-granular
   shortcut that skips them).

Symptom in production: hoodi probe of mode B succeeds (recomputed
root matches header), but the very next forward-exec block diverges
with a gas mismatch ("gas used by execution: 4343737, in header:
4432037"). Confirmed by live diagnostic
([commit bf60e88073](../../execution/commitment/commitmentdb/recompute_sdless.go))
that BEACON_ROOTS (EIP-4788) keys ARE touched by the existing
diff-replay (281 unique storage contracts; ruled out as a missing
class), and that commitment domain returns 0 touched keys (the actual
gap).

## Design — the new sub-op order

The current chain inside `Provider.Unwind` (production order):

```
1. ensureCommitmentAtBlock      (recompute + write KeyCommitmentState at lastTxNum)
2. unwindSnapshotsPastBlock     (snapshot-trim, deferred to post-commit)
3. unwindDBPastBlock            (TxNums, canonicalHash, head pointers, deleteChangeSets)
   └── WipeWritableShadowPast   (per-domain wipe + boundary-step diff-replay)
```

New chain:

```
1. ensureCommitmentAtBlock_compute    (run recompute primitive, hold result in memory; NO writes yet)
2. unwindSnapshotsPastBlock           (snapshot-trim, deferred to post-commit; unchanged)
3. unwindDBPastBlock_truncate         (TxNums, canonicalHash, head pointers, deleteChangeSets)
4. WipeWritableShadowPast_extended
   ├── wipe step > stepBoundary, all domains incl. commitment + receipts
   ├── wipe step == stepBoundary commitment entries (whole-step wipe just for commitment)
   ├── wipe step == stepBoundary receipt entries past lastTxNum (per-txnum if Receipt has history)
   ├── boundary-step diff-replay for accounts/storage/code (existing path)
   └── history.prune past lastTxNum, all domains
5. ensureCommitmentAtBlock_apply      (write the recompute's branches + KeyCommitmentState to shadow, all at txnum=lastTxNum)
```

Key changes:

- **Compute then write**: the recompute runs first to capture the
  trie state in memory, but doesn't write until AFTER the wipe. This
  guarantees no stale commitment branches survive to corrupt the trie
  the next exec sees.
- **Commitment wipe is whole-step at boundary**: commitment has no
  per-txnum diff-replay possibility (HistoryDisabled), so wipe all
  step=stepContaining entries. The recompute's branch writes
  repopulate them.
- **Receipts MUST be covered**: ReceiptDomain's config shows
  `HistoryLargeValues: false` + `HistoryIdx: kv.ReceiptHistoryIdx`
  — looks history-tracked. **Verify this is actually enabled** (no
  `HistoryDisabled: true` flag set anywhere overriding). If it IS
  enabled, add `ReceiptDomain` to the diff-replay's domain list. If
  for some reason it is NOT enabled today, ENABLE IT — receipts must
  be coverable by the boundary-step wipe; without that, RPC
  receipt-by-hash returns post-target data and any consensus check
  that walks receipts diverges. Receipts cannot be left to the
  "whole-step wipe like commitment" fallback because their values
  contain data (logs, gas-used, status) that exec doesn't naturally
  regenerate the same way it does commitment branches.
- **History prune timing unchanged**: still runs as part of the
  extended wipe, with the same `txFrom=lastTxNum+1, txTo=MaxUint64`.

## Implementation breakdown

### 1. Recompute primitive: split compute from apply

`RecomputeAtTxNumWithoutSD` today does compute. It returns
`(root, encodedTrieState, baselineTxNum, error)`. The single-result
shape is incompatible with the new sub-op order: we need to capture
branches now and write them later.

**Change all callers** (not a thin-wrapper preservation):
- The signature gains a branch collector as a return value.
- Callers update to drain the collector at the right point (mode B's
  apply phase; fork-from CLI's equivalent point in its own datadir).

API shape:

```go
// RecomputeAtTxNumWithoutSD returns the recompute's outputs PLUS a
// branch collector. Branches are not written to the DB by this
// call; the caller flushes them via a separate Apply step after any
// necessary wipe.
func RecomputeAtTxNumWithoutSD(
    ctx, tx, tmpDir, toTxNum, maxStep, stepSize,
) (root []byte, encodedTrieState []byte, baselineTxNum uint64,
   branches *etl.Collector, err error)
```

Callers to update:
- `node/components/storage/provider_unwind_commitment.go::ensureCommitmentAtBlock` — split into compute + apply phases.
- Fork-from CLI's equivalent commitment anchor (search for
  `RecomputeAtTxNumWithoutSD` usage; update its drain point).

Mechanics inside: TrieContext gains a writable putter that delegates
to the collector. PutBranch (the existing extensibility point) goes
from no-op to "Collect(prefix, branchData)" when the collector is
non-nil.

### 2. ensureCommitmentAtBlock: split compute / verify / apply

```go
func (p *Provider) ensureCommitmentAtBlock_compute(...) (*commitmentRecomputeResult, error) {
    // runs RecomputeAtTxNumWithoutSDWithBranches
    // validates root vs header.Root
    // returns the result struct (root, encodedState, baselineTxNum, branches collector)
    // NO writes to MDBX
}

func (p *Provider) ensureCommitmentAtBlock_apply(tx, result) error {
    // drains the branches collector, writes each branch via the
    // domain-buffered-writer flush path (so DupSort step replacement
    // semantics work correctly)
    // writes KeyCommitmentState at lastTxNum
    // closes the collector
}
```

### 3. WipeWritableShadowPast: extend for commitment + receipts

```go
// Existing diff-replay loop (accounts/storage/code) unchanged, BUT
// extended to include ReceiptDomain if it's history-tracked.
diffReplayDomains := []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain}
if isHistoryTracked(kv.ReceiptDomain) {
    diffReplayDomains = append(diffReplayDomains, kv.ReceiptDomain)
}

// Per-domain wipe (step > stepBoundary) — unchanged contract:
for _, name := range allWritableDomains {  // includes commitment + receipts
    if err := wipeDomainValuesPastStep(tx, a.d[name], stepBoundary); err != nil { ... }
    if err := historyPrune(...); err != nil { ... }
}

// NEW: whole-step wipe at stepContaining for non-diff-replayable
// domains (commitment, possibly receipts if HistoryDisabled).
for _, name := range nonHistoryDomains {
    if err := wipeDomainValuesAtStep(tx, a.d[name], stepContaining); err != nil { ... }
}

// Boundary-step diff-replay for history-tracked domains (existing).
for _, p := range plans { applyReplay(...) }
```

`wipeDomainValuesAtStep` is a new helper (parallel to
`wipeDomainValuesPastStep`) that wipes entries where step EQUALS the
given step. Same cursor-walk shape, just an equality check instead of
`>=`.

### 4. setHeadModeB / Provider.Unwind: reorder sub-ops

```go
func (p *Provider) Unwind(ctx, toBlock, opts) error {
    // 1. Compute the recompute result (no writes)
    result, err := p.ensureCommitmentAtBlock_compute(ctx, opts.Tx, toBlock)
    if err != nil { return err }

    // 2. Snapshot-trim (staged for post-commit, as today)
    if err := p.unwindSnapshotsPastBlock(...); err != nil { return err }

    // 3. DB truncation (TxNums, canonicalHash, head pointers, changesets)
    if err := p.unwindDBPastBlock_truncate(...); err != nil { return err }

    // 4. Wipe writable shadow, extended for commitment + receipts
    if err := p.Aggregator.WipeWritableShadowPastExtended(ctx, opts.Tx, lastTxNum); err != nil { return err }

    // 5. Apply the recompute result (writes branches + KeyCommitmentState)
    if err := p.ensureCommitmentAtBlock_apply(opts.Tx, result); err != nil { return err }

    return nil
}
```

### 5. Tests

Two layers:

**Unit (db/state)**:
- Extend `TestWipeWritableShadowPast_*` suite with a test that
  populates commitment branches at boundary step (via SD.ComputeCommitment
  during phase 2), runs the extended wipe, then asserts post-wipe
  shadow has NO commitment branches at the boundary step (or only the
  ones we wrote via the apply path).
- Similar test for receipts.

**Integration (rpc/jsonrpc)**:
- Re-enable the G3.15 assertion in the three mode-B e2e tests, but
  with the test fixture adjusted to NOT pre-truncate changesets in a
  way that blocks the post-unwind UpdateForkChoice (or use a smaller
  reorg target so ReorgTooDeep doesn't trip). The test that
  `targetBlock+1` re-executes successfully via `UpdateForkChoice` is
  the load-bearing T3.A2 check.

**Live (hoodi)**:
- Run mode B, watch Lighthouse re-anchor + chain advance forward
  past the new head with no exec failures.

**Diagnostic cleanup**:
- Remove the `[G3.15-diag]` log lines added to
  `db/state/wipe_writable_shadow.go` (`logDiffReplayDiagnostic` +
  the import + the call site). They served their purpose surfacing
  the commitment-domain gap; with the fix in place they're noise.

## Validation criteria

- All existing wipe/recompute unit tests still pass.
- New unit test: post-extended-wipe, commitment branches at boundary
  step are EITHER absent OR match the recompute's emitted branches
  (no stale forward-exec leftovers).
- E2E test: `TestSetHead_E2E_ModeB_NonAlignedCut` (and aligned
  variants) re-execute `targetBlock+1` successfully via
  `UpdateForkChoice` → `ExecutionStatusSuccess`.
- Live: hoodi mode-B cycle completes, chain advances ≥ 5 blocks past
  the new head within 5 minutes, no exec failures in the log.

## Scope deliberately out

- **Caplin recovery (W3.12b)** — separate item. The Engine API
  contract (W3.12a) is in place; Caplin's internal handling is its
  own work and not on the critical path for T3.A2 (which can run
  against Lighthouse).
- **Post-unwind retire repair (W3.13)** — also separate. The straddled
  file at boundary step stays as-is for now; W3.13's repair lands
  before T3.A3 (10-cycle reliability) but isn't a T3.A2 prerequisite.
- **Sparse snapshots, manifest-driven indexing** — these are post-T3
  items per the product overview's "future work" section.

## Estimated effort

- Recompute primitive split: ~half day (existing path, just add the
  collector + a thin wrapper).
- WipeWritableShadowPast extension: ~half day (wipeDomainValuesAtStep
  helper + the extended sub-op flow).
- Sub-op reorder in Provider.Unwind: ~quarter day.
- Tests + lint: ~half day.
- Live re-validation against hoodi + Lighthouse: ~quarter day.

Total: ~2 working days, plus debug time if the fix reveals further
gaps.

## What this unblocks

- T3.A2 — single setHead cycle against a real chain works
  end-to-end including post-unwind forward exec.
- T3.A3 — 10 consecutive cycles becomes testable (W3.13 still
  needed before A3 passes reliably).
- W3.12a live validation — once forward exec works, Lighthouse can
  observe the SYNCING + INVALID flow that W3.12a implements.
