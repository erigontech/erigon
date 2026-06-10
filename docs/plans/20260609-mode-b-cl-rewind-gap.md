# Mode-B Deep SetHead — CL-Side Rewind Gap (CL Component MVI)

**Date:** 2026-06-09
**Branch:** `feat/snapshot-flow-app-integration`
**Status:** Design phase — surfaced during minimal-soak QA on 2026-06-09; needs review before implementation.

## Summary

Mode-B `debug_setHead` correctly unwinds the EL (chaindata + snapshot
writable shadow + commitment + canonical hashes) but does **not** signal
Caplin to rewind its consensus-layer view. On a fully-synced node, this
leaves Caplin pointed at the pre-unwind tip slot, downloading beacon
blocks whose payloads are far past the EL's new head.
PersistentBlockCollector's gap-aware Case-C prune correctly deletes them
every Flush; the cycle repeats forever and the EL head never advances.

The right shape — already articulated in
[`docs/caplin-componentization-requirements.md`](../caplin-componentization-requirements.md) —
is for Caplin to be a storage-bus consumer. This document specifies the
**minimum-viable CL component (MVI)** that lets us land that
event-subscription substrate now, hook a new `flow.UnwindCompleted`
event into it, and unwedge the soak. It is intentionally not a full
componentization; it does the minimum needed to wire one event and
respond to it correctly.

## Why this came up

User-initiated `debug_setHead(2,954,363)` from a synced hoodi tip at
2,984,363 (a 30k-block unwind) wedged on recovery. Detail in
[Repro](#repro-iter-1-of-the-2026-06-09-minimal-soak) below. Yesterday's
"successful" 84k-back test on 2026-06-08 worked because the target
equaled the just-bootstrapped preverified tip — Caplin's
PersistentBlockCollector queue was at `elHead+1`, Case-B of the
gap-aware prune kept it, no CL-side rewind was needed. That coincidence
masked the gap.

## Why the existing Mode-B design didn't catch this

[`20260525-admin-sethead-unwind-design.md`](20260525-admin-sethead-unwind-design.md)
focused on the **storage / DB-reset** half of the operation. Its
post-state invariant is "MDBX empty past toBlock; snapshot files are
the only state including the commitment anchor at toBlock's step
boundary." That invariant is on the EL side. The CL side was implicitly
assumed to follow via the existing ForkChoiceUpdated path — i.e. if the
EL says "head is X", the CL learns through standard FCU semantics.

That assumption holds only when the CL's current head and the EL's new
head are reachable from each other through Caplin's fork-choice DAG. In
practice, the in-process `ExecutionClientDirect` returns a Go `error`
when the EL doesn't have the FCU's `head` block (the unwound-away
block), and Caplin's `doForkchoiceRoutine` returns from
`computeAndNotifyServicesOfNewForkChoice` with that error. The CL's
`highestSeen` / `finalizedCheckpoint` / `anchorSlot` are never reset —
the next forwardSync cycle computes the same `startSlot` and downloads
the same too-far-forward beacon blocks again.

The 2026-05-25 design's "Live verification — scenario 3" mentions
admin unwind on a fully-synced node but does not enumerate the CL-side
state that must be coherent post-unwind. This document fills that gap.

## Repro (iter 1 of the 2026-06-09 minimal soak)

Hoodi, clean datadir, minimal-prune, sync-to-tip — head reached
`2,984,363` with no startup wedge (the elHead=0 fix in commit
`6642aeef2c` was validated end-to-end). The driver called
`debug_setHead(2,954,363)` (30k blocks back). Log timeline:

```
17:04:35  [Caplin] Forward Sync from=3234144 to=3234272
17:04:47  [BlockCollector] pruned unreachable cached blocks (gap from EL head)
            elHead=2954363 firstPast=2984339 pruned=116
17:04:47  ForkChoice err: forkchoice: block 506bc69b...8716 not found
            or was marked invalid
17:05–17:34  No further block advance; Forward Sync keeps polling at
            slot ~3,234,272+; PersistentBlockCollector keeps pruning;
            ForkChoice keeps erroring on the same beacon root.
```

After 30 min (the soak's `RECOVERY_TIMEOUT_SEC`), the driver aborted
with `abort:recovery-timeout+errors=2`.

Key evidence:

- **Pruning is correct.** `elHead=2954363, firstPast=2984339` — a 30k
  gap. Case C of `pruneStaleCachedBlocks` is the right verdict; without
  it, `InsertBlocks` would loop on "parent's total difficulty not
  found" (the bug that motivated the original gap-aware prune in
  `877740b4a3`).
- **The ForkChoice error has a precise origin.**
  [`execution/execmodule/forkchoice.go:264`](../../execution/execmodule/forkchoice.go#L264) —
  `e.blockReader.HeaderByHash(originalBlockHash)` returns `nil`
  because Provider.Unwind deleted the block. The error string we see
  comes from `sendForkchoiceErrorWithoutWaiting(fmt.Errorf("forkchoice:
  block %x not found or was marked invalid", blockHash))`.
- **Caplin's `forwardSync` start slot is stale.**
  [`cl/phase1/stages/forward_sync.go:276`](../../cl/phase1/stages/forward_sync.go#L276) —
  `startSlot = cfg.forkChoice.HighestSeen()`. HighestSeen is in-memory
  (`atomic.Uint64`, [`forkchoice.go:489`](../../cl/phase1/forkchoice/forkchoice.go#L489)),
  set to `anchorState.Slot()` at startup and bumped forward each time
  Caplin processes a new block. Nothing rewinds it on EL setHead.

## Architectural target — CL as a bus consumer

[`docs/caplin-componentization-requirements.md`](../caplin-componentization-requirements.md)
already describes this. Quoting the relevant section ("Minimum viable
integration"):

> - `PersistentBlockCollector` writes block data via the storage
>   component's write path, not via a private MDBX DB plus
>   `engine.InsertBlocks` direct.
> - `chainTipSync` subscribes to the storage bus and reacts to:
>   - `flow.RetirementStarted` / `flow.RetirementDone` — already
>     published by `node/eth/backend.go`
>     (`PublishRetirementStart`/`PublishRetirementDone` bridges).
>   - A new `flow.UnwindCompleted{toBlock, lastTxNum}` event published
>     by `ExecModule.setHeadModeB` on success. The block-collector
>     handler drops cached entries past `toBlock`.
> - Removes the stop-gap prune-on-Flush guard introduced in this branch
>   (the one whose TODO points here).

This design adopts that target shape but stops at the **minimum
viable CL component** needed to subscribe to and react to one event
(`flow.UnwindCompleted`). The full block-data write-through and
componentized chainTipSync are deferred to a separate workstream so
this change stays small and reviewable.

## CL component MVI — what we build now

### Shape

A new `node/components/caplin/provider.go` that:

- Implements the same lifecycle surface as
  `node/components/storage/provider.go` and the other components
  (`Configure`, `Initialize`, `Start`, `Close`, `BindBus`).
- At `Initialize`, takes references to:
  - the existing `*persistent_block_collector.PersistentBlockCollector`
    (no ownership change yet — Caplin still constructs it inline; the
    component just holds a pointer for bus-driven mutations).
  - the existing `*forkchoice.ForkChoiceStore` (same).
- At `BindBus`, subscribes to `flow.UnwindCompleted` and reacts:
  1. Drop everything in `PersistentBlockCollector`'s persisted queue
     (`Wipe`).
  2. Rewind `ForkChoiceStore` state so the next `forwardSync` cycle's
     `startSlot` is at the slot containing `toBlock`'s execution
     payload (or a safe lower bound).
- `Start` is a no-op for now (Caplin's stages loop still runs from
  `node/eth/backend.go`).
- `Close` is a no-op for now.

The point of the MVI is: a single, narrowly-scoped component that owns
the rewind reaction. It establishes the event-subscription substrate
without disturbing Caplin's existing wiring. The full migration into
the component (block-data write-through, stages-loop ownership, single
source of truth for the queue) lives in
[caplin-componentization-requirements.md](../caplin-componentization-requirements.md)
as a separate workstream.

### New event: `flow.UnwindCompleted`

Declared in `node/components/storage/flow/events.go` (next to existing
events like `BlockHeadersReady`, `ForkBootstrapRequired`). Published by
`ExecModule.setHeadModeB` (and by mode-A's `SetHead` for symmetry) on
successful unwind commit:

```go
// node/components/storage/flow/events.go
type UnwindCompleted struct {
    ToBlock    uint64
    LastTxNum  uint64
    TipSlot    uint64 // current beacon tip slot at unwind time — for slot estimate
    TipBlock   uint64 // current EL tip block at unwind time — for slot estimate
}
```

`TipSlot` and `TipBlock` are captured *before* the unwind commits so
the CL component can compute a slot estimate without re-querying state.

Publishers:

- `execution/execmodule/set_head_mode_b.go` after `FinalizeUnwind`
  returns nil (the unwind is durable).
- `execution/execmodule/set_head.go` after the mode-A `tx.Commit`
  succeeds (same semantics for symmetry — `fork-from` will use the
  same path).

### CL component reaction: PersistentBlockCollector.Wipe + ForkChoice rewind

`PersistentBlockCollector` gains a `Wipe(ctx)` method that clears
`kv.Headers` in its private DB. Currently `pruneStaleCachedBlocks`
deletes only "past elHead with a gap" rows; a full wipe is cheaper at
unwind time and avoids one cycle of Case-C-prune log spam:

```go
// cl/phase1/execution_client/block_collector/persistent_block_collector.go
func (p *PersistentBlockCollector) Wipe(ctx context.Context) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    return p.db.Update(ctx, func(tx kv.RwTx) error {
        return tx.ClearTable(kv.Headers)
    })
}
```

`ForkChoiceStore` gains an explicit user-initiated rewind:

```go
// cl/phase1/forkchoice/forkchoice.go
//
// UserInitiatedRewindToBlock is invoked when the user has driven an
// EL unwind via debug_setHead or fork-from. It is NOT a
// consensus-driven mutation; the spec has no equivalent. Safe because
// no peer can trigger it — only the local debug_setHead RPC + the
// fork-from CLI can. Spec-conformant code paths (on_block,
// on_attestation, on_tick) continue to be the sole writers to
// highestSeen during normal operation.
func (f *ForkChoiceStore) UserInitiatedRewindToBlock(
    targetBlock uint64,
    targetSlotEstimate uint64,
) error {
    f.mu.Lock()
    defer f.mu.Unlock()

    // Lower highestSeen / highestSeenRoot — reverts the CL's notion of
    // "how far we've seen" to the post-unwind tip.
    f.highestSeen.Store(targetSlotEstimate)
    f.highestSeenRoot.Store(common.Hash{})

    // Lower finalized + justified checkpoints if past the target.
    // computeAndNotifyServicesOfNewForkChoice uses finalizedSlot as
    // the startSlot floor; if it stays past targetSlot, rewinding
    // highestSeen alone doesn't move startSlot.
    fc := f.FinalizedCheckpoint()
    finalizedSlot := uint64(fc.Epoch) * f.beaconCfg.SlotsPerEpoch
    if finalizedSlot > targetSlotEstimate {
        newEpoch := targetSlotEstimate / f.beaconCfg.SlotsPerEpoch
        f.SetFinalizedCheckpoint(
            solid.NewCheckpointFromParameters(common.Hash{}, newEpoch),
        )
    }
    return nil
}
```

Spec-defensibility hinges on `UserInitiatedRewindToBlock` being
unreachable from any network input. The Go visibility guard: the method
lives on a small interface that only the new CL component holds —
Caplin's bus / gossip / engine handlers do not. Explicit comment in
`forkchoice.go` marks it as the only intentional non-spec mutator.

### Slot estimate from block number

`ExecModule` knows the EL tip block at unwind time; Caplin knows the
beacon tip slot. Both are captured in `flow.UnwindCompleted`. The CL
component computes:

```go
slotEstimate := (event.ToBlock * event.TipSlot) / event.TipBlock
```

Inaccuracy is bounded by the empty-slot rate on the chain. For hoodi
that's ~8% (`current_slot/current_block ≈ 1.084`); for mainnet ~5%.
forwardSync downloads ~50-150 extra slots before reaching
`targetBlock+1`; those slots are no-ops on the EL side (empty blocks
or already-present blocks; `InsertBlocks` early-returns on
`block.NumberU64() < minInsertableBlockNumber`).

A precise lookup (querying Caplin's beacon DB for the slot whose
`ExecutionPayloadHeader.BlockNumber == targetBlock`) is a Phase-2
refinement — see [Open questions](#open-questions). Phase 1 ships the
estimate.

## Implementation phases

### Phase 1 — CL component MVI + rewind (target: hoodi minimal-prune soak passes)

1. Declare `flow.UnwindCompleted` in
   `node/components/storage/flow/events.go`.
2. Publish it from `execution/execmodule/set_head_mode_b.go` (after
   `FinalizeUnwind`) and `set_head.go` (after `tx.Commit`).
3. Create `node/components/caplin/provider.go` — minimal lifecycle
   surface; takes references to existing `*PersistentBlockCollector`
   and `*ForkChoiceStore`; subscribes to `flow.UnwindCompleted` at
   `BindBus`.
4. Add `PersistentBlockCollector.Wipe(ctx)`.
5. Add `ForkChoiceStore.UserInitiatedRewindToBlock(targetBlock,
   slotEstimate)`.
6. Wire the new component into `node/eth/backend.go` alongside the
   existing components.
7. Unit tests for `Wipe`, `UserInitiatedRewindToBlock`, and the CL
   component's event handler (mock bus → asserts both helpers are
   invoked with the right args).
8. e2e regression: extend
   [`rpc/jsonrpc/debug_api_set_head_e2e_test.go`](../../rpc/jsonrpc/debug_api_set_head_e2e_test.go)
   with a "CL-pointer post-condition" assertion — after `SetHead`
   returns, `forkChoice.HighestSeen()` is at or below the slot
   estimate.
9. Live-verify: re-run the 5-iter minimal soak that surfaced the gap
   (depths 30k/60k/90k/60k/30k, hoodi minimal-prune).

### Phase 2 — precise slot lookup

1. Beacon-DB lookup that returns the exact slot whose execution
   payload has `BlockNumber == targetBlock`.
2. Fallback to the ratio estimate if the lookup misses (beacon DB
   pruned past that slot).
3. Document precise-vs-estimate path and remove the estimate as a
   steady-state path on chains where precise lookup succeeds.

### Phase 3 — spec-conformance review

Independent re-read of `UserInitiatedRewindToBlock` against the Phase0
/ Gloas forkchoice spec for **non-mutation invariants**. The user
rewind is not in spec, so the check is "does it violate any invariant
the spec implicitly maintains?". Areas of concern:

- the weight store (`weight_store.go`)
- latest-message indexing (`on_attestation.go`)
- fork-graph integrity after a `highestSeen` rewind
  (`fork_graph_disk.go`)

Explicit comment in `forkchoice.go` lists the entry points permitted
to call `UserInitiatedRewindToBlock` and asserts no other caller.

### Phase 4 — fork-from CLI integration

`fork-from` will need the same rewind. The same `flow.UnwindCompleted`
publish path serves it unchanged; no extra design needed beyond moving
the trigger from `ExecModule.SetHead` to `fork-from`'s commit step.

### Phase 5 — full componentization (separate workstream)

Picked up from
[`caplin-componentization-requirements.md`](../caplin-componentization-requirements.md):
`PersistentBlockCollector` writes through the storage component;
stages-loop ownership moves into the component; the private MDBX DB
collapses into the storage component's Inventory + flow event bus.

The Phase-1 MVI deliberately does not start this — the goal is to
land the event substrate cleanly so the larger migration has a foothold.

## Open questions

1. **Should the rewind also drop forkChoice's fork graph past the
   target slot?** The graph holds beacon block headers + states.
   Leaving them means a future forwardSync may re-encounter them and
   short-circuit their re-validation. Probably fine (the spec is
   idempotent on re-encountered blocks via the `Store.blocks`
   membership check) but warrants a Phase-3 review.
2. **Interaction with checkpoint-sync URL.** If Caplin was started
   with a checkpoint-sync URL, can we just re-anchor via that URL on
   user rewind instead of mutating fork-choice state in place?
   Cleaner but slower (network round-trip) and changes the contract.
3. **Locking.** `highestSeen` is `atomic.Uint64`; finalized
   checkpoint and `Store.blocks` are behind a mutex. We need either a
   single write-lock acquisition spanning all mutations or a
   documented "rewind only acceptable during `adminUnwindInProgress`
   window so peers won't race fc updates." Probably the latter.
4. **External CL (engine-api over HTTP).** User setHead with an
   external CL (Lighthouse, Prysm, etc) talks through
   `engine_server.go`, which already returns SYNCING during
   `IsAdminUnwindInProgress`. The external CL handles its own rewind
   via FCU semantics (different path; out of scope for this design).
5. **Precise vs estimate slot lookup ordering.** Ship the estimate in
   Phase 1 (fast), or do Phase 2's precise lookup in the same change?
   Recommendation: estimate first — it unblocks the soak; the
   imprecision is bounded and recoverable.

## Critical files

| File | Role |
| --- | --- |
| `node/components/caplin/provider.go` | **New.** MVI component: subscribes to `flow.UnwindCompleted`; calls `Wipe` + `UserInitiatedRewindToBlock`. |
| `node/components/storage/flow/events.go` | Add `UnwindCompleted` event type. |
| `cl/phase1/forkchoice/forkchoice.go` | Add `UserInitiatedRewindToBlock`. |
| `cl/phase1/execution_client/block_collector/persistent_block_collector.go` | Add `Wipe`. |
| `execution/execmodule/set_head.go` | Publish `flow.UnwindCompleted` after mode-A commit. |
| `execution/execmodule/set_head_mode_b.go` | Publish `flow.UnwindCompleted` after `FinalizeUnwind`. |
| `node/eth/backend.go` | Construct + wire the new `caplin` component alongside the existing components. |
| `rpc/jsonrpc/debug_api_set_head_e2e_test.go` | Extend with CL-state post-condition assertions. |

## Verification

```bash
make erigon && make lint
go test -count=1 ./cl/phase1/forkchoice/... \
  ./cl/phase1/execution_client/block_collector/... \
  ./execution/execmodule/... \
  ./node/components/caplin/... \
  ./rpc/jsonrpc/...
```

End-to-end (the soak that surfaced the gap):

- Hoodi minimal-prune, clean datadir, sync to tip
- Run `scripts/unwind-soak.sh --iter 5 --depths 30000,60000,90000,60000,30000`
- All 5 iterations pass: each setHead succeeds, head returns to within
  1000 blocks of canonical tip within 30 min, zero forbidden log
  patterns.

## Related plans + memory

- [`docs/caplin-componentization-requirements.md`](../caplin-componentization-requirements.md) —
  the reference document this design adopts as the architectural target.
  Phase 1 MVI here is its "Minimum viable integration" applied to one
  event (`flow.UnwindCompleted`).
- [`20260525-admin-sethead-unwind-design.md`](20260525-admin-sethead-unwind-design.md) —
  the parent mode-B design. This document is its CL-side complement.
- [`20260527-sethead-external-cl-test-rig.md`](20260527-sethead-external-cl-test-rig.md) —
  external-CL test rig; uses a different FCU path
  (`engine_server.go`'s `IsAdminUnwindInProgress` short-circuit).
- [`20260530-mode-b-functional-completeness.md`](20260530-mode-b-functional-completeness.md) —
  the functional-completeness checklist. Adding "CL-side rewind on
  deep user-initiated SetHead" to its open list.
- Memory pin `snapshot-reconciliation-shipped-2026-06-09` — covered
  the startup/launch story. This doc covers the mid-run mode-B story.
- Memory pin `unwind-storage-rollback-bug-caught-2026-06-09` — the
  tombstone bug we shipped today (commit `1847f1e6eb`). Resolved
  separately; not blocking this work.

---

## 2026-06-09 EVENING UPDATE — MVI built; second architectural finding surfaced

The Phase-1 MVI shipped (event + component + Wipe + rewind primitive
+ wiring + tests, all lint-clean). Two follow-up findings emerged
during validation.

### Finding 1 — preserve roots when rewinding (fixed)

First soak attempt cleared `highestSeenRoot`, `finalizedCheckpoint.Root`
and `justifiedCheckpoint.Root` to zero. This wedged Caplin's main
loop: `updateCanonicalChainInTheDatabase` walks parent links from
`headRoot` to canonical root, gets stuck when `headRoot` is the zero
hash. Fixed by preserving the (now-stale) roots — they reference
blocks the EL just deleted, so downstream FCUs fail for a few cycles
until forwardSync re-feeds the gap and a fresh `OnBlock` updates
both atomically. Tests updated to pin "must NOT be cleared" as the
correct semantics.

### Finding 2 — Caplin's `anchorSlot` is a hard floor on forwardSync (OPEN)

The MVI's slot-pointer rewind successfully lowers `highestSeen` and
the finalized/justified checkpoint epochs. But `forwardSync` uses:

```go
startSlot = max(HighestSeen-300, finalizedSlot, anchorSlot)
```

— [cl/phase1/stages/forward_sync.go:286-287](../../cl/phase1/stages/forward_sync.go#L286-L287).

`anchorSlot` is set once at `NewForkChoiceStore` from
`anchorState.Slot()` and is **never lowered during runtime**. On a
checkpoint-synced node, anchor sits at the slot of the checkpoint
sync target — typically near chain tip at startup time.

After today's deep-unwind soak attempt (depth 30k blocks, target
slot ~3,202,958, anchor ~3,235,000):

- HighestSeen lowered to 3,202,958 ✓
- finalizedCheckpoint.Epoch lowered ✓
- anchorSlot stayed at 3,235,000 ← floor binds startSlot here
- forwardSync starts downloading at slot 3,235,000+ (which it
  shouldn't — those blocks correspond to EL blocks 2,985,000+, way
  past the post-unwind EL head at 2,955,528)
- BlockCollector's gap-aware Case-C prune deletes them every Flush
- Caplin can't fetch slots below anchor — the consensus protocol
  doesn't validate pre-anchor blocks without parent state

**Result:** deep mode-B SetHead works at the EL layer but the CL
can't bridge the gap on a checkpoint-synced node. The chain wedges
permanently at the post-unwind block.

### Mode-B SetHead — depth limits per Caplin sync mode

| Caplin sync mode | Max safe SetHead depth |
| --- | --- |
| Genesis-synced (full history) | Anchor at slot 0 — no floor; depth unbounded |
| Checkpoint-synced | Up to `(currentBlock − blockAtAnchorSlot)`, typically a few hundred blocks since anchor is near startup tip |
| Re-anchored (Phase 2 needed) | After re-anchor at the new target slot, unbounded |

The MVI's current scope ships safely within the checkpoint-synced
depth limit. Going deeper requires Phase 2.

### Phase 2 — Caplin re-anchor on deep UnwindCompleted

When `UnwindCompleted.ToBlock` maps to a slot below `anchorSlot`,
the in-process rewind alone won't recover. The CL component must
trigger a fresh checkpoint sync at the target slot. Sketch:

1. CL component detects `slotEstimate < forkChoice.AnchorSlot()` in
   the `onUnwindCompleted` handler.
2. If a checkpoint-sync URL is configured
   (`config.CaplinConfig.CheckpointSyncEndpoint`), fetch a state at
   the rewound slot from the URL.
3. Re-initialize `ForkChoiceStore` with the new anchor state.
4. Re-initialize `PersistentBlockCollector` (already wiped by the
   MVI).
5. Caplin's clstages loop continues from the new anchor.

Risks: re-anchor is a non-trivial state-mutation; doing it mid-run
needs careful synchronization with the clstages goroutine. May
require pausing the clstages loop, swapping fork-choice store
references, and resuming. The cleanest implementation is probably:
unwind triggers a controlled `caplin1.RunCaplinService` restart
inside the existing process (cancel + relaunch the goroutine), with
the rewound slot as the new anchor.

If no checkpoint-sync URL is configured, the rewind cannot proceed —
the design must surface this as an actionable error to the user
(rather than silently wedging the chain).

### What ships today (MVI Phase 1)

- New event `flow.UnwindCompleted{ToBlock, TipBlock}` published by
  `ExecModule.SetHead` (mode A + mode B).
- New `node/components/caplin/provider.go` subscribing to the event
  and reacting via Wipe + UserInitiatedRewindToBlock.
- New `PersistentBlockCollector.Wipe(ctx)`.
- New `ForkChoiceStore.UserInitiatedRewindToBlock(targetBlock,
  slotEstimate)` — preserves roots (correctness invariant).
- Storage's `eventBus` is now always-initialised (previously gated on
  `LifecycleDrivenByStorage`).
- e2e + unit tests pin the wiring behaviour.

**Scope:** correct for SetHead targets whose slot estimate ≥
`anchorSlot`. Soak coverage on checkpoint-synced hoodi confirmed the
mechanism end-to-end at the wiring + event + rewind layers. Deep-
unwind soak surfaced Finding 2 above and is gated on Phase 2.
