# Caplin Componentization — Requirements

**Status**: reference document, not a tracked workstream. Captured here so
that when Caplin componentization is picked up, the integration points and
failure modes are already articulated.

## Current state

Caplin is *not* a component. The other components under
`node/components/` (`downloader`, `manifest_exchange`, `sentry`,
`snapshotauth`, `storage`) own their domain via a `Provider` type and
publish/subscribe events over the storage bus. Caplin sits outside this
model:

- Block ingestion uses [`PersistentBlockCollector`](../cl/phase1/execution_client/block_collector/persistent_block_collector.go).
  It maintains its own MDBX database (`p.db`) keyed on block number. Beacon
  blocks arriving via gossip or `DownloadHistoricalBlocks` are appended
  here; periodically `Flush` drains the queue into the EL by calling
  `engine.InsertBlocks` and `engine.ForkChoiceUpdate` through
  [`ChainReaderWriterEth1`](../execution/execmodule/chainreader/chain_reader.go).
- The storage component (`node/components/storage`) has no visibility into
  these writes. Its `Inventory`, `lifecycle.Driver`, and bus subscribers
  see only what stage_snapshots / execmodule publish.
- Conversely, Caplin has no subscription to the storage bus. Events the
  storage component publishes — retirement-started/done, file-lifecycle
  transitions, and (today) the implicit "unwound" state after
  `Provider.Unwind` — never reach Caplin.

## Failure mode this enables

Live-confirmed 2026-06-08 on a fresh hoodi datadir at block 2,973,999.

1. `debug_setHead` to 2,890,316 dispatched mode B.
2. `ExecModule.setHeadModeB` → `Provider.Unwind(toBlock=2890316)` completed
   correctly. The commitment anchor was rewritten
   (`branches=609 lastTxNum=103125037`); kv.Headers / kv.HeaderTD /
   kv.BlockBody / kv.Senders rows past the target were deleted; all EL
   stages aligned at 2,890,316. The unit-level invariant
   (`TestSetHead_E2E_ModeB_WipesOrphanRowsPastTarget`) is honoured live.
3. Caplin's `PersistentBlockCollector` still held cached beacon blocks
   from before the unwind — block 2,974,000 and forward, with parent
   hash referring to block 2,973,999. That parent's TD was wiped in step 2.
4. Next `chainTipSync.Flush` called `engine.InsertBlocks` with the cached
   batch. `InsertBlocks` reads the parent TD; the row is gone; returns
   `parent's total difficulty not found with hash … and height 2973999`.
5. Caplin retries every ~25s, identical failure. No recovery path. Permanent
   wedge until process restart.

The storage component knew about the unwind (`Provider.Unwind` ran inside
it). Caplin did not, and had no way to.

## What needs to happen

Caplin becomes a storage consumer (and ultimately a component on the same
shape as the rest):

### Minimum viable integration (decoupled from full componentization)

- `PersistentBlockCollector` writes block data via the storage component's
  write path, not via a private MDBX DB plus `engine.InsertBlocks` direct.
- `chainTipSync` subscribes to the storage bus and reacts to:
  - `flow.RetirementStarted` / `flow.RetirementDone` — already published by
    `node/eth/backend.go` (`PublishRetirementStart`/`PublishRetirementDone`
    bridges).
  - A new `flow.UnwindCompleted{toBlock, lastTxNum}` event published by
    `ExecModule.setHeadModeB` on success. The block-collector handler drops
    cached entries past `toBlock`.
- Removes the stop-gap prune-on-Flush guard introduced in this branch (the
  one whose TODO points here).

### Full componentization

- `node/components/caplin/provider.go` analogous to the other components:
  Configure / Initialize / Start / Close lifecycle, BindBus.
- Move `cl/phase1/stages/*` orchestration that today is wired in
  `node/eth/backend.go` into the component's `Initialize`.
- `chain_tip_sync.go`'s `blockCollector.Flush` is a method on the
  component, gated by the same lifecycle the rest of the components use.
- Existing duplicated state (PersistentBlockCollector's MDBX DB vs. the
  storage component's Inventory + flow event bus) collapses into a single
  source of truth.

## Why it's not part of this work

This branch's functional scope is **Fork + Unwind**. Caplin's
componentization is independent and would significantly expand the
review surface. Captured here so the requirements survive the immediate
fix work — see the TODO comment in
[`persistent_block_collector.go`'s `Flush`](../cl/phase1/execution_client/block_collector/persistent_block_collector.go)
which links back to this document.

## Concrete touchpoints (for the eventual implementer)

- `cl/phase1/execution_client/block_collector/persistent_block_collector.go`
  — `Flush` writes via storage; remove the prune stop-gap; AddBlock /
  AddGloasBlock write to storage instead of `p.db.Update`.
- `cl/phase1/stages/chain_tip_sync.go` — clstages loop subscribes to
  storage bus events.
- `execution/execmodule/set_head_mode_b.go` — publish
  `flow.UnwindCompleted` after `tx.Commit` and `FinalizeUnwind`.
- `node/eth/backend.go` — replace the direct Caplin wiring with a
  `node/components/caplin.Provider` instance.
- Possibly `node/components/storage/flow/events.go` — declare the new
  `UnwindCompleted` event type.
