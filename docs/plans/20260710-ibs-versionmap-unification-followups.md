# IBS versionMap-unification — follow-up tasks

Tracks the work deferred after the cache-free parallel-execution change
(`noMaterialize`) landed. File these as issues when the PR is delivered and link
them from the PR description.

Context: [20260709-versionedio-single-source-bal-occ.md](20260709-versionedio-single-source-bal-occ.md).

## Delivered in this PR

The parallel execution worker runs without a `stateObject` cache: create/write
flows record only versioned-write cells and committed reads resolve from the
state reader, with the tx's own CreateContract / SelfDestruct / Code cells
reconstructed onto a transient object. Gated by `IntraBlockState.noMaterialize`,
set only at the parallel worker (`taskVersion.Reset`). genesis / RPC / serial
keep the flag off and are unchanged. Validated: `state -race`, reincarnation
oracle, `execution/tests`, and BAL hive (`eest-devnet` 2572/0).

## Follow-up 1 — genesis commit via the write-set

Move `ComputeGenesisCommitment` off `FinalizeTx`→`stateObjects` (walked over a
throwaway tmpDB) onto `FinalizedWrites().Apply(sd, tx, 0, 1, nil, &chain.Rules{},
nil, false)` — genesis records every field as a cell (versionMap is non-nil), so
`WriteSet.Apply` (the `blockCache == nil` branch) has the data. High blast radius:
this computes the genesis root of every chain.

Caveat — the returned IBS is a state carrier re-consumed by three executors with
different commit mechanisms (`txtask.go`, `historical_trace_worker.go`, and
`rpchelper/commitment.go` which discards it); `WriteGenesisState` commits via a
`NoopWriter` and `WriteGenesisBesideState` writes only block metadata. Confirm the
block-0 commit path for each consumer before setting `noMaterialize` on the
genesis IBS.

## Follow-up 2 — serial executor off stateObjects

The serial path fundamentally uses `FinalizeTx` / `MakeWriteSet` / `CommitBlock`
over `stateObjects`. Migrating it to the write-set commit path is the large piece
that (with follow-up 1) unblocks removing the maps.

## Follow-up 3 — drop the parallel-path maps

Once follow-ups 1 and 2 land: default `noMaterialize` true (or remove the flag)
and delete `stateObjects`, `stateObjectsDirty`, `nilAccounts`, `balanceInc`.
Keep `sdProbe` — it is the parallel self-destruct probe cache.

## Follow-up 4 — component D cleanup

- Remove the IBS `StateWriter` surface; the `NoopWriter`-suppress pattern is
  obsolete once writes go versionedio → `WriteSet.Apply` → SharedDomains.
- Collapse `StateV3` (superseded by that path).
- Make the reader a pure SharedDomains adapter (latest + historic).
- Tidies: `finalizeSystemTx`'s intermediary `state.New` reconstruction; the dead
  `versionedWriteCollector` type (tests still reference it); the intermittent
  `bbbb` reconcile-drop.

## Low-risk quick win

Follow-up 4's `versionedWriteCollector` removal touches no commit path and can be
done independently of the map-drop.
