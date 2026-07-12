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

## Follow-up 5 — warm-read throughput (iterative, aligned with early-detection + pause)

`noMaterialize` routes every read through `versionedReadCore`, which re-probes the
version map under a **single global `RWMutex`** (versionmap.go). On warm/repeat-read
cells the reader-counter atomic bounces across workers — profiling `warm-extcodehash`
showed `RWMutex.RLock` + its `atomic.Int32.Add` at ~17% of samples, paid ~5×/op
(`Empty()` alone does 4 field probes). Result: warm-family cells sit ~2.2× behind
geth (and further behind reth), where a warm access is a lock-free resident-map hit.
Pre-`noMaterialize` (resident `stateObject`) these were geth-comparable, so the
throughput is recoverable.

Target operating model: **early detection + pause on the _write_ side** — when a
write publishes, it detects the in-flight txs that already read the now-stale value
and pauses/reschedules them, so **readers no longer re-probe the version map under
lock on every access**. That moves detection off the read hot path and makes readers
a cheap, lock-free resident hit. Iterate toward that:

- Give readers a resident, decoded, interned-handle-keyed warm view (the erigon
  analogue of revm's `CacheState` / geth's `stateObjects`), hit directly without the
  per-read `versionedReadCore` re-probe + global `RWMutex`.
- Move conflict detection to write-publish (identify + pause dependent readers)
  instead of pull-based re-probing on every read.
- Cut redundant reads en route (`Empty()`'s 4 probes → 1 for the no-own-write case).
- Measure each iteration against geth/reth per-op cost; keep iterating until the
  warm family is geth-comparable (Gap A) and chip at the Go-vs-Rust ceiling (Gap B).

Mind intern costs: `accounts.Address`/`CodeHash` are `unique.Handle`; `unique.Make`
is a global intern-map + GC-weak-handle op per call (the CALL family re-interns per
op) — geth/reth don't pay this; the resident view should key on the interned handle.

## Low-risk quick win

Follow-up 4's `versionedWriteCollector` removal touches no commit path and can be
done independently of the map-drop.
