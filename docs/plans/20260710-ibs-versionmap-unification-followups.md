# IBS versionMap-unification — follow-up tasks

Tracks the work deferred after the cache-free parallel-execution change
(`noMaterialize`) landed. Tracking issue: erigontech/erigon#22458.

Context: [20260709-versionedio-single-source-bal-occ.md](20260709-versionedio-single-source-bal-occ.md).

End state: a **single versionedio processing model** for all execution and block
building, with no `stateObject`. This is not OCC-dependent — OCC (conflict
detection + incarnation retry) is purely a parallel concern; run serially, the
exact same versionedio path executes and simply never produces a conflict. So
there is no "serial vs parallel" commit split to preserve: serial, parallel,
genesis, block building, and RPC all commit through the write-set.

`noMaterialize` is not a first-class concept — it is redundant with the
parallel-execution decision. `EXEC_PARALLEL` (`dbg.Exec3Parallel ||
cfg.experimentalBAL` at `stage_execute.go`) selects the `parallelExecutor`, whose
`taskVersion.Reset` sets the flag; so the rule is simply **`EXEC_PARALLEL=true` ⇒
versionedio only**. The bespoke flag is a transitional artifact and is deleted once
serial moves onto the same path (follow-ups 1–3), leaving versionedio everywhere.

## Delivered in this PR

The parallel executor (the `EXEC_PARALLEL=true` path) runs without a `stateObject`
cache: create/write flows record only versioned-write cells and committed reads
resolve from the state reader, with the tx's own CreateContract / SelfDestruct /
Code cells reconstructed onto a transient object. Block building uses the same
versionedio commit. Serial execution, genesis, and RPC still commit via
`stateObjects` — the remaining leftovers the follow-ups below remove. Validated:
`state -race`, reincarnation oracle, `execution/tests`, and BAL hive
(`eest-devnet` 2572/0).

## Follow-up 1 — genesis commit via the write-set

Genesis is an **incomplete port to the versioned IO model**:
`ComputeGenesisCommitment` builds the IBS with `NewWithVersionMap(r,
&state.VersionMap{})` (so `IsVersioned()` is true and reads go through the
versioned model), but its **write side was never ported** — it still commits via
`FinalizeTx`→`stateObjects`→`stateWriter`, and the `FinalizedWrites` write-set it
produces is never applied. The `isGenesis` guard in `txtask.go` (`txTask.TxIndex ==
-1 && BlockNumber() == 0`) exists purely to route this "versioned" IBS back onto
`MakeWriteSet`; that guard is a mask, and the serial-genesis regression (broadening
the `IsVersioned()` branch produced an empty genesis root) was this bug surfacing.

This is **not an active bug today** — it works because `noMaterialize=false` keeps
the stateObjects populated for `FinalizeTx`. But it is a **hard blocker for the
map-drop**: once `noMaterialize` is the default and the resident stateObject is
gone, `FinalizeTx` finds no stateObjects → empty genesis.

Resolution: finish the port. Genesis commits via the write-set, like the builder —
move onto `FinalizedWrites().Apply(sd, tx, 0, 1, nil, &chain.Rules{}, nil, false)`
(genesis records every field as a cell, so the `blockCache == nil` branch has the
data), set `noMaterialize` on the genesis IBS, and drop the `FinalizeTx`→writer
commit and the `isGenesis` guard. There is no "keep genesis on the stateObject
path" alternative: the goal is a single versionedio processing model with no
stateObject at all, so every path — genesis included — commits through the
write-set.

High blast radius: this computes the genesis root of every chain. The returned IBS
is a carrier re-consumed by three executors with different commit mechanisms
(`txtask.go`, `historical_trace_worker.go`, and `rpchelper/commitment.go` which
discards it); `WriteGenesisState` commits via a `NoopWriter` and
`WriteGenesisBesideState` writes only block metadata. Confirm the block-0 commit
path for each consumer before setting `noMaterialize`.

## Follow-up 2 — serial executor off stateObjects

Today the serial path still commits via `FinalizeTx` / `MakeWriteSet` /
`CommitBlock` over `stateObjects` — a leftover, not a requirement. Running serially
is just the versionedio path with no conflicts, so there is nothing OCC-specific it
needs: the serial executor runs the same write-set commit as the parallel worker
(`noMaterialize` on), and conflict detection/retry simply never fires. This is the
large piece that (with follow-up 1) unblocks removing the maps.

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

## Follow-up 6 — solidify the transitional model; retire legacy-path reliance

The versionedio model is only half-established: the parallel executor and block
builder are on it, but genesis, the serial executor, RPC, and the BAL regenerator
still lean on `stateObjects` / legacy commit code. **Every one of those points is a
live correctness risk until migrated** — they can silently diverge on the
`noMaterialize` path, and merges with `main` (which keeps evolving the legacy/BAL
code) either drop subtle correctness commits or expose incompatibilities. This PR
hit that class twice: the access-set model reconciliation, and the BAL regenerator
missing the per-tx versionMap flush.

The BAL path is the sharpest example. BAL is produced or replayed in at least four
places — the parallel executor, `chain_makers`, the block builder
(`block_assembler.go`), and `bal/rederive.go` — and each must independently
reproduce the same cross-tx discipline: **flush each phase's writes to the
versionMap between phases** (`FlushWritesToVersionMap` / `FlushVersionedWrites`),
so a later tx's write (e.g. the accumulating coinbase fee) sees the running value.
Any new BAL-producing/replaying path that forgets this produces wrong BALs on the
cache-free model — a footgun re-armed at every such site.

Solidify — this needs a **whole-process audit**, not just a shared helper:
- Audit every call site that crosses a tx/phase boundary in the versionedio model
  (parallel executor, `chain_makers`, block builder, `bal/rederive.go`, plus any the
  audit surfaces) — enumerate exactly what each must call and in what order today.
- **Internalize `FlushWritesToVersionMap`.** The per-tx flush must stop being a
  separate call a site has to remember. Fold it into the tx-boundary operation
  itself (part of `MergeTxIOInto`/commit or `ResetVersionedIO`) so crossing a tx
  boundary is a single call the caller cannot get wrong — collapsing today's
  `MergeTxIOInto` → `FlushWritesToVersionMap` → `ResetVersionedIO` dance. Simplify
  the required call surface down to that one operation.
- Land follow-ups 1–3 (genesis, serial, map-drop) so the mixed model — the source
  of the risk — goes away entirely.
- Until then, treat any change to a legacy/stateObject path, or any `main` merge in
  the BAL/versionedio area, as requiring the per-tx-flush + access-model audit.

## Low-risk quick win

Follow-up 4's `versionedWriteCollector` removal touches no commit path and can be
done independently of the map-drop.
