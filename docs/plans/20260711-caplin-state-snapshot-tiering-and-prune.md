# Caplin state snapshotting: merge-tier + indexing-DB prune — handoff

Date: 2026-07-11. Ground-truthed against `origin/main` @ `4aad1d8899` (worktree
`caplin-vtable-repair` = main + the vtable fix, PR #22385).

Two coupled pieces came out of investigating "caplin DBs grow unbounded" (gnosis
v36 indexing DB +50 GB/night):

1. **Prune** the caplin indexing DB after freezing (the load-bearing fix — nothing
   prunes state today; converged design below, ready to implement).
2. **Re-tier** state snapshots `10k → 100k → 1m` (replaces the underthought flat
   50k; shrinks the DB floor prune leaves and keeps steady-state file count low).

---

## Problem

- Caplin indexing DB (`/erigon-data/caplin/indexing/mdbx.dat`, = the antiquary's
  `mainDB`, `cmd/caplin/caplin1/run.go:561`) holds the entire per-slot state
  history. Nothing deletes it after freezing to `.seg`.
- The reader `GetValFnTxAndSnapshot` (`cl/persistence/state/state_accessors.go:32-43`)
  reads a segment for any covered slot and only falls through to `tx.GetOne` for
  the un-frozen tail. So every DB row below the frozen watermark is dead weight.
- Result on gnosis (mid from-genesis reconstruction, frozen to slot ~13.9M of ~29M):
  indexing DB **112 GB** shadowing **79 GB** of snapshots, still climbing.
- Only beacon **blocks** are pruned (`pruneBeaconBlocksAndWriteProgress`,
  `cl/antiquary/antiquary.go:327` → `PruneBlocksLimit`). State tables: no prune
  anywhere (grepped `cl/`).

---

## Ground truth: how state snapshotting works today

- **Flat 50k files, no merge tier.** `blocksPerStatefulFile = CaplinMergeLimit*5 =
  50_000` (`cl/antiquary/state_antiquary.go:603`). `planStateDump`
  (`db/snapshotsync/caplin_state_snapshots.go:779`) tiles fixed `blocksPerFile`
  jobs across missing ranges. `RemoveOverlaps` (caplin_state_snapshots.go:474)
  only deletes fully-covered dupes — it does **not** merge small → large. So a
  state segment merger does not exist yet.
- **One file per type per range.** 33 types on disk (`MakeCaplinStateSnapshotsTypes`,
  caplin_state_snapshots.go:97-137 lists 34; one has no data yet). gnosis: 9,669
  `.seg` + 9,669 `.idx` at slot ~13.9M (~19k+19k projected at tip).
- **One type dominates.** Per 50k range: `ValidatorBalance` **311 MB (~88%)**,
  `BalancesDump` 20 MB, `SlotData` 7.7 MB, everything else ≲6 MB. Both snapshot
  bytes and the DB tail floor are essentially "N slots of ValidatorBalance diffs."
- **Watermark.** `BlocksAvailable() = min(segmentsMax, idxMax)`
  (caplin_state_snapshots.go:264). `idxMax = idxAvailability()` (522-550) is the
  real per-type min (walks every type's highest visible `.to`, takes the min; 0 if
  any type empty). `segmentsMax` (317-368) is a sloppy last-file-wins scalar — do
  not key anything off it; use `idxMax` / `coveredRangesForType` (268).
- **Never-frozen tail floor** = `safetyMargin + blocksPerStatefulFile` =
  `20_000 + 50_000 = 70k slots`. `safetyMargin = 20_000` (antiquary.go:47). This is
  the irreducible DB floor even with perfect pruning — dominated by ValidatorBalance.
- **Dump cadence.** Freezes only when `from + blocksPerStatefulFile + safetyMargin
  <= currentSlot` (state_antiquary.go:602-640). `SlotsPerDump = 1536`
  (`cl/clparams/config.go:129`) is unrelated — it's how often a full
  BalancesDump/EffectiveBalancesDump *row* is written, not a file boundary.
- **State snapshots are not manifest-pinned** — generated + seeded by snapgen nodes
  (`s.downloader.Seed`), not a hardcoded preverified hash list. So the tier is
  outward-facing (fleet publishes different files) but not a coordinated hard-fork
  of the manifest. Verify before shipping.

---

## Proposed re-tier: 10k → 100k → 1m, 20k locked from tip

50k flat is underthought. Replace with a real merge ladder (mirrors block snapshots):

- **Dump at 10k** near tip (base = `CaplinMergeLimit`, not `*5`).
- **20k locked from tip** — keep `safetyMargin = 20_000`; freeze the newest complete
  10k range below `tip - 20k`.
- **Merge 10 × 10k → 100k**, then **10 × 100k → 1m**.
- Uniform across all 33 types (one file size / one row layout per level — no
  per-type-selective tiering).

Why:
- Finer freezing edge → tail floor `70k → 30k slots` (`20k + 10k`). ~2.3× smaller
  floor (capped at 2.3×, not 5×, because the fixed 20k `safetyMargin` dominates
  once the file chunk drops from 50k to 10k). The win lands on ValidatorBalance.
- Merge ladder keeps **steady-state file count low** (1m files) while giving the
  fine edge — avoids the flat-10k penalty of 5× files (~96k `.seg` + 96k `.idx`/chain).
- Tier changes **zero bytes** — same ValidatorBalance data, just chunked differently.
  Its only DB effect is the smaller tail floor.

What must be built (does not exist today):
- A **state segment merger**: build a combined `.seg` from N smaller same-type
  segments, rebuild one `.idx` over the merged range, then `RemoveOverlaps` deletes
  the now-covered smaller files. Visibility already supersedes small by large via
  `isSubSetOf` (caplin_state_snapshots.go:443-448), so reads are correct during/after
  merge.
- Merge trigger in the antiquary cycle (analog of block `antiquate()`).
- `blocksPerStatefulFile` is local to state_antiquary.go (only 603-619) — the dump-
  size change is nearly one line; the merger is the real work.

Mixed 10k/50k/100k/1m coexist fine (visibility dedups). Prune is size-agnostic
(keys off per-type coverage), so it composes with any tier layout.

---

## Prune design (converged, ready to implement)

- **Prune set = `snapshotTypes.KeyValueGetters`** (the 34 frozen tables, derived —
  not a hand-list). `kv.StaticValidators`, progress markers, and `beacon_indicies`
  are excluded by construction (they are not frozen types). Self-correcting: add a
  frozen type later and it is auto-covered.
- **Boundary = per-table contiguous-from-genesis visible coverage**
  (`coveredRangesForType(T)` → end of the first unbroken run from slot 0). Safe with
  zero cross-table coordination: every `GetValFn(table, slot)` checks *that table's*
  segment, so pruning T below T's coverage can never be read. Even the balances
  reader (BalancesDump base + ValidatorBalance diffs) is safe — each sub-read
  resolves per-table. Per-table (not global `idxMax`) because during reconstruction
  types freeze at different rates (`errIncompleteStateRange` skips incomplete
  ranges); an ahead type must not wait on the slowest.
- **Mechanism.** No ranged truncate in mdbx (only whole-table `ClearBucket`, N/A —
  tail is retained). So: cursor-seek from the per-table marker, `DeleteCurrent`
  while `key < boundary`, **cap 1000 keys / RW txn**, commit, repeat. mdbx hates big
  txns; 1k keeps them small.
- **Cadence: on the antiquary's periodic-commit cadence** (every
  `stateAntiquaryMaxSlotsPerCommit = 4*SlotsPerDump = 6144` slots), **not** gated on
  the rare 50k/10k dump. Coverage only advances at dumps, but prune drains the
  backlog across the many commits between dumps. This is what lets a small EL-style
  budget keep up. Runs inline (no separate goroutine — single-writer means a
  separate goroutine's batches serialize anyway).
- **Timeout = EL's, per-chain, adaptive.** EL: `base = SecondsPerSlot/3`, `+200ms
  per 100 slots backlog`, cap `2/3·SecondsPerSlot` (`execution/execmodule/forkchoice.go:915-917`).
  Mirror it; default derived from `beaconCfg.SecondsPerSlot`. Override:
  `dbg.EnvDuration("CAPLIN_STATE_PRUNE_TIMEOUT", <computed>)`. Deadline checked at
  txn boundaries only → committed batches always consistent, resume next cycle.
- **Markers.** `kv.StatesPruneProgress`, keyed **per table name** → last-pruned
  slot. Matches the sibling `kv.StatesProcessingProgress` (db/kv/tables.go:281) —
  codebase convention is "Progress", not "Markers", and no `Caplin` prefix.
- **Fairness.** Per-cycle time budget + 34 tables: persist a rotating start-index so
  each cycle resumes where the last budget ran out. With per-table markers → natural
  round-robin.
- **Unconditional**, scoped by `ArchiveStates`. Single kill-switch:
  `dbg.EnvBool("CAPLIN_STATE_PRUNE_DISABLE", false)`. No CLI flag (caplin has enough
  tunables). Idiom: `common/dbg/dbg_env.go:69/119`.
- **Resume path safe as-is — no change to `computeSlotToBeRequested`.**
  `findNearestSlotBackwards` (`cl/antiquary/utils.go:26`) reads only
  `beacon_indicies.ReadCanonicalBlockRoot` (not a state-prune table). State load is
  `historicalReader.ReadHistoricalState` (state_antiquary.go:679) via the snapshot-
  first getter. Verified: **0** raw `tx.GetOne` in the historical reader; all 21
  state reads go through `kvGetter`.
- **Instrumentation.** Per-batch `LvlDebug` (table, from→to, keys deleted, duration);
  per-sweep `LvlInfo` summary; prometheus counters mirroring
  `mxAntiquaryPrunedBlocks` / `mxAntiquaryPruneBatchSeconds`.
- **mdbx reality.** Prune frees pages for *reuse*; the file does not shrink. Prune
  bounds *future* growth (the fix). Existing 112 GB bloat rides (ample headroom) or
  gets a one-off offline `mdbx_copy -c`. Out of scope.

TDD (repo requires red-first):
1. dump → prune → read a pruned slot (from segment) + tail slot (from DB), both correct.
2. read just above the boundary whose base BalancesDump sits below it — correct after prune.
3. E2E reconstruction with prune on = byte-identical roots to prune off.

---

## How they interact

- **Prune is load-bearing**: converts unbounded growth → a bounded ~70k-slot floor.
  Do it first; it alone solves "+50 GB/night".
- **Tier is secondary**: halves the floor (70k → 30k) and keeps file count sane via
  the merge ladder. Outward-facing → its own PR.
- `safetyMargin = 20k` is the larger remaining floor term after the tier. Trimming it
  (state is finalized, won't reorg below finality) is a cheaper floor-reducer than
  the tier, but carries its own reorg-safety call. Separate decision.

## Sequencing

1. Prune (this session's converged design) — PR, Codex-review like #22385.
2. State merge-tier `10k → 100k → 1m` — separate outward-facing PR (needs the merger).
3. (Optional) revisit `safetyMargin`.

## Key pointers

- Dump trigger + size: `cl/antiquary/state_antiquary.go:602-640`
- `planStateDump` / `DumpCaplinState`: `db/snapshotsync/caplin_state_snapshots.go:779-816`
- `RemoveOverlaps` (dupes, not merge): `caplin_state_snapshots.go:474`
- Schema / prune set: `caplin_state_snapshots.go:97-137`
- Watermark: `caplin_state_snapshots.go:264` (`idxAvailability` 522-550; `segmentsMax` 317-368)
- `coveredRangesForType`: `caplin_state_snapshots.go:268`
- Reader snapshot-first: `cl/persistence/state/state_accessors.go:32-43`
- Resume: `state_antiquary.go:645-719`; `cl/antiquary/utils.go:26`
- Beacon-block prune analog: `cl/antiquary/antiquary.go:327`
- EL prune timeout: `execution/execmodule/forkchoice.go:915-917`
- Consts: `CaplinMergeLimit=10_000` (`db/snaptype/files.go:393`); `safetyMargin=20_000`
  (`cl/antiquary/antiquary.go:47`); `SlotsPerDump=1536` (`cl/clparams/config.go:129`)
- kv sibling name: `db/kv/tables.go:281`
- dbg env idiom: `common/dbg/dbg_env.go:69` (Bool) / `:119` (Duration)
