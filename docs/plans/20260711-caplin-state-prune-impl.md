# Plan: prune the caplin indexing DB after state freezing

Design rationale, ground truth, and code pointers live in
`docs/plans/20260711-caplin-state-snapshot-tiering-and-prune.md` (the handoff).
This plan is implementation-only — it does not repeat the design. Read the handoff
first; every "why" is there.

Scope: the **prune** piece only. The `10k→100k→1m` re-tier is a separate later PR
and is out of scope here.

Branch: `awskii/caplin-state-prune` (off `origin/main`). Repo build/test:
`make lint` (repeat until clean — non-deterministic), `make erigon`, and package
tests below. TDD is mandatory (red → green); write the failing test first and
confirm it fails for the right reason.

Invariant that must hold at every step: **never delete a state row at slot ≥ that
table's contiguous-from-genesis visible coverage.** Below coverage the reader
(`GetValFnTxAndSnapshot`, `cl/persistence/state/state_accessors.go:32`) always
serves the segment, so the DB row is unreachable. Above it, the DB is the only
source.

---

### Task 1: marker table + accessors

Add the per-table prune-progress marker.

- [x] `db/kv/tables.go`: add `StatesPruneProgress = "StatesPruneProgress"` next to
  `StatesProcessingProgress` (:281) and register it in the SAME table list that
  registers `StatesProcessingProgress` (find where `StatesProcessingProgress` is
  added to the tables slice, ~:432, and add there — this is the caplin indexing DB
  config).
- [x] `cl/persistence/state/state_accessors.go`: add
  `ReadStatePruneProgress(tx kv.Tx, table string) (uint64, error)` and
  `SetStatePruneProgress(tx kv.RwTx, table string, slot uint64) error`. Key =
  `[]byte(table)`. Value encoding = same as `SetStateProcessingProgress`
  (`base_encoding.Encode64ToBytes4`). Empty value → 0.

TDD: unit test in `cl/persistence/state` — open an mdbx test DB, write markers for
two distinct table names, read them back, assert per-table isolation and that an
unset table reads 0.

Acceptance: test green; `make erigon` builds.

---

### Task 2: per-type contiguous coverage helper

Expose the safe prune boundary per type.

- [x] `db/snapshotsync/caplin_state_snapshots.go`: add
  `(s *CaplinStateSnapshots) ContiguousCoverageEnd(typeName string) uint64`. Use
  `coveredRangesForType(typeName)` (:268). Ranges are `Range{From,To}`. Sort by
  `From`, walk from slot 0, and return the `To` of the first unbroken run (a run
  breaks at the first gap). If the first range does not start at 0, return 0.

TDD: unit test in `db/snapshotsync` — drive `coveredRangesForType` via the existing
visible-segments test setup (see `caplin_state_overlap_test.go` /
`caplin_state_visibility_test.go` for how tests construct segments). Cases:
- [x] contiguous `[0,50),[50,100)` → 100
- [x] gap `[0,50),[60,100)` → 50
- [x] no coverage / first range not at 0 → 0

Acceptance: test green.

---

### Task 3: prune core

New file `cl/antiquary/state_prune.go`.

```
func pruneStateTables(
    ctx context.Context,          // carries the per-pass deadline
    db kv.RwDB,
    tables []string,              // deterministic, sorted schema table names
    boundaryFn func(table string) uint64,
    startIdx int,
    batchLimit int,               // 1000
    logger log.Logger,
) (nextStartIdx int, err error)
```

- [x] Iterate `tables` starting at `startIdx` (rotating), wrapping once.
- [x] Per table: `boundary = boundaryFn(table)`; `marker = ReadStatePruneProgress`; if
  `marker >= boundary` continue. Else open an RW txn, cursor-seek to `marker`, and
  `DeleteCurrent` while `key < boundary`, counting; at `batchLimit` deletions
  commit, persist the marker to the last-deleted-slot+1, and start a new txn.
  Keys are `base_encoding.Encode64ToBytes4(slot)`; decode to compare against
  `boundary`.
- [x] **Fully-drained tables must jump the marker to `boundary`.** Most frozen tables
  are sparse / rounded-key (EpochData rounds to epoch; `*Dump` round to
  `SlotsPerDump`; sync committees round to period — `cl/antiquary/beacon_states_collector.go`).
  After the last delete, a seek from `marker` returns nil or a key `>= boundary`
  even though `marker < boundary`. In that no-more-keys-below-boundary case,
  persist `marker = boundary` — otherwise the table is treated as backlog every
  cycle and inflates the budget forever.
- [x] Check `ctx.Err()` only at txn boundaries (never mid-txn). On deadline: commit the
  in-flight batch, return the current table index as `nextStartIdx`, nil error.
- [x] No ranged truncate in mdbx — cursor delete only. `batchLimit` keeps txns small.
- [x] Log per committed batch at `LvlDebug` (table, from→to slot, count, duration);
  per-table summary at `LvlInfo` when a table is fully drained to its boundary.
- [x] Add prometheus counters mirroring `mxAntiquaryPrunedBlocks` /
  `mxAntiquaryPruneBatchSeconds` (see `cl/antiquary/antiquary.go`).

TDD: unit test in `cl/antiquary` with an mdbx test DB and an injected `boundaryFn`
(no real `CaplinStateSnapshots` needed):
- [x] seed `kv.BlockRoot` with slots `0..99`; `boundaryFn → 50`; prune with a large
  deadline; assert slots `<50` gone, `>=50` present, marker == 50.
- [x] second call is a no-op (marker already at boundary).
- [x] tiny deadline (e.g. cancel after first batch): assert partial delete, marker
  advanced partially, and a follow-up call finishes the rest (resumable).
- [x] `batchLimit = 10` over 100 rows commits in ≥10 txns (assert via marker stepping
  or a commit counter).

Acceptance: tests green; `make erigon` builds.

---

### Task 4: budget, kill-switch, wire into the antiquary cadence

- [x] `cl/antiquary/state_prune.go`:
  - `func statePruneBudget(cfg *clparams.BeaconChainConfig, backlogSlots uint64) time.Duration`
    mirroring EL (`execution/execmodule/forkchoice.go:915-917`):
    `base = SecondsPerSlot/3`, `max = SecondsPerSlot*2/3`,
    `budget = min(base + (backlogSlots/100)*200ms, max)`. Override the whole thing
    with `dbg.EnvDuration("CAPLIN_STATE_PRUNE_TIMEOUT", <computed>)`
    (`dbg_env.go:119`) when the env var is set.
- [x] `Antiquary` struct: add `statePruneStartIdx int` and `statePruneDisabled bool`.
  Initialize `statePruneDisabled` in `NewAntiquary` from
  `dbg.EnvBool("CAPLIN_STATE_PRUNE_DISABLE", false)` (`common/dbg/dbg_env.go:69`).
  It is a **field, not a package var**, so tests can toggle it directly without
  mutating process env (which would not take effect after init anyway).
- [x] Deterministic table list: build once from `snapshotTypes.KeyValueGetters` keys,
  sorted (add a small accessor on `CaplinStateSnapshots` if needed).
- [x] **Gate: `if !s.statePruneDisabled && s.stateSn != nil`. Do NOT gate on
  `s.snapgen`** — `snapgen` only gates local dumping; an archive-state node that
  *downloaded* state snapshots must prune too. The natural scope is already
  archive-state (this loop only runs when `ArchiveStates`), and
  `ContiguousCoverageEnd == 0` makes a node with no visible coverage a safe no-op.
- [x] Two wire points in `cl/antiquary/state_antiquary.go`:
  1. **After each `commitBatch()` + fresh collector** (after ~:404) — drains
     already-visible backlog frozen by prior calls.
  2. **After `DumpCaplinState` succeeds AND `s.stateSn.OpenFolder()`** (after
     ~:620, not after the final flush) — only there is the just-frozen range
     visible in coverage; pruning before `OpenFolder` sees stale coverage and
     cannot prune what it just froze.
  At each: compute `backlog` (cheap approximation, e.g. `currentState.Slot() -
  min-marker`), `ctx, cancel := context.WithTimeout(ctx, statePruneBudget(...))`,
  call `pruneStateTables` with `boundaryFn = s.stateSn.ContiguousCoverageEnd` and
  `startIdx = s.statePruneStartIdx`, store the returned `nextStartIdx`, `cancel()`.
  `commitBatch`/`DumpCaplinState` hold no open txn at these points, so
  `pruneStateTables` can `BeginRw` freely.

TDD: kill-switch is testable via the field. Add a focused test: construct the
antiquary with `statePruneDisabled = true`, run a cycle, assert no rows deleted;
then `false`, assert rows below coverage deleted. **No `t.Skip`** — the field makes
the path directly injectable (repo forbids agent-added skips). Broader wiring is
covered by Task 5.

Acceptance: `make erigon` builds; `go test ./cl/antiquary/...` green; a prune
`LvlInfo` line is emitted on an archive-state run.

---

### Task 5: reader-correctness integration test

Prove the invariant end to end. In `cl/antiquary` (reuse the
`state_antiquary_test.go` harness) or `historical_states_reader`:

- [x] run the state antiquary far enough to freeze at least one range to `.seg`
  segments, then call `pruneStateTables` below coverage.
- [x] read a historical state at a **pruned** slot → must reconstruct correctly (served
  from segments).
- [x] read a historical state at a **tail** slot (above coverage) → correct (served
  from DB; not pruned).
- [x] balances edge (make it deterministic): `reconstructBalances`
  (`cl/persistence/state/historical_states_reader/historical_states_reader.go:648-660`)
  picks the *next* dump instead of the previous when
  `slot % SlotsPerDump > SlotsPerDump/2` and progress is past it — which would read
  an *above*-boundary dump and never exercise the pruned below-boundary path. So:
  - forward case: choose a slot with `slot % SlotsPerDump <= SlotsPerDump/2` (or
    pin `StatesProcessingProgress`) so the base dump is the below-boundary one, and
    assert it is served from the segment after its DB row was pruned.
  - reverse case: a slot that selects the next-dump path, asserting correctness
    there too.

Acceptance: both cases green.

---

### Task 6: lint, build, package tests

- [x] `make lint` (repeat until clean).
- [x] `make erigon`.
- [x] `go test ./cl/antiquary/... ./cl/persistence/state/... ./db/snapshotsync/...`

Acceptance: all green, lint clean.

---

## Out of scope (do not implement here)

- The `10k→100k→1m` state snapshot re-tier and the state segment merger.
- Trimming `safetyMargin`.
- mdbx file compaction (prune bounds growth; the file does not shrink — expected).
- Any CLI flag (kill-switch is env-only by design).
