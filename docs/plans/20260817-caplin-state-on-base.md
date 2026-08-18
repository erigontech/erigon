# Plan: caplin state snapshots on BaseRoSnapshots

Item 2 of the caplin ↔ EL snapshot parity program (epic #23024; the program doc
`docs/plans/20260729-caplin-el-snapshot-parity.md` lives on
`awskii/caplin-snapshot-parity`, not on this branch). Items 1 and 4-first-half
landed already; this plan covers only the state half.

Two stacked PRs:

- **PR-2a** — register the caplin state tables as `snaptype.Type`. Mechanical;
  file names on disk are unchanged.
- **PR-2b** — embed `BaseRoSnapshots` in `CaplinStateSnapshots` and delete the
  third lifecycle copy.

Branches: `awskii/caplin-state-snaptypes` (2a, off `origin/main`) then
`awskii/caplin-state-on-base` (2b, stacked on 2a). Ground truth pinned to main
`af897d9919f`; every file:line below is against that commit.

Build/test: `make lint` (non-deterministic — repeat until clean), `make erigon`,
and `go test ./db/snaptype/... ./db/snapshotsync/... ./db/snapcfg/... ./cl/antiquary/... ./cl/persistence/... ./cl/beacon/...`.
TDD is mandatory: write the failing test first and confirm it fails for the
right reason. New files carry a 2026 copyright header.

---

## Ground truth

Verified on `af897d9919f`, then re-verified across two plan-review rounds. Do
not re-derive; do re-check if a task finds a contradiction.

### Enums

- Caplin owns `[10, 12)` — two slots. `MinCoreEnum = 1`, `MinCaplinEnum = 10`,
  `MinBorEnum = 12`, `MaxEnum = 16` (`db/snaptype/type.go:406-410`). Bor's four
  enums are `MinBorEnum + n` (`polygon/heimdall/types.go:71-74`) and core's are
  `MinCoreEnum + n` (`db/snaptype2/block_types.go:69-77`) — no literal enum
  value anywhere, so moving a range boundary is a one-constant edit. Enums are
  never persisted.
- `MaxEnum` also sizes runtime slices: `db/snapshotsync/snapshots.go:592`
  (`make(DirtyFiles, MaxEnum)`), `:601`, and `:885` — the last allocates
  `[]VisibleSegments` of `MaxEnum` on *every* `recalcVisibleFiles` call, for
  every EL, bor and caplin instance. Raising it is allocation-only, but it is
  not "nothing changes": say so in the PR body.

### Registration

- Caplin types are not in the registry. `RegisterType` panics on anything in the
  caplin range (`db/snaptype/type.go:244-246`); `Enum.String` (`:422-435`),
  `Enum.Type` (`:437-446`) and `ParseEnum` (`:465-478`) each carry a hardcoded
  arm per caplin type.
- `RegisterType`'s body already inserts each index name into `namedTypes`
  (`:265-268`), so a caplin registration path that reuses it gives the legacy
  `blocksidecars` alias for free — `CaplinIndexes.BlobSidecarSlot` is
  `Index{Name: "blocksidecars"}` (`:157`).
- `db/snaptype` already imports `db/kv` (`type.go:36`) and `db/kv` imports
  nothing from snaptype — no cycle.
- Four state tables have an identifier that differs from the string value that
  reaches the file name (`db/kv/tables.go:275-278`):
  `ExecutionPayloadAvailabilityTable = "ExecutionPayloadAvailability"`,
  `BuilderPendingPaymentsTable = "BuilderPendingPayments"`,
  `PtcWindowTable = "PtcWindow"`,
  `LatestExecutionPayloadBidTable = "LatestExecutionPayloadBid"`.
  Register by value, not identifier. No state name collides with a registered
  core or bor type or index name.

### File names do not change

- `SnapType.FileName` falls back to `versions.Current` when handed `ZeroVersion`
  (`type.go:294-297`) and `BeaconBlocks` is `V1_1_standart`, so today's dumps
  already write `v1.1`; the published set is `v1.1` too. `Enum.String()` returns
  `Name()` verbatim, so a type registered as `PendingDepositsDump` reproduces
  the exact current name. `IdxFileNames` (`type.go:360-367`) yields
  `v1.1-<from>-<to>-<Name>.idx`, byte-identical to today's `.seg`→`.idx`
  extension swap.
- `IsFrozen` (`db/snapcfg/util.go:395-399`) resolves through `MergeLimit`, which
  returns `CaplinMergeLimit` = 10,000 (`files.go:312`) for any caplin enum. Node
  dumps are 50k wide (`blocksPerStatefulFile = CaplinMergeLimit * 5`,
  `state_antiquary.go:625`), so `50000 >= 10000` → frozen, matching today's
  hardcoded `frozen: true` (`caplin_state_snapshots.go:358`). No behavior change
  here. It does block item 3's merge tier, which needs the `MergeSteps`
  treatment before it can merge a 50k file.

### `BlocksAvailable()` — exact formula, not "a min over types"

`caplin_state_snapshots.go:263-265` is `min(s.segmentsMax.Load(), s.idxMax.Load())`,
and the two halves use different conventions:

- `segmentsMax` is set in `OpenList` (`:384-388`) to the **last listed file's**
  `To - 1` — a load-order artifact, not a max and not a min.
- `idxMax` is `idxAvailability()` (`:547-575`), a min over types of
  `segs[len-1].to` — **no `-1`**.

The base's `idxAvailability` (`snapshots.go:1039-1041`) uses `to - 1` and reads
only `s.enums[0]`. So a naive swap shifts the value by one *and* changes which
types it accounts for. Both matter, because the 13 consumers split into floors
and ceilings:

| site | use |
|---|---|
| `cl/antiquary/state_antiquary.go:113` | floor — reconstruction resume point |
| `:130`, `:133`, `:136`, `:144` | guard / log bound |
| `:141` | **ceiling** — `for slot := 0; slot <= BlocksAvailable(); slot++` with a hard `return false, fmt.Errorf("segment not found for slot %d", slot)` at `:148-149` |
| `:626`, `:679` | floor |
| `cl/antiquary/antiquary.go:214` | log |
| `cl/persistence/state/historical_states_reader/historical_states_reader.go:92` | **ceiling** — `max(latestProcessedState, …)`, then `slot > latestProcessedState` returns nil |
| `:489`, `:496` | availability reporting |
| `cl/beacon/handler/duties_proposer.go:290-295` | **ceiling** — public Beacon API; `max(stageStateProgress, …)`, then `expectedSlot > stageStateProgress` returns HTTP 400 |

A value one too high fails `state_antiquary.go:141` outright, lets the reader
serve slots the lagging table has no data for, and turns a clean 400 on
`getHistoricalProposerDuties` into a read of a slot no table covers. A
max-over-types does the same. Pin the number, do not describe it.

### Visibility

- `alignMin` must be false. `recalcVisibleFiles` with `alignMin=true` clamps
  every type to the global minimum and empties *all* types when any one has no
  visible segments (`snapshots.go:901-917`). Caplin state hits that routinely:
  `planStateDump` plans per type and `errIncompleteStateRange` skips a range for
  `BlockRoot`/`StateRoot` while the other 31 tables advance.
- The base gap-truncates: `RecalcVisibleSegments` (`snapshots.go:852-863`) drops
  everything after the first gap. Caplin's `recalcVisibleFiles` (`:448-491`)
  does not. This is a real capability for reads and a trap for the dump planner
  — see Task 10.
- `frozen` no longer gates reclaim. It is read only by the merger
  (`db/snapshotsync/merger.go:285,288`); the mvcc substrate (#21397, #22246,
  #22365, #22661) replaced the path where a frozen segment skipped refcounting.
  Embedding the base therefore gives live-safe reclaim with no extra work.
- `DirtySegment.filePath` (`snapshots.go:214`) is written in exactly one
  production place: caplin state's own `OpenList` (`caplin_state_snapshots.go:359`).
  The base's `Open` (`snapshots.go:342-351`) joins `dir` with `FileName()` and
  never sets the field. Anything reading `src.filePath` breaks silently when
  that `OpenList` goes — see Task 9.

### Two live bugs, both benign only until reclaim works

- `View` takes `visibleSegmentsLock` (`:626`) while `recalcVisibleFiles` takes
  `visibleLock` (`:454`), so no writer holds the lock the reader takes. The
  *reportable* race is narrower than the lock mismatch: `s.visible` is a
  `sync.Map`, so concurrent `Range`/`Store` is detector-clean. The genuine
  unsynchronized access is that `OpenList` defers `recalcVisibleFiles` at `:336`
  **before** deferring the unlock at `:339`, so LIFO runs the recalc after the
  lock is released, while it reads `sn.Decompressor`/`sn.indexes` through
  `isIndexed` (`:461`). The writer that makes it observable is
  `closeWhatNotInList` (`:600-614` → `closeAndDropNotProtected`,
  `snapshots.go:1418-1421`), which calls `close()` — nilling `indexes`
  (`snapshots.go:369`) and `Decompressor` (`:356`) — on any segment absent from
  the passed list.
- `CaplinStateView.VisibleSegments` reads `v.s.visible` live (`:665`) instead of
  what the view pinned.

### Unknown published types

- `BlockProposers` is the string value of `kv.Proposers` (`db/kv/tables.go:257`).
  It has no `KeyValueGetter` and no read or write call site, but it **is** a live
  entry in `ChaindataTables` (`:433`), so the constant is not dead and deleting
  it is a schema change, not a cleanup. Out of scope here.
- Production stopped at slot 10,900,000 while live types are published to
  13,500,000; 18 entries / 99.3 MiB remain in the live mainnet toml, 98.0 MiB of
  it the single `000000-010500` pair (sizes measured by HEAD against the CDN at
  design time). The vendored fixture
  `db/snapshotsync/testdata/mainnet_preverified.toml` carries exactly those 18
  lines — `caplin/v1.1-000000-010500` through `caplin/v1.1-010850-010900` — and
  is seeded into the registry for every test in `db/snapshotsync`
  (`main_test.go:44-47`). That is the realistic test bed.
- `Preverified.Typed` keeps them because its caplin branch
  (`db/snapcfg/util.go:173-186`) checks only the version, never the type name —
  and that independence from the `types` argument is a property to preserve, not
  an oversight. Production calls `pv.Typed(knownTypes[networkName])` (`:94`), and
  every ethereum-family network registers `BlockSnapshotTypes` plus
  `snaptype.CaplinSnapshotTypes` (`snaptype2/block_types.go:43-54`) — two caplin
  entries, both blocks. The generic branch drops everything absent from that
  list (`:223-240`), so routing state files through an allow-list check drops all
  33 of them.
- **Three** unguarded nil-`Type` derefs. `ParseFileName` returns `ok=true` with a
  nil `Type` on the caplin path (`db/snaptype/files.go:224-232`):
  - `AllTypedSegments` (`snapshots.go:1156`) → `f.Type.Enum()`, called from the
    base's `OpenFolder` (`:1271`) and `OpenSegments` (`:1331`). **This is the
    path PR-2b lands on first**: after Task 9 every `stateSn.OpenFolder()` goes
    through it — `state_antiquary.go:127,616,652`, `antiquary.go:175,208`,
    `capcli/cli.go:701,1389,1398`, `snapshots_cmd.go:3154`.
  - `openSegments` (`:1196-1202`) → `HasType` → `in.Enum()` (`:749-751`).
  - `FileInfo.GetGrouping()` (`files.go:337-340`) → `f.Type.Name()`, reached from
    the base `RemoveOverlaps` (`:1458-1463`) → `snaptype.Segments(s.dir)` →
    `findOverlaps` (`:124`).

  All three consume `ParseDir` output (`files.go:432-436`), which keeps
  nil-`Type` caplin entries. One guard there closes all three; guarding
  `GetGrouping` alone closes only the last.

### The published set covers 23 of the 33 configured tables

`db/snapshotsync/testdata/mainnet_preverified.toml` carries exactly the 23
pre-GLOAS state types (plus `BlockProposers`). **None of the 10 GLOAS tables is
published.** `NewCaplinStateSnapshots` seeds `visible` for all 33 keys
(`:207-209`), and `idxAvailability` sets `min = 0` and stops at the first type
with zero visible segments (`:560-565`). So on a **download-only** mainnet node
— 23 types with files, 10 without — `idxMax = 0` and `BlocksAvailable() = 0`,
and every consumer degrades to the DB. A node that produces its own snapshots
eventually has all 33, because `planStateDump` plans per type and
`dumpCaplinState` writes empty words for slots with no rows; the snap36 fleet
produces rather than downloads, which is why this has not been noticed.

This is pre-existing and arguably conservative-correct — a global scalar cannot
honestly report coverage a table does not have, which is why per-table
`ContiguousCoverageEnd` is the API pruning already uses. But it means this plan
faithfully refactors a subsystem that is inert on download-only mainnet nodes,
with every new test green, because every fixture in it uses one or two tables
that both have files. Task 8 carries the fixture that reproduces it.

**Resolved: the data lands here, the behavior change does not.** Each state type
declares the fork that introduced it in PR-2a (Task 2) — nearly free, since the
declarations are being written anyway — and `BlocksAvailable` keeps its exact
current value through PR-2b, so 2b stays a refactor whose numbers provably do
not move. A later **PR-2c** flips the computation to a min over *expected*
types, where a type introduced at a fork the chain has not scheduled is not
expected at all. On mainnet `GloasForkEpoch` is `math.MaxUint64`
(`cl/clparams/config.go:1046`), so the 10 GLOAS tables drop out and the number
reflects the 23 that exist.

The fork axis is the one with per-type granularity. The flag axis has none:
`ArchiveStates` does not gate the snapshots object at all. The
`if config.ArchiveStates` block at `cmd/caplin/caplin1/run.go:561-565` guards
only `ReadValidatorsTable`; `NewCaplinStateSnapshots` sits outside it at `:566`
and runs unconditionally, with the flag passed on to `NewAntiquary` at `:567`
to gate antiquation. So every node configures all 33 types whatever its flags
say, and the other caplin retention flags govern blocks, blobs and columns
rather than state tables.

Note what this does and does not cover. Tables always enter the code before
their fork activates, so "fork not scheduled" is the *recurring* shape and
fork-awareness inoculates against it repeating at every fork. It does not cover
a publisher that lags after a fork has activated — only per-type availability or
self-dumping does, and that is Decision B territory.

`cl/antiquary/beacon_states_collector.go` is the authoritative mapping, in two
gates per fork — the dump path (`:187` Altair, `:203` Electra, `:214` Gloas) and
the diff path (`:300` Electra, `:305` Gloas):

| fork | tables |
|---|---|
| Altair | `InactivityScores`, `CurrentSyncCommittee`, `NextSyncCommittee` |
| Electra | `PendingDeposits`, `PendingConsolidations`, `PendingPartialWithdrawals` and their three `*Dump` variants |
| Gloas | all 10 GLOAS tables |
| genesis | everything else |

### Blast radius

Non-test construction sites of `NewCaplinStateSnapshots`: `cmd/caplin/caplin1/run.go:566`,
`cmd/capcli/cli.go:700`, `:1388`, `cmd/utils/app/snapshots_cmd.go:3153`. Test
sites: `db/snapshotsync/caplin_state_overlap_test.go:111`,
`cl/antiquary/state_prune_reader_test.go:84,271`,
`cl/antiquary/state_prune_test.go:273,292,323,358`. Also touching state types:
`cmd/utils/app/snapshots_cmd.go:2113`, `:3553`,
`cmd/utils/app/publishable_check_test.go:230`.

---

# PR-2a — caplin state tables as snaptype.Type

Goal: after this PR every caplin state file on disk resolves to a registered
`snaptype.Type`, and no file is written differently. Lifecycle code does not
move; `CaplinStateSnapshots` still keys its own maps by string.

---

### Task 1: widen the caplin enum range

**Files:**
- Modify: `db/snaptype/type.go`
- Modify: `db/snaptype/enum_registry_test.go`

- [x] `db/snaptype/type.go:406-410`: `MinBorEnum` 12 → 50, `MaxEnum` 16 → 54.
  `MinCoreEnum` and `MinCaplinEnum` unchanged. Caplin owns `[10, 50)` — 40 slots
  for 35 types, five spare so the next fork adding tables does not have to move
  bor again.
- [x] confirm nothing hardcodes an enum literal: bor is `MinBorEnum + n`
  (`polygon/heimdall/types.go:71-74`), core is `MinCoreEnum + n`
  (`db/snaptype2/block_types.go:69-77`).
- [x] write a test over the constants alone — `MinCoreEnum < MinCaplinEnum <
  MinBorEnum < MaxEnum` and `MinBorEnum - MinCaplinEnum >= 35`. Do **not**
  reference `CaplinStateSnapshotTypes` here: it does not exist until Task 2 and
  a forward reference is a compile error, which fails the package build rather
  than the assertion.
- [x] run `go test ./db/snaptype/...` — must pass before task 2

---

### Task 2: declare the 33 state types

**Files:**
- Create: `db/snaptype/caplin_state_types.go`
- Create: `db/snaptype/caplin_state_types_test.go`

- [x] new file declaring one `SnapType` per caplin state table, enum
  `MinCaplinEnum + 2 + i`, `name` = the table's **string value**, `versions` =
  `version.V1_1_standart`, `indexes` = `[]Index{{Name: <same name>, Version: version.V1_1_standart}}`.
  The set is exactly the 33 keys of `MakeCaplinStateSnapshotsTypes`
  (`db/snapshotsync/caplin_state_snapshots.go:100-140`): 23 pre-GLOAS (`:103-125`)
  plus 10 GLOAS (`:127-137`). Mind the four identifier/value mismatches listed
  in Ground truth.
- [x] export each type as its own package-level var, not only through the slice
  — Task 9 needs to name one directly.
- [x] declare each type's **introducing fork**, per the mapping in Ground truth.
  Nothing reads it in this PR; PR-2c does. Declaring it here is why 2c is small,
  and why the next fork that adds tables annotates them at the moment it declares
  them instead of discovering the omission on a mainnet node.
  - expose it as an exported package-level lookup —
    `CaplinStateIntroducedIn(enum Enum) clparams.StateVersion` in the same file —
    **not** as a `SnapType` field. Every `SnapType` field is unexported
    (`type.go:225-231`) and the `Type` interface (`:208-223`) has no fork
    accessor, so a field is unreadable from `db/snapshotsync`, where PR-2c's
    check lives; 2c would end up restating the table→fork map. Do not widen the
    `Type` interface either — `SnapType` is its only implementer, but the concept
    is caplin-state-only and every EL and bor type would carry it.
  - value type `clparams.StateVersion`, so the constants are the same ones the
    collector gates on. `db/snaptype` importing `cl/clparams` is clean at the
    first level — none of clparams' erigon imports touch `db/snaptype` or
    `db/snapcfg` — but confirm with a build; if it does cycle, use the
    underlying integer and keep the values identical.
  - default is genesis/phase0, i.e. always expected — including for an enum the
    lookup does not carry. Annotate only what the collector actually gates. A
    wrong annotation excludes a table that should be required, so unknown must
    fail toward "expected", never away from it.
- [x] write a test asserting each type's declared fork matches the gate the
  collector applies to that table, reading it through `CaplinStateIntroducedIn`
  rather than any private field — this is the guard that keeps the two from
  drifting, since they live in different packages
- [x] export `CaplinStateSnapshotTypes []Type` in enum order. Leave
  `CaplinSnapshotTypes` at two entries — `freezeblocks/caplin_snapshots.go:78`
  builds the *blocks* `BaseRoSnapshots` from it, and `caplinsnapschema:19`,
  `snapshots_cmd.go:2117`, `snaptype2/block_types.go:46` all iterate it.
- [x] `IsCaplinType` (`caplin_types.go:38-46`) becomes a range check on the enum
  instead of a linear scan.
- [x] do **not** widen `allSnapshotTypes()` (`enum_registry_test.go:41-47`) here.
  `TestEnumRoundTrip` (`:49-64`) walks that list and calls `enum.String()`, which
  for an unregistered enum falls to `default:`, misses `registeredTypes` and
  **panics** (`type.go:432`), killing the package's test binary — and after Task 1
  moves `MinBorEnum` to 50 these enums are genuinely unoccupied, so nothing
  accidentally covers them. Registration is Task 3, so the widening is Task 3's.
  This task's own assertions read only `Name()` and `Enum()`, which need no
  registry.
- [x] assert enum uniqueness and contiguity **within `CaplinStateSnapshotTypes`
  itself** — a local walk over the slice, not through `allSnapshotTypes()`
- [x] write a test asserting each type's `Name()` equals its `kv` constant and
  that the 33 names match `MakeCaplinStateSnapshotsTypes`'s key set exactly —
  the guard that a new fork table cannot be added to one place and forgotten in
  the other
- [x] write a test asserting `len(CaplinSnapshotTypes) == 2`, the invariant the
  four call sites above depend on
- [x] move Task 1's width assertion here now that both lists exist:
  `len(CaplinSnapshotTypes) + len(CaplinStateSnapshotTypes) <= MinBorEnum - MinCaplinEnum`
- [x] run `go test ./db/snaptype/...` — must pass before task 3

---

### Task 3: register caplin types instead of special-casing them

**Files:**
- Modify: `db/snaptype/type.go`
- Modify: `db/snaptype/caplin_types.go`
- Modify: `db/snaptype/caplin_state_types.go`
- Modify: `db/snaptype/enum_registry_test.go`

- [x] extract `RegisterType`'s body (`type.go:240-270`) into an unexported
  `register(...)` and add `RegisterCaplinType` alongside it, asserting
  `MinCaplinEnum <= enum < MinBorEnum`. Split the four guards explicitly:
  duplicate-enum, duplicate-name and out-of-range move into `register`; the
  **caplin-range panic (`:244-246`) stays in the `RegisterType` wrapper only**.
  Moving it into `register` makes `RegisterCaplinType` panic on all 35 caplin
  types and kills every binary at init. Duplicate-name must stay in `register`
  or Task 3's own duplicate-name test is vacuous for state types.
- [x] register `BeaconBlocks`, `BlobSidecars` and the 33 state types through
  `RegisterCaplinType` in an `init`.
- [x] delete the caplin arms from `Enum.String` (`:422-435`), `Enum.Type`
  (`:437-446`) and `ParseEnum` (`:465-478`). Do **not** hand-write a
  `blocksidecars` alias: `register` already inserts index names into `namedTypes`
  (`:265-268`) and that index is named `blocksidecars` (`:157`). Assert the
  alias in a test rather than restating it in code.
- [x] now that the types are registered, add `CaplinStateSnapshotTypes` to
  `allSnapshotTypes()` (`enum_registry_test.go:41-47`) so `TestEnumRoundTrip` and
  `TestEnumUniqueness` cover the 33, and extend `TestEnumRangeDisjointness` to
  walk them. This is deliberately here and not in Task 2 — `TestEnumRoundTrip`
  panics on an unregistered enum, so the widening cannot precede registration.
- [x] update the stale docstring on `TestRegisterTypePanicsOnCaplinName`
  (`enum_registry_test.go:104-105`) — caplin names now do live in `namedTypes`.
- [x] write a test asserting `Enum.String()` round-trips exact case
  (`PendingDepositsDump`, not lowercased) for every state type, since the file
  name derives from it
- [x] write a test asserting `ParseEnum` resolves lowercased state names, that
  `blocksidecars` still resolves to `BlobSidecars`, and that duplicate enum and
  duplicate name registrations still panic
- [x] run `go test ./db/snaptype/...` — must pass before task 4

---

### Task 4: never nil-deref an unknown published type

Three call paths deref `f.Type` on a `FileInfo` that `ParseFileName` returned
with `ok=true` and a nil `Type`. All three consume `ParseDir`, so one guard
there closes all three — and it must be there: guarding `GetGrouping` alone
leaves `AllTypedSegments:1156` open, which is the path PR-2b lands on first.

**Files:**
- Modify: `db/snaptype/files.go`
- Modify: `db/snapshotsync/snapshots.go`
- Modify: `db/snaptype/files_test.go`
- Create: `db/snapshotsync/base_unknown_type_test.go`

- [ ] write a test that puts a `v1.1-000000-000050-BlockProposers.seg` in a
  caplin snapshot dir and calls `BaseRoSnapshots.OpenFolder()` over it. Use
  `datadir.New(t.TempDir()).SnapCaplin` — `IsCaplin` keys on the literal string
  `"caplin"` in the dir or file name (`files.go:130-135`), so a bare
  `t.TempDir()` makes `ParseFileName` return `ok=false` and the test passes for
  the wrong reason. Confirm the panic is in `AllTypedSegments` before fixing.
- [ ] drop nil-`Type` entries in `parseDirEntries` (`files.go:417-437`), next to
  the existing `if !ok { continue }`. `ParseDir` has one caller, `FilesWithExt`
  (`:111`). This is safe not because of extension ordering — `FilesWithExt` runs
  `ParseDir` before `FilterExt`, so the guard sees salt either way — but because
  every consumer filters by extension and salt is `.txt`, so no consumer ever
  receives it. The salt branch (`:145-151`) does return `ok=true` with a nil
  `Type` when `db/snaptype2` is not linked in; that is why the guard belongs
  here and not inside `ParseFileName`.
- [ ] also guard `f.Type == nil` in `openSegments` (`snapshots.go:1196-1202`),
  before `HasType`. `OpenFolder` derives its list from `AllTypedSegments`, so
  after the `ParseDir` guard this path is unreachable through it — but
  `OpenList(fileNames, optimistic)` (`:1303`) is public, re-parses raw strings
  with no `ParseDir` in between, and `CaplinStateSnapshots` inherits it after
  Task 9. Two lines of defence in depth on a public entry point.
- [ ] write a second test driving `findOverlaps` over the same directory,
  covering the `GetGrouping` path, asserting no panic and that the real segments
  are still grouped correctly
- [ ] write a test asserting `ParseFileName(<caplin dir>, "v1.1-000000-000050-PendingDepositsDump.seg")`
  returns a non-nil `Type`, `From=0`, `To=50000`, and
  `CaplinTypeString="PendingDepositsDump"` — the last is what
  `CaplinStateSnapshots.OpenList` keys on today and must not shift
- [ ] note in the PR body: `SegmentsCaplin`'s two `f.Type != nil` guards
  (`snapshots.go:1953,1956`) are dead — it runs on `dirs.Snap`, where `IsCaplin`
  is false and `ParseFileName` already returns `ok=false` for an unresolvable
  name. Leave them; just don't let a reader think they are the protection.
- [ ] run `go test ./db/snaptype/... ./db/snapshotsync/...` — must pass before task 5

---

### Task 5: stop downloading dead caplin types

**Files:**
- Modify: `db/snapcfg/util.go`
- Modify: `db/snapcfg/util_test.go`
- Modify: `db/snapshotsync/caplin_preverified_window_test.go`

- [ ] `db/snapcfg/util.go:173-186`: resolve the entry against the **global
  snaptype registry** and drop it when that does not know it; keep the
  version-window check unchanged. Note the shape: at `:168` `name` is the
  post-`Cut` remainder (`000000-000050-BlockProposers.seg`), and `ParseFileName`
  starts by parsing a version off the front (`files.go:155`), so it needs the
  basename reassembled and a dir argument containing `"caplin"`.
- [ ] the caplin branch stays independent of the `types` argument. Do **not**
  mirror the generic branch's allow-list walk (`:223-240`) — production passes
  `knownTypes[networkName]`, which carries two caplin entries and no state type
  (Ground truth), so an allow-list check drops all 33 and a download-only node
  fetches no state snapshots at all. A test that passes
  `CaplinStateSnapshotTypes` explicitly is blind to this; the regression below is
  the one that catches it.
- [ ] report the dropped names the same way the existing filter does, so an
  operator sees what was skipped instead of silently losing files.
- [ ] write a test feeding a `Preverified` containing `BlockProposers` plus two
  known caplin types, asserting the unknown one is dropped and the known ones
  survive with their versions intact
- [ ] write the production-shaped regression **in `db/snapshotsync`**, not in
  `db/snapcfg`: `snapcfg.KnownCfg(networkname.Mainnet)` over the vendored mainnet
  fixture, asserting `BlockRoot` and `PendingDeposits` entries survive while
  `BlockProposers` is dropped. Passing the state types explicitly would defeat
  it. It cannot live in `db/snapcfg/util_test.go`, which is `package snapcfg`:
  `knownTypes` is populated only by `db/snaptype2` and `polygon/heimdall`, both
  of which import `db/snapcfg`, so importing either from an internal snapcfg test
  is a cycle Go rejects and `knownTypes[mainnet]` is nil in that binary —
  `Typed(nil)`, silently passing. `db/snapshotsync` has both halves already: its
  `TestMain` seeds the fixture (`main_test.go:44-47`) and it links `db/snaptype2`
  (`snapshotsync.go:35`), so that init has run.
- [ ] write a test asserting a caplin entry outside the version window is still
  dropped for the old reason, so the two filters stay independent
- [ ] check `db/snapshotsync` for assertions sensitive to caplin entry counts —
  `main_test.go:44-47` seeds the real mainnet fixture, which carries the 18
  `BlockProposers` lines, into the registry for every test in that package
- [ ] run `go test ./db/snapcfg/... ./db/snapshotsync/...` — must pass before task 6

---

### Task 6: retire the workarounds the types replace

**Files:**
- Modify: `db/snapshotsync/caplin_state_snapshots.go`
- Modify: `db/snapshotsync/caplinsnapschema/caplin_snap_schema.go`

- [ ] `caplin_state_snapshots.go:690-692`: build the segment name from the state
  type itself instead of `BeaconBlocks.FileName(...)` plus
  `strings.ReplaceAll(segName, "beaconblocks", snapName)`.
- [ ] `caplinsnapschema/caplin_snap_schema.go:27-32`: take each state schema's
  data and accessor versions from its own type rather than borrowing
  `BeaconBlocks`'s.
- [ ] do **not** touch `kv.Proposers`. It is an entry in `ChaindataTables`
  (`db/kv/tables.go:433`), so removing it drops a table from the chaindata
  schema — a separate change with its own risk, unrelated to registering types.
- [ ] write a test asserting the name `dumpCaplinState` produces for a given
  table and range is byte-identical to the current one, pinning the expected
  string literally — this is the compatibility guarantee of the whole PR
- [ ] run `go test ./db/snapshotsync/...`, then `make lint && make erigon`

---

# PR-2b — CaplinStateSnapshots on BaseRoSnapshots

Goal: one lifecycle. `CaplinStateSnapshots` embeds `*BaseRoSnapshots` and keeps
only what is genuinely caplin-shaped: the dump planner, the per-table read path,
and a `BlocksAvailable` whose numeric value does not move.

---

### Task 7: pin the two lifecycle bugs with failing tests

**Files:**
- Create: `db/snapshotsync/caplin_state_lifecycle_test.go`

- [ ] write a test that opens a `CaplinStateSnapshots` over two segments of one
  table, takes a `View`, then triggers a visible-set recalculation, and asserts
  the view still reports the segments it pinned. It fails today because
  `CaplinStateView.VisibleSegments` reads `v.s.visible` live (`:658-670`).
- [ ] write a `-race` test whose driver actually writes. `View` never touches
  `sn.Decompressor`/`sn.indexes` (it wraps `VisibleSegments.BeginRo`,
  `snapshots.go:471-473`), and a repeated `OpenList` over the *same* list writes
  nothing — `openSegIfNeed` returns early on `Decompressor != nil` (`:322-325`)
  and `openIdxIfNeedForCaplinState` on `indexes[0] != nil` (`:417-420`). Race
  two goroutines calling `OpenList(all)` and `OpenList(subset)`: the subset call
  drives `closeWhatNotInList` → `close()`, nilling the fields the other's
  post-unlock deferred recalc is reading through `isIndexed`.
- [ ] confirm both fail for the stated reason and record the output in the task
  notes; they go green in Task 9, which lands with them
- [ ] run `go test -race ./db/snapshotsync/ -run CaplinState`

---

### Task 8: pin BlocksAvailable numerically

**Files:**
- Modify: `db/snapshotsync/caplin_state_lifecycle_test.go`

- [ ] the existing helper `openTestCaplinStateSnapshots`
  (`caplin_state_overlap_test.go:105-115`) is single-table. Add a multi-table
  helper here. Pick its tables freely: Task 9 derives `baseSegType` from the
  types passed in, so it is a member of the list by construction and
  `newRoSnapshots`'s membership panic (`snapshots.go:586-588`) is unreachable.
  Do not hardcode a particular table to satisfy it.
- [ ] write a test with two tables at equal height asserting the **exact**
  `BlocksAvailable()` value with `require.Equal(t, uint64(N), ...)`, not a
  relative claim. Today's value is `min(segmentsMax, idxMax)` where `idxMax` is
  min-over-types of `to` with no `-1` (`caplin_state_snapshots.go:560-575`). The
  base's is `idxMax` alone, and its `idxMax` is `enums[0]`'s `to - 1`
  (`snapshots.go:1036-1040`). The `-1` alone would only lower the number; what
  raises it is `enums[0]` in place of min-over-types, and then
  `state_antiquary.go:141`'s `slot <= BlocksAvailable()` asks
  `VisibleSegment(slot, kv.StateEvents)` for a slot a shorter table does not
  cover and hard-errors at `:148-149`. Reproduce min-over-types with no `-1`;
  do not "fix" this with a ±1.
- [ ] write a test where one table is frozen lower than the other, again
  asserting the exact value, so a change from min-over-types to `enums[0]` or to
  a max is caught
- [ ] write a test asserting a table with zero segments drives it to 0
- [ ] write the fixture that reproduces mainnet: N tables configured, N-1 with
  files, one with none — the 33-configured / 23-published shape from Ground
  truth. Assert the resulting `BlocksAvailable()` explicitly. This is the case
  no other fixture in this plan covers and the one a real download-only node
  runs in; whichever way the decision goes, the test is where it gets recorded.
- [ ] run `go test ./db/snapshotsync/ -run CaplinState` — these must pass before
  task 9

---

### Task 9: embed the base, delete the third lifecycle copy, rebase the accessors

One task. Splitting it leaves a tree that does not compile *and* a tree that
compiles against shadowed fields silently — `CaplinStateSnapshots` shadows the
base on `dir`, `cfg`, `logger`, `idxMax` and `visible`, and every accessor
listed below reads a field or a view shape that this task removes.

**Files:**
- Modify: `db/snapshotsync/caplin_state_snapshots.go`
- Modify: `db/snapshotsync/caplin_state_overlap_test.go`
- Delete: `db/snapshotsync/caplin_state_snapshots_test.go`
- Modify: `cmd/utils/app/snapshots_cmd.go`

- [ ] replace the lifecycle fields (`:145-170`) with an embedded
  `*BaseRoSnapshots`, keeping `snapshotTypes` and `tmpdir`. Drop `beaconCfg` —
  set at `:206`, never read; PR-2c re-adds it, because resolving a declared fork
  to an activation slot needs the fork epochs. Deleting dead state is this
  task's job, so delete it and let 2c bring back a field it actually reads. The
  constructor parameter stays either way, since the signature does not change.
  Keep `Salt`: it is read at `:880` even though nothing ever assigns it, so it
  is always 0 (decision F — salt stays 0 for the whole epic, and nothing here
  may assign it).
- [ ] `NewCaplinStateSnapshots` builds the base with `alignMin=false`. Derive
  the type list from the passed `snapshotTypes.KeyValueGetters` keys via
  `ParseEnum` — **not** from the global `CaplinStateSnapshotTypes`, which would
  make `caplin_state_overlap_test.go:105-115`'s single-table map silently open
  all 33. Panic on a key that does not resolve; skipping it silently would leave
  `s.dirty[enum]` absent and drop that table's segments with no error.
- [ ] `baseSegType` must be a member of the derived list — `newRoSnapshots`
  panics otherwise (`snapshots.go:586-588`) — so it cannot be a fixed
  `snaptype.BlockRoot` while the list is derived. **Sort the resolved types by
  enum, then take `[0]`**; map iteration order is random, so "the first key" is
  not a definition. Nothing depends on the choice: `baseSegType` is read only by
  `View.Ranges` (`:1866`, `:1894`), which caplin state never calls. An empty
  `KeyValueGetters` map panics with a message naming it, rather than reaching
  `panic("baseSegType is nil")` (`:583-585`) — the constructor returns
  `*CaplinStateSnapshots` alone (`:184`) and the signature stays, so an error
  return is not available. Both this and the unresolvable-key case are programmer
  errors; test both panic messages.
- [ ] note the cross-PR coupling: panicking on an unresolvable key is only
  unreachable because Task 2's "the 33 names match `MakeCaplinStateSnapshotsTypes`'s
  key set exactly" test holds. That test is in PR-2a and this panic is in PR-2b;
  weakening the former arms the latter.
- [ ] keep the constructor signature unchanged so the four non-test sites need
  no edit.
- [ ] delete `OpenList`, `OpenFolder`, `recalcVisibleFiles`, `idxAvailability`,
  `closeWhatNotInList`, `Close`, `openSegIfNeed`, `openIdxForCaplinStateIfNeeded`,
  `openIdxIfNeedForCaplinState`, `isIndexed`, `listAllSegFilesInDir` and the
  offline `RemoveOverlaps` (`:495-545`).
- [ ] `RemoveOverlaps` is the one deletion that changes a signature — the
  promoted base version takes `onDelete func([]string) error` (`snapshots.go:1458`)
  where the offline one took nothing. Update all four no-arg call sites **here**,
  passing `nil`: `caplin_state_overlap_test.go:152,178,197` and
  `snapshots_cmd.go:3553`. `nil` is the final value at all four — see Task 11.
  Deferring the mechanical edit to Task 11 leaves the tree
  uncompilable from this task through that one, and this task's own gate cannot
  run. Every other deletion is either unexported or signature-identical on the
  base (`OpenList`, `OpenFolder`, `Close`), so no other caller moves.
- [ ] **delete `caplin_state_snapshots_test.go` here**, for the same reason. Its
  only test hand-builds `&CaplinStateSnapshots{dirty: map[string]...}` (`:33`)
  and calls `closeWhatNotInList` (`:35`) — both removed by this task, so the
  package stops compiling with `unknown field dirty` and
  `closeWhatNotInList undefined`, killing this task's gate and Tasks 10 and 11's
  with it. Task 12 lists the file, but three dead gates cannot wait for it. The
  behavior is covered by the base's own close-and-retire tests.
- [ ] **retire `TestCaplinStateRemoveOverlapsKeepsSubsetWithoutIndexedSuperset`
  (`caplin_state_overlap_test.go:187-201`) here too.** It pins a guarantee the
  base does not make: the offline version restricted candidate supersets to
  indexed segments, while `findOverlaps` (`snapshots.go:111-144`) compares only
  `GetRange()` and `GetGrouping()`. With the superset `[0,150k)` unindexed and
  the subset `[100k,150k)`, `iTo >= jTo && iFrom <= jFrom` holds and the subset
  is unlinked, so `:199`'s `require.FileExists(subSeg, ...)` goes red. Deleting
  the assertion silently is what Task 12 forbids — so retire the whole test with
  the reason: the guarantee moves to the caller, which Task 11 satisfies by
  building the missing indices before the call. The test two above it
  (`:180`, indexed superset) still holds and stays. Say both retirements in the
  PR body.
- [ ] delete `LS` (`:222-240`). It reads `view.roTxs` and has no caller —
  `snapshots_cmd.go:3085` is `freezeblocks.CaplinSnapshots.LS()`, `:3075` is
  `agg.LS()`.
- [ ] `CaplinStateView` becomes a thin wrapper over the base `*View`;
  `VisibleSegments(tbl)` resolves the table to its enum through a
  `map[string]snaptype.Enum` built once in the constructor — `Get` is a per-slot
  path driven from the historical reader, so a `ParseEnum` per call does not
  belong there. `Close` (`:644-656`) iterates `v.roTxs` and must be rewritten
  with the struct, not left behind.
- [ ] rewrite `BuildMissingIndices` (`:843-889`). It does not compile after the
  swap and Task 9's "keep it caplin-owned" does not mean "leave it alone":
  `for caplinType, filesTree := range s.dirty` (`:856`) then
  `s.snapshotTypes.KeyValueGetters[caplinType]` (`:858`) assumes caplin's
  `map[string]*btree...`, while the base's `dirty` is `DirtyFiles`
  (`snapshots.go:461`, `:505`) — a slice indexed by enum, so `caplinType` is an
  `int` indexing a `map[string]`. It also calls the deleted `isIndexed(df)`
  (`:866`), whose base equivalent is `df.IsIndexed()` (`snapshots.go:291`), and
  iterating the base's `dirty` needs `dirtyLock` where today it is lock-free.
  `return s.OpenFolder()` (`:888`) is fine — same signature on the base.
- [ ] **`SegFileNames` must stop reading `src.filePath`** (`:256`). That field is
  written only by the `OpenList` this task deletes, so the signature would
  survive while the function returned empty strings — straight into
  `s.downloader.Seed(s.ctx, paths)` at `cl/antiquary/state_antiquary.go:658`,
  silently seeding nothing. Build the path from `seg.src.FileName()` and
  `s.Dir()`.
- [ ] override `BlocksAvailable()` so its numeric value is unchanged; Task 8's
  tests are the specification. **Override `SegmentsMax` and `IndicesMax` too**
  (`:214-215`) — deleting them is not available, because `LogStat` (`:217-220`)
  reads both and has a caller (`cmd/utils/app/snapshots_cmd.go:3157`). Left to
  promote, they silently become the base's max-over-types and `enums[0]`
  height, changing what the operator reads.
- [ ] rebase `coveredRangesForType` and `Get` on the base view, keeping their
  signatures. `TypeNames` (`:267-274`) reads only `snapshotTypes.KeyValueGetters`
  — leave it.
- [ ] `ContiguousCoverageEnd` must **not** open a `View` per call. `View.Close`
  → `releaseVisible` → `reclaimRetired` takes `dirtyLock.Lock()`
  (`snapshots.go:963-966`, `:980-984`), and the prune path calls it 66× per pass
  — per table in `statePruneBacklog` (`state_prune.go:78`) and again in
  `pruneStateTables` (`:139`), over 33 tables, on every antiquary iteration
  (`state_antiquary.go:621`). Today it takes `visibleLock.RLock()` with no
  writer contention. Read `s.visible.Load().segments[enum]` and copy only
  `.Range` values; a pin is not needed to read a range.
- [ ] keep `DumpCaplinState`, `planStateDump`, `dumpCaplinState` and
  `BuildMissingIndices` caplin-owned — the base's `BuildMissedIndices` takes a
  `*chain.Config` caplin does not have, the same reason PR-1 kept it for blocks.
- [ ] state owns `dirs.SnapCaplin` outright, so unlike caplin blocks it inherits
  the base's unfiltered directory scan — do not add a name filter.
- [ ] write a test asserting `SegFileNames` returns absolute, non-empty paths
  that exist on disk
- [ ] Task 7's two tests turn green here; Task 8's must stay green
- [ ] run `go test -race ./db/snapshotsync/... ./cl/antiquary/... ./cl/persistence/... ./cl/beacon/... ./cmd/utils/...` — must pass before task 10

---

### Task 10: reconcile gap truncation with the dump planner

The base gap-truncates the visible set; caplin's recalc did not. That is a
capability for reads and a trap for the planner, because
`coveredRangesForType` → `missingRanges` → `planStateDump` (`:781-819`) reads
the same set.

With `BlockRoot` at `[0,50k)` and `[100k,150k)` and `[50k,100k)` skipped by
`errIncompleteStateRange` — the exact hole this codebase produces — truncation
collapses coverage to `[{0,50k}]`, so the planner plans `[50k,150k)` and
re-dumps over the existing `[100k,150k)` file on every antiquary cycle until the
hole heals.

**Files:**
- Modify: `db/snapshotsync/snapshots.go`
- Modify: `db/snapshotsync/caplin_state_snapshots.go`
- Modify: `db/snapshotsync/caplin_state_coverage_test.go`

- [ ] split the two readers into two named accessors — `ContiguousCoverageEnd`
  currently *calls* `coveredRangesForType` (`:298`), so two backing sets need
  two functions. The planner-facing one reads dirty; `ContiguousCoverageEnd`
  keeps the visible one.
- [ ] the dirty-backed accessor is `RecalcVisibleSegments` **minus the
  gap-truncation block only** (`snapshots.go:852-863`). Keep the `IsIndexed`
  gate (`:815`), the equal-range version dedup (`:819-829`) and the subset
  suppression (`:830-843`) — a raw dirty walk returns both segments in
  `TestCaplinStateRecalcHidesInteriorSubset`
  (`caplin_state_overlap_test.go:118-133`), which asserts exactly one range for
  `[0,150k)` + `[100k,150k)`. The gap block is a contiguous trailing section
  over the local slice, so split it into a builder plus a `truncateAtGap`
  composer; `RecalcVisibleSegments` has one caller (`:890`).
- [ ] the builder's output holds `src *DirtySegment` pointers belonging to **no
  pinned generation** — it runs outside a `recalcVisibleFiles` publish, so
  nothing refcounts them. Reading `.Range` and discarding the slice is safe;
  returning these from any accessor is a use-after-munmap now that reclaim is
  live. Say so at the function, since the type looks identical to the pinned one.
- [ ] the `IsIndexed` gate is the invariant that matters, not the truncation:
  `dumpCaplinState` never calls `TryAcquireRange`, so a `.seg` written but not
  yet indexed can enter dirty, and only that gate keeps it out of the planner.
  Retired segments leave dirty before unlink, so they cannot leak in.
- [ ] read dirty through `WalkDirtySegments` (`snapshots.go:1076-1080`), which
  takes `dirtyLock.RLock()` — do not reach into the btrees directly.
  `DumpCaplinState` calls the accessor once per type (`:826-828`), sequentially
  and never nested, so a writer arriving mid-loop delays but cannot wedge, and
  no caller of `DumpCaplinState` (`state_antiquary.go:638`, `capcli/cli.go:1395`)
  holds `dirtyLock`. The one way to deadlock is for the walk callback to
  re-enter the lock — and opening or closing a `View` inside it does exactly
  that, since `View.Close` can reach `dirtyLock.Lock()` through
  `reclaimRetired`. `sync.RWMutex` is not reentrant: state the constraint at the
  callback.
- [ ] keep `ContiguousCoverageEnd` on the visible set. Not because the numbers
  differ — it truncates at the first gap itself (`:297-311`), so both inputs
  give the same answer today — but because the visible set is by construction
  what reads see, which keeps the prune boundary from desyncing from reads if a
  future visibility filter is added.
- [ ] write a test for the hole scenario: dump-plan over `[0,50k)` + `[100k,150k)`
  and assert the planner proposes only `[50k,100k)`, not a re-dump of
  `[100k,150k)`
- [ ] write a test asserting an unindexed segment is excluded from the
  planner-facing accessor, the invariant that keeps prune from advancing past
  what reads can serve
- [ ] run `go test ./db/snapshotsync/... ./cl/antiquary/...` — must pass before task 11

---

### Task 11: live-safe RemoveOverlaps at the call site

**Files:**
- Modify: `cmd/utils/app/snapshots_cmd.go`
- Modify: `db/snapshotsync/caplin_state_overlap_test.go`

- [ ] **build the missing indices before the call.** The offline version
  restricted candidate supersets to indexed segments (`caplin_state_snapshots.go:505-515`);
  the base's `findOverlaps` runs over every parsed `.seg` in `s.dir` with no
  index check. With an indexed `[0,50k)` and a dumped-but-unindexed `[0,100k)` —
  the publish-`.seg`-before-`.idx` window this plan pins a test on in Task 12 —
  the base keeps the larger file, unlinks the only readable one, and
  `RecalcVisibleSegments`'s `IsIndexed` gate then rejects the survivor, leaving
  zero visible coverage. `doRetireCommand` builds no caplin state indices;
  `caplinStateSnaps.BuildMissingIndices(ctx, logger)` at `:3022` is a different
  command. Call it immediately before `RemoveOverlaps` so no unindexed superset
  can exist at that moment.
- [ ] `doRetireCommand` (`snapshots_cmd.go:3553`) keeps the `nil` Task 9 put
  there. Do **not** wire the node's seeder-delete callback: the EL line two
  above it is `br.RemoveOverlaps(nil)` (`:3549`) and the command has no
  downloader — it passes `dbservices.NoopSeederClient{}` to `BuildFiles`. The
  real callback lives on the node path (`block_snapshots.go:333`), not here.
- [ ] note the constraint for whoever later wires a real callback on the node
  path: the base relativizes against `s.dir` (`toRelativePaths`,
  `snapshots.go:1487`), which for state is `dirs.SnapCaplin`, so the callback
  receives `v1.1-…` where caplin torrents are keyed `caplin/v1.1-…`
  (`snapshotsync.go:601`). `RpcClient.fixPath` re-roots an **absolute** path
  against `dirs.Snap` and returns a relative one untouched
  (`db/downloader/client.go:20-32`), so a raw pass-through would name a
  nonexistent root-level torrent. A caplin callback has to re-prefix.
- [ ] add a test with a stray `BlockProposers.seg` in the caplin dir across the
  call — the regression test for Task 4's guard on the `GetGrouping` path.
- [ ] add a test for the unindexed-superset case: an indexed `[0,50k)` plus an
  unindexed `[0,100k)`, asserting the readable segment survives the call and
  coverage is non-empty afterwards
- [ ] add a test with a `View` open across the call, asserting the file is
  unlinked only after the view closes — the property the offline version could
  not offer.
- [ ] note in the PR body: the base's `RemoveOverlaps` unconditionally deletes
  every `.tmp` in `s.dir` (`snapshots.go:1506-1513`, carrying an in-tree TODO
  that this may remove caplin's useful `.tmp` files). The offline version did
  not. State it; scoping the sweep is deferred to item 3a, which already owns it.
- [ ] run `go test ./db/snapshotsync/... ./cmd/utils/...` — must pass before task 12

---

### Task 12: update the existing state-snapshot tests

**Files:**
- Modify: `db/snapshotsync/caplin_state_snapshots_test.go`
- Modify: `db/snapshotsync/caplin_state_visibility_test.go`
- Modify: `db/snapshotsync/caplin_state_coverage_test.go`
- Modify: `cl/antiquary/state_prune_test.go`
- Modify: `cl/antiquary/state_prune_reader_test.go`
- Modify: `cmd/utils/app/publishable_check_test.go`

- [ ] adapt the fixtures to the embedded base. Behavioral assertions are the
  regression net for the swap — do not weaken one to make it pass. The two
  exceptions are already retired in Task 9, with reasons, and are not to be
  reinstated: `caplin_state_snapshots_test.go` and
  `TestCaplinStateRemoveOverlapsKeepsSubsetWithoutIndexedSuperset`.
- [ ] `TestCaplinStateUnindexedSegmentInvisible` (`caplin_state_visibility_test.go:35`)
  is the one test asserting an *unindexed* segment is excluded from coverage —
  its assertions are at `:48` and `:56`. It survives only if Task 10 kept the
  `IsIndexed` gate; treat a failure there as a Task 10 bug, not a fixture to
  update.
- [ ] `TestCaplinStateIndexFoundWhenDatadirPathContainsSeg` (`:63`) is a
  different property — that an **indexed** segment IS visible when the datadir
  path itself contains `.seg`. If it goes red, the cause is the base's
  version-agnostic `.idx` resolution replacing the extension swap (see the
  `ReplaceVersionWithMask` note below), not a lost `IsIndexed` gate. Do not go
  looking in Task 10.
- [ ] refresh the docstring at `caplin_state_visibility_test.go:59-62`, which
  pins the `.seg`-in-path mechanism of the deleted `openIdxIfNeedForCaplinState`.
  The assertion survives; the explanation rots.
- [ ] add a case asserting equal-range version dedup now applies to state,
  inherited from `RecalcVisibleSegments` (`snapshots.go:819-836`) and absent
  before this PR.
- [ ] note in the PR body: the base resolves the `.idx` version-agnostically via
  `ReplaceVersionWithMask` + `MatchVersionedFile` (`snapshots.go:430-445`), where
  caplin's extension swap took the segment's own path and so paired the versions
  by construction. Do not call it a pure relaxation. The base takes the
  **highest** version present, and nothing ties the matched index's version to
  its segment's — `MinSupported` only rejects too-old files, and `IdxFileNames`
  versions indexes independently of segments (`db/snaptype/type.go:360-366`), so
  an equality check is the wrong shape and would reach every block and blob type.
  State the exposure instead: `BeaconBlocks` is `V1_1_standart`, so v1.0 state
  segments are supported alongside v1.1 dumps, and in the publish-`.seg`-before-`.idx`
  window Task 10 relies on, a v1.1 segment can pick up a v1.0 `.idx`, pass
  `IsIndexed`, and be preferred by the equal-range dedup. Harm needs a genuinely
  older-format v1.0 file whose word lengths differ, so this is a recorded
  exposure rather than a fix in this PR.
- [ ] run `go test -race ./db/snapshotsync/... ./cl/antiquary/... ./cmd/utils/...`

---

### Task 13: verify acceptance criteria

- [ ] every caplin state file on disk resolves to a registered type, and an
  unknown one is skipped rather than panicking — on all three paths:
  `AllTypedSegments` (via `OpenFolder`), `openSegments`, and
  `GetGrouping` (via `RemoveOverlaps`)
- [ ] `BlocksAvailable()` returns the same number as before the swap, asserted
  numerically. Walk the full consumer set: `state_antiquary.go` 113, 130, 133,
  136, 141, 144, 626, 679; `antiquary.go:214`;
  `historical_states_reader.go` 92, 489, 496; `duties_proposer.go:290-295`.
  `state_antiquary.go:141`, `historical_states_reader.go:92` and
  `duties_proposer.go:293` are ceilings — a value one too high breaks them, not
  the floors.
- [ ] `SegFileNames` returns real paths and the antiquary still seeds
- [ ] the dump planner does not re-dump a file that already exists on the far
  side of a gap
- [ ] file names produced and consumed are byte-identical to main
- [ ] `RemoveOverlaps` is live-safe and drain-gated
- [ ] `make lint` clean (repeat until stable), `make erigon integration` builds
- [ ] `go test -race ./db/snaptype/... ./db/snapshotsync/... ./db/snapcfg/... ./cl/antiquary/... ./cl/persistence/... ./cl/beacon/... ./cmd/utils/...`

---

### Task 14: [Final] Update the program docs

- [ ] `docs/plans/20260729-caplin-el-snapshot-parity.md` is **not on this
  branch** — it exists only on `awskii/caplin-snapshot-parity` (`a8914379c33`),
  which is not an ancestor of `af897d9919f`. Do not try to edit it here and do
  not recreate it. Put what this plan established into the PR body instead: the
  exact `BlocksAvailable` formula and its three ceiling consumers,
  `alignMin=false`, the dead `frozen` flag, the three nil-`Type` paths,
  `filePath` being caplin-only, and the gap-truncation × dump-planner split.
  Folding it back into the program doc happens on that branch, separately.
- [ ] epic #23024: tick the item-2 box. Record that `CaplinMergeLimit` is 10,000
  while nodes dump 50k files, so `IsFrozen` marks them frozen and item 3's merge
  tier needs the `MergeSteps` treatment before it can merge one.
- [ ] file PR-2c from the section above, and note in the epic that mainnet
  `BlocksAvailable()` is 0 on download-only nodes until it lands — the 10 GLOAS
  tables are configured but unpublished, and `idxAvailability` zeroes on the
  first type with no segments.
- [ ] move this plan to `docs/plans/completed/`

---

---

# PR-2c — fork-aware expected set

**Not executed by this plan.** Sketched here so the fork mapping Task 2 adds has
a stated purpose and 2c does not get re-derived from scratch. File it once 2b is
merged.

`BlocksAvailable()` becomes a min over *expected* types rather than all
configured ones, reading the fork through `snaptype.CaplinStateIntroducedIn`.
A type is expected when the fork it declares has an activation
epoch the chain actually schedules — `beaconCfg.<Fork>ForkEpoch != FarFutureEpoch`
— and, for a range-sensitive form, when the range reaches that epoch's slot.
Mainnet ships `GloasForkEpoch: math.MaxUint64` (`cl/clparams/config.go:1046`),
so the 10 GLOAS tables stop dragging the min to 0 and a download-only node
reports the coverage it actually has.

Carries with it:

- re-add the `beaconCfg` field Task 9 deleted; it is what resolves a declared
  fork to an epoch
- rewrite Task 8's numeric pins to the new semantics, and the
  33-configured/23-present fixture becomes the primary test rather than a
  documentation of a wart
- Task 13's "the number does not move" criterion is retired here, deliberately
- decide whether an expected-but-absent table should log once at open, since
  after this change its absence stops being visible in `BlocksAvailable`
- it does **not** address a publisher lagging after a fork has activated. That
  needs per-type availability at the 13 consumers, which is Decision B work

## Post-Completion

**Manual verification**

- Run a mainnet caplin node that actually downloads state snapshots — none of
  our boxes do, which is why none of the three nil-`Type` panics has been
  observed. The snap36 fleet runs
  `--no-downloader=true --snap.skip-state-snapshot-download=true` and produces
  rather than fetches.
- `erigon snapshots integrity --check=Publishable` on a caplin datadir after
  `snapshots retire`, the check the offline `RemoveOverlaps` was written for
  (#22256).
- Confirm the antiquary still seeds after `SegFileNames` changed backing:
  `--no-downloader=false` on a node that produces state snapshots.
- Measure first-open cost on a real datadir. `openSegments:1215` calls
  `IsFrozen` → `MergeLimit` per newly opened segment, and for a caplin enum that
  never hits its early `break` — caplin entries are skipped by name
  (`db/snapcfg/util.go:409-411`), the rest by type mismatch — so every call
  scans all 6,686 mainnet preverified entries. Caplin state calls it zero times
  today (`frozen: true` is hardcoded). Once per file per process, so seconds at
  startup rather than a hot loop, but confirm rather than discover.
- `snapshots retire` now unlinks while a node runs. A running node keeps its
  mapping (POSIX unlink) and its next `OpenFolder` retires the vanished
  segments with `RetireReasonWasDeletedFromDisk` (`snapshots.go:1284`), which is
  close-only and never re-deletes (`:997-1000`). No action needed — but this is
  the first time capcli and a node can race over the same files, so say it in
  the PR body.

**External**

- The 99.3 MiB of `BlockProposers` files stay in the published toml; Task 5 only
  stops clients carrying the filter from fetching them. Removing them at the
  source fixes it for every client version and needs whoever owns publication.
- `kv.Proposers` remains a `ChaindataTables` entry for a table nothing reads.
  Removing it is a schema change and wants its own PR.
