# Fix snaptype enum collision: Txt vs BeaconBlocks share enum 9

## Overview

`snaptype2.Enums.Txt = MinCoreEnum+8 = 9` (`db/snaptype2/block_types.go:77`)
equals `CaplinEnums.BeaconBlocks = MinCaplinEnum = 9`
(`db/snaptype/type.go:391,401`). `RegisterType(Enums.Txt, …)`
(`db/snaptype2/block_types.go:389`) writes `registeredTypes[9]`, but
`Enum.String()`/`Enum.Type()` special-case the caplin range **before**
consulting the registry (`db/snaptype/type.go:405-429`), so `Enum(9).String()`
returns `"beaconblocks"` and `Enum(9).Type()` returns BeaconBlocks — the
registered Txt type is shadowed, and a Txt segment named via
`Enum.FileName()` would be misnamed `…-beaconblocks.seg`.

The collision is **latent, not live**: nothing produces Txt segments today
(Txt is referenced only via `E3StateTypes` → `snapcfg.RegisterKnownTypes`),
and preverified matching is by name string, not enum. It becomes live the
moment any code path resolves Txt through its enum.

Fix: renumber the caplin/bor ranges off the core range (minimal shift), panic
on duplicate registration so the next collision is caught at init, and pin the
whole registry with a round-trip test. Enums are NOT persisted anywhere
(filenames go through `Enum.String()`; verified: no numeric conversion of
`snaptype.Enum` exists in the repo — every consumer is a map key, a
MaxEnum-sized slice index, or name matching), so renumbering is safe.

This is PR A of the caplin-EL snapshot parity program PR-0
(`docs/plans/20260729-caplin-el-snapshot-parity.md` on branch
`awskii/caplin-snapshot-parity` — NOT present on this branch; do not try to
open it during execution); the registry work in that program's PR-5 /
Decision A depends on this landing.

## Context (from discovery)

- `db/snaptype/type.go:389-393` — the range constants: `MinCoreEnum=1`,
  `MinCaplinEnum=9`, `MinBorEnum=11`, `MaxEnum=15`. Core occupies 1..9
  (Salt..Txt), caplin 9..10, bor 11..14: core and caplin overlap at 9.
- All range consumers are already relative: `CaplinEnums`
  (`type.go:401-402`); the four bor types are the exported vars
  `heimdall.Events` / `Spans` / `Checkpoints` / `Milestones`
  (`polygon/heimdall/types.go:164,260,321,386`), enums assigned relative to
  `MinBorEnum` at `types.go:71-74`.
- The only absolute dependents are six `make(..., snaptype.MaxEnum)`
  allocations (`db/snapshotsync/snapshots.go:590,598,834`,
  `db/snapshotsync/freezeblocks/caplin_snapshots.go:88,89,242`) — they grow
  automatically. No loop iterates `0..MaxEnum`.
- `RegisterType` (`type.go:240-255`) blindly overwrites both
  `registeredTypes[enum]` and `namedTypes[name]` (`:246`) — no duplicate
  detection on either axis. 13 call sites, all one-shot package-level `var`
  initializers (9 in `db/snaptype2/block_types.go`, 4 in
  `polygon/heimdall/types.go`); nothing re-registers.
- Caplin types are raw `SnapType` literals, never `RegisterType`d
  (`db/snaptype/caplin_types.go:22-33`); `IsCaplinType(enum)` exists at
  `caplin_types.go:37`.
- `ParseEnum("txt")` (`type.go:448-461`) resolves via `namedTypes` and returns
  9 — the same value `ParseEnum("beaconblocks")` returns: two names, one enum.
- Existing tests cover name↔String round-trips only for subsets
  (`db/snaptype/caplin_types_test.go:36-45`,
  `db/snaptype2/block_types_test.go:40-51` — headers/bodies/transactions
  only, which is exactly why Txt slipped through;
  `polygon/heimdall/types_test.go:45+`). The real gap: nothing covers
  `E3StateTypes` or cross-range enum uniqueness.
- Commit convention: erigon prefixes with the modified package(s), e.g.
  `db/snaptype: …` — not `feat: …`. Applies to every task's commit.

## Development Approach

- **testing approach**: TDD (red → green; confirm each red fails for the
  right reason)
- complete each task fully before moving to the next
- make small, focused changes
- every task includes new/updated tests; tests are separate checklist items
- all tests must pass before starting the next task — with ONE declared
  exception: Task 1 ends expected-RED (see its final checkbox); Task 2 turns
  it green
- update this plan file when scope changes during implementation
- run `make lint` before finishing (non-deterministic — repeat until clean)

## Testing Strategy

- **unit tests**: `db/snaptype` external test package (`snaptype_test` —
  `caplin_types_test.go:17` already uses it) so the test can import
  `db/snaptype2` and `polygon/heimdall` to trigger their `init`
  registrations. No import cycle: `db/snaptype` imports neither
  (`go list -deps` verified); layering inversion is test-only and has
  precedent (`db/snapshotsync/freezeblocks/dump_test.go` imports polygon).
- no e2e; the affected surface is init-time registration + naming.
- regression breadth: package tests of `db/snaptype`, `db/snaptype2`,
  `db/snapcfg`, `db/snapshotsync`, `db/snapshotsync/freezeblocks`,
  `polygon/heimdall`, `polygon/heimdall/poshttp`, `polygon/bridge` must stay
  green after the renumber (this exact set was validated green under an
  overlay build of the renumber during plan review).

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- keep plan in sync with actual work done

## Solution Overview

Minimal-shift renumber (spaced/reserved ranges were considered and rejected —
`MaxEnum` only sizes slices; the protection comes from the guards, and with
guard 1 extended to the caplin range and duplicate names, zero headroom
between ranges is safe):

```go
const MinCoreEnum = 1    // core: 1..9 (Salt..Txt)
const MinCaplinEnum = 10 // caplin: 10..11
const MinBorEnum = 12    // bor: 12..15
const MaxEnum = 16
```

Two guards so this class of bug cannot recur silently:

1. `RegisterType` panics **before inserting** on: a duplicate enum, an enum
   in the caplin range (`IsCaplinType` — caplin types are literals that never
   pass through `RegisterType`, so the registry alone can't see them), or a
   duplicate name in `namedTypes`. No same-name idempotency exemption — no
   call site re-registers anything; strict is simpler.
2. A registry-integrity test: String/ParseEnum/Type round-trips for every
   defined type across all three ranges, pairwise enum uniqueness, and
   range-disjointness assertions.

## Technical Details

Red assertions on unfixed main (empirically confirmed during plan review):

- `snaptype2.Txt.Enum() == snaptype.BeaconBlocks.Enum()` — uniqueness fails.
- `snaptype2.Txt.Enum().String()` returns `"beaconblocks"`, not `"txt"`.
- `snaptype2.Txt.Enum().Type().Name()` returns `"beaconblocks"`.
- `snaptype2.Enums.Txt < snaptype.MinCaplinEnum` is false (9 < 9).

Note `ParseEnum("txt")` itself returns 9 == `Txt.Enum()` — the name→enum
direction alone looks fine; the test must check the enum→name/type direction
and uniqueness, not just name→enum. All other registered types round-trip
cleanly today, so Txt is the sole red source.

The type list for the round-trip test is built explicitly (registration is
package-init-driven, so the test imports pin it):
`snaptype2.BlockSnapshotTypes` + `snaptype2.E3StateTypes` (includes Txt) +
the four exported bor vars `heimdall.Events, heimdall.Spans,
heimdall.Checkpoints, heimdall.Milestones` + `snaptype.CaplinSnapshotTypes`
(BeaconBlocks, BlobSidecars). Do NOT use `heimdall.SnapshotTypes()` — it
returns only Events+Spans by default (Checkpoints only with
`recordWaypoints`, Milestones never; `types.go:457-463`), which would drop
`Milestones = MinBorEnum+3 = 15`, the very enum the `MaxEnum` bump
accommodates.

Panic-test enums (post-fix, 1..15 all taken, 0 is Unknown): duplicate-enum
case registers a new name at an already-taken registered enum (e.g.
`snaptype2.Enums.Headers`); caplin-range case registers at
`snaptype.CaplinEnums.BeaconBlocks`; duplicate-name case registers name
`"headers"` at `snaptype.Enum(snaptype.MaxEnum)` (out-of-range value is fine
for a panic test). All panics fire before any map insert, so a recovering
test leaves the global registry untouched.

## What Goes Where

- **Implementation Steps**: code + tests in this repo, on branch
  `awskii/snaptype-enum-ranges` off `origin/main` (`d8dac9fbe2a`).
- **Post-Completion**: PR body content; parity-program follow-ups.

## Implementation Steps

### Task 1: registry-integrity test (red)

**Files:**
- Create: `db/snaptype/enum_registry_test.go` (package `snaptype_test`)

- [x] build the explicit type list: `snaptype2.BlockSnapshotTypes`,
  `snaptype2.E3StateTypes`, `heimdall.Events`, `heimdall.Spans`,
  `heimdall.Checkpoints`, `heimdall.Milestones`
  (`polygon/heimdall/types.go:164,260,321,386` — NOT `SnapshotTypes()`),
  `snaptype.CaplinSnapshotTypes`
- [x] write round-trip test: for every type `t` in the list,
  `t.Enum().String() == t.Name()`, `ParseEnum(t.Name())` returns
  `(t.Enum(), true)`, and `t.Enum().Type().Name() == t.Name()`
- [x] write pairwise enum-uniqueness test over the same list
- [x] write range-disjointness test: every core enum `< MinCaplinEnum`, every
  caplin enum `< MinBorEnum`, every bor enum `< MaxEnum`
- [x] run `go test ./db/snaptype/...` — **expected RED, fails until Task 2;
  do NOT weaken the assertions to get green.** Must fail exactly on the
  Txt/BeaconBlocks assertions in Technical Details; confirm the failure
  messages name enum 9 / "txt" / "beaconblocks" (red for the right reason)
  — confirmed: `TestEnumRoundTrip` fails with `type "txt":
  Enum().String() = "beaconblocks" (enum 9)` and `Enum().Type().Name() =
  "beaconblocks"`, `TestEnumUniqueness` with `enum 9 shared by "txt" and
  "beaconblocks"`, `TestEnumRangeDisjointness` with `core type "txt" enum 9
  outside [1, 9)`; ParseEnum("txt") passes as predicted
- [x] commit: `db/snaptype: add registry round-trip and range-disjointness
  test (red: Txt shadowed by BeaconBlocks at enum 9)`

### Task 2: renumber the ranges (green)

**Files:**
- Modify: `db/snaptype/type.go`

- [x] `MinCaplinEnum` 9→10, `MinBorEnum` 11→12, `MaxEnum` 15→16
  (`type.go:389-393`); `MinCoreEnum` unchanged
- [x] grep for hardcoded absolute enum literals that could bypass the
  constants (`Enum(9)`, `Enum(10)`, … across `db/`, `polygon/`, `cl/`,
  `cmd/`) — none expected (verified during plan review); fix any found
  — repo-wide grep found none
- [x] run `go test ./db/snaptype/...` — Task 1 test now green
- [x] run package regression tests: `go test ./db/snaptype2/...
  ./db/snapcfg/... ./db/snapshotsync/... ./polygon/heimdall/...
  ./polygon/bridge/...` — must pass before task 3 — all green
- [x] commit: `db/snaptype: separate core/caplin/bor enum ranges`

### Task 3: RegisterType duplicate/collision panic

**Files:**
- Modify: `db/snaptype/type.go`
- Modify: `db/snaptype/enum_registry_test.go`

- [x] in `RegisterType` (`type.go:240-255`), before any map insert, panic on:
  (a) `registeredTypes[enum]` already present, (b) `IsCaplinType(enum)`
  (`caplin_types.go:37`), (c) `namedTypes[strings.ToLower(name)]` already
  present. No same-name idempotency exemption (no call site re-registers —
  verified; strict panic is simpler and stronger)
- [x] write test: duplicate enum panics — register a new name at
  `snaptype2.Enums.Headers` (recover-based; panic precedes insert so the
  registry is unchanged after recover)
- [x] write test: caplin-range enum panics — register at
  `snaptype.CaplinEnums.BeaconBlocks`
- [x] write test: duplicate name panics — register name `"headers"` at
  `snaptype.Enum(snaptype.MaxEnum)`
  — all three written red-first: each failed with "did not panic" before the
  guard landed, green after
- [x] run `go test ./db/snaptype/...` and the init-heavy importers
  (`./db/snaptype2/... ./polygon/heimdall/...`) — no spurious init panic
  (all 13 RegisterType call sites are distinct; verified during plan review)
  — all green, `make lint` clean
- [x] commit: `db/snaptype: panic on duplicate or caplin-range type
  registration`

### Task 4: Verify acceptance criteria

- [x] verify all Overview requirements: collision gone (round-trip green),
  duplicate/caplin-range/duplicate-name registration panics, ranges disjoint
  — `TestEnumRoundTrip`, `TestEnumUniqueness`, `TestEnumRangeDisjointness`,
  and all three `TestRegisterTypePanics*` pass fresh (`-count=1`)
- [x] `make lint` — repeat until clean (non-deterministic) — clean on two
  consecutive runs
- [x] `make erigon integration` — both binaries build
- [x] run the full regression set once more: `go test ./db/snaptype/...
  ./db/snaptype2/... ./db/snapcfg/... ./db/snapshotsync/...
  ./polygon/heimdall/... ./polygon/heimdall/poshttp/... ./polygon/bridge/...`
  — all green

### Task 5: [Final] Update documentation

- [x] no README/CLAUDE.md changes expected (registry behavior is
  self-documenting via the panic + test); confirm and skip if so
  — confirmed: no enum-range references in README/CLAUDE.md/docs outside
  this plan
- [x] move this plan to `docs/plans/completed/`

## Post-Completion

**PR** (opened manually — no push task in this plan):
- Title: `db/snaptype: separate core/caplin/bor enum ranges; panic on duplicate registration`
- Body: the collision mechanics in one paragraph — state explicitly that the
  bug is **latent** (nothing produces Txt segments today; preverified
  matching is by name) so reviewers don't hunt for a live symptom; enums are
  unpersisted so renumbering is safe; guards added so the next range overlap
  fails at init/test time. No Summary heading, no Testing section.

**Parity-program follow-ups** (not this PR):
- PR-5 registry cleanup and Decision A (state-table typing) of
  `20260729-caplin-el-snapshot-parity.md` build on this fix.
