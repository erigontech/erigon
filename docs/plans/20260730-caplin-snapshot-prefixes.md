# Caplin snapshot pre-fixes: antiquary robustness, races, verify/filter gaps

## Overview

Six independent correctness fixes in the caplin snapshot path, one task = one
commit each. They are PR B of the caplin-EL snapshot parity program's PR-0
(`docs/plans/20260729-caplin-el-snapshot-parity.md` on branch
`awskii/caplin-snapshot-parity`) — the standalone, backportable set that keeps
the later refactoring PRs reviewable:

1. Antiquary `Loop` busy-spins after ctx cancellation instead of returning.
2. `blobStorage.RemoveBlobSidecars` error is silently dropped.
3. 15s "wait for fsync" sleep in the beacon-block dump (cargo cult; the
   compressor already fsyncs before rename).
4. `FrozenBlobs()` reads `s.visible` without the lock — data race with
   `recalcVisibleFiles`.
5. Torrent piece verification never sees `snapshots/caplin/` (non-recursive
   glob).
6. `caplin/`-prefixed preverified entries bypass the version-window filter.
   Defensive today (no bad-version entry exists in any published toml) —
   consistency with the typed path and a prerequisite for the parity
   program's PR-1.

## Context (from discovery)

Ground truth `origin/main` @ `d8dac9fbe2a`:

- `cl/antiquary/antiquary.go:152-153, 249-250` — `case <-a.ctx.Done():` with
  no exit; after cancel the surrounding loops busy-spin. A third
  `ctx.Done` site at `:193-198` (inside the `onProgress` callback) has a
  `default:` — it is a non-blocking poll and must be LEFT ALONE.
- `Loop()` gates before the `:152` spin: `!a.blocks` → return (`:128`),
  `!clparams.SupportBackfilling(...)` → return (`:132`), wait loop only when
  `a.downloader != nil` (`:135`), short-circuit when `a.backfilled.Load()`
  (`:146`). With `downloader == nil` execution falls through to
  `a.sn.BuildMissingIndices` (`:157`) — nil `*CaplinSnapshots` panics there.
  A cancellation test MUST set the gates (see Task 1).
- `cl/antiquary/antiquary.go:518` — `blobStorage.RemoveBlobSidecars(...)`
  return value discarded (`blob_db.go:51` returns error).
- `db/snapshotsync/freezeblocks/caplin_snapshots.go:403-404` —
  `time.Sleep(15 * time.Second)` between `sn.Compress()` and
  `BeaconSimpleIdx`; it is the only `time.` use in the file (import becomes
  unused on deletion). The sleep is removable with no replacement:
  `seg.Compressor.Compress()` fsyncs, closes, then renames tmp→out
  (`db/seg/compress.go:373-381`), and the caplin dump never calls
  `DisableFsync()` — the `.seg` is durable and visible before `Compress`
  returns. EL runs compress→index with no wait
  (`block_snapshots.go:673,679`). Note: the existing
  `TestDumpBeaconBlocksNoPanic` errors out before reaching `Compress` on an
  empty memdb — it is NOT a safety net for this path.
- `db/snapshotsync/freezeblocks/caplin_snapshots.go:693-703` — `FrozenBlobs()`
  iterates `s.visible` unguarded; the writer `recalcVisibleFiles` (`:235-245`)
  runs under `visibleLock.Lock()`. `FrozenBlobs` early-returns 0 when
  `beaconCfg.DenebForkEpoch == math.MaxUint64` (`:694-696`) — a race test
  must use a Deneb-configured chain config.
- `db/integrity/torrent_verify.go:41` — `filepath.Glob(dir/*.torrent)`,
  non-recursive; sole caller `cmd/utils/app/snapshots_cmd.go:1589` passes
  `dirs.Snap`. Caplin state torrents live in `dirs.SnapCaplin` =
  `<snap>/caplin` (`db/datadir/dirs.go:119`). NOTE: `dirs.Snap` also contains
  `domain/`, `history/`, `idx/`, `accessor/` — a recursive walk expands
  verification to EL state torrents too (desirable, but a real runtime-scope
  change to state in the commit body).
- `db/snapcfg/util.go:124` `Preverified.Typed`; `:142-145` unconditional keep
  for `caplin`-prefixed names; `:196-222` the typed parse-version +
  min/preferred window + keep-newest dedup this fix mirrors. The typed dedup
  keys `bestVersions` on the version-STRIPPED remainder (`:137, :213-221`).
  `ver.ParseVersion("caplin/v1.1")` fails (`file_version.go:141`) — the
  `caplin/` prefix must be stripped before version parsing. Version window:
  `snaptype.BeaconBlocks.Versions()` = `{Current: V1_1, MinSupported: V1_0}`
  (`db/snaptype/caplin_types.go:25`); state tables borrow BeaconBlocks'
  versions (`caplinsnapschema/caplin_snap_schema.go:27-29`). Published-toml
  spread checked on main and release/3.6: mainnet 9706, sepolia 7592, gnosis
  38346, hoodi 4620 caplin entries — ALL v1.1 (chiado/bloatnet none;
  testdata/mainnet_preverified.toml 2702 × v1.1) — window [1.0, 1.1] drops
  nothing published today.
- Commit convention (applies to EVERY task's commit): erigon prefixes with
  the modified package(s), e.g. `cl/antiquary: return from Loop on
  cancellation` — not `feat: ...`.

## Development Approach

- **testing approach**: TDD (red → green) where a red test is expressible;
  fixes 2 and 3 are covered by the pragmatism clause (logging-only change /
  deletion with fsync evidence) — say so in the PR body.
- one task = one commit = one fix, `package: subject` style, each
  independently revertable; complete each task fully before the next.
- all tests must pass before starting the next task.
- `make lint` is non-deterministic — repeat until clean before finishing.
- update this plan file when scope changes during implementation.

## Testing Strategy

- **unit tests**: per-fix, in the owning package (`cl/antiquary` internal
  test package — the Antiquary struct is package-internal;
  `db/snapshotsync/freezeblocks`; `db/integrity`; `db/snapcfg`).
- **race coverage**: fix 4's test must run under `-race` and fail red on
  unfixed main.
- no e2e.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- keep plan in sync with actual work done

## Solution Overview

All six are local, mechanism-preserving fixes — no lifecycle refactoring
(that is PR-1/PR-2 of the parity program). Fix 6 is the only one with a
design choice: `caplin/` entries go through the same window logic as typed
entries, keyed for dedup on `"caplin/" + <version-stripped remainder>` (the
prefix keeps the key disjoint from typed keys; keying on the FULL name would
give zero dedup since the version is embedded in it). Fix 5 switches the flat
glob to a recursive `filepath.WalkDir` collecting `*.torrent`.

## Technical Details

- Fix 1: `return nil` in both blocking `ctx.Done` cases (`:152-153`,
  `:249-250`). `nil` matches the caller's clean-shutdown path
  (`cmd/caplin/caplin1/run.go:565-576`: nil → `keepGoing=false`; non-nil
  would log a spurious "Antiquary failed" on every shutdown). Red test
  construction (internal `package antiquary` test):
  `&Antiquary{ctx: cancelledCtx, blocks: true, cfg:
  &clparams.MainnetBeaconConfig, downloader: <stub>, backfilled:
  &atomic.Bool{} /* false */}` — this parks the Loop in the `:146-155` wait
  loop, which spins forever on unfixed main. The downloader stub needs
  `Seed`/`Delete`/`Download` (`dbservices/interfaces.go:157-171`);
  `dbservices.NoopSeederClient` covers Seed/Delete. Assert Loop returns via
  done-channel select with timeout. The `:249` site is not cheaply reachable
  in a unit test (needs BuildMissingIndices + DB view + OpenFolder + prune to
  all succeed) — it is covered by review, not test; the same two-line change
  applies.
- Fix 2: log at `Warn` with `slot` and `err` fields; do NOT abort the prune
  loop (a per-slot removal failure is not fatal; aborting would wedge blob
  antiquation).
- Fix 3: delete the sleep + the now-unused `time` import. Commit body cites
  `db/seg/compress.go:373-381` (fsync → close → rename before `Compress`
  returns) and the EL precedent.
- Fix 4: `s.visibleLock.RLock()` / `defer RUnlock()` around the `s.visible`
  iteration in `FrozenBlobs()`. Red test: construct via
  `NewCaplinSnapshots(cfg, &clparams.MainnetBeaconConfig, dirs, logger)`
  (Deneb configured — otherwise the `:694` early-return means no race), one
  goroutine hammering `FrozenBlobs()`, another calling `OpenFolder()` on an
  EMPTY `t.TempDir()` — `OpenList`'s deferred `recalcVisibleFiles` (`:164`)
  unconditionally reassigns `s.visible` (`:242`), so no fixture .seg files
  are needed. Run under `-race`.
- Fix 5: `filepath.WalkDir(dir, ...)` collecting `.torrent`; keep the
  failFast branch (`torrent_verify.go:96-108`) untouched. Walk-error policy:
  log-and-skip unreadable entries (Glob never failed on those; propagating
  would abort the whole verify on one bad subdir). State the EL-state-torrent
  scope expansion in the commit body.
- Fix 6: in `Preverified.Typed`, replace the unconditional keep at
  `util.go:142-145`: strip the `caplin/` prefix, `strings.Cut` the version
  prefix, `ver.ParseVersion`, window = `snaptype.BeaconBlocks.Versions()`
  min/preferred, dedup key = `"caplin/" + <remainder>`, keep-newest, emit the
  ORIGINAL item (full name intact). One deliberate simplification, note in a
  code-adjacent test comment or commit body: the typed path uses per-index
  versions for `.idx` entries (`util.go:184-188`) — caplin uses the
  BeaconBlocks window for both `.seg` and `.idx`; harmless while
  `BeaconBlockSlot`/`BlobSidecarSlot` indexes are also `V1_1_standart`
  (`snaptype/type.go:152-158`).

## What Goes Where

- **Implementation Steps**: code + tests on branch
  `awskii/caplin-snapshot-prefixes` off `origin/main` (`d8dac9fbe2a`).
- **Post-Completion**: PR body notes, backport candidacy.

## Implementation Steps

### Task 1: antiquary Loop returns on ctx cancellation

**Files:**
- Modify: `cl/antiquary/antiquary.go`
- Modify: `cl/antiquary/antiquary_test.go`

- [ ] write red test per Technical Details Fix 1: internal-package test,
  cancelled ctx, `blocks: true`, `cfg: &clparams.MainnetBeaconConfig`,
  non-nil downloader stub, `backfilled` false; assert `Loop()` returns nil
  within timeout via done-channel select; confirm it hangs on unfixed code
  (red for the right reason — the `:146-155` wait loop)
- [ ] add `return nil` to the `ctx.Done` cases at `antiquary.go:152-153` and
  `:249-250`; leave `:193-198` alone (non-blocking poll with `default:`)
- [ ] run `go test ./cl/antiquary/...` — green
- [ ] commit: `cl/antiquary: return from Loop on context cancellation`

### Task 2: log dropped RemoveBlobSidecars error

**Files:**
- Modify: `cl/antiquary/antiquary.go`

- [ ] log the `RemoveBlobSidecars` error at `:518` (`Warn`, slot + err),
  keep the loop going; no new test (logging only, pragmatism clause —
  declare in PR body)
- [ ] run `go test ./cl/antiquary/...` — green
- [ ] commit: `cl/antiquary: log blob sidecar removal failures during prune`

### Task 3: drop the 15s dump sleep

**Files:**
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots.go`

- [ ] delete the `time.Sleep(15 * time.Second)` + "Ugly hack" comment at
  `caplin_snapshots.go:403-404`
- [ ] remove the now-unused `time` import
- [ ] run `go test ./db/snapshotsync/... ./cl/antiquary/...` — green
- [ ] commit: `db/snapshotsync: drop the beacon dump fsync sleep` with body
  citing `db/seg/compress.go:373-381` (fsync→close→rename inside Compress)
  and the EL compress→index precedent (`block_snapshots.go:673,679`)

### Task 4: FrozenBlobs lock race

**Files:**
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots.go`
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots_test.go`

- [ ] write red `-race` test per Technical Details Fix 4:
  `NewCaplinSnapshots` with `&clparams.MainnetBeaconConfig` (Deneb set),
  concurrent `FrozenBlobs()` vs `OpenFolder()` on an empty `t.TempDir()`;
  confirm the race detector fires on unfixed code
- [ ] guard the `s.visible` iteration in `FrozenBlobs()`
  (`caplin_snapshots.go:693-703`) with `visibleLock.RLock()/RUnlock()`
- [ ] run `go test -race ./db/snapshotsync/freezeblocks/...` — green
- [ ] commit: `db/snapshotsync: take visibleLock in FrozenBlobs`

### Task 5: recursive torrent discovery in VerifyTorrentFiles

**Files:**
- Modify: `db/integrity/torrent_verify.go`
- Create: `db/integrity/torrent_verify_test.go` (if absent; else extend)

- [ ] write red test: temp dir with `caplin/` subdir containing a torrent
  fixture — unfixed code never visits it
- [ ] replace `filepath.Glob(dir/*.torrent)` (`torrent_verify.go:41`) with a
  recursive `filepath.WalkDir` collecting `*.torrent`; walk errors are
  logged and skipped; failFast branch (`:96-108`) unchanged
- [ ] write test for the flat case (top-level torrents still found)
- [ ] run `go test ./db/integrity/...` — green
- [ ] commit: `db/integrity: verify torrents recursively (caplin/ and state
  subdirs)` with the scope-expansion note in the body

### Task 6: version-window filtering for caplin preverified entries

**Files:**
- Modify: `db/snapcfg/util.go`
- Modify: `db/snapcfg/util_test.go` (or create if absent)

- [ ] write red test on `Preverified.Typed`: caplin state entries
  `caplin/v9.9-…-BlockRoot.seg` + `caplin/v1.1-…-BlockRoot.seg` → only v1.1
  survives; today the bypass keeps both
- [ ] write test: two in-window versions of the same caplin name
  (`caplin/v1.0-…` + `caplin/v1.1-…`) → newest kept — this pins the dedup
  key spec (version-stripped remainder, NOT the full name)
- [ ] implement per Technical Details Fix 6 (prefix strip → version parse →
  BeaconBlocks window → `"caplin/"+remainder` dedup key → emit original item)
- [ ] write test: beaconblocks/blobsidecars `.seg`/`.idx` entries are
  untouched by this change
- [ ] run `go test ./db/snapcfg/...` — green
- [ ] commit: `db/snapcfg: apply the version window to caplin preverified
  entries`

### Task 7: Verify acceptance criteria

- [ ] all six Overview items implemented, one commit each, package-prefixed
  messages
- [ ] `make lint` — repeat until clean (non-deterministic)
- [ ] `make erigon integration` — both binaries build
- [ ] full affected-package suite: `go test -race ./cl/antiquary/...
  ./db/snapshotsync/... ./db/integrity/... ./db/snapcfg/...`

### Task 8: [Final] Update documentation

- [ ] no README/CLAUDE.md changes expected; confirm and skip if so
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

**PR** (opened manually — no push task in this plan):
- Title: `cl, db: caplin snapshot pre-fixes (cancellation, races, verify and preverified filter gaps)`
- Body: the six problems in one short list, then ## Changes. Note the TDD
  pragmatism exemptions (logging fix; sleep deletion backed by the
  compressor fsync evidence) and that the untested `:249` return is
  review-covered. No Summary heading, no Testing section.

**Backport candidacy**: the FrozenBlobs race fix and the antiquary
cancellation fix are release-branch material (`release/3.5`/`3.6`) if a
backport is requested — this PR is deliberately self-contained to allow it.

**Parity-program follow-ups** (not this PR): PR-1 deletes `FrozenBlobs`'
current implementation when CaplinSnapshots moves onto `BaseRoSnapshots`; the
preverified window gains full typed treatment if Decision A lands state-table
types. The compressor-fsync finding also closes the parity program's open
question about whether the sleep needed an explicit-sync replacement — it
does not.
