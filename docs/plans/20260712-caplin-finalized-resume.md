# Caplin: resume from a locally-persisted finalized state on restart

## Overview

On every restart of an existing datadir, a default Caplin node re-fetches a fresh remote
finalized checkpoint in `ReadOrFetchLatestBeaconState`
(`cl/phase1/core/checkpoint_sync/util.go`): it fetches the remote finalized state first and
only reads a local state on network failure. The fresh checkpoint sits *ahead* of the
execution layer's persisted head, so the `DownloadHistoricalBlocks` stage must backfill the
gap ("Downloading Execution History") and blocks before the node can execute or serve RPC.

This change persists the state the node itself **finalized** and resumes from it on restart,
purely from local data — no HTTP, no P2P, no external oracle. Benefits: the node comes up at
a finalized anchor at/below the EL head (no EL backfill, no blocking catch-up), self-contained,
and fork-safe.

## Context (from discovery)

- Files/components involved:
  - `cl/phase1/core/checkpoint_sync/util.go` — `ReadOrFetchLatestBeaconState`, `ReadLocalHeadState`
  - `cl/phase1/core/checkpoint_sync/local_checkpoint_syncer.go` — disable/devnet resume path
  - `cl/phase1/stages/forkchoice.go` — `saveHeadStateOnDiskIfNeeded` (save cadence, ~line 290/358)
  - `cl/phase1/stages/forward_sync.go` — save call sites (~229, 247)
  - `cl/phase1/stages/clstages.go` — save call site (~265)
  - `cl/phase1/forkchoice/forkchoice.go` — `NewForkChoiceStore` (anchor handling 232-240,392-395), `GetStateAtBlockRoot`, `FinalizedCheckpoint`
  - `cl/phase1/forkchoice/interface.go` — `ForkChoiceStorageReader`
  - `cl/phase1/forkchoice/mock_services/forkchoice_mock.go` — generated mock
  - `cl/clparams/config.go` — `LatestStateFileName`, `CaplinConfig`
  - `cmd/utils/flags.go` — Caplin flags + assembly
- Related patterns found:
  - Existing resume file `latest.ssz_snappy` stores the HEAD state; snappy-encoded SSZ.
  - Existing checkpoint tests use a mock HTTP server + `ConfigurableCheckpointsURLs` and
    `tests.GetPhase0Random()`.
- Dependencies identified:
  - Fork graph retains the finalized state (prunes to `finalizedEpoch-3`,
    `cl/phase1/forkchoice/utils.go:161-163`), so `GetStateAtBlockRoot(finalizedRoot)` works.

## Development Approach

- **Testing approach**: TDD (Red → Green → Refactor) per task.
- Complete each task fully (impl + tests green) before the next.
- `make lint && make erigon` must pass before a task is done; `make lint` is
  non-deterministic — run until clean.
- Every task includes new/updated tests as separate checklist items (success + error/edge).
- Keep this plan in sync (`[x]`, ➕ new tasks, ⚠️ blockers) as work proceeds.

## Testing Strategy

- **Unit tests**: required per task. Pure helpers (`stateWithinResumeHorizon`,
  `writeFinalizedStateFile`) are table/round-trip tested with no fork choice. Fork-choice
  glue is tested via the generated `ForkChoiceStorageReader` mock. Resume decisions are
  tested against the existing mock HTTP server + a real temp `datadir`.
- **E2E**: none (no UI). Manual node-restart sanity is captured in Post-Completion.

## Progress Tracking

- Mark completed items `[x]` immediately.
- ➕ prefix for newly discovered tasks, ⚠️ for blockers.
- Update the plan if scope changes.

## Solution Overview

Load-bearing invariant (verify before changing): `NewForkChoiceStore`
(`forkchoice.go:232-240,392-395`) sets `finalizedCheckpoint = justifiedCheckpoint =
{anchorRoot, Epoch(anchorState)}` where `anchorRoot = anchorState.BlockRoot()` — it treats
the anchor state's *latest block* as finalized (`Epoch` is the block-slot epoch, may trail
the true finalized epoch on skipped slots — harmless, conservative). Therefore the resume
source MUST be a genuinely finalized state. A head state is reorg-eligible (its latest block
is always above the node's own finalized slot, so it can be orphaned by a post-shutdown reorg
and can't be validated as canonical from local data alone). A finalized state is FFG-final
(reorg-immune) and is the only locally-provably-safe anchor.

Why it removes the wait: finalized `F` ≤ EL persisted head always, so the CL resumes *below*
the EL and re-feeds already-present blocks (idempotent `newPayload`) instead of dragging the
EL forward through a gap — no "Downloading Execution History".

## Technical Details

- **Persistence**: reuse the existing 5-epoch save cadence; on tick, fetch
  `fc.GetStateAtBlockRoot(fc.FinalizedCheckpoint().Root, true)` and atomically write it
  snappy-encoded to `dirs.CaplinLatest/finalized.ssz_snappy` (new filename; old head-state
  files are never misread as finalized). Atomic = temp file in the **same directory** +
  `os.Rename`. Non-fatal on `GetStateAtBlockRoot` error.
- **Resume decision**: in the `remoteSync` branch of `ReadOrFetchLatestBeaconState`, before
  the remote fetch, read the finalized file; resume if present AND `GenesisValidatorsRoot()`
  matches the configured genesis AND within the resume horizon; else fall through to today's
  remote path (unchanged).
- **Resume horizon** = a **data-availability feasibility** bound, NOT weak-subjectivity (WS
  is N/A — we resume from our own previously-finalized state). Forward-syncing `F`→head needs
  peers to serve blobs/data-columns for blocks in the DA window; a finalized anchor older than
  the sidecar retention window (`MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS` /
  `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS`, ~4096 epochs ≈ 18 days) makes forward sync
  stall. Default the horizon to that window; clamp user overrides down to it.
- **Consolidation**: route both existing `LatestStateFileName` readers (the remote-failure
  fallback and the disable/devnet `NewLocalCheckpointSyncer`) to the finalized file, then
  retire the head-state write.

## What Goes Where

- **Implementation Steps** (`[ ]`): all code + tests below.
- **Post-Completion** (no checkboxes): manual node-restart verification; consensus spec review.

## Implementation Steps

### Task 1: Finalized-state file name + reader

**Files:**
- Modify: `cl/clparams/config.go`
- Modify: `cl/phase1/core/checkpoint_sync/util.go`
- Modify: `cl/phase1/core/checkpoint_sync/checkpoint_sync_test.go`

- [x] add `LatestFinalizedStateFileName = "finalized.ssz_snappy"` next to `LatestStateFileName`
- [x] add `ReadLocalFinalizedState(dirs, beaconCfg) (*state.CachingBeaconState, error)` in `util.go`, mirroring `ReadLocalHeadState` but reading the new file
- [x] write `TestReadLocalFinalizedState_RoundTrip` (write snappy state → read back → equal roots)
- [x] write `TestReadLocalFinalizedState_Absent` (missing file → error)
- [x] run tests — must pass before next task

### Task 2: Pure resume-horizon helper

**Files:**
- Modify: `cl/phase1/core/checkpoint_sync/util.go`
- Modify: `cl/phase1/core/checkpoint_sync/checkpoint_sync_test.go`

- [x] add `stateWithinResumeHorizon(localSlot, genesisTime, nowUnix, secondsPerSlot, horizonSlots uint64) bool`: guard `secondsPerSlot==0` and `nowUnix<genesisTime` → true; `localSlot>=currentSlot` → true; else `currentSlot-localSlot <= horizonSlots`
- [x] write `TestStateWithinResumeHorizon` table: equal, one-behind, exactly-at-horizon, just-beyond, far-beyond, local-ahead, now-before-genesis, zero-seconds-per-slot
- [x] run tests — must pass before next task

### Task 3: Resume-horizon default + config knob (DA-feasibility bound)

**Files:**
- Modify: `cl/clparams/config.go`
- Modify: `cmd/utils/flags.go`
- Modify: `cl/phase1/core/checkpoint_sync/util.go`

- [ ] add `ResumeMaxStalenessEpochs uint64` to `clparams.CaplinConfig` (0 = computed default)
- [ ] add flag `caplin.resume-max-staleness-epochs` in `cmd/utils/flags.go`, wired into the Caplin config assembly (default `0`)
- [ ] resolve the effective horizon at the use site: default = active fork's sidecar retention (`MinEpochsForBlobSidecarsRequests*SlotsPerEpoch` pre-Fulu; data-column-sidecar retention Fulu+); if a user value exceeds the retention window, log a warning and clamp to it
- [ ] document inline that the bound is DA-feasibility (not weak-subjectivity)
- [ ] (tests for honor/clamp are exercised in Task 5's `TestResumeHorizonHonorsAndClampsConfig`)
- [ ] run `make erigon` — flag wiring compiles before next task

### Task 4: Persist the finalized state on the save cadence (write-side)

**Files:**
- Modify: `cl/phase1/stages/forkchoice.go`
- Modify: `cl/phase1/stages/forward_sync.go`
- Modify: `cl/phase1/stages/clstages.go`
- Create: `cl/phase1/stages/finalized_state_save_test.go`

- [ ] add pure `writeFinalizedStateFile(dirs datadir.Dirs, st *state.CachingBeaconState) error`: snappy-encode and atomic-write — temp file in the SAME dir (`dirs.CaplinLatest`) then `os.Rename` to `finalized.ssz_snappy`
- [ ] add `saveFinalizedStateOnDiskIfNeeded(fc forkchoice.ForkChoiceStorageReader, dirs datadir.Dirs, headSlot uint64) error`: gate on existing cadence `headSlot%(SlotsPerEpoch*5)==0`; fetch `fc.GetStateAtBlockRoot(fc.FinalizedCheckpoint().Root, true)`; on error log at debug and return nil; else `writeFinalizedStateFile` (do NOT add a `Cfg` last-finalized-epoch field)
- [ ] call `saveFinalizedStateOnDiskIfNeeded` at ALL FOUR save sites: `forkchoice.go:358`, `forward_sync.go:229`, `forward_sync.go:247`, `clstages.go:265`
- [ ] write `TestWriteFinalizedStateFile_RoundTrip` (pure, no fork choice)
- [ ] write `TestSaveFinalizedStateOnDisk` using the generated `ForkChoiceStorageReader` mock (`mock_services/forkchoice_mock.go`): assert file round-trips to the finalized state's root, and a `GetStateAtBlockRoot` error does not propagate
- [ ] run tests — must pass before next task

### Task 5: Prefer the finalized state in ReadOrFetchLatestBeaconState (read-side)

**Files:**
- Modify: `cl/phase1/core/checkpoint_sync/util.go`
- Modify: `cl/phase1/core/checkpoint_sync/checkpoint_sync_test.go`

- [ ] in the `remoteSync` branch, before the remote fetch: read the finalized state; if present AND `GenesisValidatorsRoot()` matches the configured genesis (`genesisDB.ReadGenesisState().GenesisValidatorsRoot()`) AND within the resume horizon, return it and skip the remote fetch; else fall through to today's remote path
- [ ] log at info: resumed slot, or the fall-through reason (absent / stale / GVR mismatch)
- [ ] write `TestResumeFromFreshFinalizedStateSkipsRemote` (fresh file → remote mock not hit, returned root == finalized root)
- [ ] write `TestStaleFinalizedStateFetchesRemote` (beyond horizon → remote hit)
- [ ] write `TestForeignFinalizedStateFetchesRemote` (GVR mismatch → remote hit)
- [ ] write `TestAbsentFinalizedStateFetchesRemote` (no file → remote hit, today's behavior)
- [ ] write `TestResumeHorizonHonorsAndClampsConfig` (custom `ResumeMaxStalenessEpochs` honored; over-large clamped to sidecar-retention window)
- [ ] run tests — must pass before next task

### Task 6: Route all resume paths to the finalized file; retire the head-state save

**Files:**
- Modify: `cl/phase1/core/checkpoint_sync/util.go`
- Modify: `cl/phase1/core/checkpoint_sync/local_checkpoint_syncer.go`
- Modify: `cl/phase1/stages/forkchoice.go` (remove `saveHeadStateOnDiskIfNeeded` + call sites)
- Modify: `cl/phase1/stages/forward_sync.go`
- Modify: `cl/phase1/stages/clstages.go`
- Modify: `cl/phase1/core/checkpoint_sync/checkpoint_sync_test.go`

- [ ] point the remote-failure fallback (`util.go:41`) at the finalized file (error if absent)
- [ ] point `NewLocalCheckpointSyncer` (`local_checkpoint_syncer.go:30`) at the finalized file, falling back to genesis when absent (fixes its latent head-as-finalized bootstrap)
- [ ] grep `LatestStateFileName`; confirm zero remaining production readers, then remove the head-state write (`saveHeadStateOnDiskIfNeeded` + its four call sites)
- [ ] write `TestLocalCheckpointSyncFromFinalizedFile` (mirror of `TestLocalCheckpointSyncFromFile`)
- [ ] write `TestLocalCheckpointSyncFallsBackToGenesisWhenAbsent`
- [ ] update existing local-sync tests to the new file; run tests — must pass before next task

### Task 7: Verify acceptance criteria

- [ ] verify: default node resumes from the finalized file when fresh + GVR matches; falls back to remote when absent/stale/foreign
- [ ] verify: a finalized anchor is used everywhere (no head-state bootstrap remains); grep confirms `LatestStateFileName` has no readers
- [ ] run `make lint` (repeat until clean) and `make erigon integration`
- [ ] run `go test ./cl/phase1/core/checkpoint_sync/...`
- [ ] run the focused new tests in `cl/phase1/stages/` (full stages package is slow)

### Task 8: Final — documentation and plan close-out

- [ ] update `cl/CLAUDE.md` / `cl/phase1/forkchoice/CLAUDE.md` only if a new invariant needs recording (the anchor-must-be-finalized invariant)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — informational only.*

**Manual verification:**
- On an existing synced datadir, restart and confirm logs show "Resuming from local finalized
  state", no "Downloading Execution History" backfill, and RPC/execution ready without the
  network catch-up period.
- Confirm the DA-feasibility horizon: verify whether the forward-sync/catch-up path enforces
  data availability on the finalized→head range; if it does, the horizon is load-bearing for
  liveness (a horizon above the sidecar-retention window is a stuck-node risk).

**Consensus spec review:**
- Review against `cl/CLAUDE.md` and `cl/phase1/forkchoice/CLAUDE.md`. Property to preserve:
  the anchor handed to `NewForkChoiceStore` is a finalized state, exactly as the remote
  checkpoint path guarantees today.

**Out of scope (separate changes):**
- Rolling snapshot window / produce-new-drop-old for beacon blocks.
- Skipping the first network `fetchBlockRange` in `DownloadHistoricalBlocks` when the anchor
  block is already present locally.
- Persisting `hasDownloaded` across restarts.
