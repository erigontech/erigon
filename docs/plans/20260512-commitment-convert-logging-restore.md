# Commitment-Convert Logging Improvements + --restore Flag

## Overview

Two independent improvements to the `integration commitment convert` tool on branch `awskii/r36converter`:

1. **Logging**: Phase 1 log lines should show overall key progress in `PrettyCounter` format and per-file key/second rate. Target shape: `processing 42.0k key/s at 2.84M/8.32M (2/3 files, 68.4% overall by keys)`.
2. **--restore flag**: Replace the manual "to revert this conversion: rm ... && mv ..." instructions with an automated `--restore` flag that performs the file movements.

Both changes are scoped to phase 1 + the integration command. Phases 2–5 of the converter are untouched.

## Context (from discovery)

**Target branch**: `awskii/r36converter` (current working branch is `awskii/nibbles-consolidation_34`; all edits go to r36converter).

**Files involved**:
- `db/state/commitment_convert.go` — logging sites at lines ~508 (30s tick), ~526 (per-file done), ~628 (phase 1 summary), ~679 (DONE message with revert instructions). New `RestoreCommitmentFiles` lives here.
- `cmd/integration/commands/commitment.go` — `cmdCommitmentConvert` (line ~461), `commitmentConvert` wrapper.
- `cmd/integration/commands/flags.go` — `withConvertFlags` (line ~127).
- `db/state/commitment_convert_blackbox_test.go` — existing blackbox test; gets a new restore-flow test.

**Patterns observed**:
- `common.PrettyCounter[N number](num N) string` at `common/pretty.go:21` — already supports `uint64`/`int64`/`float64`. Use directly.
- `convertPhase3` at line ~807 uses glob `*-commitment.<from>-<to>.*` to back up every accessor sibling (`.kv`, `.bt`, `.kvi`, `.kvei`, plus `.torrent` variants). Restore mirrors this: every backup file has a same-named counterpart in `snapshots/domain/`, so `os.Rename` overwrites atomically.
- `cleanupRebuildParent` at line ~858 — pattern for removing now-empty parent dirs after the child is cleared. Generalise / reuse for backup cleanup.
- `at.KeyCountInFiles(kv.CommitmentDomain, startTxNum, endTxNum)` returns per-file key count cheaply (used at line 446 in current code).

**Dependencies**: none new. All helpers already exist.

## Development Approach

- **Testing approach**: regular (code first, then tests). Logging changes are cosmetic — no tests required. Restore flow gets one happy-path blackbox test and one no-backup-error test.
- complete each task fully before moving to the next
- make small, focused changes
- run `make lint` repeatedly until clean (non-deterministic per project CLAUDE.md)
- run `make integration` to verify build after each task touching integration command
- maintain backward compatibility — existing convert invocations must work unchanged

## Testing Strategy

- **Unit tests**: `RestoreCommitmentFiles` gets:
  - Happy-path test: pre-seed `snapshots/backup/domains/` with fake files, pre-seed `snapshots/domain/` with same-named "converted" files, run restore, assert backup files are now in domain (content matches backup, not the converted dummies) and backup dir is empty/removed.
  - No-backup test: empty/missing `snapshots/backup/domains/` → assert error message contains "no backup".
- **No tests for logging changes** — they're cosmetic. Manual eyeball via a small fixture would be overkill.
- **Integration**: `make integration` build only; no e2e tests in this codebase for the integration tool.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document blockers with ⚠️ prefix
- update this plan if scope shifts during implementation

## Solution Overview

**Part 1 (logging)** — add three pieces of state:
- `grandTotalKeys uint64` (in `ConvertCommitmentFiles`) — pre-computed sum of `at.KeyCountInFiles` over all phase-1 files (single pass before the loop). Passed into `convertPhase1`.
- `processedKeys uint64` (in `convertPhase1`) — running cumulative. Incremented by `ki` on successful file completion, AND incremented by `at.KeyCountInFiles(...)` on `errSkip` (the file is "done" from a phase-progress perspective even though we didn't read it; this is what makes the percentage converge to 100%).
- `phaseStart time.Time` (in `ConvertCommitmentFiles`) — captured immediately before calling `convertPhase1`, used at the existing phase-1 summary log line AFTER `convertPhase1` returns. Not passed in.

`convertCommitmentFile` gets two new parameters (`grandTotalKeys uint64`, `processedKeys uint64`) and one new return value (`ki uint64`). The 30s tick and the per-file-done line both compute the key-rate as `ki / time.Since(fileStart).Seconds()` (per-file only, not cumulative). The orchestrator computes the keys-overall % from `(processedKeys+ki)/grandTotalKeys` when building `progressPrefix` — and rebuilds the prefix per-tick so the % stays current mid-file. Rate display is suppressed (shown as `--`) when `time.Since(fileStart) < 1*time.Second` to avoid spurious-looking values on tiny files.

**Part 2 (--restore)** — sibling function `RestoreCommitmentFiles(ctx, at, logger)` that:
1. Refuses if `snapshots/backup/domains/` is missing or empty.
2. **Orphan sweep**: collect the set of step-ranges represented by backup entries (parsed from each backup filename's `*-commitment.<from>-<to>.*`). For each step-range, glob `snapshots/domain/*-commitment.<from>-<to>.*` and delete every match. This handles the case where the converted file uses a different `FileVersion` prefix (`.kvi` vs `.kvei`, or upgraded version bytes) than the original — naive rename-overwrite would leave the cross-version siblings as orphans alongside the restored originals, breaking erigon startup. Deleting by step-range glob first ensures clean state.
3. After the sweep, iterate backup entries and `os.Rename` each into `snapshots/domain/`. POSIX rename is atomic; destination is guaranteed empty by step 2.
4. Removes the now-empty `snapshots/backup/domains/` and `snapshots/backup/` parent if also empty (via `cleanupParentIfEmpty`).
5. Logs `"[commitment_convert] restore complete: restored N files from %s to %s; restart erigon"` with both paths.

The integration command's `convert` subcommand gets a new `--restore` flag. When set, the command dispatches to `RestoreCommitmentFiles` instead of `ConvertCommitmentFiles`. If `--restore` is combined with explicitly-set `--squeeze` or `--nibbles.v2`, the command **refuses** with an error (rather than silently ignoring) — matches erigon's `preflightBackupDir` pattern of failing loud on operator confusion. Detect explicit-set via `cmd.Flags().Changed("squeeze")` / `cmd.Flags().Changed("nibbles.v2")`.

## Technical Details

### Signature changes in `db/state/commitment_convert.go`

Before:
```go
func convertCommitmentFile(ctx context.Context, at *AggregatorRoTx, file VisibleFile,
    dstDir string, opts ConvertOpts, progressPrefix string, logger log.Logger,
) (sizeDelta int64, deltaPct float32, err error)
```

After:
```go
func convertCommitmentFile(ctx context.Context, at *AggregatorRoTx, file VisibleFile,
    dstDir string, opts ConvertOpts, fileIdx, fileTotal int,
    grandTotalKeys, processedKeys uint64, logger log.Logger,
) (sizeDelta int64, deltaPct float32, ki uint64, err error)
```

`progressPrefix` is no longer pre-built by the caller — it's built inside `convertCommitmentFile` per-tick using `fileIdx`, `fileTotal`, and the current `(processedKeys + ki) / grandTotalKeys`. Caller passes just the indices.

### Log format reference

30s tick:
```
[commitment_convert] phase 1 file=v2.0-commitment.128-160.kv processing 42.0k key/s at 5.69M/8.32M (2/3 files, 68.4% overall by keys)
```

Per-file done:
```
[commitment_convert] phase 1 file done v2.0-commitment.128-160.kv ki=2.85M sizeDelta=-35.7% in 1m8s (42.0k key/s) at 5.69M/8.32M (2/3 files, 68.4% overall by keys)
```

Phase 1 complete:
```
[commitment_convert] phase 1 complete: converted 3, skipped 0, total 3, keys=8.32M in 4m12s, sizeDelta=-1.2 GB
```

Skip line (unchanged):
```
[commitment_convert] phase 1 skip v2.0-commitment.0-32.kv (already in target state) (1/3 files, 0.0% overall by keys)
```

### `RestoreCommitmentFiles` shape

```go
// Filename pattern for step-range parsing: "<anything>-commitment.<from>-<to>.<ext>"
var commitmentStepRangeRe = regexp.MustCompile(`-commitment\.(\d+)-(\d+)\.`)

func RestoreCommitmentFiles(ctx context.Context, at *AggregatorRoTx, logger log.Logger) error {
    dirs := at.Dirs()
    backupDir := filepath.Join(dirs.Snap, "backup", "domains")
    snapDomain := dirs.SnapDomain

    entries, err := os.ReadDir(backupDir)
    if err != nil {
        if errors.Is(err, os.ErrNotExist) {
            return fmt.Errorf("[commitment_convert] no backup to restore from at %s", backupDir)
        }
        return fmt.Errorf("[commitment_convert] restore: read backup dir %s: %w", backupDir, err)
    }
    if len(entries) == 0 {
        return fmt.Errorf("[commitment_convert] no backup to restore from at %s (empty)", backupDir)
    }

    // Step 1: collect unique step-ranges from backup filenames.
    type stepRange struct{ from, to string }
    ranges := make(map[stepRange]struct{})
    files := make([]string, 0, len(entries))
    for _, e := range entries {
        if e.IsDir() {
            continue
        }
        m := commitmentStepRangeRe.FindStringSubmatch(e.Name())
        if m == nil {
            return fmt.Errorf("[commitment_convert] restore: backup entry %q does not match commitment step-range pattern", e.Name())
        }
        ranges[stepRange{m[1], m[2]}] = struct{}{}
        files = append(files, e.Name())
    }

    // Step 2: orphan sweep — remove every *-commitment.<from>-<to>.* in snapshots/domain/
    // for each step-range covered by the backup. This catches cross-version accessor
    // siblings (e.g. the converted .kvei when the original had only .kvi).
    swept := 0
    for r := range ranges {
        pattern := filepath.Join(snapDomain, fmt.Sprintf("*-commitment.%s-%s.*", r.from, r.to))
        matches, globErr := filepath.Glob(pattern)
        if globErr != nil {
            return fmt.Errorf("[commitment_convert] restore: glob %s: %w", pattern, globErr)
        }
        for _, m := range matches {
            if rmErr := os.Remove(m); rmErr != nil {
                return fmt.Errorf("[commitment_convert] restore: rm orphan %s: %w", m, rmErr)
            }
            swept++
        }
    }

    // Step 3: move backups into place.
    moved := 0
    for _, name := range files {
        if ctxErr := ctx.Err(); ctxErr != nil {
            return ctxErr
        }
        src := filepath.Join(backupDir, name)
        dst := filepath.Join(snapDomain, name)
        if renameErr := os.Rename(src, dst); renameErr != nil {
            return fmt.Errorf("[commitment_convert] restore mv %s -> %s: %w", src, dst, renameErr)
        }
        moved++
    }

    if rmErr := os.Remove(backupDir); rmErr != nil {
        logger.Warn("[commitment_convert] restore: failed to remove empty backup dir", "path", backupDir, "err", rmErr)
    }
    cleanupParentIfEmpty(filepath.Dir(backupDir), logger)

    logger.Info(fmt.Sprintf("[commitment_convert] restore complete: restored %d files from %s to %s (swept %d orphans); restart erigon",
        moved, backupDir, snapDomain, swept))
    return nil
}
```

Reuse `cleanupRebuildParent` by renaming it to `cleanupParentIfEmpty` (generic; same logic). Update the existing callers.

### Integration command dispatch

```go
// in cmdCommitmentConvert.Run after db open:
if convertRestore {
    if err := commitmentRestore(db, cmd.Context(), logger); err != nil {
        if !errors.Is(err, context.Canceled) {
            logger.Error(err.Error())
        }
    }
    return
}
// existing convert path follows
```

`commitmentRestore` is a slim wrapper that opens `agg.BeginFilesRo()` and calls `dbstate.RestoreCommitmentFiles`. No `PresetOfflineMerge`, no `ForTestReplaceKeysInValues` — restore only moves files.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code, tests, doc updates inside the repo
- **Post-Completion** (no checkboxes): user-facing manual verification (run the converted tool against a real datadir)

## Implementation Steps

### Task 0: Verify working branch and tree state

**Files:** none (verification only).

- [x] confirm current branch is `awskii/r36converter`: `git rev-parse --abbrev-ref HEAD` returns `awskii/r36converter`
- [x] confirm `db/state/commitment_convert.go` is at the expected revision: `git log -1 --oneline -- db/state/commitment_convert.go` shows `c8b470607f db/state: address commitment-convert third-pass review`
- [x] `git status` is clean except for the pre-existing modified submodule pointer `execution/tests/execution-spec-tests`. Do NOT reset or otherwise touch the submodule — it's an intentional pointer divergence on this branch.
- [x] no tests to run yet (verification only)

### Task 1: Wire `grandTotalKeys` pre-pass and thread state through phase 1

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_export_test.go`
- Modify: `db/state/commitment_convert_blackbox_test.go`

- [x] in `ConvertCommitmentFiles`, after filtering `files`, before calling `convertPhase1`: compute `grandTotalKeys` by looping `at.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())` over all `files`. Capture `phaseStart := time.Now()` here (outer scope only — do not pass into `convertPhase1`; compute `time.Since(phaseStart)` at the existing phase-1 summary log site after `convertPhase1` returns).
- [x] change `convertPhase1` signature to accept `grandTotalKeys uint64`. It owns its own local `processedKeys uint64`.
- [x] change `convertCommitmentFile` signature: replace `progressPrefix string` with `fileIdx, fileTotal int, grandTotalKeys, processedKeys uint64`. Add `ki uint64` to the return tuple (between `deltaPct` and `err`). The new tuple order: `(sizeDelta int64, deltaPct float32, ki uint64, err error)`.
- [x] inside `convertPhase1`, build the call: `delta, _, ki, convErr := convertCommitmentFile(ctx, at, f, rebuildDir, opts, i+1, N, grandTotalKeys, processedKeys, logger)`. After successful return: `processedKeys += ki`. **On `errSkip` return**: advance `processedKeys += at.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())` — the file is "done" from a phase-progress standpoint even though we didn't read it, so the keys-overall % converges to 100% when all files are processed-or-skipped.
- [x] update the existing `phase 1 skip` log to compute the new prefix shape `(N/M files, X.X% overall by keys)` using current `processedKeys` (post-increment) and `grandTotalKeys`. Extract a small helper `buildPhase1Prefix(fileIdx, fileTotal int, processedKeys, grandTotalKeys uint64) string` and use it everywhere a prefix is needed.
- [x] update test bridge `db/state/commitment_convert_export_test.go:26` — `ConvertCommitmentFileForTest = convertCommitmentFile` will pick up the new signature automatically; no code change needed in this file but verify the symbol still resolves.
- [x] update the 5 call sites in `db/state/commitment_convert_blackbox_test.go` at lines 115, 150, 223, 287, 316. Each currently passes a `progressPrefix` string ("", "(1/1 files)", "(1/1)", etc.) and assigns `_, _, convErr`. New form: pass `1, 1, uint64(0), uint64(0)` (or sensible per-test values) where `progressPrefix` was, and assign `_, _, _, convErr` to discard the new `ki` return. The prefix string is no longer a parameter, so the test-specific labels ("(1/1 files)", "(1/1)") are dropped — they only affected log output, which tests don't assert on.
- [x] build: `make integration` — must succeed
- [x] no new unit tests in this task; existing blackbox tests cover the wiring
- [x] run existing tests: `go test ./db/state/ -run TestConvertCommitmentFile -count=1` (note singular — matches both `TestConvertCommitmentFile_*` and `TestConvertCommitmentFiles_*`) — must pass

### Task 2: Update log line formats with PrettyCounter and key/s rate

**Files:**
- Modify: `db/state/commitment_convert.go`

- [x] capture `fileStart := time.Now()` near the top of `convertCommitmentFile` (after error returns but before the read loop).
- [x] add a small helper `formatRate(ki uint64, elapsed time.Duration) string` near the other top-level helpers: returns `"--"` if `elapsed < time.Second`, else `common.PrettyCounter(uint64(float64(ki) / elapsed.Seconds()))`. Tiny files finish in <1s and would otherwise print absurd-looking key/s values.
- [x] rewrite the 30s tick log (currently `progress %d/%d file=%s (%.1f%% in file) %s`) to:
  ```
  [commitment_convert] phase 1 file=%s processing %s key/s at %s/%s %s
  ```
  with `baseName`, `formatRate(ki, time.Since(fileStart))`, `common.PrettyCounter(processedKeys+ki)`, `common.PrettyCounter(grandTotalKeys)`, `buildPhase1Prefix(...)`.
- [x] rewrite the per-file-done log (currently `phase 1 file done %s sizeDelta=%.1f%% ki=%d %s`) to:
  ```
  [commitment_convert] phase 1 file done %s ki=%s sizeDelta=%.1f%% in %s (%s key/s) at %s/%s %s
  ```
  with elapsed = `time.Since(fileStart).Round(time.Second)` and rate via `formatRate(ki, time.Since(fileStart))` (raw elapsed, not rounded, so the rate calc isn't quantised).
- [x] rewrite the phase 1 summary log (currently `phase 1 complete: converted %d, skipped %d, total %d, sizeDelta=%s`) to:
  ```
  phase 1 complete: converted %d, skipped %d, total %d, keys=%s in %s, sizeDelta=%s
  ```
  with `common.PrettyCounter(processedKeys)` and `time.Since(phaseStart).Round(time.Second)`. Confirm `phaseStart` reaches `convertPhase1`'s return path. (processedKeys threaded out of convertPhase1 as a 5th return value)
- [x] build: `make integration`
- [x] no new tests (cosmetic change)
- [x] manual eyeball: grep the source for the four new log strings to confirm they compile and reference defined identifiers

### Task 3: Rename `cleanupRebuildParent` → `cleanupParentIfEmpty`

**Files:**
- Modify: `db/state/commitment_convert.go`

- [x] rename the function `cleanupRebuildParent(parent string, logger log.Logger)` to `cleanupParentIfEmpty(parent string, logger log.Logger)` (body unchanged — the logic is already generic).
- [x] update its existing call sites (search `cleanupRebuildParent` in the file; there are 2 callers in phases 1 and 4).
- [x] update the comment block to drop the "rebuild/" specificity — say "removes the parent dir if it's empty (the child has just been cleared)".
- [x] build: `make integration`
- [x] no test changes — function semantics unchanged

### Task 4: Implement `RestoreCommitmentFiles`

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_blackbox_test.go`

- [ ] add `RestoreCommitmentFiles(ctx context.Context, at *AggregatorRoTx, logger log.Logger) error` per the shape in Technical Details. Place it directly after `ConvertCommitmentFiles` for proximity.
- [ ] add the package-level `commitmentStepRangeRe` regexp at file scope (above `RestoreCommitmentFiles`).
- [ ] implement the three-step body: read backup dir + collect step-range set → orphan-glob-and-delete in `snapshots/domain/` → rename backup entries into `snapshots/domain/`.
- [ ] use `at.Dirs().Snap` to derive `backupDir` and `at.Dirs().SnapDomain`.
- [ ] respect `ctx.Done()` between file renames in step 3 (the operation can be long if there are many files).
- [ ] update the convert-success log message at line ~679 from `"To revert this conversion: rm ... && mv ..."` to `"To restore originals: re-run with --restore"`.
- [ ] write blackbox test `TestRestoreCommitmentFiles_HappyPath`. Use `testDbAggregatorWithFiles(t, cfg)` (the existing fixture pattern in this file — `AggregatorRoTx` is a struct, not stubbable). Open an `AggregatorRoTx` via `agg.BeginFilesRo()` so `at.Dirs()` returns real paths. Then **manually** seed test files: write `v2.0-commitment.0-32.kv` ("ORIG_KV") and `v2.0-commitment.0-32.bt` ("ORIG_BT") into `<datadir>/snapshots/backup/domains/`; write same-named files containing "CONV_*" bytes into `<datadir>/snapshots/domain/`. Call `state.RestoreCommitmentFiles(ctx, at, log.New())`. Assert: `<snapDomain>/v2.0-commitment.0-32.kv` reads "ORIG_KV"; backup dir is gone; backup parent is gone.
- [ ] write blackbox test `TestRestoreCommitmentFiles_NoBackup`. Use the same fixture but DO NOT seed `snapshots/backup/domains/`. Call `RestoreCommitmentFiles`. Assert error message contains "no backup".
- [ ] write blackbox test `TestRestoreCommitmentFiles_OrphanSweep`. Seed backup with `v2.0-commitment.0-32.kv` + `v2.0-commitment.0-32.bt` ("ORIG_*"). Seed `snapshots/domain/` with `v2.1-commitment.0-32.kv` + `v2.1-commitment.0-32.kvei` ("CONV_*", different version prefix and a sibling type that doesn't exist in backup). Call restore. Assert: the v2.1 files are gone, the v2.0 files exist and contain "ORIG_*", swept count in log is 2.
- [ ] run tests: `go test ./db/state/ -run TestRestoreCommitmentFiles -count=1 -v` — all three must pass

### Task 5: Wire `--restore` flag on the integration command

**Files:**
- Modify: `cmd/integration/commands/flags.go`
- Modify: `cmd/integration/commands/commitment.go`

- [ ] in `flags.go`, add `var convertRestore bool` near the existing convert flag vars.
- [ ] in `flags.go`'s `withConvertFlags`, register: `cmd.Flags().BoolVar(&convertRestore, "restore", false, "restore commitment files from snapshots/backup/domains/ (mutually exclusive with --squeeze/--nibbles.v2)")`.
- [ ] in `commitment.go`, add `commitmentRestore(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error` sibling to `commitmentConvert`. It calls `agg.BeginFilesRo()`/`defer acRo.Close()` (no `PresetOfflineMerge`, no `ForTestReplaceKeysInValues`) and then `dbstate.RestoreCommitmentFiles(ctx, acRo, logger)`.
- [ ] in `cmdCommitmentConvert.Run`, after `db` is open: **first** check flag conflicts — if `convertRestore` and (`cmd.Flags().Changed("squeeze")` or `cmd.Flags().Changed("nibbles.v2")`): `logger.Error("--restore is mutually exclusive with --squeeze/--nibbles.v2"); return`. Then dispatch: `if convertRestore { ... commitmentRestore(...); return }`. Existing convert path follows.
- [ ] extend the `Long:` help text on `cmdCommitmentConvert`:
  - mention `--restore` as a sibling mode that is **mutually exclusive** with `--squeeze`/`--nibbles.v2`
  - add example: `integration commitment convert --restore --datadir /path/to/datadir --chain mainnet`
- [ ] build: `make integration`
- [ ] no new unit tests (cobra wiring; covered manually + by build)

### Task 6: Lint pass and acceptance

**Files:** none (verification only).

- [ ] `make lint` — run repeatedly until clean (linter is non-deterministic per CLAUDE.md)
- [ ] `make integration` — must succeed
- [ ] `go test ./db/state/ -run "TestRestoreCommitmentFiles|TestConvertCommitmentFiles" -count=1` — all pass
- [ ] eyeball every log site reads correctly with a constructed example (hand-substitute numbers into the format strings)
- [ ] verify the changed `cleanupRebuildParent` → `cleanupParentIfEmpty` rename has no lingering references: `grep -n cleanupRebuildParent db/state/commitment_convert.go` returns empty
- [ ] verify `--restore` flag is visible in CLI help: `./build/bin/integration commitment convert --help | grep restore` must show the flag

### Task 7: [Final] Commit + housekeeping

**Files:** none (git only).

- [ ] stage and commit the changes in two commits to match the two independent concerns:
  - **commit A**: `db/state: per-file key rate + PrettyCounter in commitment convert logs` — Task 1, 2 changes.
  - **commit B**: `db/state, cmd/integration: add --restore flag to commitment convert` — Task 3, 4, 5 changes.
- [ ] move this plan to `docs/plans/completed/`: `mkdir -p docs/plans/completed && git mv docs/plans/20260512-commitment-convert-logging-restore.md docs/plans/completed/`
- [ ] commit the plan move: `docs: archive commitment-convert logging+restore plan`
- [ ] do NOT push — leave that to the operator

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes.*

**Manual verification against a real datadir** (one-shot before merging):
- Run `integration commitment convert --datadir <real-datadir> --chain mainnet` on a copy of a datadir; observe the new log lines for ~30s and confirm:
  - `processing X key/s` numbers look plausible (not zero, not absurdly large)
  - `at X/Y` advances monotonically and reaches the expected end value
  - `(N/M files, %.1f%% overall by keys)` percentage matches `X/Y` ratio
- Then run `integration commitment convert --restore --datadir <same-datadir>` and confirm:
  - "restore complete: restored N files" matches the number of files reported by phase 3 backup earlier
  - `snapshots/backup/domains/` is gone
  - Restart erigon and confirm it boots against the restored files

**External system updates**: none — this is an offline integration tool, no consumers.
