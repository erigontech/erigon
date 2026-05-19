# Add `--continue` flag for `integration commitment convert`

## Overview

The `integration commitment convert` command re-encodes commitment domain `.kv` files between squeeze and key-encoding (V1/V2) states. Phase 1 is the long-running step (potentially hours for mainnet) — and today's crash recovery is "delete `snap/rebuild/` and start over."

This change adds a `--continue` flag that resumes Phase 1 from where a prior interrupted run left off. Input files whose matching converted shard already exists in `snap/rebuild/domain/` with all required accessor siblings (`.bt`/`.kvi`/`.kvei`) are skipped; incomplete shards are cleaned and redone.

Lives on a new branch `awskii/convert-continue-flag` off `awskii/r36converter`.

## Context (from discovery)

**Files involved:**
- `cmd/integration/commands/commitment.go` — `cmdCommitmentConvert` cobra command, `withConvertFlags` helper, `commitmentConvert` invocation; existing flags `--squeeze`, `--nibbles.v2`, `--restore`
- `db/state/commitment_convert.go` — `ConvertCommitmentFiles` 5-phase pipeline; `ConvertOpts` struct; `preflightRebuildDir` (currently wipes), `preflightBackupDir`; `convertPhase1` through `convertPhase4`; `commitmentStepRangeRe` regex for `-commitment.<from>-<to>.` filenames
- `db/state/commitment_convert_test.go` — existing test file, target for the 5 unit tests + 1 integration test

**Patterns observed:**
- Convert is offline (no aggregator/MDBX live). Input files come from `at.Files(kv.CommitmentDomain)` already sorted by `startStep` ascending.
- Phase 1 is serial (`for _, f := range files` at `commitment_convert.go:1056`). No parallelism — "max endStep watermark" is meaningful.
- Required accessor set is determined by commitment-domain config (`.bt` and either `.kvi` or `.kvei` depending on bloomfilter/btree settings). Phase 2 (`convertPhase2`) already verifies this set; the new pre-flight reuses the same check.
- Flag-validation pattern: mutual-exclusion enforced at command-Run time, log + return (no panic). See `commitment.go:484` for `--restore` vs `--squeeze`/`--nibbles.v2`.

**Dependencies:**
- Stacked on top of `awskii/r36converter` (not yet merged to `release/3.4`). PR opens against `awskii/r36converter`; rebase to `main`/`release/3.4` once the parent merges.

## Development Approach

- **Testing approach**: Regular — code first per task, tests at end of same task verifying that task's work. Every code task has a tests checkbox before "run tests".
- Complete each task fully before moving to the next.
- Make small, focused changes.
- **CRITICAL**: every code task includes its own unit/integration tests. No deferral.
- **CRITICAL**: all tests must pass before starting next task.
- **CRITICAL**: update this plan if scope changes during implementation.
- Run `make lint` after each task — non-deterministic, run until clean.
- Maintain backward compatibility — default behavior (no `--continue` flag) stays exactly as it is today.

## Testing Strategy

- **Unit tests** in `db/state/commitment_convert_test.go`:
  - 5 tests for `preflightResume` (empty / partial / incomplete / gap / orphan)
  - All use temp-dir + synthesized empty files (`os.Create`); function only checks existence, not content
- **Integration test** in `db/state/commitment_convert_test.go`:
  - `TestConvertCommitmentFiles_ContinueResumes` — synthetic small datadir, cancel mid-Phase 1, re-run with `Continue=true`, assert convergence
- **No e2e UI tests** — this is a CLI/backend change.
- **Manual smoke test** at end (Task 6): run convert on a small chain, interrupt, resume with `--continue`, verify output matches a non-interrupted run byte-for-byte.

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates

## Solution Overview

**Design (settled in brainstorm)**:

1. **Skip criterion**: per-range + accessor check. Skip an input file iff `rebuildDir` contains a `.kv` with the same `<from>-<to>` step range AND every required accessor sibling.
2. **Resume = filter, not skip-map**: pre-flight returns the *filtered* input file slice (suffix). Phase 1 stays oblivious to resume. `grandTotalKeys` is computed over the filtered slice so progress percentage reflects work-remaining.
3. **Contiguity validation**: the set of complete shards must form a contiguous prefix of input ranges. Violation = hard error mentioning the gap.
4. **Mutual exclusion**: `--continue` and `--restore` are mutually exclusive (hard error at flag-validation time).
5. **Empty rebuildDir + `--continue`**: benign no-op, log "no prior progress, starting fresh".
6. **Trust-operator on opts**: do NOT verify `--squeeze`/`--nibbles.v2` against shard contents. Emit a `Warn` line at startup that flags must match prior run.

**Why this shape**:
- Filter > skip-map: one less parameter threaded through Phase 1; progress math is automatically correct; "rebuildDir contains only complete contiguous-prefix shards" invariant is enforced once.
- No manifest: accessor presence is a sufficient completeness signal; a second source of truth can drift.
- Explicit flag (not auto-resume on non-empty rebuildDir): silent behaviour-change is a footgun.

**Reviewer's mitigation suggestion (deferred, not adopted)**: a one-line JSON sentinel `snap/rebuild/.convert-opts` would let `--continue` detect `--squeeze`/`--nibbles.v2` mismatches at startup, removing the silent mixed-encoding failure mode that the trust-operator design accepts. The brainstorm explicitly chose trust-operator + warn. Revisit if the warn turns out to be too easy to miss in practice; the sentinel is a 5-minute follow-up if needed.

## Technical Details

**New flag wiring in `cmd/integration/commands/commitment.go`**:

```go
var convertContinue bool

// In withConvertFlags(cmd):
cmd.Flags().BoolVar(&convertContinue, "continue", false,
    "Resume a prior interrupted conversion. Skips files whose converted shard "+
    "already exists in <datadir>/snap/rebuild/domain/. Flags --squeeze and "+
    "--nibbles.v2 MUST match the original interrupted run; mismatch produces "+
    "mixed-encoding output. Mutually exclusive with --restore.")
```

**Mutual exclusion check at command Run** (in `cmdCommitmentConvert.Run`, alongside existing `--restore` checks):

```go
if convertRestore && convertContinue {
    logger.Error("--continue is mutually exclusive with --restore")
    return
}
```

**`ConvertOpts` extension in `db/state/commitment_convert.go`**:

```go
type ConvertOpts struct {
    TargetSqueeze   bool
    TargetNibblesV2 bool
    Continue        bool // new
}
```

**New `preflightResume` function signature**:

```go
// preflightResume inspects rebuildDir, removes incomplete shards, validates
// that complete shards form a contiguous prefix of `files`, and returns the
// suffix of `files` that still needs conversion. Behavior when continue=false
// is unchanged from today: wipe rebuildDir and return all input files.
func preflightResume(
    files VisibleFiles,
    rebuildDir string,
    requiredAccessors []string,
    stepSize uint64,
    continueMode bool,
    logger log.Logger,
) (VisibleFiles, error)
```

The function combines today's wipe behavior (when `continueMode=false`) and the new resume logic (when `continueMode=true`). Replaces the call to `preflightRebuildDir`.

**Logging — startup warn (when `--continue` set)**:

```
[commitment_convert] WARN --continue: assumes prior interrupted run used the SAME --squeeze and --nibbles.v2 values. Mismatch will produce mixed-encoding output silently.
```

**Logging — resume summary (after `preflightResume` discovers done set)**:

```
[commitment_convert] --continue: resuming. complete shards in rebuild dir: 47 (steps 0-12032), remaining: 13 input files (steps 12032-15360)
```

When done set is empty: `[commitment_convert] --continue: no prior progress, starting fresh`.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code changes in `commitment.go` + `commitment_convert.go` + `commitment_convert_test.go`; build/lint verification.
- **Post-Completion** (no checkboxes): manual smoke test on a real datadir, PR open, eventual rebase onto `main`/`release/3.4` once `awskii/r36converter` merges.

## Implementation Steps

### Task 0: Branch setup + atomicity check

**Files:**
- (no file changes — investigation only)

- [x] Branch off `awskii/r36converter`: `git checkout -b awskii/convert-continue-flag awskii/r36converter`
- [x] Verify local build green: `make integration` builds (the `integration` binary holds the convert command)
- [x] Verify existing tests pass: `go test ./db/state/ -run Convert -count=1`
- [x] Read `dumpStepRangeToPath` in `db/state/` to confirm `.kv` write atomicity. Record finding under `## Findings` below with one of these explicit conclusions:
  - **(a) atomic** (tmp-then-rename): Task 2 completeness check = existence of `.kv` + every required accessor. No size check needed.
  - **(b) in-place writes**: Task 2 completeness check = existence + every accessor file is non-zero size AND the .kv is non-zero size. Test fidelity rider in Task 4 (`TestPreflightResume_zeroSizeKv`) becomes mandatory.
  - **(c) ambiguous / mixed paths**: stop and surface the ambiguity to the user before starting Task 2. Do not proceed with an assumption.

### Task 1: Flag plumbing and mutex

**Files:**
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `db/state/commitment_convert.go`

- [x] Add `convertContinue bool` package var near `convertSqueeze`/`convertNibblesV2`/`convertRestore` in `commitment.go`
- [x] Add `Continue bool` field to `ConvertOpts` in `commitment_convert.go`
- [x] In `withConvertFlags(cmd)` (find via grep on `convertSqueeze`): register the `--continue` flag with full help text (carry the trust-operator caveat: "Flags --squeeze and --nibbles.v2 MUST match the original interrupted run")
- [x] In `cmdCommitmentConvert.Run`, add mutex check: `if convertRestore && convertContinue { logger.Error("--continue is mutually exclusive with --restore"); return }` — place alongside the existing `--restore` checks
- [x] In the `opts := dbstate.ConvertOpts{...}` literal, set `Continue: convertContinue`
- [x] In `ConvertCommitmentFiles`, add the startup `Warn` log line gated by `opts.Continue`
- [x] Run: `go test ./db/state/ -run Convert -count=1` and `go build ./cmd/integration/...` — both must pass before next task

(Field-existence smoke test deliberately omitted — Go's type system already enforces it; the integration test in Task 5 covers the wiring end-to-end.)

### Task 2: Implement `preflightResume`

**Files:**
- Modify: `db/state/commitment_convert.go`

- [x] Add `preflightResume(files VisibleFiles, rebuildDir string, requiredAccessors []string, stepSize uint64, continueMode bool, logger log.Logger) (VisibleFiles, error)`
- [x] When `continueMode=false`: replicate today's `preflightRebuildDir` behavior (wipe + mkdir), return `files` unchanged
- [x] When `continueMode=true`:
  - Walk `rebuildDir`; for each `*-commitment.<from>-<to>.kv` (use `commitmentStepRangeRe`), apply the completeness check.
  - **Unconditional**: every required accessor sibling file exists for the same `<from>-<to>` range.
  - **Conditional rider** (only if Task 0 finding = `(b)` in-place writes): every file (.kv + accessors) has size > 0. If Task 0 finding = `(a)` atomic, skip this rider.
  - Complete shards → add `(from,to)` to `done` set
  - Incomplete shards → remove the .kv and any partial siblings
  - Orphan files (not matching the regex) → remove
- [x] Validate contiguity: walk `files` in order; the prefix that's all-in-`done` is the resume zone. If any `file` after a non-done file is in `done` → return hard error like `non-contiguous shards: gap at steps %d-%d (rebuildDir has %d-%d but missing %d-%d before it)`
- [x] Log resume summary (or "no prior progress, starting fresh" when `done` is empty)
- [x] Return the suffix slice (input files NOT in `done`)
- [x] Helper: `requiredAccessorsForCommitment(domainCfg)` — returns the accessor extension set (`.bt`, `.kvi`, `.kvei`) consumed by `preflightResume`. Extract from existing Phase 2 logic if not already a function
- [x] Write tests immediately (Task 4) — but stub them in this task so `go test` compiles; the assertions land in Task 4

### Task 3: Wire `preflightResume` into `ConvertCommitmentFiles`

**Files:**
- Modify: `db/state/commitment_convert.go`

- [x] In `ConvertCommitmentFiles`, replace the `preflightRebuildDir(rebuildDir, logger)` call with `files, err = preflightResume(files, rebuildDir, requiredAccessors, at.StepSize(), opts.Continue, logger)`. Keep the subsequent `os.MkdirAll(rebuildDir, 0o755)` (harmless if dir exists) — implemented with deviation: introduced `pendingFiles` for the filtered suffix, kept `files` as the full input list (see Findings)
- [x] Compute `requiredAccessors` once, before the call, using the new `requiredAccessorsForCommitment` helper from Task 2
- [x] Move `grandTotalKeys` accumulation to AFTER the filter so it represents work-remaining
- [x] **Backward-compat invariant** (explicit): when `Continue=false`, `preflightResume` returns the input slice unchanged (after wipe+mkdir), so the moved `grandTotalKeys` loop iterates over the same files as today — sum is byte-identical, progress percentages are identical, no caller-visible change
- [x] If `files` is empty after filter (only possible when `Continue=true` AND all shards already present): emit info log "all input files already converted in rebuild dir; proceeding to Phase 2" and proceed (Phases 2-5 still run to verify accessors and promote)
- [x] **Remove `preflightRebuildDir`** if `grep -rn preflightRebuildDir` shows zero call sites after the replacement (it should — convert is the only caller). Don't leave dead code. — wipe logic inlined into `preflightResume`'s `!continueMode` branch
- [x] Run: `go test ./db/state/ -run Convert -count=1` — must pass before next task

### Task 4: Unit tests for `preflightResume`

**Files:**
- Modify: `db/state/commitment_convert_test.go`

- [x] `TestPreflightResume_continueFalse_wipes` — pre-populate rebuildDir with junk, call with `continueMode=false`, verify dir is wiped and full `files` slice returned
- [x] `TestPreflightResume_empty` — empty rebuildDir + `continueMode=true` → returns full input slice, no error, logs "no prior progress"
- [x] `TestPreflightResume_partial` — N complete shards (`os.Create` empty .kv + all accessor siblings) covering contiguous prefix → returns suffix (remaining files), no error
- [x] `TestPreflightResume_allDone` — every input file has a complete shard in rebuildDir → returns empty slice, no error (downstream `ConvertCommitmentFiles` proceeds to Phase 2)
- [x] `TestPreflightResume_incomplete` — `.kv` present but one accessor missing → assert .kv removed, range NOT in `done`, file returned in suffix
- [x] `TestPreflightResume_gap` — shards [0-1024] and [2048-3072] present but [1024-2048] missing → returns error; error message names the gap range
- [x] `TestPreflightResume_orphan` — random non-commitment file in rebuildDir → assert removed, no error
- [x] **Conditional**: `TestPreflightResume_zeroSizeKv` — skipped per Task 0 finding `(a)` atomic (tmp-then-rename); see Findings.
- [x] All tests use `t.TempDir()` + `os.Create`/`os.WriteFile` (latter for non-zero size) for fakes; assert via `os.Stat` and slice equality
- [x] Run: `go test ./db/state/ -run PreflightResume -count=1 -v` — all 7 (or 8 with the conditional) must pass before next task

### Task 5: Integration test for full resume flow

**Files:**
- Modify: `db/state/commitment_convert_test.go`

- [x] `TestConvertCommitmentFiles_ContinueResumes`: build on the existing convert test scaffolding in `commitment_convert_test.go` (find via grep for `ConvertCommitmentFiles`)
- [x] Set up a synthetic small datadir with a few commitment files (reuse existing test fixture if one exists; otherwise build minimally)
- [x] **Decision now (not deferred to start-of-task)**: add a test-only hook in `convertPhase1` — a `convertPhase1AfterFileHook func(idx int)` package-level var, called after each file completes, default-nil-no-op in production. The test sets it to `func(i int) { if i == 0 { cancel() } }`. This is one extra line in the production codepath, scoped, and avoids log-line counting fragility. Counting log lines is explicitly rejected as flaky across timestamp/lineno changes.
- [x] Implement the hook stub in `convertPhase1` as part of this task (one line, nil-guarded call)
- [x] Run `ConvertCommitmentFiles` with `cancel`-on-first-file via the hook
- [x] Verify rebuildDir contains 1 complete shard and the other input files are untouched
- [x] Re-run `ConvertCommitmentFiles` with `Continue: true` and a fresh ctx (hook nil this time)
- [x] Assert: second run's call count to `convertCommitmentFile` equals (total files - 1) — wire a second test-only counter hook if needed
- [x] Assert: all remaining files converted, Phases 2-5 promote everything, final state matches a non-interrupted reference run (basename match + .kv byte equality; accessor siblings exist but bytes differ across datadirs due to per-datadir random recsplit salt)
- [x] Run: `go test ./db/state/ -run TestConvertCommitmentFiles_ContinueResumes -count=1 -v` — must pass before next task

### Task 6: Verify acceptance criteria

**Files:**
- (no code changes — verification only)

- [x] `make lint` — non-deterministic; run until clean
- [x] `go test ./db/state/... -count=1` — all green
- [x] `go test ./cmd/integration/... -count=1` — all green
- [x] `make integration` — binary builds with new flag
- [x] Smoke check: `./build/bin/integration commitment convert --help` shows `--continue` with the full help text including the trust-operator caveat
- [x] Manual smoke test on a small chain (skipped — not automatable, requires dev datadir; deferred to Post-Completion)
- [x] Run full test suite: `make test-short`

### Task 7: PR open

**Files:**
- (no code changes — PR mechanics only)

- [x] **Pre-push parent-drift check**: `git fetch origin awskii/r36converter && git log --oneline awskii/convert-continue-flag..origin/awskii/r36converter -- db/state/commitment_convert.go cmd/integration/commands/commitment.go db/state/commitment_convert_test.go`. If parent has new commits touching any of these files, rebase onto the updated parent and re-run Task 6 before pushing — no parent drift found
- [x] Push `awskii/convert-continue-flag` to origin
- [x] Open PR against `awskii/r36converter` (NOT `main`/`release/3.4` — the parent branch isn't merged yet) with title `cmd/integration, db/state: add --continue flag for commitment convert` — opened as https://github.com/erigontech/erigon/pull/21283
- [x] PR body: link this plan; summarize the resume semantics and the trust-operator caveat on `--squeeze`/`--nibbles.v2`; list which manual smoke tests were run
- [x] Add `## Test plan` section: `make lint`, full `go test` suites, `--help` smoke, manual interrupt-and-resume test
- [x] Note in PR: needs rebase onto `main`/`release/3.4` once `awskii/r36converter` lands

### Task 8: Cleanup

- [ ] Verify all checkboxes marked
- [ ] `mkdir -p docs/plans/completed && mv docs/plans/20260519-convert-continue-flag.md docs/plans/completed/`
- [ ] Leave CLAUDE.md untouched unless a new pattern emerged worth capturing

## Findings

### Task 0: `dumpStepRangeToPath` write atomicity

**Conclusion: (a) atomic — tmp-then-rename for all four file types.**

Trace of how each output file lands at its final path:

1. `.kv` (data) — `db/state/domain.go:649` `dumpStepRangeToPath` → `collateETL` (`domain.go:684`) → `seg.NewCompressor(...)` (`domain.go:698`). Compressor writes via `dir.CreateTemp(c.outputFile)` at `db/seg/compress.go:326`, fsyncs (`compress.go:388`), then `os.Rename(tmpFileName, c.outputFile)` at `compress.go:394`. Final `.kv` only appears at its destination path once write is complete and fsynced.

2. `.kvi` (hash-map accessor) — built in `buildFileRange` (`domain.go:980-988`) via `buildHashMapAccessorAt` → `recsplit`. `recsplit.go:890` opens `dir.CreateTemp(rs.filePath)`, then `os.Rename(rs.indexF.Name(), rs.filePath)` at `recsplit.go:1026`.

3. `.bt` (btree accessor) — built in `buildFileRange` (`domain.go:991-997`) via `btindex.CreateBtreeIndexWithDecompressor`. `db/datastruct/btindex/btree_index.go:265` uses `dir.CreateTemp(btw.args.IndexFile)` and `os.Rename(btw.indexF.Name(), btw.args.IndexFile)` at line 312.

4. `.kvei` (existence filter) — `db/datastruct/existence/existence_filter.go:113` uses `dir.CreateTemp(b.FilePath)` and `os.Rename(cf.Name(), b.FilePath)` at line 128.

**Consequences for Task 2 / Task 4**:
- Task 2 completeness check: existence of `.kv` + each required accessor sibling is sufficient. No file-size check needed.
- Task 4 `TestPreflightResume_zeroSizeKv` is **skipped** — zero-byte `.kv` files cannot appear at the final path under normal crash recovery; a zero-byte file would only exist if someone manually `touch`ed it, which is out of scope for "interrupted previous run" resume.

### Task 3: deviation — `pendingFiles` introduced alongside `files`

The plan text directs `files, err = preflightResume(files, ...)` (overwrite the slice in place), implicitly passing only the suffix to every phase. Doing that breaks Phases 2-3: prior-run shards already present in `rebuildDir` would not appear in `convertedFiles`, so Phase 3 would not back up their originals in `snapshots/domain/` before Phase 4's `os.Rename` overwrites them — silent data loss on the originals (recovery via `--restore` would be impossible).

**Implementation**: kept `files` as the full original input list and introduced `pendingFiles` for the filtered suffix. `pendingFiles` is what Phase 1 iterates; `files` is what Phase 2 walks, so `convertPhase2` sees both prior-run shards and this-run shards and includes both in `convertedFiles`. Phase 3 then backs up both sets of originals.

**Cascading adjustments**:
- `priorCompleteCount := len(files) - len(pendingFiles)` is computed once after the preflight.
- Phase 2 mismatch check changed from `len(convertedFiles) != processedFiles` to `len(convertedFiles) != processedFiles + priorCompleteCount`.
- The early-return `if processedFiles == 0` short-circuit guarded with `&& priorCompleteCount == 0` — otherwise a `--continue` run that only adds prior shards (no work this iteration) would wipe `rebuildDir` and skip promote.
- DONE log line shows the breakdown `N files (X this run + Y from prior runs)` only when `priorCompleteCount > 0`; otherwise the message is unchanged from today.

**Backward-compat invariant** preserved: when `Continue=false`, `preflightResume` returns `files` unchanged, so `pendingFiles == files` and `priorCompleteCount == 0`. Every downstream computation collapses to today's behavior byte-for-byte.

## Post-Completion

**Manual verification**:
- Interrupt-and-resume on a real datadir (small dev chain ideally) to confirm the full crash-recovery loop works end-to-end. Test matrix: (a) interrupt mid-file, (b) interrupt between files, (c) interrupt after Phase 1 completes (no resume needed; Phases 2-5 should pick up). Case (c) may already be handled by today's "rebuildDir contains everything" path — confirm.
- Confirm the trust-operator caveat in practice: deliberately run with mismatched `--squeeze` between attempts and observe the resulting mixed-encoding output. Verify the warn log fires loudly enough that a normal operator would notice.

**External system updates**:
- Once `awskii/r36converter` merges to `release/3.4`/`main`, rebase this branch onto the new base and update the PR
- No consuming-project changes
