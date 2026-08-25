# PBin convert-format writes into a separate output datadir

## Overview

`integration commitment convert-format` rewrites binary-trie commitment `.kv` files from
the pre-version pbin record format into the current one. The record codec is done and
correct. The driver is not: it converts into `snapshots/rebuild/domain/`, moves the
originals to `snapshots/backup/domains/`, and promotes — which mutates the source datadir.

The target is `/erigon-data/bin-trie`, 440 GB across 7 commitment files, produced by a
109-hour rebuild. The source must come out of a conversion byte-identical.

This replaces the backup/promote scheme with the pattern the rebuild already uses for this
exact datadir: a required `--output.datadir`, the whole source tree hardlinked in, and
converted files written into the output.

### The invariant this plan is built on

**After staging, no code path references the source datadir.** Staging hardlinks the entire
source `snapshots/` tree — commitment files included — into the output, then reassigns
`datadirCli` to the output. Every subsequent open, temp file, accessor build and enumeration
resolves against the output datadir.

That is not a stylistic preference: it is what makes "the source is never written" checkable
by construction instead of by auditing each write in turn. The previous draft of this plan
tried to enumerate the write vectors and missed two of the three.

## Context (from discovery)

- **Keep**: `execution/commitment/pbin_convert_legacy.go` and its test. `ConvertBranch` /
  `ConvertState`, the legacy decoders, the per-record round-trip and the single-cell panic
  are correct. Tasks 1–2 add exports; nothing existing changes.
- **Replace**: `db/state/commitment_convert_pbin.go` and `cmdCommitmentConvertFormat`.
- **Untouched**: the hex converter. `convertPhase2`/`3`/`4` and `ConvertCommitmentFiles` keep
  their backup/promote/reload tail — `integration commitment convert` still needs it. This
  plan deletes no phase function.
- **Reuse, do not reimplement**: `stageRebuildOutput` (`cmd/integration/commands/commitment.go`)
  already does every refusal this needs — empty output, `pathsOverlap` both directions, the
  existing-files gate with a resume flag, `ReadErigonDBSettings`, and the hardlink walk —
  and `commitment_output_test.go` already tests them.
- **Import direction is fixed**: `linkSnapshotsExceptCommitment`, `pathsOverlap` and
  `isCommitmentFileName` are unexported in `package commands`, which imports `db/state`.
  Staging therefore lives in `cmd/integration/commands`; the driver receives no source path
  at all.
- Base: `origin/binary-trie` at `67ba2a8ec6`, branch `awskii/pbin-record-compaction`.

### Facts verified against the tree

| fact | where | consequence |
|---|---|---|
| `Aggregator.dirs` and the per-`Domain` `dirs` are unexported, set only at construction; `Dirs()` returns by value | `db/state/aggregator.go`, `db/state/domain.go` | there is **no** way to redirect an aggregator's `Tmp` after the fact |
| `cmdCommitmentRebuild` reassigns `datadirCli = out.dirs.DataDir` after staging, because `openDB` and `allSnapshots` take their dirs from it | `cmd/integration/commands/commitment.go` | this is the only seam, and it already exists |
| `buildFileRange` passes `d.dirs.Tmp` to the btree builder and sets `RecSplitArgs.TmpDir = d.dirs.Tmp`; `collateETL` passes it to `seg.NewCompressor` | `db/state/domain.go` | redirecting the compressor alone leaves recsplit and btree temps in the source |
| `Domain.dataReader` builds `seg.NewReader(g, d.Compression)` with no step-count exception; `dataWriter` mirrors it | `db/state/domain.go` | `d.Compression` is the codec authority for both sides |
| `seg.DetectCompressType` has no production caller — one `log.Info` and one benchmark — and infers "compressed" only from a recovered panic | `db/seg/seg_auto_rw.go` | it is not usable as the codec authority; this plan does not call it |
| `isCommitmentFileName` is `strings.Contains(name, kv.CommitmentDomain.String())`, with no extension or directory constraint | `cmd/integration/commands/commitment.go` | it matches `history/*commitment*.v` and `idx/*commitment*.ef` too |
| `openDB(..., applyMigrations=true, ...)` calls `datadir.New` (sixteen `dir.MustExist`), opens an MDBX **RwDB** under `<datadir>/migrations`, and on a pending migration re-opens chaindata `Exclusive(true)` and writes | `cmd/integration/commands/root.go` | the current `convert-format` passes `true` |
| `convertPBinFile` buffers into a `TemporalMemBatch` and decides `if !sawLegacy { return errSkip }` only after the full scan | `db/state/commitment_convert_pbin.go` | a streaming writer cannot make that decision late |
| `pbinTestLegacyRecord(0b01, 0b01, …)` builds a one-cell record; `pbinBranchEncoder.encode` always emits both cells | `execution/commitment/pbin_convert_legacy_test.go` | no current record decodes to a one-cell legacy record |
| `dumpStepRangeToPath` calls `static.CleanupOnError()` after `buildFileRange` | `db/state/domain.go` | omitting it leaks an mmapped `.kv`+`.kvi` per file |
| `requiredAccessorsForCommitment` is config-driven off `d.Accessors.Has(...)`; `AGG_COMMITMENT_BT=1` swaps `.kvi` for `.bt`/`.kvei` | `db/state/commitment_convert.go`, `db/state/statecfg/state_schema.go` | no code or test may name an accessor extension literally |
| `kvNewFilePathIn` stamps `kvWriteVersion()`, which for commitment varies with the references flag | `db/state/domain.go` | a `v2.1` output is read as referenced-branch data |
| `pathsOverlap` compares `filepath.Abs` strings and never resolves symlinks | `cmd/integration/commands/commitment.go` | a symlinked output pointing into the source passes the gate |

## Development Approach

- **testing approach**: TDD — failing test first, then the code, per repo CLAUDE.md
- complete each task fully before the next; the build and `make lint` stay green throughout
- **CRITICAL: every task MUST include new/updated tests**, listed as separate checklist items,
  covering success and error scenarios
- **CRITICAL: all tests must pass before starting the next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- `make lint` reports 0 issues before every commit; never add `t.Skip`
- new files carry the 2026 copyright header
- CLAUDE.md scopes the `pbin`/`PBin` prefix rule to **package-level** identifiers in
  `package commitment`; methods on an existing type are exempt
- cite by identifier name, never `file.go:NNN`
- any test that restores a bin engine must set `statecfg.ExperimentalBinCommitment`,
  `statecfg.BinCommitmentHash` and `commitment.SetPBinHashSuite`, restore them in
  `t.Cleanup`, and **must not** call `t.Parallel` — those are process-global
- `db/state/commitment_convert_export_test.go` is the existing bridge for `package state`
  internals needed by `package state_test`; use it rather than inventing another

## Testing Strategy

- **unit tests**: required for every task
- **end-to-end**: Task 9 synthesises a legacy datadir and converts it; that is this repo's
  equivalent of an e2e suite
- commands:
  - `go test ./execution/commitment/... -count=1`
  - `go test ./db/state/... -count=1`
  - `go build ./cmd/integration/`
  - `make lint`

## Progress Tracking

- mark completed items `[x]` immediately
- add newly discovered tasks with ➕, blockers with ⚠️

## Solution Overview

```
integration commitment convert-format \
  --datadir SRC --output.datadir DST [--resume] [--verify.sample=N]
```

**Staging hardlinks everything, commitment included.** `stageRebuildOutput` runs unchanged for
the refusals and the non-commitment walk; a second walk then links the commitment files it
skipped — `.kv`, accessors, and the `history/`/`idx/` files `isCommitmentFileName` also
matches. The rebuild path is not perturbed and its tests keep asserting commitment is omitted
there.

**Then `datadirCli` becomes the output.** The aggregator, its temp, its accessor builds and
its file enumeration all resolve against the output. The driver takes no source path.

**Migrations are off.** `openDB(ctx, dbCfg(dbcfg.ChainDB, chaindata), false, chain, logger)`,
matching the rebuild's `out == nil`. Chaindata is still opened read-write from the source path
— that is unavoidable without reimplementing aggregator construction, and it is the rebuild's
own accepted behaviour. It touches `chaindata/mdbx.lck`; it creates no `migrations/` tree.

**Conversion replaces a link, never writes through one.** A hardlinked `.kv` shares its inode
with the source, so opening it `O_TRUNC` would destroy the source file. Per file: remove the
output's link and its accessor links first, then write a fresh file at that path. Only the
output's directory entry is ever removed; the source keeps its own.

**Classification is a separate pass.** Whether a file is already current is only knowable after
reading it, and a streaming writer cannot unwind. A cheap first pass scans for a legacy record
and stops at the first one; an already-current file keeps its hardlink and is never rewritten.

**No promote, no etl.** Converted files are written where they finally belong. The pbin
transform is value-only — no `keyXform`, so no `etl.Collector`. Recovery is `rm -rf` on the
output.

**No compression detection.** Read with `d.dataReader`, write with `d.dataWriter(comp, false)`.
Both resolve `d.Compression`, which is how erigon reads these files in production, so the
output is readable by construction. The `merge.go` / `collateETL` step-rule conflict governs
neither read path and does not enter.

## Technical Details

**Per-file write path**

```go
// dirs are the OUTPUT's — datadirCli was reassigned before the aggregator was built.
path := d.kvNewFilePathIn(d.dirs.SnapDomain, stepFrom, stepTo)
if filepath.Base(path) != filepath.Base(srcName) { fail }   // kvWriteVersion() may differ

removeLinkAndAccessors(path)                                 // never write through a hardlink

comp, _ := seg.NewCompressor(ctx, "pbin_convert", path, d.dirs.Tmp, d.CompressCfg, ...)
w := d.dataWriter(comp, false)
for each (k, v): w.Write(k); w.Write(convert(k, v))
coll := Collation{valuesComp: comp, valuesPath: path, valuesCount: pairs}
static, err := d.buildFileRange(ctx, stepFrom, stepTo, coll, ps, d.dirs.SnapDomain)
defer static.CleanupOnError()                                // else an mmapped .kv+.kvi leaks
```

`buildFileRange` owns `Compress()` and every accessor the domain configures.
`integrateDirtyFiles` is never called.

**Record dispatch**

| record | action |
|---|---|
| key == `commitmentdb.KeyCommitmentState` | `ConvertState`, or copy when `ValidatePBinStateFormat` passes |
| first value byte == 0 | `ConvertBranch` |
| otherwise | copy verbatim |

`pbinRecordIsLegacy(v) = len(v) > 0 && v[0] == 0`: a legacy record opens with the high byte
of `touchMap`, always zero; a current one opens with a cell-fields byte, always non-zero.

**File dispositions**

| classification | action |
|---|---|
| holds a legacy record | remove the link, convert into the output |
| no legacy record | leave the hardlink in place — nothing is written |
| complete in the output and not a link, `--resume` | skip |

**Failure leaves nothing name-complete.** On any per-file error — verification failure,
ctx-cancel, or the single-cell panic — the output `.kv` and its accessors are removed before
the error propagates. A `--resume` run therefore never skips a shard that failed, and the
source's copy is always still there to redo it from.

**Verification**

1. per record — the round-trip already inside `ConvertBranch`
2. per file — `coll.valuesComp.Count()/2` against the source pair count (comparing the write
   loop's own counter to itself proves nothing); and the converted state blob's root against
   the source's, which needs `LegacyStateRoot` because `SetState` rejects a legacy blob
3. per run — `--verify.sample=N` records every N-th **legacy-branch** record's key and offset
   during the write (a copied-verbatim record has no legacy header and would fail
   `CompareLegacy`), then re-reads the finished file **sequentially** and compares at those
   positions. No index is opened, so the pass is independent of which accessors the domain
   configures; no second aggregator is opened, which would re-resolve `erigondb.toml` into
   process-global state.

## What Goes Where

- **Implementation Steps**: code, tests, docs in this repo
- **Post-Completion**: the real 440 GB run and its measurements

## Implementation Steps

### Task 1: Export legacy encoders for test corpora

**Files:**
- Modify: `execution/commitment/pbin_convert_legacy.go`
- Modify: `execution/commitment/pbin_convert_legacy_test.go`

The only legacy encoders today are `pbinTestLegacyAppendCell` and `pbinTestLegacyRecord`, in a
`_test.go` file in `package commitment`. Every driver test in `package state` needs a legacy
corpus and cannot reach them. Without this task, Tasks 5–7 and 9–10 have no fixture.

Both a record encoder and a state-blob encoder are needed: Task 6's root check and Task 9's
datadir both require a legacy `KeyCommitmentState` blob, and `pbinStateMarker`,
`pbinRecordFormat` and `pbinPath.appendPackedBits` are all unexported.

- [x] write a failing test that `PBinEncodeLegacyRecord` round-trips: current record in,
      legacy bytes out, `ConvertBranch` back to the identical current record
- [x] add `func PBinEncodeLegacyRecord(key, current []byte) ([]byte, error)` — decode the
      current record, re-spell it in the legacy format
- [x] write a failing test that `PBinEncodeLegacyState` produces a blob `ConvertState` accepts
      and `ValidatePBinStateFormat` rejects
- [x] add `func PBinEncodeLegacyState(current []byte) ([]byte, error)`
- [x] keep `pbinTestLegacyAppendCell` as-is — it is cell-level and its callers need shapes no
      current record can express (a one-cell record, and a cell appended into a state blob),
      so it cannot be expressed in terms of the record-level encoder
- [x] update the file header comment, which currently says nothing outside the converter may
      use this — the corpus generators are now legitimate callers
- [x] write tests for the error cases: malformed input, a record that is already legacy
- [x] run `go test ./execution/commitment/ -count=1` — must pass before task 2

### Task 2: Add CompareLegacy and LegacyStateRoot to the converter

**Files:**
- Modify: `execution/commitment/pbin_convert_legacy.go`
- Modify: `execution/commitment/pbin_convert_legacy_test.go`

- [x] write a failing test that `CompareLegacy` accepts a legacy record with its correct
      conversion and rejects a mismatched pair
- [x] add `func (c *PBinRecordConverter) CompareLegacy(key, legacy, current []byte) error`,
      decoding each side with its own reader and comparing cells internally so `pbinCell`
      stays unexported
- [x] write a failing test that `LegacyStateRoot` returns the root hash from a legacy state
      blob built by `PBinEncodeLegacyState`, which `SetState` refuses
- [x] add `func (c *PBinRecordConverter) LegacyStateRoot(blob []byte) ([]byte, error)`
- [x] write tests for both on malformed input
- [x] run `go test ./execution/commitment/ -count=1` — must pass before task 3

### Task 3: Command surface, output datadir, and migrations off

**Files:**
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `cmd/integration/commands/flags.go`
- Modify: `cmd/integration/commands/commitment_output_test.go`

Lands before the driver is replaced, so the build never goes red.

- [x] write a failing test that `convert-format` refuses a missing `--output.datadir`
- [x] parameterise `stageRebuildOutput` so the converter reuses it and copies the source
      `erigondb.toml` verbatim instead of writing a rebuild target's settings — `trie_variant
      = 'bin'` / `trie_hash = 'blake3'` must survive or the output reads as hex. Do not add a
      second stager, and do not duplicate the refusal tests it already has
- [x] reuse `withRebuildOutputDatadir`; its help already reads "the source datadir stays a
      read-only input". Add `--verify.sample`, and reuse `--resume` rather than adding
      `--continue` — `stageRebuildOutput`'s own refusal text names `--resume`, and a flag the
      command does not define would be unactionable advice
- [x] write a failing test that an `--output.datadir` symlinked into the source is refused;
      make `pathsOverlap` resolve symlinks before comparing (this also tightens the rebuild)
- [x] reassign `datadirCli = out.dirs.DataDir` after staging, and pass `false` for
      `applyMigrations` — mirroring `cmdCommitmentRebuild`
- [x] write a test that a staged run creates no `migrations/` directory in the source
- [x] write a test that staging leaves the source `snapshots/` tree unchanged
- [x] run `go build ./cmd/integration/` and the command tests — must pass before task 4

### Task 4: Hardlink commitment files into the output

**Files:**
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `cmd/integration/commands/commitment_output_test.go`

`linkSnapshotsExceptCommitment` skips every path matching `isCommitmentFileName`. The
converter needs those files present in the output — that is what lets the aggregator enumerate
them and what makes "already current" a free no-op.

- [x] write a failing test that after converter staging the output holds every source file,
      commitment included, each as the same inode
- [x] write a failing test covering the files `isCommitmentFileName` also matches —
      `history/*commitment*.v` and `idx/*commitment*.ef` with their accessors — since a
      substring test is not extension- or directory-scoped
- [x] add the commitment link walk, running after `stageRebuildOutput` so the rebuild path and
      `TestStageRebuildOutput`'s omission assertion are untouched
- [x] write a test that the rebuild path still omits commitment
- [x] run `go build ./cmd/integration/` and the command tests — must pass before task 5

### Task 5: Classification pass and the direct seg write path

**Files:**
- Delete: `db/state/commitment_convert_pbin.go`
- Create: `db/state/commitment_convert_pbin.go` (rewritten)
- Create: `db/state/commitment_convert_pbin_test.go`
- Modify: `cmd/integration/commands/commitment.go` (the sole caller of `ConvertPBinRecordFiles`)

The new signature takes no destination: `datadirCli` was reassigned in Task 3, so `d.dirs` is
already the output's.

- [x] write a failing test that a file holding no legacy record keeps its hardlink — same
      inode as the source, nothing rewritten
- [x] write a failing test that a file holding a legacy record is replaced by a **different**
      inode, and that the source file's bytes are unchanged
- [x] implement the classification pass: scan for the first legacy record and stop there
- [x] implement the link removal — the `.kv` and every accessor sibling — before the
      compressor opens, so no write ever goes through a shared inode
- [x] write a failing test that the output basename equals the source basename, and that a
      mismatch fails the run rather than writing
- [x] implement the direct write: `seg.NewCompressor` into `d.dirs.Tmp`, `d.dataWriter`,
      per-record dispatch, `Collation`, then `buildFileRange` with `static.CleanupOnError()`
- [x] write a test covering a sub-`DomainMinStepsToCompress` file, asserting it round-trips
      through `d.dataReader` — the codec comes from `d.Compression` on both sides and no step
      rule is consulted
- [x] run `go test ./db/state/... -count=1` and `go build ./cmd/integration/` — must pass
      before task 6

### Task 6: Per-file verification

**Files:**
- Modify: `db/state/commitment_convert_pbin.go`
- Modify: `db/state/commitment_convert_pbin_test.go`
- Modify: `db/state/commitment_convert_export_test.go`

- [x] write a failing test that a dropped record fails the run, using a corpus where the
      written count and the source count genuinely differ
- [x] implement the count check against `coll.valuesComp.Count()/2`, not the write loop's own
      counter
- [x] write a failing test that a mangled state record fails the root check
- [x] implement the root check with `LegacyStateRoot` on the source blob and a restored engine
      on the converted blob; set the bin globals in `t.Cleanup` and do not use `t.Parallel`
- [x] run `go test ./db/state/... -count=1` — must pass before task 7

### Task 7: Dispositions, --resume, and failure cleanup

**Files:**
- Modify: `db/state/commitment_convert_pbin.go`
- Modify: `db/state/commitment_convert_pbin_test.go`

- [x] write a failing test that a shard whose verification failed is **removed**, so a
      following `--resume` redoes it rather than skipping a name-complete broken file
- [x] write a failing test that ctx-cancel mid-file removes the partial `.kv` and its
      accessors
- [x] implement the cleanup path on every per-file error exit
- [x] write a failing test that `--resume` skips a converted shard and redoes an incomplete
      one (`.kv` present, accessor missing)
- [x] write a failing test that without `--resume` a non-empty output is **refused** — the
      reused `stageRebuildOutput` gate returns an error and never wipes a user-supplied
      directory
- [x] write a failing test that the enumeration catches a source `.kv` on disk but not
      visible — a missing accessor makes it invisible, and it would be silently absent
- [x] run `go test ./db/state/... -count=1` — must pass before task 8

### Task 8: Sampled positional cross-check

**Files:**
- Modify: `db/state/commitment_convert_pbin.go`
- Modify: `db/state/commitment_convert_pbin_test.go`
- Modify: `db/state/commitment_convert_export_test.go`
- Modify: `cmd/integration/commands/commitment.go`

- [x] write a failing test that a record written under the wrong key is caught
- [x] write a failing test that `--verify.sample=0` disables the pass
- [x] implement strided sampling — every N-th record that took the legacy branch; a
      copied-verbatim record has no legacy header and must not enter the sample
- [x] implement the read-back as a **sequential** re-scan of the finished output file,
      comparing at the recorded positions via `CompareLegacy`. Open no index and name no
      accessor extension — `requiredAccessorsForCommitment` is config-driven and `.kvi` does
      not exist under `AGG_COMMITMENT_BT=1`
- [x] run `go test ./db/state/... -count=1` — must pass before task 9

### Task 9: End-to-end conversion test

**Files:**
- Create: `db/state/commitment_convert_pbin_e2e_test.go`

- [x] build a two-file legacy datadir: real bin commitment files via the existing `state_test`
      datadir helpers, each record rewritten backwards with `PBinEncodeLegacyRecord` and the
      state blob with `PBinEncodeLegacyState`
- [x] checksum the **whole** source datadir before the run, not just `snapshots/`
- [x] convert into an output datadir and assert non-commitment files arrive as hardlinks
      (same inode)
- [x] assert commitment files are converted and decode under the current format
- [x] assert record counts equal, roots equal, sampled cells equal
- [x] assert the source checksum is unchanged, and separately that `<SRC>/temp` gained no
      files and `<SRC>/migrations` was not created — the compressor `.idt` and the recsplit
      temps are the regression this redesign exists to prevent, and a `snapshots/`-scoped
      check cannot see them
- [x] run `go test ./db/state/... -count=1` — must pass before task 10

### Task 10: Failure-mode coverage

**Files:**
- Modify: `db/state/commitment_convert_pbin_test.go`
- Modify: `cmd/integration/commands/commitment.go`

- [x] write a test that a legacy record naming one cell panics and the source stays unchanged
- [x] write a test that context cancellation mid-file leaves the run resumable and the source
      untouched — a compressor fault has no injection point, and ctx-cancel is the reachable
      equivalent
- [x] document in the command help that a single-cell panic leaves a partial output that must
      be investigated, not resumed
- [x] run `go test ./db/state/... -count=1` — must pass before task 11

### Task 11: Verify acceptance criteria

- [ ] verify every requirement in the Overview is implemented
- [ ] verify the staging invariant holds: after `datadirCli` is reassigned, grep the driver
      for any reference to a source path — there must be none
- [ ] confirm no `t.Skip` was added by this branch
- [ ] run `go test ./execution/commitment/... ./db/state/... -count=1`
- [ ] run `go build ./cmd/integration/`
- [ ] run `make lint` — must report 0 issues

### Task 12: [Final] Update documentation

- [ ] rewrite the `convert-format` long help for the output-datadir model — it currently says
      originals are preserved at `<datadir>/snapshots/backup/domains/` and restored with
      `integration commitment convert --restore`, both false under this design
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Requires the real datadir.*

**Manual verification on snap-arb1**

- run `9524-9526` first, 0.59 GB, and confirm the root restores from the converted output
- then all 7: source 430 GB commitment, expected output ~398 GB at −7.5%
- **disk**: source + output on one filesystem is ~830 GB for commitment, plus the compressor's
  `.idt` intermediate and the recsplit temps, which land in `<DST>/temp`. The `.idt` exceeds
  the `.kv` it produces — on the 320 GB shard that is several hundred GB more
- accounts/storage/code cost nothing as hardlinks; commitment files that need no conversion
  cost nothing either, since their hardlink is kept
- record wall-clock and throughput against the 109 h rebuild; this is sequential I/O bound
- start erigon against the output and confirm it reads as bin, not hex

**Measurements to log**

- per-file byte delta and total, against the −7.5% measured on the synthetic corpus
- whether any file reported a single-cell record — expected zero, since `foldPropagate`
  collapses a sole survivor and only `foldBranch` writes a record
