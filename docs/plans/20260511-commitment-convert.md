# Commitment Domain File Converter (`integration commitment convert`)

## Overview

Add a new offline subcommand `integration commitment convert` that re-encodes existing `CommitmentDomain` `.kv` files in a datadir between two independent axes:

- **Value squeeze state**: squeezed (plain account/storage keys in BranchData replaced by offsets into matching `account.kv` / `storage.kv` files) vs unsqueezed (full plain keys inline).
- **Key encoding**: V1 (`commitment.HexNibblesToCompactBytes` / `commitment.uncompactNibbles` — the latter needs export to `UncompactNibbles`) vs V2 (`nibbles.EncodeKeyV2` / `nibbles.DecodeKeyV2` — prefix-sort matches trie subtree locality, see #17838).

Both flags default false. Flag value = target state, not direction. The tool detects the current state of each file via sampling and converts only what is needed, so the operation is idempotent and re-runnable after crashes.

This is foundational infrastructure for a future global V2 cut-over: it lets an existing datadir flip to V2 without a full re-sync. The squeeze axis exists already in production (`SqueezeCommitmentFiles`) but as a one-way migration tied to rebuild; this converter generalises both axes into one offline tool, with explicit staging and backup so a failed conversion is always recoverable.

## Context (from discovery)

- **Worktree**: `/Users/awskii/org/wrk/erigon-r36converter` on branch `awskii/r36converter` (based off `origin/awskii/nibblesv2`).
- **V1 codec on this branch** (verified):
  - Forward: `execution/commitment/keys_nibbles.go:55 HexNibblesToCompactBytes` (public).
  - Reverse: `execution/commitment/keys_nibbles.go:91 uncompactNibbles` — **private**. Must be exported as `UncompactNibbles` in Task 1.
  - Live commitment domain keys are produced by `HexNibblesToCompactBytes` (call sites: `hex_patricia_hashed.go:1771,2066`, `hex_concurrent_patricia_hashed.go:320`, `warmuper.go:197`, `verify_test.go` × 4).
  - Read side uses `uncompactNibbles` (call sites: `commitment.go:1168`, `verify.go:52`).
  - **Note**: `execution/commitment/trie/encoding.go` has lowercase `hexToCompact` / `compactToHex` (`:47, :64`) — a different trie-spec codec used only by the trie subpackage. **Not** the commitment-domain key codec. Ignore it for this work.
- **V2 codec**: `execution/commitment/nibbles/nibbles_v2.go` — `EncodeKeyV2`, `DecodeKeyV2`, `MaxPathNibbles=128`. Pure additive on the branch base; not yet wired into any runtime callsite. **Note: `EncodeKeyV2` panics on malformed input (length > 128 or nibble > 0x0F)**; converter must validate via the V1 decode step before calling it, otherwise a corrupt source file panics the run.
- **Squeeze migration template**: `db/state/squeeze.go:121 SqueezeCommitmentFiles` — the closest existing analogue. Reads commitment `.kv`, applies `commitmentValTransformDomain` to each non-state value, writes to `<orig>.kv.squeezed.tmp`, renames to `<orig>.kv.squeezed`, then renames back at the end. Range matching uses `rawLookupFileByRange` (verified: `squeeze.go:199,203,207`), returning `*FilesItem`. The converter follows the same idioms (range matching, log ticker, size delta tracking) but with a different staging strategy.
- **`SqueezeCommitmentFiles` early-returns when `at.a.Cfg(kv.CommitmentDomain).ReplaceKeysInValues == false`** (`squeeze.go:125-128`). The converter **does not** mirror this guard: it operates on what is on disk, independent of runtime config. Document this difference; in particular, `--squeeze=true` must work even if the live config flag is false.
- **Read-path unsqueeze**: `db/state/domain_committed.go:50-52 replaceShortenedKeysInBranch` — method on `*AggregatorRoTx`. Per project memory it's also called from `db/integrity/commitment_integirty.go` (5 sites) and `db/state/aggregator.go`. The inner expansion logic needs to be extracted into a free function so the converter can call the same code path.
- **`commitmentValTransformDomain`** is a private method on `*DomainRoTx` (`domain_committed.go:291`). Since `commitment_convert.go` lives in the same `db/state/` package, it can call this method directly without export.
- **`Aggregator.BuildMissedAccessors`** (verified: `aggregator.go:728`): `func (a *Aggregator) BuildMissedAccessors(ctx context.Context, workers int) error`. **Not called** by this iteration — indexes are produced per file in Phase 1 via `dumpStepRangeToPath` → `buildFileRange`. Documented here only as a reference signature in case a fallback rebuild path becomes necessary.
- **Integration command boilerplate**: `cmd/integration/commands/commitment.go:258 cmdCommitmentRebuild` (db lifecycle, error logging), `cmd/integration/commands/commitment.go:407 cmdCommitmentPrint` (aggregator wiring via `DisableAllDependencies` + `BeginFilesRo`, no temporal Rw tx). The converter mirrors `print`'s aggregator setup (read-only files view).
- **Existing squeeze flag**: `cmd/integration/commands/flags.go:120 withSqueeze` — default `true` for rebuild. Converter wants default `false`, so a separate variable / helper is needed.
- **Torrent siblings**: each commitment file has up to 4 separate `.torrent` siblings — `.kv.torrent`, `.bt.torrent`, `.kvi.torrent`, `.kvei.torrent` (verified at `squeeze.go:68-74`). All become **stale** after Phase 5 because content hashes change. Phase 4 must remove them; regeneration is **out of scope** (downloader handles, or user runs a separate command).
- **`.kvei` may be absent**: not all commitment files have all four index extensions. Phase 1 must tolerate missing siblings (link-if-exists, not link-each-mandatory).

## Development Approach

- **Testing approach**: Regular (code first, then tests) — Go convention, brainstorm did not request TDD.
- Each task is one logical unit; later tasks depend on earlier ones (extract → detect → xform → per-file → orchestrator → CLI).
- **Every task must include new/updated tests for code introduced in that task.** Tests for the per-axis round-trip suite live in `db/state/commitment_convert_test.go`; tests for the extracted free function live next to where it now lives.
- **All tests must pass before starting the next task.** `make test-short` for the package under change is the gate.
- **Update this plan file when scope changes.**
- Run `make lint && make erigon integration` before any push.

## Testing Strategy

- **Unit tests**: required for every code-introducing task. Synthetic in-memory or tmpdir-backed mini commitment files; no external dependencies, no real datadir.
- **No e2e tests**: this is a CLI-only feature; integration sanity is a manual step listed under Post-Completion. Erigon does not have UI-based e2e tests applicable here.
- **Detection vote tests** are particularly important — a wrong vote silently corrupts a file's encoding. Cover all-V1 / all-V2 / mixed-within-file / single-pair files explicitly.
- **Round-trip equivalence** is the load-bearing correctness invariant: byte-equal output after forward + reverse on each axis (and composed).

## Progress Tracking

- Mark completed items with `[x]` immediately when done (not batched).
- Add newly discovered tasks with ➕ prefix.
- Document blockers with ⚠️ prefix.
- Keep `[ ]` / `[x]` in sync with reality — this file is the single source of truth during implementation.

## Solution Overview

The converter is one new file `db/state/commitment_convert.go` exposing `ConvertCommitmentFiles(ctx, at, opts) error`, plus a thin Cobra wrapper in `cmd/integration/commands/commitment.go`. All file access goes through aggregator APIs (`AggregatorRoTx` / `DomainRoTx`) — no direct `seg.NewDecompressor`. The dump path reuses `(*Domain).dumpStepRangeOnDisk` (the same helper `RebuildCommitmentFiles` uses at `squeeze.go:1091`) via a new variant that accepts a custom destination directory and a flag to skip `integrateDirtyFiles`.

**Refactor required (Task 1.5)**: extract the body of `(*Domain).dumpStepRangeOnDisk` (`domain.go:610`) into `dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, vt, dstDir string, integrate bool) error`. The existing method becomes a thin wrapper calling `dumpStepRangeToPath(..., d.dirs.SnapDomain, true)`. The converter calls the new helper with `dstDir = <datadir>/snapshots/rebuild/domain/` and `integrate = false`. Adjust `coll.valuesPath` (currently `d.kvNewFilePath(stepFrom, stepTo)`) to be built off `dstDir` when overridden; same for the path passed to `buildFileRange` for index outputs.

The orchestrator runs five sequential phases per invocation. **Phase 1 must complete for every file before Phase 2 begins** — no per-file backup/promote interleaving. The originals in `snapshots/domain/` remain untouched (and live, readable via the aggregator) for the entire duration of Phase 1; if anything goes wrong during conversion, the user can simply `rm -rf snapshots/rebuild/` and the datadir is back to its starting state with no manual recovery needed.

1. **Convert all** — for each commitment file enumerated via `at.Files(kv.CommitmentDomain)`:
   - Take 48 distributed samples (BT ordinal lookup) to detect current `(squeezed, keysV2)` state.
   - If state == target → skip (log "already in target state").
   - Otherwise: read source pairs via `at.d[kv.CommitmentDomain].dataReader(file.decompressor)`, apply keyXform per pair, push `(newKey, oldValue)` into the commitment domain's wal in a fresh `*TemporalMemBatch`. The value-side squeeze/unsqueeze transform is supplied to `dumpStepRangeToPath` as the `vt valueTransformer` parameter (same hook `RebuildCommitmentFiles` already uses for the squeeze pass).
   - Call `dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, vt, <rebuild/domain/>, false)` — collates (ETL sort), writes `.kv` + builds `.bt`/`.kvi`/`.kvei`, **does not** integrate into the aggregator's dirty files. Originals in `snapshots/domain/` remain readable via the aggregator throughout this phase.
   - Track `sizeDelta` per file (size of original vs new); log `[commitment_convert] phase 1 file done <name> <delta%>`.
   - At the end of Phase 1: log `[commitment_convert] phase 1 complete: converted <m>, skipped <n>, total <m+n>`. Only proceed to Phase 2 once every source file has been processed (or skipped). If any conversion failed → abort and tell the user to clean up by running `rm -rf snapshots/rebuild/`.
2. **Pre-swap check** — for every commitment file in `snapshots/domain/`, verify a matching `.kv` (and its full set of newly-built indexes per the commitment domain config) exists in `snapshots/rebuild/domain/`. Abort listing missing pairs if any.
3. **Backup originals** — `mkdir snapshots/backup/domains/` (refuse if non-empty). `mv` each original commitment `.kv` + `.bt` + `.kvi` + `.kvei` (move-if-exists; `.kvei` may be absent) plus all four `.torrent` siblings (`.kv.torrent`, `.bt.torrent`, `.kvi.torrent`, `.kvei.torrent`) from `snapshots/domain/` → `snapshots/backup/domains/`. `mv` not `cp`: `snapshots/domain/` is empty of commitment files after this phase.
4. **Promote** — `mv snapshots/rebuild/domain/*` → `snapshots/domain/` (all commitment files plus their built indexes). `rmdir snapshots/rebuild/domain/` (and parent `rebuild/` if empty).
5. **Reopen + log revert instruction** — `agg.OpenFolder()` picks up the new files. No `BuildMissedAccessors` call: indexes were already built per-file in Phase 1 by `dumpStepRangeToPath` → `buildFileRange`. **Emit a prominent revert instruction** describing the final filesystem state and the exact recovery command:
   ```
   [commitment_convert] DONE. converted <n> files. Originals preserved at:
       <datadir>/snapshots/backup/domains/
   To revert this conversion:
       rm snapshots/domain/*-commitment.* && mv snapshots/backup/domains/* snapshots/domain/ && restart erigon
   ```
   This is the single instruction the user keeps; the intermediate per-phase logs are for progress only.

Inner expansion logic from `(*AggregatorRoTx).replaceShortenedKeysInBranch` is extracted to a free function in `db/state/squeeze.go` so the unsqueeze direction in valXform calls the same code path the read path uses. The V1 reverse codec `uncompactNibbles` is exported as `UncompactNibbles` in `execution/commitment/keys_nibbles.go` so the converter's V2→V1 keyXform can call it.

Detection uses 48 distributed samples (via BT ordinal lookup) plus codec error checking: call `nibbles.DecodeKeyV2(k)` per sample — any error → V1; all canonical → V2. For value squeeze state, parse BranchData and look for any plain-key field shorter than 10 bytes (`binary.MaxVarintLen64`) — one short key proves squeezed. Mixed key-encoding signals within a single file abort the run with that file's path.

**Note about torrents**: `.torrent` siblings move to backup with their files but are **not** regenerated for the new files in this iteration. Torrents become stale after Phase 4 because the new files have different content hashes. Regeneration is out of scope; documented as a follow-up (downloader handles, or user runs a separate command).

## Technical Details

### Data structures

```go
// db/state/commitment_convert.go (same package as squeeze.go)
type ConvertOpts struct {
    TargetSqueeze   bool
    TargetNibblesV2 bool
}

type fileState struct {
    keysV2   bool
    squeezed bool
}

// errSkip is the only sentinel error compared by callers (orchestrator skips file).
// Mixed-detection and range-mismatch errors are fmt.Errorf with context — never compared.
var errSkip = errors.New("file already in target state")
```

`KeyCommitmentState` is handled by a single `if bytes.Equal(k, commitmentdb.KeyCommitmentState)` carve-out in the streaming loop (pass-through for both key and value today). No struct-field hook — re-introduce only when a follow-up task actually needs to mutate the state blob. The carve-out itself is the reserved location.

### Aggregator-level access (use API, not seg directly)

All file enumeration, reading, and index work goes through `AggregatorRoTx` / `DomainRoTx`, never raw `seg.NewDecompressor`. The converter only opens `seg.Compressor` for writing new files into `snapshots/rebuild/domain/` (mirroring `SqueezeCommitmentFiles` at `squeeze.go:229-234`).

- **File enumeration**: `at.Files(kv.CommitmentDomain)` returns `VisibleFiles` — each has `StartRootNum()`, `EndRootNum()`, `Fullpath()`, and an underlying decompressor + BT index handle.
- **Range matching**: `at.d[kv.AccountsDomain].rawLookupFileByRange(start, end)` and the storage equivalent — returns `*FilesItem`. Same pattern as `SqueezeCommitmentFiles` (`squeeze.go:199-207`). Missing match → abort with the range in the error.
- **Compression-aware reader**: `at.d[kv.CommitmentDomain].dataReader(file.decompressor)` returns `*seg.Reader` honoring the domain's compression config.
- **Compression-aware writer**: `at.d[kv.CommitmentDomain].dataWriter(compressor, forceNoCompress)`.
- **Pair counts** for sampling-stride math: `at.KeyCountInFiles(kv.CommitmentDomain, startRootNum, endRootNum)` (already used by `printCommitment` at `commitment.go:446`).
- **Ordinal lookup for distributed sampling**: only the `.bt` index supports getting the i-th key directly. Implementation locates the BT API on the file's accessor and uses it for the 48 sample reads.

### Detection (48 distributed samples; codec + short-key signals)

```go
func detectFileState(at *AggregatorRoTx, file VisibleFile, samples int) (fileState, error)
```

**Sampling strategy — distributed, not consecutive.** Commitment files are sorted, so the first N entries cluster on a shared prefix and trailing-byte / branch-shape distributions there are not representative of the file. Either (a) use BT ordinal lookup if available on the BT index (check at implementation time), or (b) fall back to stride-skip on a sequential `seg.Reader`: `stride = max(1, totalPairs / samples)`, read one (k,v) then skip `stride − 1` pairs, repeat. Skip uses `seg.Reader.Skip()` if available; otherwise read-and-discard.

Reader is obtained via aggregator-level API: `at.d[kv.CommitmentDomain].dataReader(file.decompressor)` — compression-aware, do not open `seg.Decompressor` directly. `totalPairs` derived from `at.KeyCountInFiles(kv.CommitmentDomain, file.StartRootNum(), file.EndRootNum())`.

**Key encoding (V1 vs V2) — call the codec, check the error:**

For each non-state key, call `nibbles.DecodeKeyV2(k)`. The four sentinel errors (`ErrV2KeyLength`, `ErrV2KeyParity`, `ErrV2KeyShape`, `ErrV2NonCanonicalPad`) cover every non-canonical V2 input.

- Any sample returns an error → file is V1; stop scanning.
- All samples decode canonically → file is V2.

V1 and V2 canonical forms can overlap byte-wise — a real V1 key may coincidentally also decode V2-canonically. For an all-V1 file, the probability that **every** sampled key decodes V2-canonically is vanishingly small under uniformly distributed nibble content (each sample independently has roughly ~1% probability of accidentally satisfying V2 canonicality due to the trailing-byte + pad-nibble constraints; across 48 distributed samples this is ~10⁻⁹⁶). Document this floor in the code comment; rely on distributed sampling rather than consecutive reads to avoid prefix-clustered false matches.

If sample yields zero non-state keys → return error: "cannot detect key encoding from file <name>: no non-state keys in <n> sampled pairs". Caller widens or aborts.

**Value squeeze detection — single decisive short-key signal:**

Squeezed BranchData encodes plain account/storage keys as compact varints (≤ `binary.MaxVarintLen64 = 10` bytes). Plain keys come in exactly two sizes: **20 bytes** (account: address) or **52 bytes** (storage: 20-byte address + 32-byte storage slot). **Any sampled BranchData containing a plain-key field with length `< 10` (and necessarily neither 20 nor 52) proves the file is squeezed.**

Algorithm:
- For each sampled non-state (k, v), parse `v` as `commitment.BranchData`, iterate embedded plain-key fields.
- If **any** embedded key has length `< binary.MaxVarintLen64` → return `{squeezed: true}` immediately.
- If all sampled keys are `≥ 10` (in practice exactly 20 or 52) → return `{squeezed: false}`.
- Asymmetric on purpose: one short key is decisive; "unsqueezed" requires absence of short keys across all samples. Real varint offsets within a step typically fit in ≤ 4 bytes, well clear of the 20/52-byte plain-key sizes.

### keyXform (4 cases)

Codec functions live in two packages on this branch:
- V1 forward (nibbles → compact bytes): `commitment.HexNibblesToCompactBytes` (public, `keys_nibbles.go:55`).
- V1 reverse (compact bytes → nibbles): `commitment.UncompactNibbles` — to be exported in Task 1 from the current private `uncompactNibbles` (`keys_nibbles.go:91`).
- V2 forward and reverse: `nibbles.EncodeKeyV2` / `nibbles.DecodeKeyV2`.

| Detected | Target | Transform                                                            |
|----------|--------|----------------------------------------------------------------------|
| V1       | V1     | pass-through                                                          |
| V1       | V2     | `nibbles.EncodeKeyV2(commitment.UncompactNibbles(k))`                |
| V2       | V1     | `commitment.HexNibblesToCompactBytes(nibbles.DecodeKeyV2(k))` (DecodeKeyV2 error propagates) |
| V2       | V2     | pass-through                                                          |

`EncodeKeyV2` panics on out-of-range nibbles or path length > 128. The V2 forward path is always preceded by `DecodeKeyV2` (V2→V1) or `UncompactNibbles` (V1→V2), both of which produce nibble arrays bounded by the source path length. Path length > 128 should not occur for valid commitment keys; if it does, the converter intentionally panics so a corrupt source file fails loudly rather than silently producing garbage.

### valXform (4 cases)

| Detected   | Target     | Transform                                                                                                  |
|------------|------------|------------------------------------------------------------------------------------------------------------|
| unsqueezed | unsqueezed | pass-through                                                                                                |
| unsqueezed | squeezed   | `commitment.commitmentValTransformDomain(rng, accounts, storage, af, sf)` — method on `*DomainRoTx`, callable from same package |
| squeezed   | unsqueezed | new free function `ExpandShortenedKeysInBranch(branch, accounts, storage, af, sf, startTxNum, endTxNum)` (extracted from `replaceShortenedKeysInBranch` in Task 1) |
| squeezed   | squeezed   | pass-through                                                                                                |

`af` (accounts FilesItem) and `sf` (storage FilesItem) come from `at.d[kv.AccountsDomain].rawLookupFileByRange(startTxNum, endTxNum)` and the storage equivalent — both return `*FilesItem` (same pattern `SqueezeCommitmentFiles` uses at `squeeze.go:199-207`). Missing match → abort with the range in the error.

The converter does **not** mirror `SqueezeCommitmentFiles`'s `at.a.Cfg(kv.CommitmentDomain).ReplaceKeysInValues` early-return (`squeeze.go:125-128`). The runtime config flag governs what *new* commitment writes emit; the converter operates on existing on-disk files regardless. `--squeeze=true` works even when the live runtime flag is `false`.

### KeyCommitmentState

`commitmentdb.KeyCommitmentState` value is opaque metadata. Today: pass through both transforms unchanged. The orchestrator dispatches state values through `xformFns.state` (a stub that returns input) so a later iteration can wire real transformation without touching the rest of the pipeline.

### Per-file pipeline

```go
func convertCommitmentFile(ctx context.Context, at *AggregatorRoTx, file VisibleFile, dstDir string, opts ConvertOpts, logger log.Logger) (sizeDelta datasize.ByteSize, deltaPct float32, err error)
```

The conversion reuses `dumpStepRangeToPath` (new helper extracted in Task 1.5 from `dumpStepRangeOnDisk`) for the actual file production — it handles ETL collate (sort), `seg.Compressor` write, `buildFileRange` (indexes), and is told to skip `integrateDirtyFiles`. The converter's job is to detect file state, transform keys, push (newKey, oldValue) into the commitment domain's wal in a fresh `TemporalMemBatch`, and pass the value transformer as `vt`.

1. Compute pair count: `total := at.KeyCountInFiles(kv.CommitmentDomain, file.StartRootNum(), file.EndRootNum())`.
2. Sample state: `state, err := detectFileState(at, file, 48)`.
3. If `state.keysV2 == opts.TargetNibblesV2 && state.squeezed == opts.TargetSqueeze` → return `errSkip`.
4. Resolve range: parse `(stepFrom, stepTo)` from the file (e.g. `file.StartRootNum()/stepSize`, `file.EndRootNum()/stepSize`); for offset lookups also resolve `(startTxNum, endTxNum)`. Look up matching `af`/`sf` via `at.d[kv.AccountsDomain].rawLookupFileByRange(startTxNum, endTxNum)` and the storage equivalent. Missing → return error naming the range.
5. Build keyXform from `(state.keysV2, opts.TargetNibblesV2)`.
6. Build `vt valueTransformer` for the squeeze axis (see "valXform (4 cases)"):
   - if both `state.squeezed == opts.TargetSqueeze` → `vt = nil` (pass-through).
   - else if target=squeezed → `vt = at.d[kv.CommitmentDomain].commitmentValTransformDomain(rng, accounts, storage, af, sf)`.
   - else if target=unsqueezed → `vt = func(v []byte, _, _ uint64) ([]byte, error) { return ExpandShortenedKeysInBranch(v, ...) }` adapted to the `valueTransformer` signature.
7. Allocate a fresh `TemporalMemBatch` for this conversion (or borrow one from a pool); obtain the commitment domain's `wal := batch.domainWriters[kv.CommitmentDomain]`.
8. Read source pairs via aggregator: `reader := at.d[kv.CommitmentDomain].dataReader(file.decompressor)`; `reader.Reset(0)`. For each (k, v):
   - if `bytes.Equal(k, commitmentdb.KeyCommitmentState)` → push `(k, v)` into the wal unchanged.
   - else → push `(keyXform(k), v)` into the wal. **Do not** apply `vt` here — `dumpStepRangeToPath` will apply it during collate.
9. 30s `time.Ticker` inside the read loop logs `progress <ki>/<total> file=<name>`.
10. Call `at.d[kv.CommitmentDomain].d.dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, vt, dstDir, false)`. The helper:
    - Runs `collateETL` against the wal → sorted (k, v) stream with `vt` applied to each non-state value.
    - Writes to `dstDir/<commitment.kvFileName(stepFrom, stepTo)>` via `seg.Compressor` honoring the domain's compression config.
    - Runs `buildFileRange` → produces `.bt` / `.kvi` / `.kvei` per the commitment domain config alongside the `.kv` in `dstDir`.
    - Skips `integrateDirtyFiles` because `integrate=false` — the aggregator's view of `snapshots/domain/` remains the original files.
11. Compute `sizeDelta, deltaPct` from `os.Stat` of the new `.kv` vs the source `file.Fullpath()` (same `getSizeDelta` pattern as `squeeze.go:156-166`).

The function does **not** modify `snapshots/domain/` or the aggregator's dirty-file state; only writes into `dstDir`. The orchestrator handles backup, promote, and reopen.

### Aggregator wiring

Same setup as `printCommitment`:

```go
agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
agg.PresetOfflineMerge()
agg.SetSnapshotBuildSema(semaphore.NewWeighted(int64(runtime.NumCPU())))
agg.DisableAllDependencies()
acRo := agg.BeginFilesRo()
defer acRo.Close()
defer acRo.MadvNormal().DisableReadAhead()
```

Plus `agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)` during Phase 2 so the writer emits exactly the xform output without domain-config post-processing.

### Logging (mirror SqueezeCommitmentFiles)

The orchestrator computes one `progressPrefix` string per file (e.g. `"(3/12 files, 25.0% overall)"`) and passes it to `convertCommitmentFile`. The per-file function appends it to its own log lines verbatim — no overall-% arithmetic inside the conversion loop. Per-file `<file_pct>` is computed locally during the read loop as `100*ki/totalForFile`.

Per-phase progress lines:
```
[commitment_convert] phase 1 file done <name> sizeDelta=<delta%> ki=<count> (<m>/<N> files, <overall_pct>% overall)
[commitment_convert] progress <ki>/<totalForFile> file=<name> (<file_pct>% in file) (<m>/<N> files, <overall_pct>% overall)   # 30s ticker inside phase 1
[commitment_convert] phase 1 complete: converted <m>, skipped <n>, total <m+n>
[commitment_convert] phase 2 pre-swap check: <n> files ok
[commitment_convert] phase 3 backup: <n> files moved to snapshots/backup/domains/
[commitment_convert] phase 4 promote: <n> files moved to snapshots/domain/
```

Final revert instruction (emitted only after Phase 5 completes):
```
[commitment_convert] DONE. converted <n> files. Originals preserved at:
    <datadir>/snapshots/backup/domains/
To revert this conversion:
    rm snapshots/domain/*-commitment.* && mv snapshots/backup/domains/* snapshots/domain/ && restart erigon
```

Error path: if Phase 1 fails for any file, the log line tells the user that originals are intact in `snapshots/domain/` and the partial outputs in `snapshots/rebuild/domain/` can be deleted (`rm -rf snapshots/rebuild/`). No reversion needed because nothing in `snapshots/domain/` was touched yet.

## What Goes Where

- **Implementation Steps** (checkboxes): code in `db/state/`, `cmd/integration/commands/`, and their tests.
- **Post-Completion** (no checkboxes): manual integration sanity against a real dev datadir.

## Implementation Steps

### Task 1: Extract `ExpandShortenedKeysInBranch` free function

**Files:**
- Modify: `db/state/squeeze.go`
- Modify: `db/state/domain_committed.go`
- Modify or create: `db/state/squeeze_test.go` (existing) or `db/state/domain_committed_test.go`

- [x] Read `db/state/domain_committed.go` to locate `(*AggregatorRoTx).replaceShortenedKeysInBranch` and identify the inner expansion body (the portion that takes a branchData byte slice, an `af`/`sf` pair, and txNum range and returns the expanded branchData).
- [x] Extract the inner body into a new free function in `db/state/squeeze.go`: `func ExpandShortenedKeysInBranch(branch commitment.BranchData, accounts, storage *DomainRoTx, accountFile, storageFile *FilesItem, startTxNum, endTxNum uint64) (commitment.BranchData, error)`. Signature carries `*DomainRoTx` for accounts/storage so the function can reuse `lookupByShortenedKey` (which needs the per-domain scratch buffer + logger); same shape `commitmentValTransformDomain` already uses.
- [x] Update `replaceShortenedKeysInBranch` to delegate to `ExpandShortenedKeysInBranch`. The method keeps all read-path guards (config flag, empty branch, KeyCommitmentState carve-out, files-not-empty, threshold-reached range) and the `branchKeyDerefSpent` metric (semantic shift: per-branch instead of per-key; debug-gated and unused in non-debug builds).
- [x] Update 5 call sites in `db/integrity/commitment_integirty.go` — N/A: integrity functions (`checkDerefBranch` et al.) use their own `BranchData.ReplacePlainKeys` closure with a `seg.Reader`-backed lookup; they do not call `replaceShortenedKeysInBranch`. Project-memory cross-ref was stale; no edits needed.
- [x] Write tests for `ExpandShortenedKeysInBranch`. Added `TestExpandShortenedKeysInBranch_ReadPath` in `db/state/squeeze_test.go`: builds squeezed commitment files via existing `testDbAggregatorWithFiles` + `SqueezeCommitmentFiles`, walks every non-state branch via the public read path (`DebugGetLatestFromFiles`), and asserts each expanded BranchData (a) `.Validate(branchKey)`s and (b) has only plain (20-byte / 52-byte) embedded keys. Direct in-package testing was rejected because `db/state` tests cannot import `db/kv/temporal` (import cycle).
- [x] Run `go test ./db/state/... -run ExpandShortenedKeys -count=1 -failfast` — passes (2.5s).

### Task 1.5: Factor `dumpStepRangeOnDisk` to accept a destination directory

**Files:**
- Modify: `db/state/domain.go`
- Modify or create: `db/state/domain_test.go` (existing has tests for collate/build path)

- [x] In `db/state/domain.go:610`, extract the body of `(d *Domain) dumpStepRangeOnDisk` into a new method: `func (d *Domain) dumpStepRangeToPath(ctx context.Context, stepFrom, stepTo kv.Step, batch *TemporalMemBatch, vt valueTransformer, dstDir string, integrate bool) error`.
- [x] Inside `dumpStepRangeToPath`: when `dstDir != ""`, use it as the output directory instead of `d.dirs.SnapDomain`. `dstDir` is threaded through `collateETL` (sets `coll.valuesPath` via `kvNewFilePathIn`) and `buildFileRange` (uses `kviAccessorNewFilePathIn`, `kvBtAccessorNewFilePathIn`, `kvExistenceIdxNewFilePathIn`).
- [x] Added 4 `*In` helpers (kv, kvi, kvei, bt) accepting a base directory; original helpers delegate with `""`. Also split `buildHashMapAccessor` into a thin wrapper plus `buildHashMapAccessorAt(ctx, idxPath, …)` so `buildFileRange` can supply an explicit idx path rooted at `dstDir`.
- [x] Skip `d.integrateDirtyFiles(...)` when `integrate == false`; close the in-memory `StaticFiles` handles via `CleanupOnError()` so they don't leak (on-disk files in `dstDir` remain).
- [x] Original `dumpStepRangeOnDisk` is now `return d.dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, vt, "", true)`. Existing caller `squeeze.go:1144` unchanged.
- [x] `TestDumpStepRangeToPath` in `db/state/domain_test.go` calls `dumpStepRangeToPath` with `dstDir=tmpDir, integrate=false`: asserts `.kv`, `.bt`, `.kvi` land in `tmpDir`; asserts the file does NOT appear in `d.dirs.SnapDomain`; asserts `d.dirtyFiles` count is unchanged (aggregator view stable). The Domain-level dirtyFiles check is the test surface equivalent of the plan's `at.Files(kv.CommitmentDomain)` invariant.
- [x] Run `go test ./db/state/... -run TestDumpStepRangeToPath -count=1 -failfast` — passes (0.04s).

### Task 2: Detection helpers

**Files:**
- Create: `db/state/commitment_convert.go`
- Create: `db/state/commitment_convert_test.go`

- [x] Create `db/state/commitment_convert.go` with package header, copyright (2026), and types: `ConvertOpts`, `fileState`, `xformFns`, sentinel errors (`errSkip`, `errMixedKeyEncoding`, `errMixedSqueezeState`, `errRangeMatch`, `errNoNonStateSamples`).
- [x] `detectKeyEncoding(samples [][]byte)` — superseded by the `sampledPair` signature below; only the latter is implemented.
- [x] Implement `detectKeyEncoding(samplePairs []sampledPair) (v2 bool, err error)` — for each non-state key call `nibbles.DecodeKeyV2(k)`; any error → V1 (return false); all canonical → V2 (return true). Error on zero non-state samples.
- [x] Implement `detectSqueezeState(samplePairs []sampledPair) (squeezed bool, err error)` — uses `commitment.BranchData.ReplacePlainKeys` with an inspection-only callback that flips a `short` flag when any embedded plain-key field has length `< binary.MaxVarintLen64`. Returns squeezed immediately on first short sighting; unsqueezed if all sample plain-keys are `≥ 10`. Parse failures skip the sample (decisive on what remains).
- [x] Implement `detectFileState(at *AggregatorRoTx, file VisibleFile, samples int) (fileState, error)` — type-asserts to internal `visibleFile`, samples via `sampleViaBT` (BT ordinal lookup) when `fi.bindex` is non-empty, else falls back to `sampleViaStride` on the compression-aware `dataReader`. Delegates to the two detectors and wraps errors with the file path.
- [x] Write tests in `commitment_convert_test.go` covering: all-V1 keys (`TestDetectKeyEncoding_AllV1`), all-V2 (`...AllV2`), one V1 among V2 (`...OneV1AmongV2`), all-state samples (`...StateKeysOnly`), known-ambiguous V1-as-V2 (`...KnownAmbiguous`); squeeze axis: all-squeezed (`TestDetectSqueezeState_AllSqueezed`), all-unsqueezed (`...AllUnsqueezed`), partially-squeezed (`...PartialSqueezed`), state-only (`...StateKeysOnly`), empty-value skipping (`...EmptyValuesSkipped`).
- [x] **Known-ambiguous V1/V2 detection test**: `TestDetectKeyEncoding_KnownAmbiguous` hand-crafts V1 keys via `HexNibblesToCompactBytes` whose final byte is 0x00, exposing the V2-canonical parity=0 false positive. The detector returns V2 (documenting the limitation); the godoc on `detectKeyEncoding` notes the ~10⁻⁹⁶ floor probability under uniform nibble content with 48 distributed samples.
- [x] Run `go test ./db/state/... -run TestDetect -count=1 -failfast` — passes (10 tests, 0.016s).

### Task 3: Key and value transformers

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_test.go`

- [x] Implement `keyXform(detected, target bool) func([]byte) ([]byte, error)` returning the appropriate function from the 4-case table. V1→V2: `nibbles.EncodeKeyV2(commitment.UncompactNibbles(k))`. V2→V1: `commitment.HexNibblesToCompactBytes(decoded)` where `decoded, err := nibbles.DecodeKeyV2(k)` and error propagates. Pass-throughs for matching axes. (`uncompactNibbles` was still private despite being listed as Task 1's job — exported here as a prerequisite; 2 internal callers in `execution/commitment/{verify,commitment}.go` updated.)
- [x] Implement `buildValueTransformer(detectedSqueezed, targetSqueezed bool, commitmentRo, accounts, storage *DomainRoTx, af, sf *FilesItem, rng MergeRange) (valueTransformer, error)` returning a `valueTransformer` (the same type `dumpStepRangeOnDisk` and `commitmentValTransformDomain` use):
  - Both axes match → return `nil` (pass-through; `dumpStepRangeOnDisk` already treats `nil` as no-op).
  - unsqueezed → squeezed → call `commitmentRo.commitmentValTransformDomain(rng, accounts, storage, af, sf)` and return its result. (Renamed the receiver parameter from `commitment` to `commitmentRo` to avoid shadowing the imported `commitment` package.)
  - squeezed → unsqueezed → return a closure that calls `ExpandShortenedKeysInBranch(v, accounts, storage, af, sf, startTxNum, endTxNum)` and adapts its signature to `valueTransformer`. Empty values short-circuit unchanged.
- [x] **No state-value transformer**: confirmed acceptable. KeyCommitmentState values have no embedded plain keys; both `commitmentValTransformDomain` and `ExpandShortenedKeysInBranch` would pass them through unchanged. Task 4's streaming loop will additionally route state rows around `keyXform` (per the per-file pipeline spec). No dedicated state-value hook needed.
- [x] Write tests for each of the 4 keyXform cases with hand-crafted V1/V2 key bytes — added `TestXform_KeyXform_{V1ToV1_Passthrough,V2ToV2_Passthrough,V1ToV2,V2ToV1,V1V2_RoundTrip,V2ToV1_ErrorPropagates}` covering a fixture of 8 nibble paths (empty/odd/even/zeros) drawn from `execution/commitment/nibbles/nibbles_v2_test.go`'s vectors.
- [x] Write tests for `buildValueTransformer`: added `TestXform_BuildValueTransformer_PassThrough` verifying both matching-axes cases return `nil`. The two non-nil cases (squeezed↔unsqueezed) delegate entirely to `commitmentValTransformDomain` and `ExpandShortenedKeysInBranch`; those building blocks are already round-trip-verified end-to-end by the existing `TestAggregator_SqueezeCommitment` and `TestExpandShortenedKeysInBranch_ReadPath` (full aggregator + real squeezed files). The composed converter round-trip lands in Task 4/5's `convertCommitmentFile` tests.
- [x] Run `go test ./db/state/... -run TestXform -count=1 -failfast` — passes (6 tests, 0.017s).

### Task 4: Per-file conversion routine

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_test.go`

- [ ] Implement `convertCommitmentFile(ctx, at *AggregatorRoTx, file VisibleFile, dstDir string, opts ConvertOpts, progressPrefix string, logger log.Logger) (sizeDelta datasize.ByteSize, deltaPct float32, err error)`. The orchestrator builds `progressPrefix` once per file (e.g. `"(3/12 files, 25.0% overall)"`) and passes it in; `convertCommitmentFile` just appends it to its own log lines, no arithmetic inside.
- [ ] Detect state via `detectFileState(at, file, 48)`; return `errSkip` if state == target on both axes.
- [ ] Resolve `(stepFrom, stepTo)` from `file` (e.g., `file.StartRootNum()/stepSize`, `file.EndRootNum()/stepSize`). Resolve `(startTxNum, endTxNum)` similarly. Look up `af := at.d[kv.AccountsDomain].rawLookupFileByRange(startTxNum, endTxNum)` and `sf := at.d[kv.StorageDomain].rawLookupFileByRange(startTxNum, endTxNum)`. Missing → return error naming the range.
- [ ] Build keyXform via Task 3 helper. Build `vt valueTransformer` via Task 3's `buildValueTransformer`.
- [ ] Allocate a fresh `*TemporalMemBatch` for this conversion; obtain the commitment domain's writer (`batch.domainWriters[kv.CommitmentDomain]`).
- [ ] Open source reader via aggregator: `reader := at.d[kv.CommitmentDomain].dataReader(file.decompressor)`; `reader.Reset(0)`.
- [ ] Stream loop with progress accounting. `totalForFile := at.KeyCountInFiles(...)`. Read (k, v); apply keyXform unless `bytes.Equal(k, commitmentdb.KeyCommitmentState)`; push transformed `(newK, v)` (or unchanged `(k, v)` for state) into the commitment wal. **Do not** apply `vt` here; `dumpStepRangeToPath` applies it during collate.
- [ ] 30s `time.Ticker` inside the loop logs `progress <ki>/<totalForFile> file=<name> (<file_pct>% in file) <progressPrefix>` where `<file_pct> = 100*ki/totalForFile`. No overall-% computation here.
- [ ] After the stream completes: `err := at.d[kv.CommitmentDomain].d.dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, vt, dstDir, false)`. This produces `.kv` + indexes in `dstDir` and **does not** integrate them into the aggregator's dirty files.
- [ ] Compute `sizeDelta`, `deltaPct` from `os.Stat` of the new `.kv` (`dstDir/<basename(file).kv>`) vs the source (`file.Fullpath()`); same pattern as `squeeze.go:156-166`.
- [ ] Log file-done line: `phase 1 file done <name> sizeDelta=<delta%> ki=<count> <progressPrefix>`.
- [ ] Write tests: build a synthetic in-memory mini commitment (3–5 branches + 1 state key) plus matching synthetic account/storage stubs in a tmpdir, convert through every (detected, target) combination on both axes individually, assert that the round-trip produces a byte-equal `.kv`. Verify indexes (`.bt`, `.kvi`, `.kvei`) are present in `dstDir` after the call.
- [ ] Run `go test ./db/state/... -run TestConvertCommitmentFile -count=1 -failfast` — must pass before Task 5.

### Task 5: Orchestrator `ConvertCommitmentFiles` (5 phases)

**Files:**
- Modify: `db/state/commitment_convert.go`
- Modify: `db/state/commitment_convert_test.go`

- [ ] Implement `ConvertCommitmentFiles(ctx context.Context, at *AggregatorRoTx, opts ConvertOpts, logger log.Logger) error`.
- [ ] **Pre-flight**: enumerate commitment files via `at.Files(kv.CommitmentDomain)`; if empty → log "no commitment files to convert" and return.
- [ ] **Pre-flight**: refuse if `snapshots/backup/domains/` already exists with content (preserves a prior conversion's backup). Wipe `snapshots/rebuild/domain/` if leftover from a prior crashed run (log it).
- [ ] **Phase 1 (convert all)**: mkdir `snapshots/rebuild/domain/`. For each file: build `progressPrefix` string (`(<idx+1>/<N> files, <overall_pct>% overall)`); call `convertCommitmentFile(..., dstDir=rebuild/domain/, progressPrefix, logger)`. On `errSkip` → log skip line, do not increment converted count; on real error → abort with file path and tell user to `rm -rf snapshots/rebuild/`. Accumulate `totalSizeDelta` and `processedFiles`.
- [ ] **Early exit**: if `processedFiles == 0` after Phase 1 → clean up empty `rebuild/domain/`, log "no files needed conversion", return.
- [ ] **Phase 1 wait gate**: phases 2–5 only begin after the Phase 1 loop completes for **every** file (no per-file interleaving with backup/promote). Log `[commitment_convert] phase 1 complete: converted <m>, skipped <n>, total <m+n>` at the boundary.
- [ ] **Phase 2 (pre-swap check)**: enumerate `rebuild/domain/` for every commitment-file basename present in `snapshots/domain/` (filter `*-commitment.*.kv`); verify each has a matching `.kv` (and its full index set per the commitment domain config) in `rebuild/domain/`. Abort listing the missing files.
- [ ] **Phase 3 (backup)**: `mkdir snapshots/backup/domains/`. For each original commitment file in `snapshots/domain/`: `mv` `.kv` + any of `.bt`/`.kvi`/`.kvei` that exist (move-if-exists) + any of the four `.torrent` siblings that exist → `snapshots/backup/domains/`. After this phase, `snapshots/domain/` contains no commitment files.
- [ ] **Phase 4 (promote)**: `mv` every file in `snapshots/rebuild/domain/` (the converted `.kv` + its newly-built indexes) → `snapshots/domain/`. `rmdir snapshots/rebuild/domain/` and `snapshots/rebuild/` (if empty).
- [ ] **Phase 5 (reopen + revert instruction)**: `agg.OpenFolder()`. No `BuildMissedAccessors` call — indexes were already built per file in Phase 1. Emit the revert instruction (verbatim format in Logging section): `DONE. converted <n> files. Originals preserved at <backup_path>. To revert: rm snapshots/domain/*-commitment.* && mv snapshots/backup/domains/* snapshots/domain/ && restart erigon`.
- [ ] **Full-flow test (`TestConvertCommitmentFiles_FullFlow`)**: build 2–3 synthetic commitment files in a tmpdir with matching synthetic account/storage stubs; run the orchestrator with `--squeeze=true --nibbles.v2=true`; assert: `snapshots/backup/domains/` has the originals (kv + indexes + torrents); `snapshots/domain/` has new kv + freshly-built indexes; `snapshots/rebuild/domain/` is gone; agg.OpenFolder reads the new files successfully; commitment root hash via `domains.GetCommitmentCtx().Trie().RootHash()` is unchanged before vs after.
- [ ] **Composed round-trip test (`TestConvertCommitmentFiles_RoundTripComposed`)** — the load-bearing correctness test: starting state V1+unsqueezed, run convert with `--squeeze=true --nibbles.v2=true`, then convert again with defaults (`--squeeze=false --nibbles.v2=false`). Final `.kv` bytes must equal the starting `.kv` bytes (or at minimum the post-Phase-5 commitment root hash equals the pre-Phase-1 root hash; pick whichever invariant is stable across re-compression).
- [ ] **Edge-case tests**:
  - Pre-populated `snapshots/backup/domains/` → abort with clear error pointing at the existing path.
  - All files already in target state → no `rebuild/domain/`, no `backup/domains/` created; clean return.
  - Leftover `snapshots/rebuild/domain/` from a prior crash → wiped at Phase 1 entry, conversion proceeds.
  - Datadir with zero commitment files → no-op return.
  - Range mismatch (commitment file with no matching account/storage range) → abort during Phase 1 with the offending range in the error, `rebuild/domain/` left in place for user inspection.
- [ ] Run `go test ./db/state/... -run TestConvertCommitmentFiles -count=1 -failfast` — must pass before Task 6.

### Task 6: `integration commitment convert` Cobra command

**Files:**
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `cmd/integration/commands/flags.go`

- [ ] Add `cmdCommitmentConvert` cobra command in `cmd/integration/commands/commitment.go`, registered as a sibling of `cmdCommitmentRebuild` / `cmdCommitmentPrint`. Body mirrors `cmdCommitmentPrint`: `openDB`, get agg, dispatch to a worker `commitmentConvert(db, ctx, logger, opts)`.
- [ ] Implement `commitmentConvert(db kv.TemporalRwDB, ctx context.Context, logger log.Logger, opts dbstate.ConvertOpts) error` — mirrors `printCommitment`'s aggregator setup, then calls `dbstate.ConvertCommitmentFiles(ctx, acRo, opts, logger)`.
- [ ] In `flags.go`: add package vars `convertSqueeze`, `convertNibblesV2` (both `bool`, default `false`). Do **not** reuse `withSqueeze` because its default is `true`.
- [ ] Add helper `withConvertFlags(cmd *cobra.Command)` that registers `--squeeze` and `--nibbles.v2` on the convert subcommand. Use `cmd.Flags().BoolVar(...)` with descriptive help strings stating target-state semantics.
- [ ] Register the new subcommand under the `commitment` parent (find where `cmdCommitmentRebuild` and `cmdCommitmentPrint` are added — same `AddCommand` block).
- [ ] Wire `--datadir` and `--chain` via existing helpers (already attached to the parent; no extra work expected).
- [ ] Build smoke test: `make integration` succeeds; `./build/bin/integration commitment convert --help` shows both flags with default `false`.
- [ ] No new unit tests for cobra wiring (existing `_test.go` files in the cmd package don't cover registration); the smoke test above is sufficient.

### Task 7: Verify acceptance criteria

- [ ] Run full package tests: `go test ./db/state/... -count=1 -failfast`.
- [ ] Run integration package build: `make integration`.
- [ ] Run `make lint` until clean (linter is non-deterministic per CLAUDE.md — repeat).
- [ ] Run `make erigon integration`.
- [ ] Confirm every requirement from Overview is implemented:
  - both flags exist with default `false`
  - target-state semantics (no-op when state matches target)
  - idempotent re-run
  - 48 distributed samples via BT ordinal lookup
  - 5-phase flow: Convert all → Pre-swap check → Backup (mv) → Promote (mv) → Reopen
  - `dumpStepRangeToPath` reused for write path (with `integrate=false`)
  - Revert instruction emitted at end of run
  - Both axes (squeeze, nibbles.v2) work in both directions
  - `KeyCommitmentState` passed through unchanged
  - Per-file and overall progress percentages logged
- [ ] Confirm edge cases land: empty datadir; pre-populated backup (refuse); leftover rebuild dir (wipe + log); range mismatch (abort with offending range).

### Task 8: Update documentation and finalize

**Files:**
- Modify: `docs/plans/20260511-commitment-convert.md` (this file)
- Possibly modify: `CLAUDE.md` (only if a non-obvious pattern was discovered worth recording)

- [ ] Verify all checkboxes above are marked `[x]`.
- [ ] Mark any deviations or scope changes inline.
- [ ] `mkdir -p docs/plans/completed && git mv docs/plans/20260511-commitment-convert.md docs/plans/completed/`.

## Post-Completion

*Manual steps, not part of the implementation checklist.*

**Manual integration sanity** (validates the load-bearing correctness invariant):

1. Take a small synced dev-chain datadir (or copy a partial mainnet datadir; see `erigon-datadir` skill).
2. Run `./build/bin/integration commitment print --datadir <path> --chain <chain>` and record the root hash.
3. Run `./build/bin/integration commitment convert --datadir <path> --chain <chain> --squeeze=true --nibbles.v2=true`.
4. Run `integration commitment print` again — root hash must match.
5. **Clear backup** before the second convert run: `rm -rf <path>/snapshots/backup/domains/` (Task 5 pre-flight refuses to overwrite an existing backup). The state in `snapshots/domain/` is the V2+squeezed version and remains intact.
6. Run `./build/bin/integration commitment convert --datadir <path> --chain <chain>` (defaults: `--squeeze=false --nibbles.v2=false`) — converts back to V1+unsqueezed.
7. Root hash unchanged.
8. Spot-check `snapshots/domain/` lists the new `.kv` + freshly-built `.bt`/`.kvi`/`.kvei`; `snapshots/backup/domains/` now holds the V2+squeezed version from step 3.

**Follow-up work tracked separately** (do not include in this plan):

- Commitment history (`.v`, `.ef`) re-encoding.
- `KeyCommitmentState` value transform (state-blob hook is reserved here).
- `--recover` subcommand (today: the Phase 5 end-of-run log line prints the exact `mv` command users need to revert from `snapshots/backup/domains/`).
- Parallel per-file conversion (today: sequential).
- Same converter for other domains (Account/Storage/Code) — note: squeeze and nibbles V2 are commitment-specific concepts, so this is not a generic feature.
- `--dry-run`.

**Reference for global V2 cut-over (out of scope)**: switching the runtime to V2 means changing call sites in `execution/commitment/{hex_patricia_hashed.go, hex_concurrent_patricia_hashed.go, trie_reader.go, verify.go, warmuper.go, trie/hasher.go}` plus an aggregator-level flag. The converter only handles on-disk file format; runtime selection is a separate change.
