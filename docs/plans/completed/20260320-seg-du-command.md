# `erigon seg du` — Snapshot Disk Usage Command

## Overview

Add a `du` subcommand to `erigon seg` (aka `erigon snapshots`) that reports snapshot disk usage broken down by category, with estimated sizes for each node type (archive/full/minimal).

Replaces the draft implementation in PR #18807 with a corrected design that properly reflects prune mode semantics.

## Context (from discovery)

**Files to modify:**
- `cmd/utils/app/snapshots_cmd.go` — add `du` subcommand + handler + helpers

**Key reference files:**
- `db/datadir/dirs.go` — `Dirs` struct with `Snap`, `SnapDomain`, `SnapHistory`, `SnapIdx`, `SnapAccessors`, `SnapCaplin` paths
- `db/snaptype/files.go` — `ParseFileName()` returns `FileInfo{From, To, Ext, TypeString, ...}`
- `db/kv/prune/storage_mode.go` — prune mode definitions, `DefaultPruneDistance=100_000`
- `db/kv/tables.go` — domain string constants: `CommitmentDomain→"commitment"`, `RCacheDomain→"rcache"`
- `db/config3/config3.go` — `DefaultPruneDistance = 100_000`
- `db/snapshotsync/snapshotsync.go` — `isStateSnapshot()`, `isStateHistory()` classification helpers (reference only)

**Patterns to follow:**
- Command registration: same as `ls` subcommand (no file locking needed, read-only)
- Datadir: `datadir.New(c.String(utils.DataDirFlag.Name))` — no `MustFlock()`
- Chain name: open chaindata MDBX readonly via `fromdb.ChainConfig(chainDB).ChainName` (same as `doLS`)

## Prune Mode Semantics (verified from code)

| Mode | History | Blocks | Effect |
|------|---------|--------|--------|
| Archive | MaxUint64 (keep all) | KeepAll (MaxUint64-1) | Everything |
| Blocks | Distance(100k) | KeepAll (MaxUint64-1) | Prunes old history/idx, **keeps all block segments** |
| Full | Distance(100k) | DefaultBlocksPruneMode (MaxUint64) | Prunes old history/idx, **prunes pre-merge transaction segments** |
| Minimal | Distance(100k) | Distance(100k) | Prunes old history/idx AND old block segments |

Key insights:
- Full mode prunes pre-merge transaction segments (EIP-4444 history expiry) using `IsPreMerge(s.From)`.
- Blocks mode keeps ALL block segments but prunes history like Full.
- Domain files (including .kvi/.kvei/.bt accessors in domain/) are never pruned.

## Output Format

### Human-readable (default)
```
mainnet | archive | blocks 0–21,500,000 | steps 0–2,048

── Breakdown ──────────────────────────────────────────────────
  domains              420.3 GB      33.8%    847 files
  history              380.1 GB      30.6%   1204 files
  block segments       250.8 GB      20.2%    645 files
  accessors             80.5 GB       6.5%    847 files
  inverted indices      50.2 GB       4.0%    602 files
  commitment hist       35.2 GB       2.8%     96 files
  caplin                20.1 GB       1.6%    128 files
  rcache                 5.8 GB       0.5%     48 files
  ─────────────────────────────────────────────────────────────
  total                1.24 TB               4417 files

── Estimated Size by Node Type ────────────────────────────────
  archive       1.24 TB       —          all blocks      all history
  full          1.02 TB     -220 GB      all blocks      last 100k
  minimal        750 GB     -490 GB      last 100k       last 100k
```

### JSON (`--json` flag)
```json
{
  "chain": "mainnet",
  "detected_mode": "archive",
  "block_range": [0, 21500000],
  "step_range": [0, 2048],
  "total_bytes": 1363148800000,
  "total_files": 4417,
  "categories": {
    "domains": {"bytes": 451122000000, "files": 847},
    ...
  },
  "estimates": {
    "archive": {"bytes": 1363148800000, "delta": 0},
    "full": {"bytes": 1095000000000, "delta": -268148800000},
    "minimal": {"bytes": 805000000000, "delta": -558148800000}
  }
}
```

## Development Approach

- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- Run `make lint` after code changes

## File Classification Logic

Files are classified by **directory** and **filename content**:

| Category | Directory/Pattern | Included in Archive | Included in Full | Included in Minimal |
|----------|-------------------|---------------------|------------------|---------------------|
| domains | `domain/` (all files including .kvi, .kvei, .bt) | yes | yes | yes |
| accessors | `accessor/` | all | recent only (last 100k steps) | recent only |
| history | `history/` | all | recent only (last 100k steps) | recent only |
| inverted indices | `idx/` | all | recent only | recent only |
| block segments | `snapshots/*.seg` + `snapshots/*.idx` | all | all | transactions: recent only (last 100k blocks); headers/bodies: all |
| caplin | `caplin/` | yes | yes | yes |
| commitment hist | `history/*commitment*` or `idx/*commitment*` | yes | no | no |
| rcache | `domain/*rcache*` or `history/*rcache*` or `idx/*rcache*` | yes | no | no |

**Range parsing for pruning estimation:**
- State files (history/, idx/, accessor/): parse step range from filename via `snaptype.ParseFileName` → `FileInfo.From, To`
- Block files (snapshots/*.seg, *.idx): parse block range from filename → `FileInfo.From, To` (multiply by 1000 for actual blocks)
- Prune cutoff: `maxRange - DefaultPruneDistance/stepSize` for state, `maxBlock - DefaultPruneDistance` for blocks

## Implementation Steps

### Task 1: File walker and category classifier

**Files:**
- Modify: `cmd/utils/app/snapshots_cmd.go`

- [x] Define `duCategory` string constants: `"domains"`, `"history"`, `"inverted indices"`, `"accessors"`, `"block segments"`, `"caplin"`, `"commitment hist"`, `"rcache"`, `"other"`
- [x] Define `duFileInfo` struct: `{Path, Name, Size int64, Category string, From, To uint64, IsState bool}`
- [x] Implement `duClassifyFile(dir, name string) string` — maps directory + filename to category
- [x] Implement `duWalkSnapshots(dirs datadir.Dirs) ([]duFileInfo, error)` — walks all snapshot subdirs, stats each file, classifies, parses ranges via `snaptype.ParseFileName`
- [x] Write tests for `duClassifyFile` covering all categories + edge cases
- [x] Write tests for `duWalkSnapshots` using a temp directory with mock files
- [x] Run tests — must pass before task 2

### Task 2: Prune estimation logic

**Files:**
- Modify: `cmd/utils/app/snapshots_cmd.go`

- [x] Define `duEstimate` struct: `{Mode string, TotalBytes int64, Delta int64, BlocksDesc string, HistoryDesc string}`
- [x] Implement `duComputeEstimates(files []duFileInfo, maxBlock, maxStep uint64) []duEstimate` — computes estimated sizes for archive/full/minimal by summing files that survive each mode's pruning rules
- [x] Archive estimate: sum all files
- [x] Full estimate: sum all except old history/idx (step < maxStep - pruneDistance) and commitment hist and rcache
- [x] Minimal estimate: full minus old block segments (block < maxBlock - pruneDistance)
- [x] Implement `duDetectNodeType(files []duFileInfo) string` — infers current mode from presence of history/commitment/rcache files
- [x] Write tests for `duComputeEstimates` with synthetic file lists covering all three modes
- [x] Write tests for `duDetectNodeType` for archive/full/minimal detection
- [x] Run tests — must pass before task 3

### Task 3: Output formatting (human + JSON)

**Files:**
- Modify: `cmd/utils/app/snapshots_cmd.go`

- [x] Define `duResult` struct aggregating all output data: chain, mode, ranges, categories map, estimates slice, totals
- [x] Implement `duFormatHuman(w io.Writer, result duResult)` — prints the formatted human-readable output (header line, breakdown table sorted by size, estimates table)
- [x] Implement `duFormatJSON(w io.Writer, result duResult) error` — marshals to JSON
- [x] Implement `duAggregateCategories(files []duFileInfo) map[string]duCategoryStat` with `{Bytes int64, Files int}` per category
- [x] Write tests for human output format (check key lines present)
- [x] Write tests for JSON output (unmarshal and verify fields)
- [x] Run tests — must pass before task 4

### Task 4: Wire up CLI subcommand

**Files:**
- Modify: `cmd/utils/app/snapshots_cmd.go`

- [x] Register `du` subcommand in `snapshotCommand.Subcommands` with `--datadir` and `--json` flags (follow `ls` pattern, no file locking)
- [x] Implement `doDU(cliCtx *cli.Context) error` — orchestrates: open chaindata for chain name, walk snapshots, compute estimates, format output
- [x] Handle missing/empty datadir gracefully (clear error message)
- [x] Handle datadir with no snapshots (print zeros)
- [x] Run `make lint` — fix issues
- [x] Run `go test ./cmd/utils/app/... -run DU` — must pass
- [x] Run `make erigon` — must compile

### Task 5: Verify acceptance criteria

- [x] Verify all three output sections render correctly (header, breakdown, estimates)
- [x] Verify `--json` flag produces valid parseable JSON
- [x] Verify estimates are consistent (archive >= full >= minimal)
- [x] Verify category sizes sum to total
- [x] Run `make lint` (multiple times, non-deterministic)
- [x] Run `go test ./cmd/utils/app/... -short`

### Task 6: [Final] Cleanup

- [x] Move this plan to `docs/plans/completed/`

## Technical Details

### Step size for range parsing
- State snapshot filenames encode step ranges (not block ranges)
- Step size is read from `erigondb.toml` or defaults to `config3.DefaultStepSize`
- For `du` estimation, we only need the relative position (old vs recent), not absolute block mapping
- Prune cutoff for state: files with `To <= maxStep - (DefaultPruneDistance / stepSize)` are "old"
- For simplicity, since we're estimating, use `DefaultPruneDistance` directly against step ranges

### Block range parsing
- Block segment filenames: `v1.0-000000-000500-headers.seg` → From=0, To=500 (×1000 = 500,000 blocks)
- `ParseFileName` returns these as `FileInfo.From` and `FileInfo.To`
- Prune cutoff: files with `To*1000 <= maxBlock - DefaultPruneDistance` are "old"

### Chain name resolution
- Opens chaindata MDBX readonly (same pattern as `doLS`)
- If chaindata doesn't exist or can't be opened, falls back to `"unknown"`

## Post-Completion

**Manual verification:**
- Test against a real mainnet archive datadir
- Test against a pruned (full/minimal) datadir
- Verify estimates match actual disk usage after switching prune modes
