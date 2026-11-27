# Caplin retention regimes, run A: regimes, stored regime, `capcli seg` tooling (plan v4.1)

Epic #23024 item 4. Design: `design.md` v2.2 in this folder. Code base: erigon main at the run's start. Local build only (owner, 2026-11-25): nothing is pushed or posted. v3 is `plan-run-a-v3.md`; this version folds in the owner's notes of 2026-11-25.

## Overview

Run A gives Caplin the regime vocabulary, a stored regime per datadir, and the `capcli seg` tools that inspect a datadir and move it between regimes. It does not change what an exactly mapped node keeps. Retention enforcement, always-freeze, the head-first start, `minimal` and `tip` are run B.

After run A:
- `--caplin.prune.mode=archive|pruned|spec` is accepted. `spec` is the default.
- `pruned` has three knobs: `--caplin.prune.distance` (blocks), `--caplin.prune.sidecars` (blobs and columns) and `--caplin.prune.states` (state history).
- Each value maps onto today's retention fields, so behaviour equals the legacy flag set it replaces. A knob value that today's code cannot honour is refused with "available in a later version", and so are `minimal` and `tip`.
- The regime is stored in the Caplin indices DB. An existing datadir keeps working with no resync.
- capcli has a `seg` group:
  - `du` shows what a datadir holds;
  - `retire` moves MDBX data into segments;
  - `reset` returns the Caplin files to the published set;
  - `regime` shows or sets the stored regime.
- `erigon snapshots reset --caplin` also resets Caplin.

## Context (from discovery)

All facts are at main `c2d70f9492f`. Sources in this folder: `tooling-facts.md`, `el-minimal-mdbx-capcli.md`, `startup-waits.md`, `claims-check.md`.

- **Legacy flags** are parsed in `cmd/utils/flags.go:1895-1942`. No Caplin setting is derived from `--prune.mode`. Caplin starts in `cmd/caplin/caplin1/run.go` `RunCaplinService` (`:194`).
- **The Caplin indices DB** opens with `ChaindataTablesCfg` (`run.go:114-121`), so `kv.DatabaseInfo` exists for both embedded and standalone Caplin. The store pattern to copy is `HighestCustodyGroupCount` (`beacon_indicies/indicies.go:80-125`).
- **No lowest-slot accessor exists.** The sources are the visible-segment view, `snaptype.Segments(dir)`, a cursor `First()` on `kv.BeaconBlocks`, and `bucketStore.earliestStoredSlot` (`bucket_store.go:236`).
- **The retire steps live in the antiquary:**
  - `antiquate` (`antiquary.go:417-483`);
  - `mergeBlocks` (`:485-505`);
  - `removeBlockOverlaps` (`:507-515`);
  - `RetireBlobSnapshotsBelow` (`:528-538`);
  - state `RemoveOverlaps` (`caplin_state_snapshots.go:263`);
  - `pruneFrozenStateTables` (`state_prune.go:48`).
- **The capcli dump commands only dump.** `dump-snapshots` (`cli.go:402`), `dump-blobs-snapshots` (`:1147`) and `dump-state-snapshots` (`:1501`) do no prune, merge or overlap removal. `check-snapshots` (`:443`) and `check-blobs-snapshots` (`:1290`) are read-only checks. capcli uses urfave/cli/v3, so a `seg` group has the shape of erigon's `snapshots` command.
- **`erigon seg du`** (`snapshots_cmd.go:4322`) prints a breakdown and estimates per EL mode. It counts Caplin as one total "on top of every mode".
- **`erigon seg retire`** only indexes Caplin segments and removes their overlaps. **`erigon snapshots reset`** never visits `caplin/` (#17645).
- **Caplin MDBX tables:** only the block index tables are rebuilt from segments (`antiquary.go:338-374`). `BeaconBlockHeaders`, `ParentRootToBlockRoots` and the Kzg/DataColumn tables are not. So a retire can prune frozen rows and compact the DB, but it cannot drop the DB whole. No per-table byte counts exist yet.
- **Published manifests (erigon-snapshot `release/3.10`, read 2026-11-25):**
  - Blocks are in 100k-slot files, blobs in 10k-slot files.
  - State tables do not share one layout. On mainnet, 16 tables are merged into one 10.5M-slot file, 6 Electra tables into one 11.2M-slot file, and 11 Gloas tables exist only as 50k-slot files. Sepolia has 22 tables at 7.15M. Gnosis has 50k files only: 20,517 state files.
  - Every table starts at slot 0 with no gaps. `PtcWindow` ends 200k slots early on mainnet (15.40M vs 15.60M); it looks like the old name of `PtcWindowEpoch` (not checked in code).
  - The hoodi blobs are one stray file at 0–10k. Chiado has no Caplin files.
  - Today the filter is all-or-nothing: `caplin/` files go only with `caplinState` (`snapshotsync.go:691`).
- **Open PRs on the same files:**
  - #23525: block segment grid, `caplin_snapshots.go`.
  - #26317: `flags.go`, `caplinflags`.
  - #23249: draft; history stage and fork graph, which is run B ground.

## Decisions in this plan (the owner can override any of them)

1. **Knobs and alignment.** A knob is a distance in epochs, `all`, or (stored only) a fixed slot. A floor is `head epoch − knob`, and each family rounds it **down**, so the family keeps more, never less, to its own unit:
   - blocks and blobs to 10,000-slot segments;
   - state to 1,536-slot dumps, because a state read needs its dump and diffs.

   Start-up checks after rounding that sidecar floor ≥ block floor and state floor ≥ block floor. A violation is refused, and the error names the knob to change. Each knob has a minimum: blocks 33,024 epochs, sidecars 4,096, states 0.

   Mainnet example, head slot 15,660,000 (epoch 489,375), `--caplin.prune.distance=100000 --caplin.prune.sidecars=8192 --caplin.prune.states=4096`:

   | family | floor slot before rounding | unit | floor slot | check |
   |---|---|---|---|---|
   | blocks | (489,375 − 100,000) × 32 = 12,460,000 | 10,000 | 12,460,000 | |
   | sidecars | (489,375 − 8,192) × 32 = 15,397,856 | 10,000 | 15,390,000 | ≥ 12,460,000, so it holds |
   | states | (489,375 − 4,096) × 32 = 15,528,928 | 1,536 | 15,527,424 | ≥ 12,460,000, so it holds |

   With `--caplin.prune.sidecars=200000` instead, the sidecar floor is 9,260,000, which is below 12,460,000. Start-up refuses it: "sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars".
2. **The legacy map**, now exact for every common set:

   | legacy flags | regime |
   |---|---|
   | none | `spec` |
   | `blocks-archive` | `pruned` blocks=all |
   | `states-archive` (implies blocks) | `pruned` blocks=all, states=all |
   | `blobs-archive` (implies blocks) | `pruned` blocks=all, sidecars=all |
   | `blobs-archive` + `states-archive` | `archive` |
   | `blobs-archive-from-slot=N` | `pruned` blocks=all, sidecars from slot N |
   | `columns-keep-slots=K` | `pruned` sidecars=max(K, spec) |

   `--caplin.snapgen` keeps its meaning in run A.
3. **Inference at the first start of an existing datadir** (owner):
   - A full archive set gives `archive`. The log line gives the Caplin MDBX size and the hint `capcli seg retire`. The retire moves the frozen-ready rows into segments and compacts the DB.
   - A `pruned` set gives its knob values. No flags give `spec`.
   - A set with no exact map on a datadir with data gives `pruned`, with fixed lower bounds at the oldest stored block, blob and state slot. The node stores it, logs it, and prints the command to reach `archive`.
   - The same set on a fresh datadir is refused with a hint.
4. **Direction rule.**
   - archive → pruned → spec → (run B: minimal → tip);
   - inside `pruned`, a smaller knob or a later fixed slot is toward tip.
   - The node refuses the other direction with the `capcli seg` sequence. `capcli seg regime set` does it on purpose.
5. **Stored keys** live in the Caplin indices DB `kv.DatabaseInfo`: the regime, and three floors, each with its kind (distance, slot or all).
6. **Published files per regime.**
   - One pure function, `CaplinFiles(items, retention, headSlot)`, picks a file when its To is above its family's floor. The file is the unit, so a floor inside a file keeps the whole file.
   - `archive`: all files. `spec`: blocks in the 33,024-epoch window and blobs in 4,096 epochs, no state files. `pruned`: per knob. `minimal` and `tip`: none.
   - Only registered snaptypes are picked.
   - In any regime that keeps state, `StateEvents` is picked from slot 0: the state antiquary replays it from genesis (`state_antiquary.go:137-150`).
   - Coverage is the regime floor, not the file edges. The API answers from the floor even where a file holds more.
   - In run A the function feeds `seg du` and `seg check-manifest`. In run B, `SnapshotsToDownload` calls it.

## Development approach

- Red → green → refactor (AGENTS.md). A behaviour test fails at the task's base for the missing behaviour, before the code.
- Each task, or each named part, ends in one commit, builds, and leaves the gate green.
- No code comments (owner rule). Erigon naming: no Factory, Provider, Builder or Manager; `*Func` for function types. New files carry 2026 in the license header. Commit subject: package prefix, 120 chars max, subject only.
- Reuse first. The capcli dump and check commands and the antiquary steps above are called, not rewritten. New code goes only where no function exists: the lowest-slot probe, the stored keys, the regime functions.

## Implementation steps

### Task 1: regime model and flags, mapped onto today's fields

Files: `cl/clparams/` (new `caplin_retention.go` + test), `cmd/utils/flags.go`, `cmd/caplin/caplinflags/flags.go` and the standalone CaplinConfig parse.

- [x] Add `CaplinPruneMode` (archive, pruned, spec, minimal, tip) and `CaplinRetention{Mode, Blocks, Sidecars, States}`, where each floor is a distance, a fixed slot, or all. Add the `Floors(headSlot)` rounding and the nesting check of decision 1. Test: the mainnet example rows, the refusal row, and each minimum.
- [x] Write `LegacyRetention(cfg CaplinConfig) (CaplinRetention, error)` per decision 2. Test: a table over every combination of the six legacy retention flags plus snapgen.
- [x] Write `ApplyRetention(r CaplinRetention, cfg *CaplinConfig) error`. It sets today's fields to the exact legacy equivalent, and refuses `minimal`, `tip`, and a knob with no legacy equivalent, with "available in a later version". Test: the round trip `LegacyRetention(ApplyRetention(r)) == r` for each accepted regime.
- [x] Write `CaplinFiles(items, retention, headSlot)` per decision 6. Test: a synthetic manifest with merged and unmerged state tables, an unregistered type and a stray blob file, for each regime.
- [x] Add the flags `--caplin.prune.mode` (default `spec`), `--caplin.prune.distance`, `--caplin.prune.sidecars` and `--caplin.prune.states`. A knob outside `pruned` is refused, and so is a new flag mixed with a legacy retention flag. The legacy flags keep working and log their regime.
- [x] Gate green.

### Task 2: stored regime, the lowest-slot probe, the start-up rule

Files: `cl/persistence/beacon_indicies/` (the regime keys), the probe (its location is given in the task's direction section), `cmd/caplin/caplin1/run.go`.

- [x] Read and write the stored regime per decision 5. Test: round trip and absent key.
- [x] Write `OldestStored(...)`: the lowest stored slot per family. Blocks: the lower of the visible segment From and the first `kv.BeaconBlocks` key. Blobs: the lower of the segment From and the hot store's earliest bucket. States: the lowest visible range per table. Test: fixture datadirs with segments only, MDBX only, both, and empty.
- [x] Write `ResolveRetention(stored, requested, legacyExact, oldest) (CaplinRetention, Action, error)` per decisions 3 and 4. Test: a table that includes inheritance, the archive retire hint, toward tip, away from tip, and a fresh datadir.
- [x] Wire it in `RunCaplinService` before any stage. Resolve, store, log one line (regime, floors, and on archive the MDBX size plus `capcli seg retire`), then `ApplyRetention`. A refusal stops start-up with the hint.
- [x] Gate green.

### Task 3: `capcli seg` group and `seg du`

Files: `cmd/capcli/` (the `seg` group, `seg du`), `cmd/utils/app/snapshots_cmd.go` (part b).

- [x] Part a: `capcli seg du --datadir=<dir> [--json] [-v]`, in the format of `erigon seg du`.
  - Header: chain, stored regime and floors, head slot.
  - One row per family and tier: from slot, to slot, files, size.
  - Caplin MDBX, total and its largest tables (`-v`: every table).
  - Estimated size per regime from the published manifest, through `CaplinFiles`. `--manifest <toml>` estimates from a given manifest.

  Sizes come from `os.ReadDir` plus MDBX stats, without opening segments. Test: a fixture datadir gives the expected rows and JSON. Output format, with illustrative numbers only:

  ```
  $ capcli seg du --datadir=/data/erigon
  chain mainnet · regime spec (stored) · head slot 15,660,000

  family          tier  from slot    to slot      files  size
  beacon blocks   cold  14,600,000   15,630,000     206   11.2 GiB
  beacon blocks   hot   15,630,000   15,660,000       -    0.9 GiB
  blob sidecars   cold  15,520,000   15,630,000      22  118.4 GiB
  blob sidecars   hot   15,630,000   15,660,000       -   36.0 GiB
  data columns    hot   15,528,928   15,660,000       -   16.1 GiB
  beacon state    -              -            -       0      0 B
  caplin mdbx     -              -            -       -    4.2 GiB  (BeaconBlocks 3.1 GiB, StateEvents 0.4 GiB)
  total                                                    186.8 GiB

  estimated size per regime (published segments)
  archive   ~2,974 GiB
  pruned    by knobs (blocks, sidecars, states per day)
  spec      ~187 GiB
  ```
- [x] Part b: `erigon seg du` prints the Caplin breakdown from part a, in place of the single "on top of every mode" total.
- [x] Gate green after each part.

### Task 4: `capcli seg retire`

Files: `cmd/capcli/` (the command), `cl/antiquary/` (export the per-family steps only where capcli cannot call them).

The command takes the datadir flock and uses no seeder, as `erigon seg retire` does. Each part calls the existing dump command's function, then the antiquary's merge, RemoveOverlaps and prune steps, then the existing check command.

- [x] Part a, beacon blocks: `DumpSnapshots` path → `mergeBlocks` → `removeBlockOverlaps` → prune MDBX bodies below the frozen end (20,000-slot margin) → `check-snapshots`. Test: an MDBX fixture with 3 × 10,000 slots gives segments, no body below the frozen end is left, and a second run changes nothing.
- [x] Part b, blob sidecars: `DumpBlobsSnapshots` path, with the #26607 gap check → remove dumped hot sidecars → `check-blobs-snapshots`. Test: a gap stops the dump at the gap.
- [x] Part c, state tables: `DumpStateSnapshots` path (without its leftover debug print) → `RemoveOverlaps` → `pruneFrozenStateTables`. Test: the state segments exist, and the MDBX rows below coverage are gone.
- [x] Part d: compact the Caplin MDBX after the prunes and report the bytes reclaimed. Test: the file size goes down on a fixture.
- [x] Gate green after each part.

### Task 5: `capcli seg reset` and `erigon snapshots reset --caplin`

Files: `cmd/capcli/`, `db/datadir/reset/` (reuse `doSnapshots` for `snapshots/caplin`), `cmd/utils/app/reset-datadir.go`.

- [x] Build one read-only Caplin reset plan. It lists local Caplin segments and bad torrents selected by the existing removal decision, replacement coverage per family and state table, derived Caplin index rows for deleted block ranges, the progress marker, and the stored regime.
- [x] Refuse to delete an only-copy Caplin segment unless `--force` is given. A run deletes the planned files and bounded derived rows, lowers progress for the next index pass, and keeps hot stores, hot beacon rows, the stored regime, and `preverified.toml`.
- [x] Add `capcli seg reset --datadir=<dir> [--chain=<name>] [--dry-run] [--force]`, with the datadir lock held for both plan and execution. Dry-run prints the plan and execution prints deleted files and rows.
- [x] Add `erigon snapshots reset --caplin` and `--force`, using the same plan and executor. Without `--caplin`, keep the existing reset path unchanged; with `--keep-local-snapshots`, unknown Caplin files are kept.
- [x] Test the plan, only-copy refusal, per-state-table coverage, bounded index cleanup, capcli dry-run and execution, and the unchanged reset path without `--caplin`.
- [x] Gate green.

### Task 6: `capcli seg regime show|set`

Files: `cmd/capcli/`.

- [x] `show`: the stored regime and floors, the oldest stored slot per family, and the legacy flags it maps from.
- [x] `set <mode> [--distance] [--sidecars] [--states]`: write the regime in either direction. Away from tip, it prints the sequence:
  1. `capcli seg reset`, when data must be fetched again;
  2. start the node with the matching flags, so the snapshot sync downloads the published segments.
- [x] The node's refusal hint (task 2) names these exact commands. Test: the hint text and the `set` output.
- [x] Gate green.

### Task 7: `capcli seg check-manifest <toml>` (for publishers)

Files: `cmd/capcli/`.

- [x] Per family, report:
  - gaps and overlaps;
  - state tables whose file boundaries or end slots differ from the rest (today: 10.5M vs 11.2M vs 50k-only, and `PtcWindow` ending early);
  - the largest file per table;
  - files of unregistered types;
  - the file count.

  It exits non-zero on a gap or a table end that differs. Test: the synthetic manifest from Task 1.
- [x] Gate green.

### Task 8: verification and documentation

- [x] Flag help and `capcli seg` usage, with the alignment example.
- [x] A start-up matrix test over the legacy flag sets: each maps with no change in the resulting CaplinConfig fields, or is refused with its hint.
- [x] Move this plan to `docs/plans/completed/`.

## Open questions for the owner

1. `blobs-no-pruning` without `blocks-archive` keeps blobs longer than blocks. Inheritance cannot express it without the nesting check failing. Options: (a) refuse with a hint (this plan), (b) inherit a sidecar floor at the oldest stored block, which drops the blobs below it.
2. The knob names: `--caplin.prune.distance` (blocks, settled in FORK 7), `--caplin.prune.sidecars`, `--caplin.prune.states`.
3. For run B, `minimal` aligned with the EL minimal node, which keeps all headers and bodies plus 100,000 blocks of history. (a) Every Caplin family at 100,000 slots (3,125 epochs). That is below the spec windows, so it does not serve per spec. (b) EL-shaped: beacon blocks from genesis like EL bodies, and sidecars at the spec window. `tip` stays the experimental extreme.

## Publisher side (not code in this repo, listed for the epic)

- One merge schedule for all Caplin state tables, with a size cap (epic item 3: 10k → 100k → 1M). Today a `pruned` state floor below 10.5M on mainnet fetches a 10.5M-slot file.
- Merge the 20,517 gnosis 50k state files.
- Drop retired types (`PtcWindow`, `BlockProposers`) from the manifests.
- Hoodi blob coverage; chiado Caplin coverage.
- `capcli seg check-manifest` gates a manifest before it is published.

## Run B (not in this run)

The head-first history stage; keeping the blocks since finalized across a restart; #26350; always freeze (`--caplin.snapgen` removed); enforcement of finite knobs; `minimal`; `tip`; docs.
