# PBT convert and attach: no re-sort, background ETL merge, parallel phases, progress in the log

## Overview
- On a mainnet-sized state (355M accounts, 3.56B leaves, 16 CPUs), `convert-pbt` takes 8 h 38 min even with
  `--skip-verify`. With verification on, it took 27 h on pruned mainnet, and attach added 14 h. Other clients finish
  in about two hours.
- Measured on glamsterdam-pbt-1 erigon-2 (2026-10-22/23):

  | phase | wall | why |
  |---|---|---|
  | leaf collection | 1 h 25 min | one goroutine walks the source into ETL A |
  | leaf load + re-collect | 2 h 43 min | one goroutine merges A, hashes the stream root, and re-collects every leaf into ETL B, which re-sorts sorted data |
  | B → `pbin-ops-*.bin` | 44 min | one goroutine merges B, no log |
  | fold, 35,687 batches | 2 h 21 min | 75 ms of each 237 ms batch is compute; read, stamp tracker and overlay flush are serial |
  | `.kv` + `.kvi` | 1 h 24 min | ranges one at a time; the 164 GB range's `.kvi` took 49 min |

- Verification makes it worse. The default path re-sorts the full leaf set three more times through ETL:
  - `requirePBinSourceEnd`, only to compare stamps with a bound;
  - `verifyPBTOutputStandalone`, which re-roots the output's hard-linked copy of the same source files;
  - attach, twice: `validatePBTAttachLeafStamps` and `pbtAttachNodePbtRoot`.
  All of them run on one goroutine. `--skip-verify` dropped them and put nothing in their place.
- This plan fixes the pipeline as it is, and the output stays byte-identical:
  - **progress in the log** for every phase, including today's silent merges, the file build and every verify pass;
  - **no re-sort**:
    - ETL B goes, and sorted leaves are written straight into the partition file;
    - the stamp check walks the source unsorted;
    - the output's state files are checked by file identity, not re-rooted;
    - attach checks a digest manifest instead of re-deriving roots;
    - rows reach the range files through one ordered lane per (range, partition) plus a small skip collector, so the
      leaf sort is the only full sort;
  - **background ETL merge**: the k-way merge runs on its own goroutine and hands batches to the consumer, so merging
    overlaps the consumer's work;
  - **parallelism**:
    - the source walk and the stamp check by address range;
    - the stream root on its own stage;
    - the fold loop as three overlapped stages (read/stamp, compute, flush);
    - file ranges and their recsplit indexes built concurrently;
    - attach digests.
- `--skip-verify` stays as an option on convert and attach. Without it, every check runs, and with these changes the
  checks cost minutes rather than hours. With it, the checks that read file content are skipped, and each skipped
  check is logged by name.
- `convert-pbt --resume` builds only the missing steps when hex execution has run past an existing PBT. It folds the
  keys of the source files that end after the PBT point on top of the existing trie.

## Context (from discovery)
- Convert: `convertPBTWithOptions` (`cmd/integration/commands/commitment_convert_pbt.go`) → `ConvertPBin`
  (`db/state/pbt_convert.go`).
  - `ForEachPBinLeaf` (`db/state/pbt_leaf_stream.go`) runs three `DomainLatestIterFile` cursors with no key bounds,
    on one goroutine, and collects into ETL A (`pbin-leaf-stream`) in the source `temp/`. `pbinLoadSortedLeaves`
    merges A, dedups (max stamp, equal value) and emits sorted leaves.
  - `PBinRootFromStream` (`eip8297.StreamRootBuilder`) and `PBinRangeWriter.WriteAtBlock` (`pbt_range_writer.go`)
    consume the same stream.
  - `pbinForEachRebuildOpStream` (`db/state/squeeze.go`): ETL B (`[rebuild_commitment_pbin]`, keyed by
    `batchOperationKey`, equal to the leaf key here) → `pbin-ops-*.bin` with a 12-bit partition offset table →
    round-robin batches (`maxOps/len(live)` per partition) → `visit`.
  - The other caller is `import-pbt`, whose artifact leaves are already in tree-key order.
  - `visit` runs `tracker.observe/advance`, then `ComputeCommitmentWithDiff` (PBT workers), then the serial
    `[concurrent_branch]` drain, then `pbinRangeWriterOverlay.FlushFinished` (sort, `onRow` → stamp → ETL D per
    range).
  - `buildFiles` builds the ranges in a loop: ETL D `Load` → `seg.Compressor` (`.idt`, then `.kv`) →
    `buildFileRange` (`.kvi`; recsplit workers only from `COMPRESS_WORKERS`) → `integrateDirtyFiles`.
  - Verify: `requirePBinSourceEnd` (C-a, a sorted leaf walk used only for a stamp bound), the engine root vs stream
    root (C-b), the header root after the fork (C-c), `verifyPBTOutputRows` → `VerifyPBinDomainRoot` (C-d, already
    parallel by `PBT_WORKERS` over chains ≥ 12 bits), and `verifyPBTOutputStandalone` → `PBinStateRoot` (C-e).
  - Output state files are hard links of the source's (`linkSnapshotsExceptCommitment`).
- Partitions: `pbinPartitionOf` is the zone byte plus the first hash nibble, so at most 48 are live.
- Attach: `attachPBTWithHooks` (`commitment_attach_pbt.go`).
  - Content passes:
    - A-a, `validatePBTAttachLeafStamps`: a sorted leaf walk that yields the published leaf root;
    - A-b, `verifyPBTAttachPublishedBin`: `VerifyPBinDomainRoot`;
    - A-c, `pbtAttachNodePbtRoot`: a sorted walk of the node's state, fresh attach only.
  - Cheap checks: the node hex root against the published hex root, the header at a block end, salts, genesis,
    layout, and the state records.
  - Then `adoptPBTFiles` (`linkOrCopyPBTFile`, file set from `pbtAttachAdoptsFile`), `resetPBTExecution`, the shadow
    root, and `WriteErigonDBSettings`. Nothing hashes file bytes.
- ETL (`db/etl/collector.go`): `SortAndFlushInBackground` overlaps one sort/flush per collector. `Load` →
  `mergeSortFiles` is a heap merge on the caller goroutine that calls `loadFunc` inline. `MergeLoad` merges several
  collectors in one heap. No background merge exists.
- `--skip-verify` is commit `2c2060ec29d`. Its tests are in `commitment_convert_pbt_test.go` and
  `commitment_attach_pbt_test.go`.
- Docs naming today's pipeline: `docs/pbt-migration.md`, `cmd/integration/Readme.md`, and the root
  `CLAUDE.md`/`AGENTS.md` PBT paragraph.
- Conventions: no code comments (hook-enforced); `*Func` for function types; no Factory/Builder/Manager names; new
  files carry the 2026 license header; commit subjects ≤ 120 chars; no line numbers in docs.

## Development Approach
- **testing approach**: TDD.
  - The safety net is output identity. Every task keeps the converter's output byte-identical (the bin `.kv` file
    set, the record maps, and range placement) to the pre-change converter on the test corpora.
  - Task 1 adds that check.
  - New behaviour gets a test that fails on its assertion first; signatures land before their tests.
- complete each task fully before moving to the next; small focused commits.
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task, success and error paths.
- **CRITICAL: all tests must pass before starting next task** — the gate below, every task.
- **CRITICAL: update this plan file when scope changes during implementation**.
- Every change is on the production path (`convert-pbt`, `attach-pbt`, `import-pbt`). Tests drive it
  through those entry points or the functions they call, and a concurrency test asserts that the stage actually
  runs with more than one goroutine.
- Gate per task:
  - `go build ./...`, `make lint`;
  - `go test` of `./db/etl/...`, `./db/state/...`, `./db/integrity/...`, `./db/seg/...`, `./db/recsplit/...`,
    `./execution/commitment/v3/pbt/...`, `./cmd/utils/app/...` and `./cmd/integration/commands/...`
    (`-timeout 30m`);
  - `go test -race ./db/etl/ ./db/state/ -run 'PBin|Background|Parallel'`.

## Testing Strategy
- **Output identity** (Task 1, kept green by every task): a golden harness converts state-level corpora and compares
  the result to golden data recorded from the pre-change converter. The golden data is the bin file names, every
  file's (key, value) sequence, and the root. Corpora:
  - accounts with header slots;
  - code shared across many accounts;
  - small and large storage buckets;
  - empty zones; a single account;
  - multiple account-file ranges, with a past-frontier range when the source allows one.
  Both `--keep-hex` and bin-only, keccak and blake3. They reuse `newPBTConversionSource*`,
  `pbtAcceptanceReferenceEntries` and `writePBinConvertStateN`.
- **Concurrency**: each parallel stage exposes its worker count through the progress line or a counter that a test
  reads, and `-race` runs on the touched packages.
- **Detection**: each case must give its own named error:
  - a stamp past the point in any address range;
  - a changed byte, a missing, extra or resized file, or a wrong root in the manifest or the published files;
  - a corrupted copy in attach;
  - a hard-linked output state file that is not the source's inode.
- **Progress**: a log-capture test with a short interval asserts that every phase logs a progress line with
  done/total and rate.
- **Scale** (benchmark): a ≥ 100M-leaf generated datadir; per-phase wall time and CPU-seconds at workers 1/4/8/16.

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview
```mermaid
flowchart LR
  S[source files] -->|N workers by address range| A[N ETL A collectors<br/>sort+flush in background]
  A -->|background merge goroutine| L[sorted leaves<br/>dedup max stamp]
  L --> R[stream root stage]
  L --> C[pbin-ops partition file<br/>written in order, no ETL B]
  C --> F1[read + decode + stamp stage]
  F1 --> F2[compute stage, PBT workers]
  F2 --> F3[flush stage: rows to per-range,<br/>per-partition lanes + skip collector]
  F3 --> K[ranges in parallel:<br/>lanes merged with skip → .kv + .kvi]
  K --> M[manifest: point, roots, digests]
```
- **D1 — output identity is the contract.** No change touches row content or range placement. The golden harness
  proves it after every task.
- **D2 — no re-sort anywhere.**
  - ETL B is replaced by an in-order writer of the partition file, which fails if its input is out of order.
  - C-a walks the source in parallel without sorting.
  - C-e becomes a file-identity check: every output state file is the source file's inode, plus the state records
    at the point. That holds because C-b already roots exactly those bytes.
  - Attach replaces A-a and A-c with the manifest, the node hex root against the manifest hex root (already
    checked), and the root record.
- **D3 — background merge lives in `db/etl`.** It is `Collector.MergeInBackground(true)`, symmetric with
  `SortAndFlushInBackground`, and also available to `MergeLoad`. A goroutine runs the heap merge into a ring of
  batches, and `loadFunc` consumes them on the caller. Order, errors and cancellation behave exactly as the
  serial merge does.
- **D4 — parallel source walk.** N workers walk disjoint address ranges with `DebugRangeLatestFromFiles(from, to)`.
  Each feeds its own ETL A collector, and the collectors are merged in the background with `MergeLoad`. Dedup at
  load stays as today, so the leaf set and its stamps are unchanged.
- **D5 — fold loop pipeline.** Stage 1 reads and decodes the next batch from the partition file and runs
  `tracker.observe/advance`; it is in order and single-goroutine. Stage 2 is `ComputeCommitmentWithDiff`. Stage 3 is
  the flush. `FlushFinished` splits into a synchronous detach of the finished rows, which the next compute must not
  see in the overlay, and a background collect of the detached rows (into the lanes of D8). Each stage runs one batch
  ahead of the next, so a batch's wall time approaches its slowest stage.
- **D6 — files in parallel.** `buildFiles` builds every range concurrently. Recsplit workers are set from
  `--workers` and split by range size. Concurrent builds are capped by memory. `integrateDirtyFiles` runs at the
  end.
- **D7 — manifest; `--skip-verify` is an option.**
  - Convert writes `snapshots/pbt-manifest.toml` last on every run, `--skip-verify` included (temp, fsync, rename):
    - point, `pbt_root`, `hex_root`, `trie_hash`, `format = 1`;
    - name, size and blake3 of every file `pbtAttachAdoptsFile` selects.
  - Attach verifies the digests in parallel, plus the root record, the point and the hex root. A file adopted by copy
    is digested again at the destination. Attach's `--skip-verify` skips only the digest passes.
  - Convert's `--skip-verify` skips C-a (the file-range check runs instead), C-d and C-e. C-b and C-c always run.
  - Outputs without a manifest are refused with "re-run convert-pbt with this binary".
- **D8 — rows in lanes, no second sort.** For each range, there is one append-only lane file per partition and one
  skip collector. A row goes to its partition's lane if its key is above the lane's last key. Otherwise it goes to
  the skip collector, as do rows above bit 12, the global root and the `state` key. A range's `.kv` input is its
  lanes concatenated in partition order, merged with the skip collector. Partition runs are contiguous in row-key
  order. Measured: 0.14% of rows go to the skip collector at `MaxOps` 100k.
- **D9 — resume.**
  - Base: the PBT at X, taken from an earlier output or from a hex+bin source whose bin domain stopped.
  - Gap ops: the keys of every source `.kv` that ends after X, as leaves at Y:
    - deletions are all-zero values;
    - chunks are added for new code and never deleted;
    - a removed account becomes per-leaf deletions, because the writer takes no Drop.
  - Fold: the ops go through the one leaf sort and fold on the base trie. Every written row and tombstone goes to the
    newest range, so no stamp tracker is needed and the latest view is exact.
  - Output layout: every source commitment range is produced. Each range merges its nested base bin files with its
      delta rows, newest wins, so the bin layout copies the source's commitment layout, which attach requires.
  - Refused:
    - a zero-start source file that spans X, because its deletions are gone;
    - a base bin file that straddles a source commitment range boundary;
    - a gap that is not post-Cancun, or that holds a bytecode rewrite.
- **D10 — partition-parallel fold (Tasks 15–18; owner's call, 2026-10-25).** Superseded: Dropped 2026-10-26 (owner): the partition scheduler made convert 4.25× slower on Hoodi w16 (540.9 s against 127.2 s); the design at `4b2be4b990c` stays.
- **D11 — one fold, two wirings (Task 17 here, exec in a follow-up; owner's call, 2026-10-25).** Superseded: Dropped 2026-10-26 (owner): the partition scheduler made convert 4.25× slower on Hoodi w16 (540.9 s against 127.2 s); the design at `4b2be4b990c` stays.

## Technical Details
- **Partition file writer** (replaces ETL B in `pbinForEachRebuildOpStream`):
  - writes length-prefixed op records in arrival order;
  - opens a new partition offset whenever `pbinPartitionOf(key)` changes;
  - rejects a key ≤ the previous key;
  - keeps the batch cutter and `visit` unchanged.
  `import-pbt`'s stream is sorted already; a test pins that.
- **Background merge**: a batch is up to 4096 entries or 4 MB, with keys and values copied into the batch's arena.
  The ring holds 4 batches, and memory is reported. `loadFunc` errors stop the merge goroutine.
- **Parallel C-a**: `--workers` address ranges, each an unsorted walk that stops at the first stamp > txNum and
  reports it.
- **Progress lines**, at least every 30 s:
  - phase, done/total, rate, ETA, RssAnon, Go heap and temp bytes;
  - ETL merges report entries merged out of total;
  - the fold reports batches and leaves done, with the per-stage busy share;
  - files report per range and stage (`.idt`, `.kv`, `.kvi`);
  - verify and attach report bytes or rows done.
- **Flags**:
  - `convert-pbt` adds `--workers` (default `PBT_WORKERS`) and `--resume`, and keeps `--skip-verify`;
  - `attach-pbt` keeps `--skip-verify`.

## What Goes Where
- **Implementation Steps**: code, tests, and the docs in this repo.
- **Post-Completion**: the arb1-dev mainnet run, hive on snap-arb1, and the devnet rollout.

## Implementation Steps

### Task 1: Output-identity golden harness
**Files:**
- Create: `cmd/integration/commands/commitment_convert_pbt_golden_test.go`
- Create: `cmd/integration/commands/testdata/pbt-convert-golden/` (generated)

- [x] state-level corpora (Testing Strategy) and a `-update` path that records the golden data from the current
      converter.
- [x] the comparison test: file names, per-file (key, value) sequence, root, and both modes. It passes on its first
      run, as a characterization test.
- [x] mutation check: perturb one row's range placement in a scratch copy of the writer and confirm the test goes red
      on the placement assertion; then restore the copy.
- [x] run the gate — must pass before Task 2.

### Task 2: Progress in the log for every phase
**Files:**
- Modify: `db/etl/collector.go`, `db/etl/etl.go`, `db/state/pbt_leaf_stream.go`, `db/state/squeeze.go`,
  `db/state/pbt_range_writer.go`, `db/state/pbt_convert.go`, `cmd/integration/commands/commitment_convert_pbt.go`,
  `cmd/integration/commands/commitment_attach_pbt.go`, `cmd/integration/commands/commitment_attach_pbt_test.go`
- Create: `db/state/pbt_progress.go`, `db/state/pbt_progress_rss_linux.go`, `db/state/pbt_progress_rss_darwin.go`,
  `db/state/pbt_progress_rss_other.go`, `db/state/pbt_progress_test.go`

- [x] one progress helper shared by all phases: interval, done/total, rate, ETA, memory and temp.
- [x] wire it into the ETL merge (via `TransformArgs`), the partition-file write, the fold, each range's file stages,
      every verify pass, and attach.
- [x] log-capture test with a short interval: every phase emits a line.
- [x] run the gate — must pass before Task 3.

### Task 3: Background ETL merge
**Files:**
- Modify: `db/etl/collector.go`
- Create: `db/etl/merge_background.go`
- Create: `db/etl/merge_background_test.go`

- [x] `MergeInBackground(true)` on `Collector`, plus the `MergeLoad` equivalent. The test is written first: random
      data, many spill files, and in-RAM providers, and the background merge must give exactly the serial order.
- [x] tests: a `loadFunc` error stops the goroutine with no leak; context cancel stops it; an empty collector; one
      provider.
- [x] enable it on ETL A in `pbinLoadSortedLeaves`.
- [x] run the gate — must pass before Task 4.

### Task 4: No re-sort in the fold input
**Files:**
- Modify: `db/state/squeeze.go`
- Modify: `db/state/squeeze_pbin_rebuild_test.go`

- [x] test first: an out-of-order stream fails with a named error. Today it is silently sorted.
- [x] replace ETL B with the in-order partition-file writer; batches and `visit` are unchanged.
- [x] tests: the batches equal the old path's on the existing corpora; `import-pbt`'s stream passes the order check;
      the golden harness stays green.
- [x] run the gate — must pass before Task 5.

### Task 5: Parallel source walk and stream-root stage
**Files:**
- Modify: `db/state/pbt_leaf_stream.go`
- Modify: `db/state/pbt_convert.go`
- Create: `db/state/pbt_leaf_stream_parallel_test.go`

- [x] N address-range walkers, each with its own ETL A collector, merged with background `MergeLoad`. The worker
      count comes from `--workers`.
- [x] `PBinRootFromStream` runs as its own stage beside the partition-file writer, fed from the same merged stream.
- [x] tests: the leaf set and stamps equal the serial walk's for workers 1/4/16, shared code across ranges included;
      the concurrency counter is > 1; the golden harness stays green.
- [x] run the gate — must pass before Task 6.

### Task 6: Fold loop pipeline
**Files:**
- Modify: `db/state/squeeze.go`, `db/state/pbt_range_writer.go`
- Create: `db/state/pbt_range_writer_pipeline_test.go`

- [x] split `FlushFinished` into detach (synchronous) and collect (background).
- [x] the three-stage pipeline of D5, with errors propagating from any stage and stopping the others.
- [x] tests: equal output to the serial loop on corpora with `MaxOps` 7, 1000 and 100000; an error in each stage;
      the golden harness stays green; `-race`.
- [x] run the gate — must pass before Task 7.

### Task 7: Rows in lanes, no second sort
**Files:**
- Create: `db/state/pbt_row_lanes.go`, `db/state/pbt_row_lanes_test.go`
- Modify: `db/state/pbt_range_writer.go`, `db/state/pbt_range_writer_test.go`

- [x] per range: one append-only lane file per partition (`pbinPartitionOf` of the row's path, at most 48) and one
      skip collector (ETL with the background merge). A row goes to its lane when its key is above the lane's last
      key. Otherwise it goes to the skip collector, as do rows above bit 12, the global root and the `state` key that
      `buildFiles` injects. ETL D is deleted.
- [x] a range's `.kv` input is its lanes concatenated in partition order, merged with the skip collector. An equal or
      decreasing key in the merged output is a named error. Lane files are removed after their range is built, and on
      any failure.
- [x] progress per range: lane bytes, skip rows, and their share of the range.
- [x] tests:
  - lanes plus skip give exactly the sorted sequence for random emission orders that include F9 parents, shallow rows,
    the global root and the `state` key;
  - a duplicate key fails with its error;
  - the golden harness stays green at `MaxOps` 7, 1000 and 100000;
  - a counter shows rows took both paths.
- [x] GUARDS: make the merge drop the skip collector, and name the golden assertion that fires.
- [x] run the gate — must pass before Task 8.

### Task 8: Parallel file ranges and indexes
**Files:**
- Modify: `db/state/pbt_range_writer.go`
- Modify: `db/state/pbt_range_writer_test.go`

- [x] build all ranges concurrently:
  - each range's lane merge, `.idt` → `.kv`, and `.kvi`;
  - recsplit workers from `--workers`, split by range size;
  - a memory cap on concurrent builds;
  - one `integrateDirtyFiles` at the end.
- [x] tests:
  - identical files at workers 1 and 16;
  - a counter shows at least two ranges built at once;
  - a failure in one range removes every partial `.kv`, `.kvi`, `.idt` and lane file;
  - the golden harness stays green; `-race`.
- [x] GUARDS: make the cleanup skip one range's partial `.kvi`, and name the assertion that fires.
- [x] run the gate — must pass before Task 9.

### Task 9: Verify without re-sorting; manifest; `--skip-verify` as an option
**Files:**
- Create: `db/state/pbt_manifest.go`, `db/state/pbt_manifest_test.go`
- Modify: `cmd/integration/commands/commitment_convert_pbt.go`
- Modify: `cmd/integration/commands/commitment_convert_pbt_test.go`

- [x] C-a as a parallel unsorted stamp walk. C-e as file identity: every output state file is the source file's
      inode, plus the state records at the point. C-b, C-c and C-d stay.
- [x] the manifest (D7): written last on every run, `--skip-verify` included; read; checked with parallel digests.
- [x] `--skip-verify` stays. It skips C-a (the file-range check `requirePBinSourceFilesEnd` runs instead), C-d and
      C-e, and logs each skipped check by name. C-b and C-c always run.
- [x] tests:
  - a stamp past the point in the last address range;
  - a non-linked output state file;
  - manifest round trip and every tamper case;
  - with `--skip-verify`, the manifest is written and the skipped checks are logged;
  - the golden harness stays green.
- [x] run the gate — must pass before Task 10.

### Task 10: attach-pbt verifies the manifest
**Files:**
- Modify: `cmd/integration/commands/commitment_attach_pbt.go`
- Modify: `cmd/integration/commands/commitment_attach_pbt_test.go`

- [x] require the manifest (named error for old outputs), and check:
  - the point, `trie_hash` and file set;
  - the digests in parallel;
  - the root record against `pbt_root`;
  - the published and node hex roots against `hex_root`;
  - past the fork, the header root against `pbt_root`.
  Re-digest a file adopted by copy at the destination.
- [x] A-a, A-b and A-c leave the default path. `--skip-verify` stays and skips only the digest passes; every other
      check above always runs. Write the shadow root from `pbt_root` at a block end before the fork, with or without
      the flag. Copy the manifest into the node.
- [x] tests:
  - refused before adopting: a changed byte; a missing or extra file; a wrong root; a wrong hex root;
  - with `--skip-verify`, a changed byte passes, but a wrong root is still refused;
  - a corrupted copy is refused after the copy;
  - a clean attach writes the shadow root and settings;
  - resume after a crash at each `hooks.step`.
- [x] run the gate — must pass before Task 11.

### Task 11: Resume, part 1 — the gap leaf stream
**Files:**
- Create: `db/state/pbt_resume_leaves.go`, `db/state/pbt_resume_leaves_test.go`
- Modify: `db/state/pbt_leaf_stream.go`

- [x] input: the base point X and the source point Y. Output: the accounts, storage and code keys of every source
      `.kv` file whose range ends after X, walked in parallel by address range as in D4. History files are not
      read.
- [x] per touched address, emit the leaves at Y the way execution's feed (`FeedOpEmitter.EmitAccount`) defines them, as
      plain ops with 32-byte values:
  - the header leaves always: basic data, plus code hash or delegation, with an all-zero deletion for the one that
    is absent;
  - code chunks for code written after X; chunks are never deleted;
  - each touched storage slot, with a deletion for an empty value;
  - for an account absent at Y, a deletion for every header leaf and for each storage tombstone. No Drop ops.
  Stamps are the source file's `endTxNum − 1`. The ops go through the one leaf sort (ETL A).
- [x] refusals, each with a named error:
  - a source file that starts at step 0 and spans X;
  - a gap that is not entirely post-Cancun, or a chain with a bytecode rewrite (AuRa `rewriteBytecode`, Gnosis's
    Balancer `Merge.Initialize`).
- [x] tests: applying the gap ops to the leaf set at X gives the leaf set at Y, on corpora with:
  - updates and deletions;
  - an EIP-161 deletion;
  - EIP-7702 delegation set and clear;
  - new code and shared code;
  - a source merge with a non-zero start that spans X.
  Each refusal has its own test.
- [x] run the gate — must pass before Task 12.

### Task 12: Resume, part 2 — fold on top of the existing PBT
**Files:**
- Modify: `db/state/pbt_range_writer.go`, `db/state/pbt_convert.go`
- Create: `db/state/pbt_resume_test.go`

- [x] a resume mode of the range writer:
  - seek the commitment state at X from the base bin files instead of `WithoutCommitmentSeek`;
  - read base rows through the trie context, and make the overlay's `Absent` consult the base;
  - accept a base domain that holds files up to X and no MDBX rows.
- [x] every row and tombstone written goes to the newest output range through the lanes (Task 7). This mode has no
      stamp tracker. The state record at Y goes to the same range.
- [x] verification: C-b does not apply to a delta. The default runs:
  - C-d (rows and root);
  - C-e (the stream root of the full leaf set at Y, compared with the state record);
  - C-c after the fork.
  `--skip-verify` skips C-d and C-e.
- [x] tests:
  - the resumed latest row view and root equal a fresh convert at Y on the Task 11 corpora;
  - a deleted subtree leaves tombstones;
  - an empty gap gives the base root;
  - `-race`.
- [x] run the gate — must pass before Task 13.

### Task 13: Resume, part 3 — `convert-pbt --resume`
**Files:**
- Modify: `cmd/integration/commands/commitment_convert_pbt.go`
- Modify: `cmd/integration/commands/commitment_convert_pbt_test.go`

- [x] `--resume` takes the base PBT from one of two places:
  - the output datadir, from an earlier convert;
  - when the output is empty, the source's own commitment-bin domain (a hex+bin node whose bin domain stopped).
  X is the bin state record in the base files.
- [x] the output's bin file layout equals the source's commitment layout, which attach requires:
  - link a base bin file whose range matches;
  - merge base bin files where the source merged them;
  - refuse a merge that starts at step 0 and spans X.
  The state files are linked again from the source at Y. The settings and the manifest are written at Y.
- [x] progress lines for the gap walk, the fold, the merges and the files. The log names X, Y and the gap's key
      count.
- [x] tests:
  - resume of an earlier output;
  - resume of a stopped hex+bin source;
  - a source that merged across X;
  - attach of the resumed output on a node at Y passes;
  - the golden harness stays green.
- [x] run the gate — must pass before Task 14.

### Task 14: Scale benchmark
- [ ] Use a hard-linked clone of a Hoodi datadir, `src4` (61 GB, commitment layout `0-16` and `16-17`);
      no node runs.
- [ ] Run the arms:
  - the pre-change converter built from `c4892e8a808`;
  - this branch at `--workers` 1, 4, 8 and 16;
  - one resume with X at step 16, the newest unmerged step file removed from a clone, and Y at the full `src4`,
    timed against a fresh convert at Y. Only one gap size is possible: `0-16` is a zero-start file, and resume
    refuses it.
- [ ] Measure per-phase wall time from the progress lines, CPU seconds and peak memory footprint with
      `/usr/bin/time -l`, the skip-lane share per range, and the machine and its load at each start.
- [ ] Check that bin files are byte-identical across worker counts, and that the resumed root equals the fresh
      convert's, with C-d and C-e passing on both.
- [ ] Drop `BenchmarkPBinConvert`.

### Task 15: Engine — fold an owned subtree across batches
Dropped 2026-10-26 (owner): the partition scheduler made convert 4.25× slower on Hoodi w16 (540.9 s against 127.2 s); the design at `4b2be4b990c` stays.

### Task 16: Partition ops cursors and a single row sink
Dropped 2026-10-26 (owner): the partition scheduler made convert 4.25× slower on Hoodi w16 (540.9 s against 127.2 s); the design at `4b2be4b990c` stays.

### Task 17: Partition-parallel fold, fresh and resume
Dropped 2026-10-26 (owner): the partition scheduler made convert 4.25× slower on Hoodi w16 (540.9 s against 127.2 s); the design at `4b2be4b990c` stays.

### Task 18: C-d in adaptive structural tasks with an admission limit
Dropped 2026-10-26 (owner): the partition scheduler made convert 4.25× slower on Hoodi w16 (540.9 s against 127.2 s); the design at `4b2be4b990c` stays.

### Task 19: Verify acceptance criteria
- [ ] every Overview item is implemented and reached from `convert-pbt` or `attach-pbt`.
- [ ] exec's fold path is unchanged: `ProcessParallelContext` → `processParallelPhaseA`, with the exec-side
      binary-trie packages green in the gate.
- [ ] Hoodi real data on the Mac (offline clone):
  - bin files identical to the pre-change converter's;
  - attach on a clone, then dual re-execution over ≥ 1000 blocks with no shadow stop;
  - per-phase wall time recorded.
- [ ] Hoodi resume: impossible on `src4` (see Task 14). Resume acceptance rests on the Task 11–13 tests, plus a
      resume of a stopped hex+bin datadir if one becomes available.
- [ ] full gate, `go test -race` on the touched packages, and `make lint`.

## Post-Completion
*Items requiring manual intervention or external systems — informational only*

**Mainnet measurement** (arb1-dev, `/erigon-data/pbt-min/src`, 3.0B leaves, 8C/16T, 125 GB; the owner's call):
- per-phase wall time against the erigon-2 table above and against the earlier mainnet run (27 h 05 min +
  13 h 56 min);
- bin files identical to the old converter's at the same point.

**Conformance**: hive `pbt-artifacts` on snap-arb1, 231/0 as before.

**Next lever, if the totals stay above ~2 h**: the fold stays serial in its batch cutter, and the stream root is one
goroutine. A partitioned bulk build would run the engine per partition, with one spill and rows straight
into the range files. It replaces both, and needs its own plan.

**Rollout**: outputs without a manifest are refused, and convert must be rerun.

**Deferred by the owner (2026-10-24)**: a `verify-pbt` command, and the docs (`docs/pbt-migration.md`,
`cmd/integration/Readme.md`, the root `CLAUDE.md`/`AGENTS.md` PBT paragraph).
