# PBT convert, export, import and migration attach on pbt-v3

## Overview

- Live erigon nodes move to the PBT this way:
  - a producer converts the latest state of a hex datadir into pbt-v3 bin commitment files and publishes them through
    bittorrent;
  - each live node attaches the published files and re-executes from their end S in dual mode;
  - after the fork, dual mode lasts only while the fork block is within the unwind window; then the node runs PBT only.
- Any node can export the EIP-8347 PBT snapshot (one file, the latest state at a block end) together with the preimage
  file.
- Import is a test-only bootstrap. It proves an exported snapshot is complete by running a node from it.
- Missing on `awskii/pbt-v3` today:
  - `integration commitment rebuild --experimental.bin-commitment` builds a bin-only datadir at the tip, shard by shard,
    through the incremental engine. There is no path from a hex datadir to hex+bin, and no published-file workflow.
  - there is no EIP-8347 artifact code;
  - `export-preimages` pins against the root of whatever `CommitmentDomain` holds, which is wrong on post-fork hex+bin
    and pre-fork bin-only datadirs;
  - nothing stops the hex domain after the fork.
- The converter replaces rebuild's bin target. The hex rebuild stays.
- Legacy hex to v3 hex conversion (`integration commitment convert --v3`) is unchanged.
- This plan starts after the witness plan (`docs/plans/20260930-pbt-v3-witnesses.md`) has merged `awskii/v3-commitment`
  into `awskii/pbt-v3-witness`. Main's pinned `export-preimages` (`7853b9226e3`, #24326) arrives through that merge.

## Context (from discovery)

- Spec: the local EIP-8347 revision in `~/org/wrk/wt/eips-8347-leaf-encoding`, `EIPS/eip-8347.md` at `d85af9da`:
  sections "Preimages", "The converter", "PBT snapshot artifact" (integer encoding, header record, groups and storage
  records, leaf derivation, canonical digests), "Verification (dual-check)", "BAL-replay". EIP-8297 is in the same
  directory.
- Rebuild pieces reused:
  - `db/state/squeeze.go`: the sorted batch feed `pbinForEachRebuildOpStreamLookaheadAfterWithSample` and
    `pbinRebuildOverlay` with `FlushFinished`;
  - `cmd/integration/commands/commitment.go`: `stageRebuildOutput`, `linkSnapshotsExceptCommitment`,
    `isCommitmentFileName`, `validateStagedOutput`, `rebuildOutput.settings`;
  - `cmd/integration/commands/stages.go`: the source opening options `SkipPBinStateDBCheck`, `DisableInterDomainDeps`,
    `SkipFilesDBGapCheck`.
- Latest-range iteration: `DomainRoTx.DebugRangeLatest` and `DebugRangeLatestFromFiles` return `*DomainLatestIterFile`
  (`db/state/domain.go`, `db/state/domain_stream.go`, `CursorItem`). A file item's `endTxNum` is the file end minus 1;
  a DB item's is its step start.
- Translation: `execution/commitment/v3/pbt/feed.go` (`FeedOpEmitter`, `NewRebuildFeedOpEmitter`) and
  `execution/commitment/commitmentdb/pbin_feed.go`. The ordinary emitter drops identical chunks and emits zero chunks
  and merge operations unless `CodeWritten` is set.
- File building: `(*Domain).buildFileRange` in `db/state/domain.go` writes a `.kv` and its accessors from a `Collation`
  of sorted pairs and skips history. Empty domain files build and merge. An internal gap in a domain's files only
  warns; a missing trailing range lowers the aggregator's commitment horizon and hides newer state files.
- Dual startup: `SeekCommitments` needs a commitment-state record in every active commitment domain at equal
  (block, txNum). Executors skip transactions through the restored checkpoint.
- Reset: `rawdbreset.ResetExec` (`execution/stagedsync/rawdbreset/reset_stages.go`) does the following, keeping block
  data:
  - clears Execution progress, the state tables and state history;
  - clears every domain table, both commitment domains included;
  - clears the stop markers and the branch cache.

  On restart `SeekCommitments` restores the checkpoint from files. This is erigon's snapshot-sync reset.
- Settings: `db/state/erigondb_settings.go` records `trie_variant`, `trie_hash` (a stored suite that differs from the
  configured one is refused) and `frozen_at_txnum`. `frozen_at_txnum` rejects future writes, so it cannot carry a
  conversion point.
- Unwind sites: `CanUnwindToBlockNum` and `CanUnwindBeforeBlockNum` (`db/rawdb/rawtemporaldb/accessors_commitment.go`),
  `UnwindExecutionStage` and `unwindExec3` (`execution/stagedsync/stage_execute.go`), `SharedDomains.Unwind`
  (`db/state/execctx/domain_shared.go`, no error return today).
- Integrity: `db/integrity/commitment_integrity.go` (`CheckCommitmentRoot` requires per-file state records, rejects
  the zero root, and hard-codes `CommitmentDomain`).
- Preimages: main's `cmd/utils/app/export_preimages_cmd.go`:
  - `runExport`, `pinnedStateRoot`, `checkRootPin`, `collectHashedPreimages`, `writeHashedPreimages`, `preimagesMeta`;
  - bytes and keccak ordering follow the spec;
  - its pin helper opens with `WithSequentialCommitment`, which forces v3 hex to legacy hex and breaks hex+bin opening.
- Shadow stop:
  - `execution/stagedsync/committer.go` stops a shadow domain only when its fold fails (`finishDualFolds`,
    `stopShadowDomain`, `recordStoppedCommitmentDomains`);
  - `debug_migrationProgress` already reports `ShadowStopped`;
  - the stage loop's unwind window is the sync config's `MaxReorgDepth`, defaulting to `dbg.MaxReorgDepth`, which is 96.
- binary-trie's mainnet bin conversion (old engine, per-shard) took 109h13m for 430.37 GB on snap-arb1 on 2026-08-18.

## Development Approach

- **testing approach**: TDD.
  - Every behaviour change starts with a test that fails for the missing behaviour: read the assertion that fires, not
    the exit code.
  - For a new API, first add its signature, or a stub returning a sentinel error, so the red is the named assertion and
    not a compile error.
  - Mutation checks: copy the file to the scratchpad, break the guarded code, run the whole package, name the
    `file:line` that went red, copy the file back. Never `git checkout` or `git restore` a file holding uncommitted
    work.
- complete each task fully before moving to the next; small, focused changes.
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task.
- **CRITICAL: all tests must pass before starting next task** - no exceptions.
- **CRITICAL: update this plan file when scope changes during implementation.**
- conventions:
  - no code comments;
  - new files carry the 2026 license header;
  - identifiers in package `commitment` carry the `pbin`/`PBin` prefix;
  - Go names without Java-style suffixes (erigon uses `*Func` for function types);
  - benchmarks live in `*_bench_test.go`.
- Tests restore every process-global they change in `t.Cleanup` and never call `t.Parallel`. The BLAKE3 suite is
  selected explicitly for key derivation, the reference root, the engine and output metadata; both `HashBytes` and the
  reference default to keccak.
- commits:
  - at the end of each task: subject line only, at most 120 characters, erigon package prefix, no body, no trailer, no
    attribution;
  - before each commit: `make lint` until clean (it is non-deterministic) and `make erigon integration`.
- all tasks are committed on `awskii/pbt-v3-snapshot` (worktree `~/org/wrk/wt/pbt-v3-snapshot`), fast-forwarded to the
  witness plan's task 1 merge commit before task 1 starts.

## Testing Strategy

- **unit tests**: required for every task.
- **integration tests**: small test datadirs built by the existing test helpers (temporal test DB, test chains with a
  scheduled `binaryTrieTime`) for conversion, attach, export and import.
- no UI or browser e2e suites exist in this repository; devnet and mainnet-scale runs go to Post-Completion (remote
  hosts only).
- package runs: `go test ./db/state/... ./db/integrity/... ./db/rawdb/... ./execution/commitment/...
  ./execution/stagedsync/... ./cmd/integration/... ./cmd/utils/app/...`; full gate `make lint && make erigon integration`.

## Progress Tracking

- mark completed items with `[x]` immediately when done.
- add newly discovered tasks with ➕ prefix.
- document issues/blockers with ⚠️ prefix.
- update the plan if implementation deviates from original scope.

## Solution Overview

1. One pass over the latest range of accounts, storage and code produces EIP-8297 leaves sorted by tree key. Each leaf
   carries the step range its value came from. Tree-key order already equals the artifact's section order: account
   zone `0x00`, code zone `0x01`, storage zone `0xff`, grouped by stem. So the same stream feeds the converter, the
   artifact writer and a new streaming reference root.
2. The converter feeds the stream through the pbt engine in sorted batches. A range writer places each finished row in
   the published state-file range of the newest write under it. There is no history and no intermediate state record;
   every range up to S gets a file.
3. Attach adopts published files, resets the node's execution state with `ResetExec` and re-executes from S. The
   automatic hex stop ends dual mode after the fork window.
4. Export writes the artifact and the preimage file from one shared pin at a block end. Import rebuilds a test node's
   state from them through the ordinary commitment fold and runs it forward.

## Technical Details

### Leaf stream

- sources:
  - the converter reads `DebugRangeLatestFromFiles`: the latest state held in files, at the files-only frontier S;
  - export reads `DebugRangeLatest`: files plus DB, at the pinned block end.
- provenance: `advanceInFiles` records `nextStamp` beside the selected key and value, before equal-key cursors advance.
  A file item stamps its file range; a DB item stamps its step.
- translation: values go straight into `NewRebuildFeedOpEmitter` with `CodeWritten=true`; zero values and deletions are
  dropped. That gives, per account:
  - the header stem (basic data; code hash or delegation; slots 0-63 at subs 64-127);
  - overflow storage stems;
  - code chunks by code hash, all-zero chunks absent.

  The stamp attaches in the emit callback; `pbt.Op` is unchanged.
- stamps combine by max: basic data takes max(account range, code range); a shared chunk takes the max over its
  sources; ETL duplicate handling keeps the max stamp of identical payloads and still rejects conflicting ones.

### Reference root

- `eip8297` gains a streaming builder over tree-key-sorted leaves, with memory bounded by depth. It follows EIP-8297's
  tree embedding: prefix-free keys, compressed bit prefixes, full leaf keys, zones `0x00/0x01/0xff`, 34- and 66-byte
  keys. It uses the selected hash suite explicitly and equals `reference.go`'s root on every input.

### Range writer

- a row's stamp is the max stamp of the leaves under its prefix. The sorted leaf stream is the source: a row created
  after its leaves were folded has no earlier overlay write that could carry their stamps.
- a stamp maps to the range of the published accounts files that contains it. A file is written for every range up
  to S, empty when no row lands there.
- files are built by `(*Domain).buildFileRange` from a sorted `Collation`, in `CommitmentBinDomain` (hex+bin) or
  `CommitmentDomain` (bin), with file names and versions the downloader accepts.
- the commitment-state record goes in the newest range, at S.

### Conversion point and unwind floor

- `erigondb.toml` gains the conversion point (block, txNum).
- `CanUnwindToBlockNum`, `CanUnwindBeforeBlockNum`, `UnwindExecutionStage`/`unwindExec3` and the in-memory unwind path
  refuse an unwind argument U <= C (an unwind to U drops `[U, inf)`).
- `SharedDomains.Unwind` has no error return: either its callers check the floor first, or it gains an error return,
  whichever is the smaller diff. Each site gets its own test.

### Converter

- `integration commitment convert-pbt [--keep-hex] --output.datadir <dir>`.
- the source opens through rebuild's opening path with both commitment domains' files excluded.
  `isCommitmentFileName` learns `commitmentbin`; today a dual source's bin files would be linked.
- outputs:
  - `--keep-hex`: hex commitment files hardlinked, rows in `CommitmentBinDomain`, `trie_variant = hex+bin`. The source's
    hex must be v3 (state key `0x42`, marker `0x04`) and its state record must sit at S; otherwise refuse and say to run
    `commitment convert --v3` or to collate first.
  - without `--keep-hex`: rows in `CommitmentDomain`, `trie_variant = bin`, refused unless S is post-fork (a pre-fork
    bin-only output cannot boot: execution checks its bin root against an MPT header).
- the output records `trie_hash` and the conversion point at S.
- completion check: a recursive `Verify` over the written rows; the root equals the reference root; the header root is
  compared only when S is a block end after the fork. The settings file is written last; an incomplete output is
  removed.
- deterministic: the same source files give byte-identical output files.
- the empty state converts to the zero root.

### Attach

- `integration commitment attach-pbt --from <published dir>` on a stopped node:
  1. check the published settings: same step size and ranges as the node's files up to S, hex and bin both present, and
     a `trie_hash` equal to the node's configured suite;
  2. adopt the published state and commitment files up to S and remove the node's own state and commitment files past
     S;
  3. run `ResetExec` (state, history, commitment tables and stop markers cleared; block data kept);
  4. write `trie_variant = hex+bin`, the published `trie_hash` and the conversion point.
- on restart `SeekCommitments` restores the checkpoint at S; the node re-executes from there in dual mode.
- a mid-block S is covered: executors skip transactions through the restored checkpoint, and the rest of the block
  executes once.
- the command never wipes chaindata or block files.

### Automatic hex stop

- once the head is more than the stage loop's `MaxReorgDepth` blocks past the activation block, the committer stops the
  hex domain through the existing stop-marker path (`stopShadowDomain`, `recordStoppedCommitmentDomains`).
- unwinds across the activation block are refused after that point.

### Shared export pin

- one pin serves `export-pbt` and `export-preimages`:
  - the live commitment checkpoint (B, T) of the domain canonical at B;
  - a real mapping from B to T, with T equal to B's last txNum (`Max(B)` falls back silently when B is absent);
  - pre-fork bin-only and post-fork hex-only datadirs refused;
  - before the fork the hex root must equal `header(B).Root`, after it the bin root;
  - no restore of every active domain just to accept a lagging shadow.
- it replaces main's `pinnedStateRoot`, which opens with `WithSequentialCommitment`.
- operator path, documented: `integration stage_exec --block B` stops a node at a block end
  (`STOP_AFTER_BLOCK` exits without committing B).

### Artifact (spec "PBT snapshot artifact")

- layout: `pbtRoot[32] | headerCount[8] | headerRecord* | codeCount[8] | group* | storageCount[8] | storageRecord*`,
  counts big-endian, no trailing bytes.
- integers: `name[≤w]` is a one-byte length followed by a minimal big-endian integer (zero is length 0). Erigon's compact
  code-leaf codec trims trailing zeros and is not used.
- header record: `addressHash | nonce[≤8] | balance[≤16] | kind | codeRef | slotCount[1] | (slot[1] | value[≤32])*`,
  every slot below `HEADER_STORAGE_SLOTS` (64).
  - kind 0: no code, and nonce and balance not both zero (even with storage). The account's code hash is
    `keccak256("")`.
  - kind 1: `codeHash[32] | codeSize[≤4]`, size > 0, the code never a designator.
  - kind 2: `target[20]`, for exactly 23 bytes starting `ef0100`. The account's code hash is
    `keccak256(ef0100 ‖ target)`.
- code group: `stemHash | n[1] | (subIndex[1] | value[≤32]) * (n+1)`, non-zero values only.
- storage record: `addressHash | groupCount[≤8] | group*`, with `groupCount > 0`.
- the writer refuses records the spec forbids (kind 0 with nonce and balance both zero, kind 1 with size 0) rather than
  writing them.
- the writer buffers each storage record, spilling past a threshold, because `groupCount` is variable-width and
  precedes the groups. `pbtRoot` and the three counts are patched at their offsets at the end.
- the codec takes a plain (key, value) iterator defined in `eip8297/artifact`, so `db/state` adapts to it without an
  import cycle.
- `snapshotDigest` is keccak256 of the finished file.
- preimage file reader and join: the same package reads the preimage file strictly (records sorted by
  `keccak256(address)`, no duplicate address, slots sorted by `keccak256(slot)` with no duplicates, no truncated record,
  no trailing byte). It joins the two files in tree-key order as an exact set: every header record, header slot and
  overflow entry has a preimage, and every preimage has one.

### Export

- `erigon snapshots export-pbt --out <dir>`, registered beside `export-preimages` in `cmd/utils/app/snapshots_cmd.go`.
- uses the shared pin; one temporal transaction pins the aggregator and block-file views for both files.
- `pbtRoot` comes from the reference root and must equal the datadir's bin root when one exists at (B, T).
- before finishing, the export reads back both of its output files with the strict readers and the join.
- output files: `pbt-snapshot.bin`, the preimage file, and a meta JSON with chain id, block number and hash, T, the hash
  suite, stateRoot, pbtRoot, section counts, snapshotDigest, preimageDigest and a finalized flag. The canonical artifact
  bytes carry none of these.

### Import (test-only)

- `integration commitment import-pbt --snapshot <file> --preimages <file> --block <hash> --datadir <dir>`, run on a
  copy of a node's datadir.
- `--block` is checked against the local canonical header. N must be a block where bin is canonical (PBT from genesis,
  or after the fork); the output is bin-only.
- steps:
  1. `ResetExec`;
  2. strict readers and the exact-set join;
  3. write accounts (incarnation 1 for accounts with code, 0 otherwise), storage and address-keyed code through
     `SharedDomains` at N's last txNum T; the ordinary bin commitment fold then writes the rows and the commitment-state
     record at (N, T), as the genesis path does.
- checks:
  - the bin root equals `pbtRoot`, which equals `header(N).stateRoot`;
  - code: missing chunks read as 31 zero bytes, truncated to `codeSize`, the keccak equals the code hash, re-chunking
    gives the artifact's chunks, one `codeSize` per code hash, no surplus code groups.
- shared bytecode expands into address-keyed code rows; delegated accounts store their 23-byte designator; codeless
  accounts have no code row.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code, tests and documentation in this repository.
- **Post-Completion** (no checkboxes): mainnet-scale conversion and publication, devnet laps on remote hosts,
  cross-client digest comparison.

## Implementation Steps

### Task 1: Source provenance in the latest-range iterator

**Files:**
- Modify: `db/state/domain_stream.go`
- Create: `db/state/domain_stream_stamp_test.go`

- [x] add the stamp accessor with a stub. Write tests:
      - a key whose newest value is in a file reports that file's range;
      - a DB value reports its step;
      - a key present in several files and the DB reports the newest source.

      Confirm they fail at the stamp assertions.
- [x] record `nextStamp` in `advanceInFiles` beside the selected key and value, before equal-key cursors advance
- [x] write tests for iteration through `DomainRoTx.DebugRangeLatest` and `DebugRangeLatestFromFiles`, and for keys
      deleted in a newer range
- [x] run tests - must pass before task 2

### Task 2: Streaming reference root

**Files:**
- Create: `execution/commitment/eip8297/stream_root.go`
- Create: `execution/commitment/eip8297/stream_root_test.go`

- [x] add the builder signature with a stub. Write tests:
      - on random sorted leaf sets, the streaming root equals `reference.go`'s root under both suites; the sets cover
        all zones, header and overflow storage, code stems and groups of 1 to 256 values;
      - the empty stream gives the zero root.

      Confirm they fail at the root comparison.
- [x] implement the depth-bounded builder with compressed prefixes and full leaf keys; take the hash suite explicitly
- [x] write tests rejecting unsorted input, duplicate keys and zero values
- [x] run tests - must pass before task 3

### Task 3: Shared leaf stream

**Files:**
- Create: `db/state/pbt_leaf_stream.go`
- Create: `db/state/pbt_leaf_stream_test.go`

- [ ] add the stream signature with a stub. Write tests:
      - the stream over a test datadir yields the same leaves as the pbt engine's state after executing the same chain;
      - stamps combine by max: basic data over account and code, and a chunk shared by two accounts.

      Confirm they fail at the leaf and stamp assertions.
- [ ] implement the stream over accounts, storage and code (files-only or files-plus-DB), translating through
      `NewRebuildFeedOpEmitter` with `CodeWritten=true`, dropping zero values, attaching stamps in the emit callback
- [ ] sort by tree key through ETL, keeping the max stamp of identical payloads and rejecting conflicting ones
- [ ] write tests for delegated accounts, all-zero bytecode, code shared across accounts and an empty state
- [ ] run tests - must pass before task 4

### Task 4: Conversion point and unwind floor

**Files:**
- Modify: `db/state/erigondb_settings.go`
- Modify: `db/rawdb/rawtemporaldb/accessors_commitment.go`
- Modify: `execution/stagedsync/stage_execute.go`
- Modify: `db/state/execctx/domain_shared.go`
- Create: `db/state/erigondb_settings_conversion_test.go`
- Create: `db/rawdb/rawtemporaldb/accessors_commitment_floor_test.go`
- Create: `execution/stagedsync/stage_execute_floor_test.go`
- Create: `db/state/execctx/domain_shared_floor_test.go`

- [ ] add the conversion-point fields; write a settings round-trip test and confirm it fails before the fields exist
- [ ] write one test per site: an unwind argument U <= C is refused by `CanUnwindToBlockNum`/`CanUnwindBeforeBlockNum`,
      by `UnwindExecutionStage`/`unwindExec3`, and on the in-memory `SharedDomains.Unwind` path; U > C passes. Confirm
      each fails at its refusal assertion
- [ ] enforce the floor at each site, choosing between caller-side checks and an error return on
      `SharedDomains.Unwind` by the smaller diff
- [ ] run tests - must pass before task 5

### Task 5: Engine feed and range writer

**Files:**
- Create: `db/state/pbt_range_writer.go`
- Create: `db/state/pbt_range_writer_test.go`

- [ ] add the writer signature with a stub. Write tests on a test datadir:
      - the leaf stream runs through `pbinForEachRebuildOpStreamLookaheadAfterWithSample` and `pbinRebuildOverlay` into
        the range writer;
      - row stamps equal the max leaf stamp under each prefix, including a row created after its leaves were folded;
      - every range up to S gets a file, and an empty range yields an empty file that merges.

      Confirm they fail at the stamp and file assertions.
- [ ] implement row stamping from the sorted leaf stream and the mapping of stamps to the published accounts ranges
- [ ] build one file per range with `(*Domain).buildFileRange` from a sorted `Collation`, writing the commitment-state
      record in the newest range
- [ ] write tests:
      - the aggregator's visible files after writing: no hidden state files;
      - a merge across the written ranges
- [ ] run tests - must pass before task 6

### Task 6: convert-pbt command

**Files:**
- Create: `db/state/pbt_convert.go`
- Create: `db/state/pbt_convert_test.go`
- Create: `cmd/integration/commands/commitment_convert_pbt.go`
- Create: `cmd/integration/commands/commitment_convert_pbt_test.go`
- Modify: `cmd/integration/commands/commitment.go`

- [ ] add the command with a stub. Write tests on a hex test datadir converted with `--keep-hex`:
      - the hex+bin output's bin root equals the reference root;
      - its rows equal the engine's rows built incrementally over the same state;
      - converting twice gives byte-identical files;
      - the file names and versions are accepted by the downloader's parser;
      - the output's `erigondb.toml` holds `trie_hash` and the conversion point at S.

      Confirm they fail at the first assertion.
- [ ] implement source opening (rebuild's options, both commitment domains excluded; `isCommitmentFileName` covers
      `commitmentbin`) on top of task 5's feed and writer
- [ ] implement the outputs:
      - `--keep-hex`: the v3 hex check and the alignment of its state record with S;
      - bin-only: refused before the fork;
      - the completion check: recursive `Verify`, reference root, header root at a post-fork block end;
      - settings written last.
- [ ] write tests:
      - a corrupted non-root row makes the completion check fail and the output get removed;
      - refusals: a legacy hex source with `--keep-hex`, a hex record not at S, a pre-fork bin-only output;
      - a source that is itself hex+bin or bin;
      - the empty state
- [ ] run tests - must pass before task 7

### Task 7: Bin-aware commitment integrity

**Files:**
- Modify: `db/integrity/commitment_integrity.go`
- Create: `db/integrity/commitment_integrity_bin_test.go`

- [ ] write tests:
      - integrity passes on a converted hex+bin datadir;
      - a corrupted bin row or bin root there fails `CheckCommitmentRoot`;
      - the zero root is accepted.

      Confirm they fail at those assertions.
- [ ] check the bin domain on hex+bin datadirs, accept converted ranges without per-file state records, accept the zero
      root
- [ ] run tests - must pass before task 8

### Task 8: Attach published files

**Files:**
- Create: `cmd/integration/commands/commitment_attach_pbt.go`
- Create: `cmd/integration/commands/commitment_attach_pbt_test.go`

- [ ] add the command with a stub. Write a test:
      - a node past S attaches files converted from a copy of its datadir at S;
      - it restarts and re-executes to its tip in dual mode;
      - its bin roots equal the shadow roots of a node that ran dual from genesis on the same chain.

      Confirm it fails at the root comparison.
- [ ] implement the command:
      - check the published settings (ranges, step size, both commitment domains, `trie_hash` against the node's suite);
      - swap the files;
      - run `ResetExec`;
      - write `trie_variant`, `trie_hash` and the conversion point.
- [ ] write a test with an S that ends mid-block: the remainder of that block executes exactly once after restart
- [ ] write tests for refusals: mismatched ranges or S, a published set without hex or bin, a different `trie_hash`, a
      node behind S
- [ ] run tests - must pass before task 9

### Task 9: Automatic hex stop after the fork window

**Files:**
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/stagedsync/stage_execute.go`
- Create: `execution/stagedsync/committer_hex_stop_test.go`

- [ ] write tests:
      - the hex domain keeps folding until the head is the stage loop's `MaxReorgDepth` blocks past activation, and
        stops on the next block;
      - `debug_migrationProgress` then reports `ShadowStopped`;
      - an unwind across the activation block is refused after the stop.

      Confirm they fail at the stop assertion.
- [ ] implement the stop through `stopShadowDomain` and `recordStoppedCommitmentDomains`, reading the window from the
      sync config
- [ ] write tests for a restart after the stop and for a reorg inside the window before the stop
- [ ] run tests - must pass before task 10

### Task 10: Shared export pin

**Files:**
- Create: `cmd/utils/app/export_pin.go`
- Create: `cmd/utils/app/export_pin_test.go`
- Modify: `cmd/utils/app/export_preimages_cmd.go`

- [ ] confirm `7853b9226e3` is an ancestor of HEAD
- [ ] add the pin signature with a stub. Write the pin-matrix tests; confirm they fail at the pin assertions. Cases:
      - hex-only, hex+bin before and after the fork, bin-only;
      - a block end, a mid-block checkpoint, a missing B mapping;
      - a lagging or frozen shadow.
- [ ] implement the pin; `export-preimages` switches to it, replacing `pinnedStateRoot` and its
      `WithSequentialCommitment`
- [ ] write a test that `export-preimages` keeps the v3 hex variant when opening a hex+bin datadir
- [ ] run tests - must pass before task 11

### Task 11: PBT snapshot codec and preimage join

**Files:**
- Create: `execution/commitment/eip8297/artifact/writer.go`
- Create: `execution/commitment/eip8297/artifact/reader.go`
- Create: `execution/commitment/eip8297/artifact/preimages.go`
- Create: `execution/commitment/eip8297/artifact/artifact_test.go`
- Create: `execution/commitment/eip8297/artifact/testdata/golden.json`

- [ ] write a golden artifact by hand from the spec text (kinds 0, 1 and 2, header slots, code groups, storage groups)
      with its snapshotDigest. Add the codec signatures with stubs; write tests that the writer reproduces the golden
      artifact and the reader accepts it; confirm they fail at the byte comparison
- [ ] implement the writer over a plain (key, value) iterator:
      - minimal big-endian integers;
      - section counts and root patched at the end;
      - storage records buffered with a spill threshold;
      - refusal of kind-0 empty accounts and kind-1 size-0 code;
      - the digest over the finished file.
- [ ] implement the strict reader:
      - widths and leading zeros;
      - kinds;
      - strict ordering;
      - non-zero values;
      - header slots below 64;
      - storage records matched to header records by a second cursor over the header section;
      - counts and trailing bytes.
- [ ] implement the strict preimage reader and the exact-set join
- [ ] write the reject tables (one case per artifact rule; for preimages: unsorted address, duplicate address, unsorted
      or duplicate slot, truncated record, trailing byte; for the join: a missing and a surplus preimage), round trips on
      random states, and the empty snapshot
- [ ] run tests - must pass before task 12

### Task 12: export-pbt command

**Files:**
- Create: `cmd/utils/app/export_pbt_cmd.go`
- Create: `cmd/utils/app/export_pbt_cmd_test.go`
- Modify: `cmd/utils/app/snapshots_cmd.go`

- [ ] add the command with a stub. Write tests:
      - an export on a hex+bin test datadir reads back through the strict readers and the join;
      - `pbtRoot` equals the datadir's bin root;
      - a tampered bin record in the datadir makes the export refuse.

      Confirm they fail at those assertions.
- [ ] implement the command: the shared pin, the leaf stream into the codec with `pbtRoot` from the reference root, the
      bin-root cross-check, the preimage file in the same view, the read-back check, the meta JSON
- [ ] write tests:
      - both digests are stable across two runs;
      - the empty state;
      - a node stopped with `integration stage_exec --block B`;
      - replay equals conversion: at the same block, the export from task 8's attached node and from a node converted
        at that block give equal `snapshotDigest`
- [ ] run tests - must pass before task 13

### Task 13: export-preimages exact set and metadata

**Files:**
- Modify: `cmd/utils/app/export_preimages_cmd.go`
- Modify: `cmd/utils/app/export_preimages_cmd_test.go`

- [ ] write tests: the exact-set check fails on a missing and on a surplus preimage (header slots 0-63 and overflow
      entries), and the meta carries `preimageDigest` and the block hash; confirm they fail first
- [ ] run the exact-set join from task 11 on the written file, and add `preimageDigest` and the block hash to the meta
- [ ] add a spill threshold for the per-account preimage buffer and test a large account
- [ ] run tests - must pass before task 14

### Task 14: import-pbt test bootstrap

**Files:**
- Create: `db/state/pbt_import.go`
- Create: `db/state/pbt_import_test.go`
- Create: `cmd/integration/commands/commitment_import_pbt.go`
- Create: `cmd/integration/commands/commitment_import_pbt_test.go`

- [ ] add the command with a stub. Write the acceptance test in `commitment_import_pbt_test.go` on a chain with PBT
      from genesis. The chain covers kinds 0, 1 and 2, shared code, code with an all-zero 31-byte chunk, and header and
      overflow slots. The test:
      1. exports at N from node A;
      2. copies A's datadir;
      3. imports with `--block` set to N's hash;
      4. asserts Execution progress is N;
      5. executes to the tip, where every root must equal A's.

      Confirm it fails at the progress or root assertion.
- [ ] implement:
      - the `--block` check against the local canonical header, refusing N where bin is not canonical;
      - `ResetExec`;
      - the readers and the join;
      - writes through `SharedDomains` at T with incarnation normalized and address-keyed code;
      - the ordinary bin fold producing the rows and the commitment-state record.
- [ ] implement the checks (roots against `pbtRoot` and `header(N).stateRoot`, code rules, kind 2 code hash
      `keccak256(ef0100 ‖ target)`)
- [ ] write tests for each failing check (wrong chunk, `codeSize` disagreement, designator under kind 1, missing and
      surplus preimage) and for the empty state
- [ ] run tests - must pass before task 15

### Task 15: Remove rebuild's bin target

**Files:**
- Modify: `db/state/squeeze.go`
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `cmd/integration/commands/flags.go`
- Modify: `cmd/integration/commands/commitment_output_test.go`
- Modify: `execution/stagedsync/stage_commit_rebuild.go`
- Modify: `db/state/rebuild_variant_test.go`
- Delete bin-only tests:
  - `db/state/rebuild_variant_bin_code_test.go`
  - `db/state/rebuild_variant_bin_shard_tombstone_test.go`
  - `db/state/squeeze_pbin_resume_test.go`
  - `db/state/squeeze_pbin_checkpoint_test.go`
  - `db/state/rebuild_pbin_state_test.go`
- Modify: `execution/commitment/backtester/pbin_rebuild_code_test.go`, `execution/commitment/backtester/pbin_m1a_test.go`

- [ ] move each bin case worth keeping into the converter's tests first. Confirm each moved case fails against a broken
      converter. The cases:
      - code spanning groups;
      - shared code chunked once;
      - zero chunks absent;
      - delegation without code leaves;
      - root and record parity;
      - right-edge reads.
- [ ] remove the bin path, its flags and settings, `pbinRebuildCheckpoint` and spill files, and
      `validatePBinRebuildState`; keep `PBinValidateRowStateFormat` and the open-time refusal
- [ ] keep the parts the converter or the hex rebuild still use:
      - the hex rebuild itself;
      - the shared wrapper in `stage_commit_rebuild.go`;
      - the hex assertions in mixed test files;
      - `squeeze_pbin_feed_test.go` and `squeeze_pbin_rebuild_test.go`, which cover the feed and batches the converter
        reuses.
- [ ] drop the checkpoint cases in `commitment_output_test.go`; point every "rebuild the bin commitment domain" message to
      `convert-pbt`
- [ ] run tests - must pass before task 16

### Task 16: Migration documentation

**Files:**
- Create: `docs/pbt-migration.md`
- Modify: `cmd/integration/Readme.md`
- Modify: `docs/pbin-dual-commitment.md`

- [ ] document these in `docs/pbt-migration.md`:
      - producer conversion and publication;
      - attach;
      - the dual window and the automatic hex stop;
      - export, with its operator path;
      - import (test-only);
      - the conversion point;
      - the command entry points.
- [ ] replace the bin rebuild instructions in `cmd/integration/Readme.md` and `docs/pbin-dual-commitment.md`
- [ ] cite code by name, never by line number
- [ ] run `make lint` - must pass before task 17

### Task 17: Verify acceptance criteria

**Files:**
- none (verification only)

- [ ] every requirement in the Overview is implemented
- [ ] the pin matrix, the reject tables and the refusal cases are covered by tests
- [ ] run the full suites: `go test ./db/state/... ./db/integrity/... ./db/rawdb/... ./execution/commitment/...
      ./execution/stagedsync/... ./cmd/integration/... ./cmd/utils/app/...`
- [ ] run `make lint` until clean and `make erigon integration`
- [ ] mutation-check the key guards; each must turn a named test red when reverted:
      - row stamping from the leaf stream;
      - the unwind floor at each site;
      - the recursive `Verify` completion check;
      - the pin rule;
      - the exact-set join;
      - the hex-stop unwind refusal.

### Task 18: [Final] Update documentation

**Files:**
- Modify: `docs/plans/completed/20260925-pbt-v3-rows.md`
- Move: this plan to `docs/plans/completed/`

- [ ] mark the rows plan's rebuild tasks as superseded by `convert-pbt`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- convert a mainnet datadir on a remote host; record time, output size and peak memory against binary-trie's 109h13m
  and 430.37 GB on snap-arb1.
- publish the converted files to a test torrent. On a second remote node: attach, re-execute and follow the chain in
  dual mode, then check the automatic hex stop on a devnet fork.
- export on two nodes at the same block and compare `snapshotDigest` and `preimageDigest`.

**External system updates**:
- add the bin commitment files to the published snapshot set and the downloader's preverified list when the owner
  decides to publish.
- cross-client digest comparison waits until geth-pbt adopts the leaf-encoding proposal. geth-pbt `origin/pbt`
  `793dedb` writes the upstream RLP leaf records.
