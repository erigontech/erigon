# commitment-v3 handoff — 2026-09-02

Branch `awskii/commitment-v3`, worktree `~/org/wrk/wt/commitment-v3`, off `origin/main @ 0124ab5a0c`.
Nothing of this branch is pushed; the calcState fix it carries is PR #23737 against main. Full evidence trail: `~/org/e/research/commitment-v3-singleton-storage.org`.

## Where the model stands

v3 stores one commitment record per touched trie edge instead of one bundled row per branch.
`changed = bitmap | (touchMap &^ afterMap)` with `bitmap = touchMap & afterMap` gives
`changed == touchMap`, so v3 writes exactly the touched edges. The **3.03x record multiplier is
inherent, not a defect**, and it is the cost.

Measured on hoodi from 0, both arms on identical hardware (AMD EPYC 4344P, 16 threads, 125 GiB),
v2 on snap-arb1 and v3 on edev, at equal `domain_commitment_keys` (1.150e7):

| measure                        |      v2 |      v3 | ratio |
|--------------------------------|---------|---------|-------|
| records written                | 6.37e6  | 1.93e7  | 3.03  |
| commitment compute (batched)   | 11.88s  | 23.86s  | 2.01  |
| commitment `.kv` on disk       | 605 MB  | 637 MB  | 1.05  |
| commitment `.bt` index         | 3.68 MB | 12.5 MB | 3.39  |
| commitment `.kvei` existence   | 3.54 MB | 14.1 MB | 3.97  |
| chaindata (mdbx)               | 7.38 GB | 11.5 GB | 1.56  |
| objects allocated (cumulative) | 825M    | 932M    | 1.13  |

The `.kv` byte premise holds at near parity. Everything keyed **per record** tracks the 3.03x
multiplier: btree index, existence filter, mdbx. The storage story is not "v3 writes more trie",
it is "v3 writes the same trie as 3x as many keys, and every per-key structure pays for it".

## Commitment history

`CommitmentDomain.Hist` sets `HistoryDisabled: true` **and** `SnapshotsDisabled: true`, so by
default commitment produces no `.v` history and no `.ef` inverted index. They are off for
different reasons and should not be lumped:

- history is `.v` (in `history/`) plus `.vi` (in `accessor/`) — disabled outright.
- the inverted index is `.ef` (in `idx/`) plus `.efi` (in `accessor/`) — `IiCfg.Enabled: true`,
  it is live, but `SnapshotsDisabled` keeps it out of files. It exists only in chaindata as
  `TblCommitmentIdx` and `TblCommitmentHistoryKeys`, so it is inside the 1.56x chaindata delta
  and has **never been broken out per table**.

`statecfg.EnableHistoricalCommitment()` flips both, gated on `cfg.KeepExecutionProofs`, set by
`--prune.include-commitment-history`. The setting is persisted per datadir at first start and
requires `TblAccountVals` to be empty, so it can only be chosen on a fresh chaindata.

Every non-archive prune mode keeps `CommitmentHistory: KeepAllBlocksPruneMode`, so
`--prune.mode=full --prune.include-commitment-history` is the right shape for measuring
commitment history without running an archive node. `DefaultMode = ArchiveMode`, so dropping
`--prune.mode` does nothing — non-archive has to be explicit.

## Resolved: the per-block regime was throttled by two map scans in the calculator

Under `--prune.include-commitment-history` (`forcePerBlockCompute`), run 5 showed v3 at 22 blk/s
against v2's 40-95 and `parallel committed blk=0` for a 21790-block first cycle. Neither number
meant what the previous version of this file said:

- `committed blk=0` is a log artifact. `computeAndCheck` publishes only on a root mismatch and
  `lastCommittedBlockNum` moves only when the apply channel closes, so any cycle longer than the
  log interval prints the previous cycle's end. `domain_commitment_took_count` tracked the executed
  block on both arms (v3: 33327 calls at blk 33324), so per-block compute was running all along.
- `invalid=3.20k` / `repeat%=36.87` is chain content. v2 shows repeat 22-33% and invalid
  1.8-4.7k over the same block range (1744-21567, and again from ~44k); the old table compared
  v2 at blk 36013, a light range, with v3 at 18401, the heavy one.

The real cost was in `calc_state.go`, identical on main: `cs.accounts` keeps every account written
since the batch started, and `FlushToUpdates` plus `ResetBlockFlags` walked the whole map once per
block. A 30 s CPU profile of v3 at blk ~31k (edev, 09:54Z) put 27.5 s of the calculator's 30 s in
those two scans, about 60 ms per block. A cycle ends on the byte cut (2 x sd.mem > 512 MB); v2 gets
there in ~350 blocks (bundled rows plus their history, ~750 KB/block), v3 in ~2300 (~117 KB/block),
so v3 ran 6x longer cycles and paid the quadratic 6x harder. v2's own slow-range cycles (7-8k
blocks at 43-57 blk/s) pay it too.

Fix: a per-block dirty list (`markDirty`), PR #23737 (`awskii/calcstate-dirty-accounts`, commit
`2ab3850420b`), cherry-picked here as `315bb3404bb`. Benchmark at 300k accumulated accounts:
5.0 ms -> 22 us per block. Run 6 (both arms from 0 on `d7a9d63f`, 10:14Z) is the rig check: at
four minutes in, v2 blk 12893 at 42 blk/s and v3 blk 11911 at 39-42 blk/s, where run 5 had v3 at
blk 5016 and 22 blk/s. Evidence: research log, section "Two map scans in the calculator".

## What was fixed on this branch, and what it bought

Read path, in order of what the profile said:

1. `4e1726f0ea` — the db read did one B-tree descent per nibble (up to 16 per node) while the file
   path already walked with one seek plus `Next()`. Both now share `scanCommitmentRecordRun`.
   The key encoding makes the run correct: for even-length `P` the term byte is `0x00` and every
   descendant sorts at `0xf0` or higher at that offset, so the 16 edge records are contiguous;
   only a path ending in nibble `0xf` can have a descendant sort between them, and the run
   re-seeks past those.
2. `88d009ddc4` — `loadLatestCollectorRecords` cloned key and value on every etl row but keeps
   only the last per key. Now reuses buffers. **This one shipped a bug** (see below).
3. `a24f91ab1e` + `b2b41e3a27` — v3 record reads were entirely unmetered, so every
   `kv_read_count{domain="commitment"}` read zero on a v3 node while v2 reported millions. The
   gap was in `asOfStateReader` (execution/stagedsync), the reader the parallel commitment
   calculator hands its workers: `Read()` reaches the accumulator through its getter, but a
   record read never uses a getter and was passing a literal nil.
4. `fbcf29d7f4` then `a3c5ddc4ef` — branch cache. First batched the fill per node, then keyed the
   whole entry by node.

The cache work is the one to understand before touching it again. `v3TrunkSlot` routed a record
for edge `P->n` by the **child's** nibble path at depth `d+1`; with `trunkDepthFull = 4`, every
node at depth 4 had all 16 children spill past the trunk into the LRU tail, while v2 kept that
same node in the `d4` array. v3 lost a whole level of trunk coverage *and* multiplied entries by
the branching factor. Hit rate was 56.2% on v2 against 16.7% on v3.

Node-granularity caching (one entry per node, `present:2 | len:2 per child | payloads`) took the
v3 hit rate to 32.1% and cut db record reads 18.4% — **and moved commitment compute by 1.1%**.
So the reads were never what made v3's commitment slow. Do not spend more effort on the read side.

Three details in that entry that are not optional:

- `branchCacheEntry.node` — the v3 root's node key is byte-identical to `KeyCommitmentState`, and
  an edge record for `P->n` routes to the same trunk slot as node `P||n`. Without the
  discriminator `Get` hands a blob back as a value.
- `step` and `txN` are both the **max** over children. `IsStale` is
  `epoch != s.epoch && txNum >= s.floor`, monotone in txNum, so max makes the entry stale exactly
  when any child would be. Min is wrong.
- `PutChildren` merges rather than replaces: a publish carries only the changed records.

## Traps this branch has already paid for

- **nil is not empty.** `append(nil[:0], empty...)` returns nil, and `DomainPut` refuses a nil
  value outright. An empty record is a tombstone and must stay non-nil. This killed v3 at block
  34028 and the guard test passed the whole time by ordering luck — its tombstone row came after
  a row that had already given the buffer capacity. The test that bites puts the empty record
  first (`TestLoadLatestCollectorRecordsKeepsFirstEmptyRecordNonNil`).
- **A typed nil in an interface reads as non-nil.** A nil `*kvmetrics.DomainMetrics` assigned
  straight into a `kv.GetLatestMetrics` parameter silences the request-scoped fallback.
- **Test coverage claims need mutation.** `TestCommitmentReadsAreMeteredInBothFormats` drives
  `ComputeCommitment` serial and parallel and still does not cover `asOfStateReader` — it passes
  with the nil restored. The guard that bites is `TestAsOfStateReaderWorkerMetersRecordReads`.
  Likewise the first branch-cache collision test did not fire: it used a node key whose own
  lookup missed the trunk. The real collision needs `Get(recordKey for P->n)` against a stored
  node `P||n`.
- **`AggOpts.NewTest()` pins test aggregators to v2.** A green suite proves nothing about v3
  unless the test calls `ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)`.
- The staleness rejection in the db cursor run is **not** observable through a latest-view oracle
  (a stale db record and the file record hold the same bytes). No guard test exists for it; it
  stays because it mirrors `getLatestFromDb`'s contract.

## Benchmark rig

Two arms, identical hardware, differing only in `COMMITMENT_EDGE_RECORDS`:

| arm | host      | datadir                   | metrics | pprof | run dir            |
|-----|-----------|---------------------------|---------|-------|--------------------|
| v2  | snap-arb1 | `/erigon-data/hoodi-v2`   | 6061    | 6062  | `~/hoodi-runs/v2`  |
| v3  | edev      | `/erigon-data/hoodi-v3`   | 6081    | 6082  | `~/hoodi-runs/v3`  |

Scripts live in `~` on both hosts: `hoodi-arm.sh` (one arm, wipes derived state and runs from 0),
`restart-arm.sh` (stop, install `/tmp/erigon-new`, relaunch from 0), `measure-state.sh` (stop and
report per-domain on-disk sizes), `sync-prof.sh` (synchronized 90s cpu + alloc capture).

- Builds happen on **snap-arb1** only (`~/erigon-cv3`, commits arrive as `git format-patch` +
  `git am`, so its SHAs differ from this branch: `d7a9d63f54` there = `315bb3404bb` here). edev's
  `~/erigon-cv3` is repointed to `awskii/r36converter` and has no commitment-v3 history. snap-arb1
  has no ssh key for edev; stream the binary through the Mac,
  `ssh snap-arb1 'gzip -1 -c /tmp/erigon-new' | ssh edev 'gunzip -c > /tmp/erigon-new'` (93 s),
  and never run two transfers at the same destination.
- Restart sequence: stop both arms (the first half of `restart-arm.sh`: kill the monitor, `kill -INT`
  the node pid, wait), build, `cp build/bin/erigon /tmp/erigon-new`, stream to edev, then
  `bash ~/restart-arm.sh v2 false 6061 6062 30303 42069` on snap-arb1 and
  `bash ~/restart-arm.sh v3 true 6081 6082 30403 42169` on edev. The script moves the old run dir
  to `~/hoodi-runs/<arm>-<ts>` and relaunches from 0.
- Build while the nodes are **stopped**. A build on snap-arb1 steals CPU from the v2 arm only and
  biases the comparison.
- `KV_READ_METRICS=true` gates `domain_commitment_took` and the per-domain read counters.
- `metrics.csv`'s `block` column is wrong: it greps the last `blk=` in the log, which lands on the
  `parallel committed blk=0` line. Read `parallel executed` instead.
- Patch transfer: use a dedicated directory. A stale `/tmp/0*.patch` glob re-applied landed
  commits once.
- `seg rm-state` hardcodes `promptUser := true`; pipe `printf "1\n" |` or it silently deletes
  nothing.
- Per-table chaindata sizes: `integration print_table_sizes`. It opens the db **read-write**
  (`openDB`'s third arg is `applyMigrations`, not readonly), so the node must be stopped.
  `integration` is not built on either host yet.

## Run 6 at 1M blocks (2026-09-03)

Both arms reached the 1M target: v2 in 22389 s, v3 in 20609 s, **v3 8.0% faster end to end** and
ahead in every 100k range. Full tables in the research log, section "Run 6 at 1M blocks". Every
non-commitment file is byte-identical across arms.

| commitment on disk         |      v2 |      v3 | v3/v2 |
|----------------------------|---------|---------|-------|
| `.kv` (4 files)            | 9.61 GB | 7.54 GB | 0.78  |
| `.v` history               | 28.70 GB | 17.59 GB | 0.61 |
| `.vi` + `.ef` + `.efi`     | 3.93 GB | 7.64 GB | 1.94  |
| `.bt` + `.kvei`            | 89 MB   | 267 MB  | 3.0   |
| all commitment files       | 42.34 GB | 33.02 GB | 0.78 |
| chaindata high-water mark (set in the first 15 min, file builds lagging execution) | 36.67 GB | 19.86 GB | 0.54 |

The history ratio depends on the merge state. Step files are unpaged; merged files zstd 64-entry
pages. A v2 history entry is the previous bundled row, 482 B raw, 58-76 B merged, because
consecutive versions of one branch share most child hashes. A v3 entry is the previous edge
record, 34 B raw and 35-36 B merged, nothing for the codec to find. Raw step 44 is 5.49 GB on v2
against 0.50 GB on v3; merged, v3 is 0.62-0.78x per range. v3 writes 2.14x the domain keys at step
44 (down from 3.03x at step 0) and 1.30x the history entries.

Time: in-cycle execution 19342 vs 19101 s; the between-cycle flush 2748 vs 1209 s (mdbx sync 984
vs 159 s) is where v2 loses; commitment compute 785 vs 1512 s is where v3 pays. v2 wrote 1133 GB
to storage against v3's 502 GB, took 1.25M major page faults to v3's 40k, and peaked at 80.8 GB
RSS to v3's 66.4 GB. Process CPU 49687 vs 51447 s. On the warmup path v3 reads 17.6x the file
records (439M vs 25.0M) for that +3.5% CPU.

## Decisions still open

1. Resolved by run 6: v3 holds parity past the heavy range and leads by 8%; its cycles are 3.1x
   longer in blocks (234 vs 731 cycles) and the flush cost per block is 0.44x.
2. Whether the record multiplier is acceptable at all. At step 44 it is 2.14x in keys and 1.30x in
   history entries, and the bytes now favour v3 in every data file. What still scales with it is
   every per-key index: `.bt`/`.kvei`/`.ef`/`.efi` are 5.35 GB on v3 vs 2.07 GB on v2, and `.vi`
   2.55 vs 1.95 GB, 3.9 GB of index against 13.2 GB of data saved, 9.3 GB net. If that is still no, the design fork worth considering is
   bundled rows for the trunk and edge records only in storage subtrees.
3. `#6` from the earlier review: eight `*ForFormat` parse-guard wrappers whose `edgeRecords=true`
   argument has no production caller. Deliberately skipped as a wider refactor than the finding
   warrants.
4. Resolved by run 6: the chaindata delta is `CommitmentHistoryVals`, 6.23 GB on v2 vs 1.35 GB on
   v3 at the pre-prune peak (bundled rows land on overflow pages); `CommitmentHistoryKeys` is the
   per-key table and stays 1.34x on v3. The 1.56x the other way was a step-0 reading.
5. Whether the child mask must always travel with the parent. A node read without a mask wants
   all 16 children, absent nibbles never resolve, so `readCommitmentRecords` walks every v3 file
   and probes each file's existence filter for every absent nibble. Invisible at from-0 with 1-2
   files; at the tip it is files x absent nibbles per such read. Counters are in place (see
   "State at handoff"); the rig binary has to be rebuilt to report them.

## State at handoff

Run 6: both arms finished (v2 blk 1,000,117 at 16:42Z, v3 blk 1,002,609 at 16:12Z on 2026-09-02),
nodes stopped, datadirs kept (118.6G / 93.1G). They ran from 0 on `3.7.0-dev-d7a9d63f` (md5
`d7ac02c70c`), started together at 10:29:35Z (v3) / 10:29:36Z (v2) with `--prune.mode=full
--prune.include-commitment-history`, `KV_READ_METRICS=true`, on wiped chaindata and no state
snapshot files (checked on disk). `integration` is now built on snap-arb1 at the rig commit and
copied to edev as `/tmp/integration-new`; `print_table_sizes` needs the node stopped. A first
10:14Z start had a 26 s offset between the arms and was replaced; its records are in
`~/hoodi-runs/{v2,v3}-20260902T1029*`, run 5's in `-20260902T1014*`. To start the arms together,
stop both, then run each host's `restart-arm.sh` behind `sleep $((T - $(date +%s)))` with one
shared epoch `T`. The calcState fix is PR #23737 against main from
`~/org/wrk/wt/calcstate-dirty`, `make lint` clean, `execution/stagedsync` green, judged HOLDS;
Copilot review requested. `MACHINES.org` carries both arms under snap-arb1 and edev.

Mask-knowledge counters (commit after 8b7e733e04f, not yet in the rig binary). Trie level, in
execctx: `domain_commitment_node_reads{mask="known"|"unknown"}` per node read. Aggregator level,
keyed by what the walk did rather than by the caller's mask, because the trie forwards every read
as a narrowed known mask: `domain_commitment_record_reads{walk="satisfied"|"exhausted"}`,
`domain_commitment_record_files_consulted{walk=...}` (a v3 file reached with something still
missing, before its existence filter) and `domain_commitment_record_files_scanned{walk=...}` (a
file actually seeked). An exhausted walk is one that ran out of files with children still wanted,
which is what a maskless read does every time, warm cache or not. Per block: delta of a counter over
the delta of `domain_commitment_took_count` between two `metrics-last.prom` dumps, or
`increase(...[5m])` on both. Read them on the 1M run or a synced node, not in the first steps,
since the cost is files x absent nibbles. Tests: `TestCommitmentV3RecordWalkIsCountedByOutcome`
and `TestCommitmentV3UnknownMaskNodeReadExhaustsTheFileWalk` (db/state),
`TestReadCommitmentRecordsCountsMaskKnowledge` (execctx).
