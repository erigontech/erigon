# commitment-v3 handoff — 2026-09-02

Branch `awskii/commitment-v3`, worktree `~/org/wrk/wt/commitment-v3`, off `origin/main @ 0124ab5a0c`.
Nothing pushed. Full evidence trail: `~/org/e/research/commitment-v3-singleton-storage.org`.

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

## Open finding: v3 collapses under per-block commitment

`KeepExecutionProofs` sets `forcePerBlockCompute` (exec3_parallel.go), so enabling commitment
history moves commitment from 5000-block batches to every block. Under that regime, at equal
wall clock:

| measure          |     v2 |      v3 |
|------------------|--------|---------|
| block height     | 36013  | 18401   |
| blk/s            | 91     | 22      |
| repeat%          | 2.83   | 36.87   |
| abort            | 126    | 1.87k   |
| invalid          | 11     | 3.20k   |
| parallel committed | blk=36013 | **blk=0, blks=0** |

**Commitment compute is not the cause** — per call v3 is *faster* here: 11.25s/18500 = 0.608 ms
against v2's 30.93s/36231 = 0.854 ms. The loss is in parallel execution: v3 aborts and
re-executes a third of its work, and has committed nothing at all.

`invalid=3.20k` against v2's 11 does not look like a performance characteristic. Treat this as a
suspected defect that the per-block regime exposed, not as a v3 cost. **This is the first thing to
investigate.** Nothing has been done about it.

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

- Builds happen on **snap-arb1** only. edev's `~/erigon-cv3` is repointed to `awskii/r36converter`
  and has no commitment-v3 history. Stream the binary with `scp -C` — 34s against ~10 min without,
  and never run two scp's at the same destination, they fight and neither finishes.
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

## Decisions still open

1. The `invalid=3.20k` / `repeat%=36.87` / never-commits behaviour under per-block commitment.
   Suspected defect. Unstarted.
2. Whether the 3.03x record multiplier is acceptable at all. Every per-key structure scales with
   it, and the read-side lever turned out to be worth ~1%. If the answer is no, the design fork
   worth considering is bundled rows for the trunk and edge records only in storage subtrees.
3. `#6` from the earlier review: eight `*ForFormat` parse-guard wrappers whose `edgeRecords=true`
   argument has no production caller. Deliberately skipped as a wider refactor than the finding
   warrants.
4. Per-table breakdown of the 1.56x chaindata delta — never measured.

## State at handoff

Both arms running from 0 on `3.7.0-dev-49237559` (md5 `e9851ec56f`), started 09:29Z with
`--prune.mode=full --prune.include-commitment-history`, `KV_READ_METRICS=true`. `make lint`
clean, `db/state/...`, `execution/commitment/...` and `execution/stagedsync/...` green.
