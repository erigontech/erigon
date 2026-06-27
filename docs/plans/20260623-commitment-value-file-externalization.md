# Externalizing Commitment-domain values to per-step value-files (.cvl)

Status: implemented and **dormant** (gated off by default) on `alex/external_val_36`
(commit `a0f6ffc8da`). This doc is the design/reference for the feature and the
checklist for enabling it in production.

## Problem

`chaindata` (MDBX) outgrows RAM, and the dominant occupant is the Commitment
domain: `TblCommitmentVals` holds the latest Patricia branch nodes — hundreds of
bytes to KBs each, rewritten ~every block. Once the B-tree exceeds RAM, the three
per-step maintenance operations thrash:

- **Flush** (`DomainBufferedWriter.Flush`) — KB-sized values cause B-tree page
  splits and dirty-page spill.
- **Collate** (`Domain.collate`) — full DupSort scan of `TblCommitmentVals`.
- **Prune** (`DomainRoTx.prune`) — per-key deletes churn the MDBX freelist (GC).

The values are large and dominate the bytes; the *keys* are small. Classic case
for **key–value separation**: keep a small reference in the B-tree, move the bytes
to an external file, so the B-tree stays RAM-resident.

## What other databases do (and the lesson)

| System | Mechanism | Dead-value reclamation |
|---|---|---|
| WiscKey / Badger | append-only vLog, key→(file,offset) in LSM | **scan+relocate GC** |
| Bitcask | append log + in-mem keydir | merge rewrites live, deletes old files |
| RocksDB BlobDB / Titan | blob files + pointer in LSM | GC piggybacks on compaction |
| Pebble (CRDB v25.4) | blob files, handle `{BlobFileID,ValueLen,BlockID,ValueID}`, separate ≥256B | blob-file rewrite compaction + RLE liveness bitmaps + logical→physical id map |
| Postgres | TOAST: large value out-of-line, tuple holds pointer | heap + VACUUM |
| LMDB / MDBX | big value → overflow pages (in-tree) | freelist GC — *this is the pain* |
| TSDB (Influx/Prom) | partition by time, drop whole shard | `unlink` — no GC scan |

Lesson: the expensive part of KV-separation is **value-log GC**. Everyone who
avoids it partitions the value store along the data's natural deletion axis and
drops whole partitions. For Erigon that axis is the **step**. Pebble needs a
rewrite-GC precisely because its blob files span LSM levels rather than a single
deletion unit; per-step partitioning replaces that with an `unlink`.

## Design decisions

- **Value-log (offsets in MDBX), not a deeper LSM rewrite.** MDBX keeps a small
  handle; the bytes live in external per-step `.cvl` files. Frozen `.kv` snapshot
  files are **unchanged** (values stay inline) — the `.cvl` is only an
  MDBX-write-relief buffer for the *un-collated* recent steps, never a permanent
  store, never internally rewritten, unlinked whole at collate.
- **Protocol A: per-step append, no watermark.** One growing `.cvl` per step,
  appended across the step's flushes. The original sketch carried a durable
  "valid length" watermark with truncate-on-recovery; it is **not needed**.
  Reads never scan the file linearly (collate iterates MDBX handles; GetLatest
  resolves a handle), so any bytes past the last committed handle are unreachable
  and reclaimed when the whole file is unlinked at prune. Recovery just reopens
  for append. The one surviving invariant: **`fdatasync` the file before
  committing the MDBX handles that reference the appended bytes** (else a
  power-loss leaves a handle pointing at lost data).
- **Reader-safety reuses the existing file lifecycle.** A sealed `.cvl` is a
  `FilesItem` surfaced into `domainVisible.valFiles`; reclamation rides the
  `aggregatorVisible` generation machinery (see
  `20260525-lockfree-file-reclamation-spec.md`), so "alive until the last reader
  drains" comes for free — no bespoke refcount.
- **`.cvl` files live INSIDE `domainVisible`** (`valFiles map[kv.Step]*FilesItem`),
  not as a new top-level bundle slot — they are per-domain and per-step.
- **pread, not mmap.** A `.cvl` is read while still being appended, so a `FilesItem`
  can be created at the step's *first flush* and read immediately (`ReadAt` of a
  committed offset is always valid as the file grows — no remap, no SIGBUS). There
  is no "active vs sealed" transition.
- **Size threshold (Pebble-style).** Values `< threshold` stay inline in MDBX;
  `≥ threshold` are externalized. Avoids extra IO / a file entry for tiny values.

## On-disk / on-DB format

- **`.cvl` file**: `[magic "CVL\x01"][raw value bytes…]`. No in-file framing — the
  length lives in the handle. Named `{ver}-{base}.{from}-{to}.cvl` in
  `dirs.SnapDomain` (one step: `from=step, to=step+1`).
- **MDBX `TblCommitmentVals` value** (DupSort): `^step(8B) || flag(1B) || rest`
  where `rest` is the inline bytes (`flag=0`) or a `Handle` (`flag=1`). The 8-byte
  `^step` prefix is unchanged, so DupSort `SeekBothRange` / dedup is unaffected.
- **`Handle{Offset uint64, Len uint32}`**, varint-encoded. The step (from the MDBX
  key/value prefix) selects the file; the handle gives position within it.

## Architecture / lifecycle

```
WRITE (per Flush)                          READ (GetLatest / iterators)
  addValue → ETL collector (raw)             getLatestFromDb finds ^step||flag||rest
  Flush load callback:                       decodeVal:
    encode(step, value):                       inline  → return rest
      < threshold → flag=inline||bytes         external→ dt.visible.valFiles[step]
      ≥ threshold → append to .cvl,                       .valReader.Get(handle)   (MVCC-safe)
                    flag=ext||handle                     (fallback: write-side map for
    Put ^step||payload into MDBX                          the current step not yet republished)
  after load: valFiles.sync() (fdatasync)
              valFiles.ensureFilesItems()     COLLATE (step → frozen .kv)
              → publish per-step FilesItem       read handles from MDBX, resolve via .cvl,
                                                  emit a NORMAL inline .kv (byte-identical)

PRUNE (after collate, per step)            UNWIND
  delete ^step||payload rows from MDBX        delete current-step entry
  retire(step) → *FilesItem                   overwrite (delete any existing unwind-step dup,
  AggregatorRoTx.prune collects retired         then Put) the restored value, re-encoded
    → recalcVisibleFiles(retired)               via encode(); sync()+ensureFilesItems() after
    → drain-deleted when last reader leaves

RECOVERY (Domain.OpenList → openFolder)
  scan dirs.SnapDomain for *.cvl:
    step < firstStepNotInFiles → orphan (already collated+pruned) → unlink
    else → reopen as a readable FilesItem
```

Reclamation correctness (per the lock-free spec): the retired `.cvl` `FilesItem`
is attached to the *outgoing* generation's `retired` set and physically removed
(`closeFilesAndRemove`, which closes+unlinks the `valReader`) only once every
reader-generation that could see it has drained.

## Code map

- `db/state/valfile/` — the primitive (no Erigon deps beyond `common`):
  `valfile.go` (`Writer`: `Append`/`Sync`/`Close`/`OpenWriter`; `Reader`: `Get` via
  `ReadAt`), `handle.go` (`Handle` codec), `payload.go`
  (`EncodeInline`/`EncodeExternal`/`DecodePayload`).
- `db/state/domain_valfiles.go` — `domainValFiles` write-side manager: `writers`
  map + `items map[kv.Step]*FilesItem`; `encode`/`sync`/`ensureFilesItems`/
  `visibleValFiles`/`decode`/`getHandle`/`retire`/`openFolder`/`Close`.
- `db/state/dirty_files.go` — `FilesItem.valReader`; `closeFilesAndRemove` closes+unlinks it.
- `db/state/domain.go` — `domainVisible.valFiles`; `Domain.valFiles` (init in
  `NewDomain`, guarded against `LargeValues`); `calcVisibleFiles` populates
  `dv.valFiles`; `DomainRoTx.decodeVal`; `getLatestFromDb`/`Flush`/`collate`/
  `prune`/`unwind`/`OpenList` wiring.
- `db/state/domain_stream.go` — `DomainLatestIterFile.decodeVal` +
  `debugIteratePrefixLatest` DB-value decode sites (used by RPC range/debug).
- `db/state/aggregator.go` — `AggregatorRoTx.prune` routes retired `.cvl` items
  through `recalcVisibleFiles(retired)`.
- `db/state/statecfg/statecfg.go` — `DomainCfg.ValueFileThreshold`.
- `db/state/statecfg/state_schema.go` — `COMMITMENT_VALFILE_THRESHOLD` env
  (`dbg.EnvInt`, default 0 = off).

## Testing approach

The feature is gated by `ValueFileThreshold>0`, so a default test run would never
exercise it. `db/state/valfile_testenable_test.go` flips the entire db/state suite
onto the external path (`init()` sets `ValueFileThreshold=1` on the
`LargeValues:false` schema domains). This caught two real bugs an isolated test
could not:

1. **unwind relied on DupSort *value* ordering.** Repeated unwinds leave multiple
   dups for a key at the same step; the read picks the first, ordered by value
   bytes. Inline that *is* value order; externalized the value is a handle, so the
   order is arbitrary file-offset order → wrong value. Fixed by making unwind
   **overwrite** (delete any existing unwind-step dup before `Put`) — a key has one
   value per step.
2. **A latent test bug** (`TestDomain_Unwind`/`RangeAsOf` passed the *expected*
   domain's tx to the *unwound* domain), masked by inline (value in the MDBX
   record) and exposed by externalization (the handle needs the matching domain's
   `.cvl`). Fixed to use the correct tx.

Tradeoff: with the suite defaulting to externalized, broad **inline** coverage now
comes only from that toggle + `TestValFiles_DisabledIsByteIdenticalPath`. If both
are wanted in CI long-term, make the `init` a 2-value (0/1) env-driven matrix.

## Verification done

- `valfile` unit tests; dedicated `TestValFiles_*` (write→flush→GetLatest→collate,
  sealed/visible/retire, openFolder recovery, disabled path).
- Full `db/state` suite green **externalized** and **inline**; `execution/tests`
  (block exec → state roots) green externalized — bit-for-bit-equivalent roots.
- `make erigon integration` builds; `make lint` 0 issues on the changed packages.

## Remaining work to ENABLE (do atomically with flipping the threshold)

While dormant (threshold 0) the on-disk format is byte-identical to today, so none
of this is needed yet — but all of it must land *together with* enabling the
feature in production:

- **`DBSchemaVersion` bump + `db/migrations/` entry** — the `TblCommitmentVals`
  value format changes and old/new binaries must not silently mix; existing
  datadirs need a migration (rebuild handles+`.cvl`, or refuse cross-version open).
- **`.cvl` extension/version via `cmd/bumper`** + downloader allow-list / `seg
  integrity` / `seg retire` / `rm-state` handling for the new file type. Run the
  `erigon-version-review` skill.
- **Real-chain validation**: `erigon-exec-from-0` with the threshold on, compare
  trie roots; benchmark `chaindata` size and Flush/Collate/Prune on a `>RAM` dataset.

## Known issues to fix at enablement

- **`decodeVal` write-side fallback is not MVCC-safe.** A `.cvl` `FilesItem` is
  published only by the *next* `recalcVisibleFiles` (e.g. at buildFiles), so reads
  of a just-flushed step fall back to the live write-side map; an RPC RoTx whose
  MDBX snapshot predates a prune can `decode(S)` after the item was retired →
  spurious "no reader for step". The robust fix is to publish the `.cvl` into the
  visible bundle at flush time so the fallback is never load-bearing (an earlier
  per-flush republish trigger was removed as redundant — revisit it here).
- **`openFolder` rebuilds the `.cvl` path from the *current* version string**, not
  the on-disk filename; a future `.kv` version bump would make `OpenReader` miss
  the file. Tie to the `.cvl` versioning work.
- **`valfile.Writer` duplicates `seg.RawWordsFile`** (append + bufio + reopen-at-EOF)
  and drops its bufio-pool; consider extending/ wrapping `RawWordsFile` instead.

## Honest assessment of the win

- **Flush**: big win (sequential append vs B-tree page-split; ~V/R less write amp).
- **Collate**: moderate (handle records are dense → far fewer pages faulted; values
  read sequentially from the `.cvl`).
- **Prune**: the DupSort scan still visits **one entry per key per step** —
  entry *count* is unchanged; each entry just shrinks from KBs to ~12B, so
  pages-faulted and freelist churn drop, and value bytes reclaim via `unlink`.
  Prune does **not** become O(1). Profile to confirm the bottleneck is value-bytes,
  not entry-count, before over-investing.
