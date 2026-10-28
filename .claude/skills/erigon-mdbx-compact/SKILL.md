---
name: erigon-mdbx-compact
user-invocable: false
description: Compact an existing MDBX database instance to reclaim free/reclaimable space. Use this when the user wants to shrink mdbx.dat, reclaim MDBX garbage-collected pages, or fix MDBX_MAP_FULL issues.
allowed-tools: Bash, Read, Glob
---

# MDBX Database Compaction

`erigon db compact` rewrites every mdbx database of a datadir without its free
pages, producing smaller files. MDBX reuses free pages instead of returning them
to the filesystem, so `mdbx.dat` never shrinks on its own.

The user provides a **datadir** path. The command finds every database under it
(`chaindata`, `txpool`, `downloader`, `migrations`, `nodes/*`, `caplin/*`, ...),
not only `chaindata`.

## When to Use

- `mdbx_stat -ef` shows a high "Reclaimable" percentage
- The database hit `MDBX_MAP_FULL`
- The user wants to shrink the on-disk size of `mdbx.dat`

## Prerequisites

- **Erigon and rpcdaemon must be stopped.** The command takes the datadir lock
  and opens each database exclusively, so it fails cleanly rather than
  corrupting a database in use. Still confirm with the user first.
- **Free space for a second copy of each database, on the volume that database
  already lives on.** The copy is written to a `<db>/compacting/` subdirectory,
  then moved back over the original.

## Procedure

### 1. Report Size and Check Disk Space

1. Run `du -sh <datadir>/chaindata/mdbx.dat` and report the size to the user.
2. Check free space per database, not once for the datadir: a database may sit
   on its own volume through a symlink or mount, and its copy is staged next to
   it. Run `find <datadir> -name mdbx.dat -not -path '*/compacting/*' -print0 | while IFS= read -r -d '' f; do du -sh "$f"; df -h "$(dirname "$f")" | tail -1; done`
   and compare each database against the free space on its own volume. If any
   has less free space than its own `mdbx.dat`, **abort and tell the user**.

### 2. Diagnose (Optional)

If the user wants to see whether compaction is worthwhile before proceeding:

```bash
make db-tools
./build/bin/mdbx_stat -ef <datadir>/chaindata
```

Look at the **Reclaimable** line to estimate the space savings.

### 3. Run Compaction

```bash
make erigon
./build/bin/erigon db compact --datadir=<datadir>
```

This can take hours for large databases. Run in background.

Each database logs `[compact] compacting` with its label, then
`[compact] compacted` with the before/after sizes. Mode and owner of the
original file are preserved, and the stale `mdbx.lck` is removed.

### 4. Verify

Run `du -sh <datadir>/chaindata/mdbx.dat` and report the new size compared to
the original.

The user can now restart erigon.

## Notes

- A crashed run can leave a `<db>/compacting/` directory behind. The next run
  deletes it, so no cleanup is needed.
- To change `--db.pagesize` you need a copy, not a compaction: the destination
  keeps whatever page size it was created with, and `mdbx_to_mdbx` passes a
  `targetPageSize` of 0, so it inherits the source's. Pre-create the new db with
  `ONLY_CREATE_DB=true erigon --datadir=<new> --chain=<chain> --db.pagesize=8kb`,
  then `integration mdbx_to_mdbx --datadir=<old> --chaindata=<old>/chaindata --chaindata.to=<new>/chaindata`.
  Both `--chaindata` flags name a database directory, not a datadir root, and
  `--datadir` is required even though the copy does not read it — the full
  recipe is in `cmd/integration/Readme.md`.
