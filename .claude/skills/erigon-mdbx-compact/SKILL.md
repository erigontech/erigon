---
name: erigon-mdbx-compact
user-invocable: false
description: Compact an existing MDBX database instance to reclaim free/reclaimable space. Use this when the user wants to shrink mdbx.dat, reclaim MDBX garbage-collected pages, or fix MDBX_MAP_FULL issues.
allowed-tools: Bash, Read, Glob
---

# MDBX Database Compaction

Compacts an MDBX database file by copying it without garbage-collected pages, producing a smaller file.

The user provides a **datadir** path. The database being compacted is always `<datadir>/chaindata/mdbx.dat`.

## When to Use

- `mdbx_stat -ef` shows a high "Reclaimable" percentage
- The database hit `MDBX_MAP_FULL`
- The user wants to shrink the on-disk size of `mdbx.dat`

## Prerequisites

- **Erigon and rpcdaemon must be stopped.** The database must not be in use. Always confirm this with the user before proceeding.
- **~2x free disk space** relative to the current `mdbx.dat` size — the compacted copy is written alongside the original.

## Procedure

### 1. Validate

Verify that `<datadir>/chaindata/mdbx.dat` exists. Abort if it does not.

### 2. Report Size and Check Disk Space

1. Run `du -sh <datadir>/chaindata/mdbx.dat` and report the size to the user.
2. Run `df -h <datadir>/chaindata` and check available space. If available space is less than the `mdbx.dat` size, **abort and tell the user** there is not enough disk space.

### 3. Diagnose (Optional)

If the user wants to see whether compaction is worthwhile before proceeding:

```bash
make db-tools
./build/bin/mdbx_stat -ef <datadir>/chaindata
```

Look at the **Reclaimable** line in the output to estimate space savings.

### 4. Build `mdbx_copy`

```bash
make db-tools
```

This places `mdbx_copy` at `./build/bin/mdbx_copy`.

### 5. Run Compaction

Create a sibling directory for the compacted output. The destination argument to `mdbx_copy` must be a **file path**, not a directory.

```bash
mkdir -p <datadir>/chaindata-compacted
./build/bin/mdbx_copy -c <datadir>/chaindata <datadir>/chaindata-compacted/mdbx.dat
```

- Source is the **directory** `<datadir>/chaindata`
- Destination is a **file** `<datadir>/chaindata-compacted/mdbx.dat`

This can take hours to days for large databases. Run in background.

### 6. Replace the Original

After `mdbx_copy` completes successfully:

1. Delete the old files:
   ```bash
   rm <datadir>/chaindata/mdbx.dat <datadir>/chaindata/mdbx.lck
   ```
2. Move the compacted file in:
   ```bash
   mv <datadir>/chaindata-compacted/mdbx.dat <datadir>/chaindata/mdbx.dat
   ```
3. Clean up:
   ```bash
   rmdir <datadir>/chaindata-compacted
   ```

### 7. Verify

Run `du -sh <datadir>/chaindata/mdbx.dat` and report the new size compared to the original.

The user can now restart erigon.
