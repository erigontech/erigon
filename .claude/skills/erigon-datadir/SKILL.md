---
name: erigon-datadir
user-invocable: false
description: Reference for manipulating Erigon datadirs. Use this when the user wants to perform operations on Erigon data directories.
allowed-tools: Bash, Read, Glob
---

# Erigon Datadir Operations

## Duplicating a Datadir

Given a **source** path and **destination** path, follow this procedure exactly.

### Precondition Checks

All checks must pass before copying. Abort on the first failure.

1. **Source exists:** `test -d <src>` — abort if missing
2. **Source is a valid Erigon datadir:** verify all three exist:
   - `<src>/nodekey` (file)
   - `<src>/snapshots/` (directory)
   - `<src>/chaindata/` (directory)
3. **Destination does not exist:** `test ! -e <dst>` — abort if it already exists
4. **Destination parent is writable:** `test -w "$(dirname <dst>)"` — abort if not
5. **Report source size:** run `du -sh <src>` and **always tell the user** the size before proceeding
6. **Detect APFS:** find the mount device for the destination parent directory from `mount` output. If the mount entry contains `apfs`, use the APFS CoW procedure below; otherwise use the full-copy procedure.

### APFS Procedure (copy-on-write)

Use this when the destination is on an APFS volume.

1. Run `cp -ac <src> <dst>` — this creates a CoW clone that is near-instant and consumes no extra disk space.
2. **No disk space check is needed** for CoW clones.
3. After the copy completes, **delete chaindata from the destination:** `rm -rf <dst>/chaindata`

### Other Filesystems Procedure (full copy)

Use this when the destination is NOT on APFS.

1. **Check disk space:** run `df -h "$(dirname <dst>)"` and compare available space to the source size. If available space is less than source size, **abort and tell the user** — do not proceed.
2. Run `cp -a <src> <dst>` to recursively copy the entire datadir. This is a long-running operation for large datadirs — run in background.

### Post-Copy

After either procedure completes, confirm success by listing the destination contents and comparing against the source (minus `chaindata/` for APFS copies).
