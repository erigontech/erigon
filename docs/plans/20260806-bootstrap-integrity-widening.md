# Plan — bootstrap integrity widening (SIGSEGV-safe redesign)

**Date:** 2026-08-06
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`
**Status:** design; prior attempt (2026-08-04) reverted

## Context

`node/components/storage/commitment_validator.go` at boot runs `extractBootstrapCommitmentAnchors` (line ~479) over every commitment `.kv`, invoking `safeExtractCommitmentRecord` (line ~565) inside a `defer/recover` wrapper and quarantining any file whose extract PANICS via `quarantineCorruptStateFileFamily` (line ~582).

**Coverage gap:** the same defensive scan does NOT run for sibling state-domain `.kv` files (accounts/storage/code/receipt). In the leg P v6 frozen datadir per [[mode-b-deep-compute-tx-pin-2026-08-04]] pre-fix regen produced corrupt `.kv` files in these domains too, and they slipped past bootstrap into runtime state reads.

## Prior attempt (2026-08-04) — reverted

Added `safeIterateDomainFile` that ran `NewDecompressor + Getter.Next` in a `defer/recover` block over every state-domain `.kv` at bootstrap. Unit tests passed; end-to-end on the leg P v6 frozen datadir **crashed erigon at bootstrap with SIGSEGV** in `runtime.memmove` from decompressor bounds violation.

**Root cause of the crash:** Go's `defer/recover` does NOT catch SIGSEGV. Access to unmapped memory during `runtime.memmove` propagates through `runtime.sigpanic` and `runtime.throw` — kills the process regardless of recover. The pre-fix regen wrote btree accessor offsets pointing past the mmap end; `Getter.Next` follows those offsets into a segfault.

The probe function itself was correct for THE class of corruption that produces Go bounds panics (in-bounds-but-invalid data). It fails for the OUT-OF-BOUNDS-offset class the actual datadir had.

## Right approach for a redesign

### Pre-open cross-check (recommended)

**Don't open the decompressor at all** until the accessor's max offset is verified against the .kv's on-disk size:

1. `os.Stat(kvPath).Size()` → get the actual data length.
2. Open the accessor (`.bt` or `.kvi`) lightly — read just its metadata block, extract `MaxOffset` (the highest byte-offset it will ever ask for).
3. If `MaxOffset >= kvSize`, the accessor is broken vs the .kv → mark corrupt without touching the mmap.
4. Only if the cross-check passes, proceed to open the decompressor and iterate.

Cheap, safe, catches the exact class the prior attempt tried to catch. Accessor metadata reads don't dereference offsets — they just READ the header block.

### Implementation sketch

New primitive in `node/components/storage/state_file_probe.go` (previously reverted, would be re-added with the pre-open approach):

```go
// safeProbeDomainFile validates a domain .kv against its BT+existence
// accessors without ever opening the .kv's decompressor. Returns
// ErrDomainFileSegInvalid if the accessor's advertised max-offset
// exceeds the .kv's on-disk size — the exact shape of corruption the
// pre-fix mode-C regen produced.
func safeProbeDomainFile(kvPath, btPath string) error {
    kvSt, err := os.Stat(kvPath)
    if err != nil { return err }
    kvSize := uint64(kvSt.Size())

    // BT header has MaxOffset as a fixed field in its first block.
    // Read only that block, not the whole tree.
    bt, err := btindex.OpenBtreeIndexHeaderOnly(btPath)
    if err != nil { return err }
    defer bt.Close()

    if bt.MaxOffset() >= kvSize {
        return fmt.Errorf("%w: accessor MaxOffset=%d >= kv size=%d",
            integrity.ErrDomainFileSegInvalid, bt.MaxOffset(), kvSize)
    }
    return nil
}
```

Analogous for `.kvi` (hash-map accessor) domains — hash-map has its own metadata block encoding max-offset.

### Wiring

In `Provider` bootstrap (`node/components/storage/provider.go` around line 905, same call site as the existing commitment probe):

```go
if err := probeBootstrapDomainFiles(deps.Inventory, snapDir, logger); err != nil {
    // Log + continue: probe is defense-in-depth, not correctness-critical.
    logger.Warn("[storage] bootstrap domain file probe encountered issues", "err", err)
}
```

`probeBootstrapDomainFiles` iterates `snapshot.AllDomains` except commitment (already covered by existing Phase A), enumerates via `inv.AllDomainFiles(domain)`, resolves accessor path via existing `Domain.kvBtAccessorPathForItem` / `kviAccessorPathForItem` helpers, calls `safeProbeDomainFile`, and on error runs `quarantineCorruptStateFileFamily(snapDir, entry.Name, "seg-accessor-mismatch", logger)` + `inv.RemoveFile(entry.Name)` — same quarantine mechanism the commitment path uses.

## Testing strategy

Unit test in `state_file_probe_test.go`:

- **Valid case**: write a valid .kv via `seg.Compressor`, build .bt via `BuildBtreeIndexWithDecompressor`, assert `safeProbeDomainFile` returns nil.
- **Truncated-.kv case**: build a valid pair, then truncate the .kv by 1 KB. Assert `safeProbeDomainFile` returns `ErrDomainFileSegInvalid` (accessor's max-offset now exceeds kv size).
- **Missing-accessor case**: build valid .kv, don't build the accessor. Assert distinct error (not our new one — a missing accessor is already caught by other machinery).

End-to-end test: seed a datadir with a known-corrupt-pattern .kv (e.g. copy the leg P v6 frozen datadir's post-corruption commitment file), launch erigon, verify:

- No SIGSEGV.
- Corrupt file quarantined + inventory updated.
- Subsequent state reads for the affected range fall through to older files or fail gracefully with a distinct error.

## Constraints

- **No decompressor open on suspect files.** The prior attempt failed exactly because it opened + iterated. This design routes around by cross-checking metadata BEFORE any offset-reading operation.
- **btindex API surface may need an addition**: `OpenBtreeIndexHeaderOnly` (or equivalent that returns just `MaxOffset` without loading the full tree) may not exist. If not, add a minimal helper in `db/datastruct/btindex/`.
- **Race with in-flight downloads.** The probe should run AFTER Aggregator.OpenFolder completes but BEFORE bootstrap declares readiness. Same window the existing commitment probe uses.

## Non-goals

- Auto-repairing corrupt files. Quarantine only. Repair is a subsequent redownload flow ([[prune-gap-redownload-principle-2026-08-02]]).
- Catching corruption that manifests only during runtime state reads (mid-.kv-iteration bounds violations). The pre-open cross-check catches the accessor-advertises-more-than-file class; runtime-only crashes would need per-op guards not addressed here.

## Cross-links

- [[mode-b-deep-compute-tx-pin-2026-08-04]] — first attempt's failure log.
- `node/components/storage/commitment_validator.go` at `extractBootstrapCommitmentAnchors` — the existing pattern to mirror.
- `db/datastruct/btindex/` — where the new header-only opener would land.
- [[prune-gap-redownload-principle-2026-08-02]] — the natural repair mechanism after quarantine.
