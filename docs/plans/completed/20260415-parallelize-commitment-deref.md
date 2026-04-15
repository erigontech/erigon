# Parallelize Commitment Deref Check

## Overview
- Parallelize the inner branch key verification loop in `checkCommitmentKvDeref` using a pipelined producer-consumer pattern
- Problem: the v1.1 commitment file has 682M keys and takes 9+ hours at ~20k keys/sec because branch verification is sequential
- Solution: one sequential producer iterates commitment keys, N workers verify branches in parallel with independent readers

## Context (from discovery)
- Files involved: `db/integrity/commitment_integrity.go` (sole file to modify)
- `checkDerefBranch` already takes readers as parameters — no changes needed
- `Decompressor.MakeGetter()` creates independent readers from shared immutable mmap — safe for concurrent use
- Each `seg.Reader` wraps a `Getter` with mutable position state — one per goroutine required
- Atomic counters already used for cross-file aggregation in the outer function

## Design Decisions (confirmed via brainstorm)
1. **Pipelined architecture**: one sequential producer reads commitment keys, N workers verify branches in parallel
2. **Simple byte copies**: producer copies branchKey/branchValue before sending on channel (no sync.Pool — GC not a concern for integrity check)
3. **Per-worker readers**: each worker gets its own accReader + storageReader via `Decompressor.MakeGetter()`
4. **Worker count**: configurable via `CHECK_COMMITMENT_KVS_DEREF_WORKERS` env var, default 4
5. **Progress reporting**: atomic counter + separate ticker goroutine (replaces inline per-1024-key reporting)
6. **`checkDerefBranch` unchanged**: already takes readers as parameters, works as-is

## Development Approach
- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- run `make lint` after code changes (non-deterministic — run repeatedly until clean)
- run `make erigon integration` to verify build
- run tests after each change

## Solution Overview

```
┌─────────────┐     chan workItem      ┌──────────────┐
│  Producer    │ ──────────────────→   │  Worker 1    │
│  (1 goroutine)│     (buffered)       │  Worker 2    │
│              │                       │  Worker 3    │
│  commReader  │                       │  ...         │
│  .HasNext()  │                       │  Worker N    │
│  .Next()×2   │                       │              │
│              │                       │  Each has own│
│  sends       │                       │  accReader   │
│  {key,value} │                       │  storageReader│
└─────────────┘                       └──────────────┘
                                            │
                                      atomic counters
                                      + mutex merge at end
```

- Producer: single goroutine iterating `commReader` sequentially (Huffman-compressed, no random access)
- Workers: N goroutines, each with own `accReader`/`storageReader` created from shared `Decompressor`
- Channel: buffered (`workers*16` capacity), carries copied `branchKey`/`branchValue` byte slices
- Progress: separate goroutine reads atomic counter on 30s ticker
- Error handling: `failFast` → errgroup with context cancellation; non-failFast → accumulate integrity errors

## Implementation Steps

### Task 1: Add `derefCounts.add()` helper and `deriveDecompForOtherDomain`

**Files:**
- Modify: `db/integrity/commitment_integrity.go`

- [x] add `func (dc *derefCounts) add(other derefCounts)` method that sums all five fields
- [x] add `deriveDecompForOtherDomain(baseFile string, oldDomain, newDomain kv.Domain) (*seg.Decompressor, seg.FileCompression, error)` that returns decompressor + compression config (reuse `derivePathForOtherDomain` for path derivation)
- [x] verify build: `make erigon`

### Task 2: Rewrite `checkCommitmentKvDeref` with producer-consumer pattern

**Files:**
- Modify: `db/integrity/commitment_integrity.go`

- [x] read worker count from env: `dbg.EnvInt("CHECK_COMMITMENT_KVS_DEREF_WORKERS", 4)`
- [x] replace single `accReader`/`storageReader` setup with opening `accDecomp`/`storageDecomp` via `deriveDecompForOtherDomain`
- [x] define `type workItem struct { branchKey, branchValue []byte }` (local to function)
- [x] create buffered channel: `ch := make(chan workItem, workers*16)`
- [x] implement producer goroutine:
  - iterate `commReader.HasNext()` / `.Next()` as before
  - skip `commitmentdb.KeyCommitmentState` keys
  - copy key/value with `append([]byte{}, ...)` before sending on channel
  - handle invalid key/value pairs (odd count) — send error or log depending on failFast
  - close channel when done or on context cancellation
- [x] implement progress reporter goroutine:
  - `var processed atomic.Uint64` shared with workers
  - 30s ticker, logs rate/ETA/percentage using `processed.Load()` and `totalKeys`
  - exits when done channel or context is cancelled
- [x] implement N worker goroutines via `errgroup.Group`:
  - each creates own `accReader` and `storageReader` from shared decompressors via `MakeGetter()`
  - each allocates own scratch buffers (`newBranchValueBuf`, `plainKeyBuf`)
  - `for item := range ch` loop calling `checkDerefBranch` unchanged
  - accumulate local `derefCounts`, increment `processed` atomically per key
  - merge local counts under mutex after loop
  - handle failFast vs non-failFast error accumulation
- [x] after `eg.Wait()`: stop progress reporter, close decompressors, return merged counts + integrity error
- [x] handle producer error: if producer encounters fatal error, propagate via separate error channel or shared variable
- [x] verify build: `make erigon integration`

### Task 3: Verify correctness and lint

**Files:**
- Modify: `db/integrity/commitment_integrity.go` (lint fixes only)

- [x] run `make lint` — fix any issues, run repeatedly until clean
- [x] run `go test ./db/integrity/... -count=1` to verify existing tests pass
- [x] run `make test-short` for broader verification
- [x] verify `CHECK_COMMITMENT_KVS_DEREF_WORKERS=1` produces same behavior as sequential (logical correctness)
- [x] verify `CHECK_COMMITMENT_KVS_DEREF_SEQUENTIAL=true` still works at the outer level (env var in `CheckCommitmentKvDeref`)

### Task 4: Final verification

- [x] verify all requirements from Overview are implemented
- [x] verify edge cases: empty files, files below min steps threshold, single-key files
- [x] run full test suite: `make test-short`
- [x] run `make lint` one final time

## Post-Completion

**Manual verification:**
- Run against actual datadir with large commitment files to measure speedup
- Compare output (counts) between `CHECK_COMMITMENT_KVS_DEREF_WORKERS=1` and default to verify identical results
- Monitor memory usage with different worker counts (4 vs 8) to ensure no excessive allocation
