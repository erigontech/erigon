# Collapse the caplin hot sidecar stores onto one bucket store

Issue: #23413. Epic: #23024, item 4.

## Overview

`BlobStore` and `dataColumnStorageImpl` are the same object — an afero store bucketed by `slot / subdivisionSlot` — with path building and bucket pruning copy-pasted between them. Extract the shared half, fix the two latent bugs the duplication hides, and replace the prune walk.

Three concrete problems this closes:

1. Both `Prune` implementations walk every 10,000-slot bucket from slot 0 on every call, and `cleanupAndPruning` runs once per slot. That is `(head-keep)/10_000` `RemoveAll` syscalls every 12 s on mainnet and every 5 s on gnosis, against directories the first pass already removed.
2. `data_column_db.go:98-107` — the cleanup-on-error defer never fires, because `if err := ssz_snappy.EncodeAndWrite(...)` shadows the outer `err`. A failed encode leaves a truncated file, and the `Stat` guard at `:94` then treats it as complete forever.
3. `blob_db.go:238` — `RemoveBlobSidecars` returns on the first missing file and leaves the MDBX count row behind, so a half-removed set is permanently inconsistent.

It also moves the retention distance out of the stores and into the caller, which is the attachment point the freeze gate needs later.

## Context (from discovery)

- `cl/persistence/blob_storage/blob_db.go` — 344 lines, `BlobStorage` interface at `:49`, `blobSidecarFilePath` at `:71`, `Prune` at `:165`
- `cl/persistence/blob_storage/data_column_db.go` — 230 lines, `DataColumnStorage` interface at `:22`, `dataColumnFilePath` at `:62`, `Prune` at `:214`
- `cl/phase1/stages/cleanup_and_pruning.go` — the caller, already holds `ethClock` and `caplinConfig`
- `cl/das/peer_das.go:332` — `PeerDas.Prune` wrapper
- `cmd/caplin/caplin1/run.go:131,292-297,417` — construction and `pruneBlobDistance`

Byte-identical between the two files: the path shape `<slot/10000>/<root>_<idx>` (same format string), `MkdirAll → Create → EncodeAndWrite → Sync`, `Open → DecodeAndReadNoForkDigest`, `Stat`-as-exists, `Open → io.Copy`, and the prune walk.

Genuinely different, and staying in the façades: the "how many" index (blobs use `kv.BlockRootToKzgCommitments`, columns enumerate), the write-side event emit, and the decode version (blobs hardcode `DenebVersion`, columns use `GetCurrentStateVersion`).

Verified before planning: no caller lists a directory or discovers a file it did not already name; nothing needs "all roots at slot N" or "all slots for root R"; `RemoveAllColumnSidecars` has no production callers.

## Decisions

- **Shape** — extract a `bucketStore`, keep both façades. Not one type with a kind descriptor: the MDBX index and the column enumeration do not fit one, and the families are diverging, not converging.
- **Locking** — per-slot stripes plus a store-level `RWMutex`; per-file ops take `mu.RLock()` then their stripe, `pruneBelow` takes `mu.Lock()` and no stripe. Strictly one direction.
- **Write** — temp file then rename. Fixes problem 2 structurally rather than by unshadowing.
- **Durability** — unchanged: file `fsync`, no directory `fsync`.
- **`read`** — returns `(found, err)`; each façade maps it, because blobs want absence to mean "reschedule" and columns want an error.
- **`remove`** — tolerates ENOENT. Behaviour change, fixes problem 3.
- **Prune floor** — absolute slot, not a distance. `PruneBelow(slot uint64)` everywhere; `slotsKept` leaves both constructors.
- **Prune walk** — readdir the store root and remove buckets below the cutoff. No persisted low-water state.
- **Prune datum** — blocks keep cutting against `args.seenSlot`, blobs against the wall clock. Preserved and commented, not unified.
- **Flag semantics** — untouched. `ColumnKeepSlots == 0` still resolves to the spec window, even though 0 now reads as "keep nothing". That is #23411.
- **`RemoveAllColumnSidecars`** — deleted; no production callers.
- **Write rule** — OPEN, not ruled. Overwrite always vs skip when the target exists. Leaning overwrite: last-write-wins self-heals and makes a `capcli` re-import a repair path, at the cost of a redundant encode and fsync per duplicate. Task 3 implements overwrite unless this is ruled the other way first.

## Out of scope

No freeze gate, no flag changes, no cold expiry. Layout is #23426, the column custody bitmap #23433, the index-row leak #23432.

## Development Approach

- TDD, red before green, per the repo rule. No `t.Skip` outside the canonical `testing.Short()` guard.
- Every task leaves the tree building and the full package green before the next starts.
- Mutation-check each guard by copying the file to a scratch directory, mutating, running, and copying back. Never `git checkout --` a file holding uncommitted work.
- Comments only where a reader would otherwise guess wrong. No narration of the diff.
- Run `go test ./cl/persistence/blob_storage/... ./cl/phase1/stages/... ./cl/das/...` after each task, and `make lint` before the final task.

## Implementation Steps

### Task 1: Counting afero.Fs test helper

**Files:**
- Create: `cl/persistence/blob_storage/counting_fs_test.go`

- [ ] wrap `afero.Fs` recording per-method call counts for `RemoveAll`, `Stat`, `Create`, `Rename`, `Open`
- [ ] expose a reset and a snapshot accessor so a test can assert an exact count
- [ ] write a test proving the wrapper counts what it claims, over `afero.NewMemMapFs()`
- [ ] run `go test ./cl/persistence/blob_storage/...` — must pass before task 2

### Task 2: bucketStore with path, init and pruneBelow

**Files:**
- Create: `cl/persistence/blob_storage/bucket_store.go`
- Create: `cl/persistence/blob_storage/bucket_store_test.go`

- [ ] move `subdivisionSlot` and `rwLocksCount` into the new file
- [ ] add `bucketStore{fs, mu, locks}` with an `init(fs)` method on the pointer receiver, so no value carrying a mutex is ever copied
- [ ] add `path(slot, root, idx) (dir, file string)` reproducing today's `<slot/subdivisionSlot>/<root>_<idx>` exactly
- [ ] add `pruneBelow(slot uint64)`: readdir the root with no lock, collect directories whose numeric name is below `slot / subdivisionSlot`, return early when none, otherwise take `mu.Lock()` and `RemoveAll` each
- [ ] write tests: removes only buckets strictly below the cutoff; `pruneBelow(0)` is a no-op; non-numeric entries survive; idempotent on a second call
- [ ] write the count test: `RemoveAll` is called exactly once per existing expiring bucket, using the task 1 helper
- [ ] mutation-check each: `<` to `<=`, drop the early return, drop the `ParseUint` skip, and revert `pruneBelow` to `for i := uint64(0); i < currentSlot; i += subdivisionSlot`
- [ ] run tests — must pass before task 3

### Task 3: bucketStore write, read, exists, remove, stream

**Files:**
- Modify: `cl/persistence/blob_storage/bucket_store.go`
- Modify: `cl/persistence/blob_storage/bucket_store_test.go`

- [ ] add `slotLock(slot) *sync.RWMutex` returning the stripe for that slot
- [ ] add `write(slot, root, idx, v ssz.Marshaler)`: `mu.RLock`, stripe `Lock`, `MkdirAll`, create `<file>.tmp`, `ssz_snappy.EncodeAndWrite`, `Sync`, `Close`, `Rename` onto the target; remove the temp on any error
- [ ] add `read(slot, root, idx, out ssz.EncodableSSZ, v clparams.StateVersion) (found bool, err error)` mapping a missing file to `found == false` and leaving the not-found convention to callers
- [ ] add `exists`, `remove` (ENOENT tolerated, returns nil) and `stream(w io.Writer, ...)`, each taking `mu.RLock` plus its stripe
- [ ] comment on `stream` why the payload lives on the filesystem — it is copied straight to a network writer with no transaction open, which is the reason this is not in MDBX
- [ ] write tests: a failing encoder leaves nothing at the target path and no stray temp; `remove` of a missing file returns nil; round-trip write/read; `stream` reproduces the written bytes
- [ ] mutation-check: revert `write` to writing in place, and `remove` to the erroring form
- [ ] run tests — must pass before task 4

### Task 4: BlobStore onto bucketStore

**Files:**
- Modify: `cl/persistence/blob_storage/blob_db.go`
- Modify: `cl/persistence/blob_storage/blob_db_test.go`

- [ ] embed `bucketStore` in `BlobStore`, call `init(fs)` from `NewBlobStore`, drop the local path helper
- [ ] route `WriteBlobSidecars`, `ReadBlobSidecars`, `BlobSidecarExists`, `WriteStream` and `RemoveBlobSidecars` through the shared methods, keeping the MDBX count row where it is and still written after the files
- [ ] keep `Prune()` and the `slotsKept` field for now, implemented as `pruneBelow(floorFor(head, slotsKept))`, so this task changes no signature
- [ ] add a test that `RemoveBlobSidecars` succeeds when a file is already gone and still deletes the count row
- [ ] run `go test ./cl/persistence/blob_storage/...` — must pass before task 5

### Task 5: dataColumnStorageImpl onto bucketStore

**Files:**
- Modify: `cl/persistence/blob_storage/data_column_db.go`
- Modify: `cl/persistence/blob_storage/data_column_db_test.go`
- Modify: `cl/persistence/blob_storage/mock_services/data_column_storage_mock.go`

- [ ] embed `bucketStore`, call `init(fs)` from `NewDataColumnStore`, drop the local path helper and the local stripe array
- [ ] route every per-file method through the shared ones, keeping the event emit and the version-aware decode in the façade
- [ ] delete `RemoveAllColumnSidecars` from the interface and the implementation, and its test
- [ ] keep `Prune(keepSlotDistance)` for now, implemented via `pruneBelow`, so this task changes no signature
- [ ] regenerate the mock with `go generate ./cl/persistence/blob_storage/...`
- [ ] run `go test ./cl/persistence/blob_storage/... ./cl/das/...` — must pass before task 6

### Task 6: PruneBelow signature and the floor moving to the caller

**Files:**
- Modify: `cl/persistence/blob_storage/blob_db.go`
- Modify: `cl/persistence/blob_storage/data_column_db.go`
- Modify: `cl/das/peer_das.go`
- Modify: `cl/phase1/stages/cleanup_and_pruning.go`
- Modify: `cmd/caplin/caplin1/run.go`
- Modify: `cl/persistence/blob_storage/mock_services/blob_storage_mock.go`
- Modify: `cl/persistence/blob_storage/mock_services/data_column_storage_mock.go`
- Modify: `cl/das/mock_services/peer_das_mock.go`
- Modify: `cl/persistence/blob_storage/data_column_db_test.go`

- [ ] replace `Prune()` and `Prune(keepSlotDistance)` with `PruneBelow(slot uint64)` on `BlobStorage`, `DataColumnStorage` and `PeerDas`
- [ ] drop `slotsKept` from both constructors and from `run.go`, and move the `pruneBlobDistance` expression into `cleanupAndPruning`
- [ ] add `floorFor(head, keep uint64) uint64` returning 0 when `head <= keep`, so a `MaxUint64` keep falls out as a no-op with no special case
- [ ] resolve `ColumnKeepSlots == 0` to the spec window exactly as `cleanup_and_pruning.go:30-33` does today, unchanged
- [ ] comment why blocks cut against `args.seenSlot` while blobs cut against the wall clock, and that the freeze gate replaces both
- [ ] update the three `Prune(N)` calls in `data_column_db_test.go` and the ~12 constructor calls across the tree that pass a distance
- [ ] regenerate all three mocks
- [ ] run `go test ./cl/... ./cmd/caplin/...` — must pass before task 7

### Task 7: Concurrency guards

**Files:**
- Modify: `cl/persistence/blob_storage/bucket_store_test.go`

- [ ] write a `-race` test driving concurrent `write` and `read` against one slot through the blob façade
- [ ] write a `-race` test driving `pruneBelow` against concurrent writes into a surviving bucket
- [ ] mutation-check: drop the stripe from the write path, then drop `mu` from `pruneBelow` — each must report a race
- [ ] run `go test -race ./cl/persistence/blob_storage/...` — must pass before task 8

### Task 8: Verify acceptance criteria

- [ ] confirm the three problems in the Overview are each pinned by a test that fails when reverted
- [ ] confirm no public interface changed beyond `Prune` becoming `PruneBelow` and `RemoveAllColumnSidecars` being deleted
- [ ] confirm no flag semantics changed: same retention for the same flags on every network
- [ ] run `go test ./cl/... ./cmd/caplin/...` and `go test -race ./cl/persistence/blob_storage/...`
- [ ] run `make lint`

### Task 9: Documentation and close-out

- [ ] confirm no comment in the diff narrates the change, references an issue number, or restates a signature
- [ ] update `docs/plans/` progress marks
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

Manual verification: run a node with a populated blob store and confirm the per-slot prune no longer scales with head — the count test proves the syscall count, but not the wall-clock effect on a real datadir.

Follow-ups this unblocks: #23426 becomes a change to `bucketStore.path` plus the pruner; #23433 puts its read-modify-write inside the shared write path.
