# Collapse the caplin hot sidecar stores onto one bucket store

Issue: #23413. Epic: #23024, item 4.

## Overview

`BlobStore` and `dataColumnStorageImpl` are the same object — an afero store bucketed by `slot / subdivisionSlot`, path shape `<slot/10000>/<blockRoot>_<index>` — with path building and bucket pruning copy-pasted between them. This extracts the shared half into a `bucketStore`, fixes two latent bugs the duplication hides, and replaces the prune walk.

Three problems it closes, all verified against `origin/main`:

1. **The prune walk scales with head.** Both `Prune` implementations walk every 10,000-slot bucket from slot 0 on every call (`blob_db.go:178-180`, `data_column_db.go:223-228`), and `cleanupAndPruning` runs once per slot — its stage transitions straight to `SleepForSlot` (`clstages.go:350-359`). That is `(head-keep)/10_000` `RemoveAll` syscalls every 12 s on mainnet and every 5 s on gnosis, against directories the first pass already removed.
2. **A failed column write leaves a truncated file forever.** At `data_column_db.go:98-107` the cleanup-on-error defer never fires, because `if err := ssz_snappy.EncodeAndWrite(...)` shadows the outer `err`. The `Stat` guard at `:94` then treats the truncated file as complete and never rewrites it.
3. **Blob removal is not idempotent.** `RemoveBlobSidecars` returns on the first missing file (`blob_db.go:238`) and leaves the MDBX count row behind, so a half-removed set stays permanently inconsistent.

It also moves the retention distance out of the stores and into the caller, which is the attachment point the freeze-gate work needs later: one call site computing `min(floor, frozenTo)` instead of a clamp bolted into three pruners.

## Context (from discovery)

Files and components involved:

- `cl/persistence/blob_storage/blob_db.go` — 344 lines. `BlobStorage` interface `:49`, `blobSidecarFilePath` `:71`, `Prune` `:165`, `RemoveBlobSidecars` `:222`
- `cl/persistence/blob_storage/data_column_db.go` — 230 lines. `DataColumnStorage` interface `:22`, `dataColumnFilePath` `:62`, `WriteColumnSidecars` `:69`, `RemoveAllColumnSidecars` `:154`, `Prune` `:214`
- `cl/das/peer_das.go:332` — the `PeerDas.Prune` wrapper
- `cl/phase1/stages/cleanup_and_pruning.go` — the caller; already holds `ethClock` and `caplinConfig`
- `cmd/caplin/caplin1/run.go:131,292-297,417` — construction sites and the `pruneBlobDistance` expression
- `cl/persistence/blob_storage/mock_services/`, `cl/das/mock_services/` — generated mocks that follow every interface change

Patterns found — byte-identical between the two files: the path shape (same format string), `MkdirAll → Create → EncodeAndWrite → Sync`, `Open → DecodeAndReadNoForkDigest`, `Stat`-as-exists, `Open → io.Copy`, and the prune walk.

Genuinely different, and staying in the façades: the "how many" index (blobs read `kv.BlockRootToKzgCommitments`, columns enumerate `0..NumberOfColumns`), the write-side event emit, and the decode version (blobs hardcode `DenebVersion`, columns use `GetCurrentStateVersion`).

Dependencies identified — a caller survey established that no consumer constrains the layout: nothing lists a directory or discovers a file it did not already name, nothing needs "all roots at slot N" or "all slots for root R", and `RemoveAllColumnSidecars` has no production callers.

## Development Approach

- **Testing approach: TDD**, red before green, as the repo `CLAUDE.md` mandates. No `t.Skip` outside the canonical `testing.Short()` guard.
- Complete each task fully before moving to the next; make small, focused changes.
- **Every task includes new or updated tests** for the code it touches, listed as separate checklist items — success and error cases both.
- **All tests pass before the next task starts.** No exceptions. Ralphex runs these unattended, so every task must leave the tree building and the package green.
- **Update this plan when scope changes during implementation** — `[x]` on completion, `➕` for newly discovered tasks, `⚠️` for blockers.
- Backward compatibility: public interfaces change only where the Decisions section says so.
- Conventions: comments only where a reader would otherwise guess wrong — never narrating the diff, never citing an issue number. New files carry 2026 in the license header. Go naming with no `Factory`/`Provider`/`Manager`.

## Testing Strategy

- **Unit tests**: required for every task, per Development Approach.
- **Race tests**: `go test -race ./cl/persistence/blob_storage/...` — the store gains locking blobs do not have today, so the guards have to be real.
- **Mutation checks**: each guard is verified by reverting the code it protects and confirming the test goes red. Copy the file to a scratch directory, mutate, run, copy back — never `git checkout --` a file holding uncommitted work.
- **No e2e tests**: this project has no UI-based e2e suite, and this change has no user-facing surface.
- Commands: `go test ./cl/persistence/blob_storage/... ./cl/phase1/stages/... ./cl/das/...`, then `go test -race ./cl/persistence/blob_storage/...`, then `make lint`.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with `➕` prefix
- document issues and blockers with `⚠️` prefix
- update the plan if implementation deviates from the original scope
- keep the plan in sync with the work actually done

## Solution Overview

One shared type, two façades. `bucketStore` owns everything that is a function of the path — bucket derivation, write, read, exists, remove, stream, prune — and nothing that is a function of what is being stored. Both existing types embed it by value and keep their own public interface, so no consumer changes.

Rejected: a single type parameterised by a kind descriptor. The blob MDBX index and the column enumeration do not fit one descriptor without nilable hooks, and the two families are diverging — columns toward DAS custody, blobs toward a freeze path — not converging.

The second design decision is that the store holds no policy. `PruneBelow` takes an absolute slot rather than a distance, and `slotsKept` leaves both constructors. The caller computes the floor. That keeps the store dumb, puts every retention decision in one function, and deletes the dead `slotsKept` field behind #23410 as a side effect.

The prune walk becomes a readdir. The directory is the state, so there is nothing to persist and nothing to get wrong across a restart or a moved datadir, and only buckets that exist are touched.

## Technical Details

New type, `cl/persistence/blob_storage/bucket_store.go`:

```go
type bucketStore struct {
    fs    afero.Fs
    mu    sync.RWMutex    // pruneBelow against everything else
    locks []sync.RWMutex  // per-slot stripes, rwLocksCount
}

func (b *bucketStore) init(fs afero.Fs)
func (b *bucketStore) path(slot uint64, root common.Hash, idx uint64) (dir, file string)
func (b *bucketStore) slotLock(slot uint64) *sync.RWMutex
func (b *bucketStore) write(slot uint64, root common.Hash, idx uint64, v ssz.Marshaler) error
func (b *bucketStore) read(slot uint64, root common.Hash, idx uint64, out ssz.EncodableSSZ, v clparams.StateVersion) (found bool, err error)
func (b *bucketStore) exists(slot uint64, root common.Hash, idx uint64) (bool, error)
func (b *bucketStore) remove(slot uint64, root common.Hash, idx uint64) error
func (b *bucketStore) stream(w io.Writer, slot uint64, root common.Hash, idx uint64) error
func (b *bucketStore) pruneBelow(slot uint64) error
```

`init` is a method on the pointer receiver rather than a constructor returning a value, because a value carrying a `sync.RWMutex` trips `go vet`'s copylocks on assignment.

**Lock order, strictly one direction.** Per-file operations take `mu.RLock()` then their slot's stripe. `pruneBelow` takes `mu.Lock()` and no stripe. Nothing takes a stripe before `mu`, so no deadlock is constructible.

**Write path.** `mu.RLock`, stripe `Lock`, `MkdirAll(dir)`, create `<file>.tmp`, `ssz_snappy.EncodeAndWrite`, `Sync`, `Close`, `Rename(tmp, file)`. Any error removes the temp. A file at the target path is therefore complete by construction, which is what fixes problem 2 — unshadowing the `err` would only narrow the window. The temp name needs no randomness: the stripe lock excludes the only writer that could target the same path. Durability is unchanged — file `fsync`, no directory `fsync`. `afero.MemMapFs` rename-over-existing was verified to replace the target and remove the source, so tests need no special case.

**Prune path.** `cutoff := slot / subdivisionSlot`; readdir the store root with no lock held; collect directories whose numeric name is below the cutoff; return early when none, so the common tick takes no lock at all; otherwise `mu.Lock()` and `RemoveAll` each. Non-numeric entries are skipped rather than erroring. `pruneBelow(0)` is a no-op, which retires the `slotsKept == MaxUint64` sentinel.

**Caller.** `cleanupAndPruning` gains:

```go
func floorFor(head, keep uint64) uint64 {
    if head <= keep { return 0 }
    return head - keep
}
```

A `MaxUint64` keep falls out as a no-op with no special case. The `pruneBlobDistance` expression moves here from `run.go:292`, making this the single place all three CL hot floors are computed.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): everything achievable in this repo — the new type, both façades, the interface change, the mocks, the tests.
- **Post-Completion** (no checkboxes): the wall-clock effect on a real populated datadir, which the syscall-count test cannot show, and the follow-up issues this unblocks.

## Decisions

- **Shape** — extract a `bucketStore`, keep both façades. Not one type with a kind descriptor.
- **Locking** — per-slot stripes plus a store-level `RWMutex`; per-file ops take `mu.RLock()` then their stripe, `pruneBelow` takes `mu.Lock()` and no stripe.
- **Construction** — `init(fs)` on the pointer receiver, to avoid a copylocks vet failure.
- **Write** — temp file then rename; fixes problem 2 structurally.
- **Durability** — unchanged: file `fsync`, no directory `fsync`.
- **`read`** — returns `(found, err)`; each façade maps it, because blobs want absence to mean "reschedule" and columns want an error.
- **`remove`** — tolerates ENOENT. Behaviour change, fixes problem 3.
- **Prune floor** — absolute slot, not a distance; `slotsKept` leaves both constructors.
- **Prune walk** — readdir the store root; no persisted low-water state.
- **Prune datum** — blocks keep cutting against `args.seenSlot`, blobs against the wall clock. Preserved and commented, not unified.
- **Flag semantics** — untouched; `ColumnKeepSlots == 0` still resolves to the spec window. Fixing that reading is #23411.
- **`RemoveAllColumnSidecars`** — deleted; no production callers.
- **Write rule** — OPEN, not ruled: overwrite always vs skip when the target exists. Leaning overwrite (last-write-wins self-heals, and a `capcli` re-import becomes a repair path) at the cost of a redundant encode and fsync per duplicate. Task 3 implements overwrite unless this is ruled the other way first.

## Out of Scope

No freeze gate, no flag changes, no cold expiry. The directory layout is #23426, the column custody bitmap #23433, the index-row leak #23432.

## Implementation Steps

### Task 1: Counting afero.Fs test helper

**Files:**
- Create: `cl/persistence/blob_storage/counting_fs_test.go`

- [ ] wrap `afero.Fs` recording per-method call counts for `RemoveAll`, `Stat`, `Create`, `Rename` and `Open`
- [ ] expose a reset and a snapshot accessor so a test can assert an exact count
- [ ] write tests proving the wrapper counts each recorded method correctly over `afero.NewMemMapFs()`
- [ ] write a test proving unrecorded methods still delegate to the wrapped filesystem
- [ ] run `go test ./cl/persistence/blob_storage/...` — must pass before task 2

### Task 2: bucketStore with path, init and pruneBelow

**Files:**
- Create: `cl/persistence/blob_storage/bucket_store.go`
- Create: `cl/persistence/blob_storage/bucket_store_test.go`

- [ ] move `subdivisionSlot` and `rwLocksCount` into the new file from `blob_db.go:44-46` and `data_column_db.go:44`
- [ ] add `bucketStore{fs, mu, locks}` with `init(fs)` on the pointer receiver, so no value carrying a mutex is copied
- [ ] add `path(slot, root, idx)` reproducing today's `<slot/subdivisionSlot>/<root>_<idx>` exactly
- [ ] add `pruneBelow(slot)`: readdir the root unlocked, collect directories numerically below `slot / subdivisionSlot`, return early when none, otherwise `mu.Lock()` and `RemoveAll` each
- [ ] write tests for `path` pinning the exact strings both existing helpers produce today
- [ ] write tests for `pruneBelow` success cases: removes only buckets strictly below the cutoff, and is idempotent on a second call
- [ ] write tests for `pruneBelow` edge cases: `pruneBelow(0)` is a no-op, non-numeric entries survive, a readdir error propagates
- [ ] write the syscall-count test: `RemoveAll` is called exactly once per existing expiring bucket, using the task 1 helper
- [ ] mutation-check each guard: `<` to `<=`, drop the early return, drop the `ParseUint` skip, and revert `pruneBelow` to `for i := uint64(0); i < currentSlot; i += subdivisionSlot`
- [ ] run tests — must pass before task 3

### Task 3: bucketStore write, read, exists, remove and stream

**Files:**
- Modify: `cl/persistence/blob_storage/bucket_store.go`
- Modify: `cl/persistence/blob_storage/bucket_store_test.go`

- [ ] add `slotLock(slot)` returning the stripe for that slot
- [ ] add `write`: `mu.RLock`, stripe `Lock`, `MkdirAll`, create `<file>.tmp`, `EncodeAndWrite`, `Sync`, `Close`, `Rename` onto the target, removing the temp on any error
- [ ] add `read` returning `(found, err)`, mapping a missing file to `found == false` and leaving the not-found convention to callers
- [ ] add `exists`, `remove` tolerating ENOENT, and `stream`, each taking `mu.RLock` plus its stripe
- [ ] comment on `stream` why the payload lives on the filesystem — it is copied straight to a network writer with no transaction open
- [ ] write success tests: write/read round trip, `exists` after a write, `stream` reproduces the written bytes, `remove` then `exists` is false
- [ ] write error tests: a failing encoder leaves nothing at the target path and no stray temp, `remove` of a missing file returns nil, `read` of a missing file returns `found == false` with no error
- [ ] mutation-check: revert `write` to writing in place, and `remove` to the erroring form
- [ ] run tests — must pass before task 4

### Task 4: BlobStore onto bucketStore

**Files:**
- Modify: `cl/persistence/blob_storage/blob_db.go`
- Modify: `cl/persistence/blob_storage/blob_db_test.go`

- [ ] embed `bucketStore` in `BlobStore`, call `init(fs)` from `NewBlobStore`, delete `blobSidecarFilePath`
- [ ] route `WriteBlobSidecars`, `ReadBlobSidecars`, `BlobSidecarExists`, `WriteStream` and `RemoveBlobSidecars` through the shared methods, keeping the MDBX count row where it is and still written after the files
- [ ] keep `Prune()` and the `slotsKept` field, now implemented as `pruneBelow(floorFor(head, slotsKept))`, so this task changes no signature
- [ ] write a test that `RemoveBlobSidecars` succeeds when a file is already gone and still deletes the count row
- [ ] write a test that a partially written sidecar set is never observable through `ReadBlobSidecars`
- [ ] run `go test ./cl/persistence/blob_storage/...` — must pass before task 5

### Task 5: dataColumnStorageImpl onto bucketStore

**Files:**
- Modify: `cl/persistence/blob_storage/data_column_db.go`
- Modify: `cl/persistence/blob_storage/data_column_db_test.go`
- Modify: `cl/persistence/blob_storage/mock_services/data_column_storage_mock.go`

- [ ] embed `bucketStore`, call `init(fs)` from `NewDataColumnStore`, delete `dataColumnFilePath`, the local `rwLocks` array and `acquireLock`
- [ ] route every per-file method through the shared ones, keeping the event emit and the version-aware decode in the façade
- [ ] delete `RemoveAllColumnSidecars` from the interface, the implementation and `TestRemoveAllColumnSidecars`
- [ ] keep `Prune(keepSlotDistance)` implemented via `pruneBelow`, so this task changes no signature
- [ ] regenerate the mock with `go generate ./cl/persistence/blob_storage/...`
- [ ] write a test that a failed column write leaves no file that a later write would skip over
- [ ] write a test that `GetSavedColumnIndex` still reports exactly the written indices
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
- [ ] add `floorFor(head, keep uint64) uint64` returning 0 when `head <= keep`
- [ ] resolve `ColumnKeepSlots == 0` to the spec window exactly as `cleanup_and_pruning.go:30-33` does today, unchanged
- [ ] comment why blocks cut against `args.seenSlot` while blobs cut against the wall clock, and that the freeze gate replaces both
- [ ] update the three `Prune(N)` calls at `data_column_db_test.go:345,360,374` and every constructor call across the tree that passes a distance
- [ ] regenerate all three mocks
- [ ] write tests for `floorFor`: `head <= keep` gives 0, a `MaxUint64` keep gives 0, a normal window gives `head - keep`
- [ ] write a test that `PruneBelow(0)` removes nothing
- [ ] run `go test ./cl/... ./cmd/caplin/...` — must pass before task 7

### Task 7: Concurrency guards

**Files:**
- Modify: `cl/persistence/blob_storage/bucket_store_test.go`

- [ ] write a `-race` test driving concurrent `write` and `read` against one slot
- [ ] write a `-race` test driving `pruneBelow` against concurrent writes into a surviving bucket
- [ ] write a test that a write into an expiring bucket during prune leaves the store consistent, whichever order wins
- [ ] mutation-check: drop the stripe from the write path, then drop `mu` from `pruneBelow` — each must report a race
- [ ] run `go test -race ./cl/persistence/blob_storage/...` — must pass before task 8

### Task 8: Verify acceptance criteria

- [ ] verify each of the three problems in the Overview is pinned by a test that fails when the fix is reverted
- [ ] verify no public interface changed beyond `Prune` becoming `PruneBelow` and `RemoveAllColumnSidecars` being deleted
- [ ] verify no flag semantics changed — same retention for the same flags on every network, `ColumnKeepSlots == 0` included
- [ ] verify edge cases are handled: empty store, store with only non-numeric entries, `PruneBelow` above every bucket
- [ ] run the full suite: `go test ./cl/persistence/blob_storage/... ./cl/phase1/stages/... ./cl/das/... ./cmd/caplin/...`
- [ ] run `go test -race ./cl/persistence/blob_storage/...`
- [ ] run `make lint`

### Task 9: [Final] Update documentation

- [ ] verify no comment in the diff narrates the change, cites an issue number, or restates a signature
- [ ] verify `bucket_store.go` carries 2026 in its license header
- [ ] update `CLAUDE.md` only if a new pattern was established that future work should follow
- [ ] mark every checkbox in this plan and record any `➕` or `⚠️` entries that arose
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only.*

**Manual verification:**

- Run a node with a populated blob store and confirm the per-slot prune no longer scales with head. The syscall-count test proves the call count; it cannot show the wall-clock effect on a real datadir with hundreds of thousands of files.
- Confirm on a Fulu node that column write throughput is unchanged, since columns gain a second lock acquisition per file operation.

**Follow-ups this unblocks:**

- #23426 — the per-slot leaf directory becomes a change to `bucketStore.path` plus the pruner, rather than the same edit in two files.
- #23433 — the column custody bitmap puts its read-modify-write inside the shared write path.
- The freeze gate under epic #23024 item 4 edits one call site, `cleanupAndPruning`, to `min(floor, frozenTo)`.
