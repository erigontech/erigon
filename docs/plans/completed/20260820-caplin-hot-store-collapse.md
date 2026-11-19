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
- **Race tests**: `go test -race ./cl/persistence/blob_storage/...` runs, but `-race` cannot falsify these locks. `bucketStore` holds no mutable Go state beyond the mutexes; what the locks protect is a filesystem invariant. `afero.MemMapFs` guards its map with its own `sync.RWMutex` (`memmap.go:33`) and `mem.FileData` embeds a `sync.Mutex` (`mem/file.go:57`), and on a real `OsFs` the work happens in the kernel. So the concurrency guards are written as observable invariant tests driven by a filesystem wrapper that blocks inside `RemoveAll`, not as race-detector assertions.
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

New file `cl/persistence/blob_storage/bucket_store.go`, two types:

```go
type bucketStore struct {
    fs afero.Fs
}

func (b *bucketStore) init(fs afero.Fs)
func (b *bucketStore) path(slot uint64, root common.Hash, idx uint64) (dir, file string)
func (b *bucketStore) write(slot uint64, root common.Hash, idx uint64, v ssz.Marshaler) (created bool, err error)
func (b *bucketStore) read(slot uint64, root common.Hash, idx uint64, out ssz.EncodableSSZ, v clparams.StateVersion) (found bool, err error)
func (b *bucketStore) exists(slot uint64, root common.Hash, idx uint64) (bool, error)
func (b *bucketStore) remove(slot uint64, root common.Hash, idx uint64) error
func (b *bucketStore) stream(w io.Writer, slot uint64, root common.Hash, idx uint64) error
func (b *bucketStore) pruneBelow(slot uint64) error

type slotLocks struct{ locks []sync.RWMutex }

func (s *slotLocks) init()
func (s *slotLocks) forSlot(slot uint64) *sync.RWMutex
```

**`bucketStore` never locks.** Each of its methods is one filesystem operation against a complete file, and temp-then-rename means a reader observes either the old file or the new one, never a partial. Locking belongs to the façades, which alone know the granularity of the operation being performed — that is what makes the whole-scan and whole-batch guarantees expressible.

**What the façade locks do and do not cover.** They serialise façade operations against each other for one slot. They do **not** exclude `pruneBelow`, which takes nothing — no lock the façades could take would, since a bucket spans 10,000 slots and therefore every stripe, which is the store-wide lock this design exists to avoid. Everything a prune removes is below the retention floor, so an operation it interferes with concerns data no caller is entitled to: a scan that mixes pre- and post-prune observations, or a batch split partway, describes an expiring slot either way.

Both façades embed `bucketStore` and `slotLocks` by value and take `forSlot(slot)` for:

- every write, to protect the fixed `<file>.tmp` name from a second writer to the same path
- `WriteBlobSidecars`, once around the whole batch, so a concurrent single-sidecar write cannot interleave with it. The method takes no slot — it derives one per sidecar from `SignedBlockHeader.Header.Slot` (`blob_db.go:88`) — so the lock is taken on the first sidecar's slot and only when the batch is non-empty. An empty batch writes no files and still records its zero count row, which is what distinguishes "this block has no blobs" from "unknown"; that path is reached per block from `on_block.go:566`.
- `RemoveBlobSidecars` and `ReadBlobSidecars`, so the files and the MDBX count row move together *and* that is observable — the guarantee is worthless if the only reader of both takes no lock
- `GetSavedColumnIndex` and `RemoveColumnSidecars`, once around the whole loop, so a scan cannot interleave with a concurrent write to the same slot

Single-file reads take nothing, and `stream` in particular takes nothing: it runs `io.Copy` into a libp2p stream under a 5 s deadline (`cl/sentinel/handlers/handlers.go:192-195`), and a lock held across that would let one slow peer stall writers on the same stripe for the whole deadline.

`pruneBelow` takes no lock either. The race it admits is benign because everything it touches is below the retention floor: a file written into an expiring bucket is data nobody is entitled to, and a directory recreated by a racing write is removed on the next tick. A read or stream whose file disappears mid-operation fails cleanly rather than returning corruption, which is the same outcome as the file having been pruned a moment earlier.

`pruneBelow` continues past a failed `RemoveAll`, attempts every expiring bucket, and returns the first error. A single stuck bucket must not wedge the rest — on Windows an open file blocks its directory's removal, so a bucket being served can legitimately fail and succeed on the next tick.

**Write path.** `Stat` the target to set `created`, `MkdirAll(dir)`, create `<file>.tmp`, `ssz_snappy.EncodeAndWrite`, `Sync`, `Close`, `Rename(tmp, file)`. Any error removes the temp. The `created` return exists because the column façade's event emit must stay deduplicated: today the `Stat` guard at `data_column_db.go:94-97` returns above the `SendDataColumnSidecar` emit at `:115`, so it is the only thing keeping a duplicate delivery off the Beacon API `data_column_sidecar` stream. Under overwrite the façade emits only when `created` is true.

Known consequence, accepted rather than engineered around: a file left truncated by the pre-fix write bug makes `Stat` succeed, so the delivery that repairs it reports `created == false` and publishes no event. Subscribers miss one event per legacy truncated file, once, on nodes that hit the bug before upgrading.

**Prune path, and the invariant that governs it.** `cutoff := slot / subdivisionSlot`; readdir the store root; treat an entry as a bucket **only** if it is a directory whose name parses as a `uint64` and formats back to the byte-identical string; remove those below the cutoff. `pruneBelow(0)` is a no-op, retiring the `slotsKept == MaxUint64` sentinel.

That allowlist is not defensive tidiness. `OpenCaplinDatabase` roots the blob store's afero at `dirs.CaplinBlobs` (`run.go:131`) while placing the blob index MDBX at `<CaplinBlobs>/chaindata` (`run.go:96`), so **the directory this readdirs contains the live blob database as a sibling of the numeric buckets**. Today's prune builds its names from integers and never enumerates, so it cannot reach it; switching to readdir is what puts `chaindata` in front of `RemoveAll` for the first time. Relaxing the filter to a denylist, or to "any directory", deletes the blob database on the first prune tick. This earns an inline comment under `.claude/rules/comments.md` — a non-obvious invariant not enforced by types.

**Caller.** `cleanupAndPruning` gains:

```go
func floorFor(head, keep uint64) uint64 {
    if head <= keep { return 0 }
    return head - keep
}
```

`floorFor` lives in package `stages` and is introduced only when the signature flips, so no earlier task may reference it — the stores are in package `blob_storage` and cannot reach an unexported helper in `stages` without inverting the dependency. Until then each `Prune` computes its own floor inline from the fields it already holds.

A `MaxUint64` keep falls out as a no-op with no special case. The `pruneBlobDistance` expression moves here from `run.go:292`, making this the single place all three CL hot floors are computed. `cleanupAndPruning` checks and logs both `PruneBelow` errors — the new implementation propagates real filesystem failures, and the current calls discard them.

**`PeerDas.PruneBelow` keeps its second job.** `peerdas.Prune` does not only delegate: it advances `EarliestAvailableSlot` (`cl/das/peer_das.go:337-345`), and that value is advertised to peers in the status handshake (`cl/sentinel/handshake/handshake.go:115`) and heartbeats (`cl/sentinel/handlers/heartbeats.go:152`). Dropping it would leave the node advertising a floor it can no longer serve.

The translation is exact, including the zero case, which today bypasses the monotonic guard rather than being subject to it:

```go
if slot == 0 {
    d.state.SetEarliestAvailableSlot(0)          // keep-everything; today's curSlot < keepSlotDistance branch
} else if slot > d.state.GetEarliestAvailableSlot() {
    d.state.SetEarliestAvailableSlot(slot)
}
```

Ordering against a prune error is a decision this plan makes rather than inherits: the floor advances even when the column prune returned an error, because removal below it has already begun and claiming a *later* floor is the safe direction — advertising data that was partially deleted is not. Today the error returns before the update, which errs the unsafe way.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): everything achievable in this repo — the new type, both façades, the interface change, the mocks, the tests.
- **Post-Completion** (no checkboxes): the wall-clock effect on a real populated datadir, which the syscall-count test cannot show, and the follow-up issues this unblocks.

## Decisions

- **Shape** — extract a `bucketStore`, keep both façades. Not one type with a kind descriptor.
- **Locking** — `bucketStore` never locks; the façades do, via a shared `slotLocks`, at the granularity of the real operation. Every write takes its slot lock, as does any operation spanning more than one file. Single-file reads and `stream` take nothing. Rejected: a store-level `RWMutex`, which stalls all 64 stripes whenever a prune queues behind a slow peer, and cannot be guarded by a falsifiable test.
- **Construction** — `init(fs)` on the pointer receiver, to avoid a copylocks vet failure.
- **Write** — temp file then rename; fixes problem 2 structurally.
- **Durability** — unchanged: file `fsync`, no directory `fsync`.
- **`read`** — returns `(found, err)`; each façade maps it, because blobs want absence to mean "reschedule" and columns want an error.
- **`remove`** — tolerates ENOENT. Behaviour change, fixes problem 3.
- **Prune floor** — absolute slot, not a distance. `slotsKept` and `ethClock` both leave both constructors: the clock is read only by `Prune` today (`blob_db.go:171`, `data_column_db.go:215`), so it dies with the floor.
- **Prune walk** — readdir the store root, no lock, no persisted low-water state. It continues past a failed `RemoveAll` and returns the first error.
- **Prune is not excluded** — façade locks serialise façade operations against each other, never against the pruner. No lock could: a bucket spans every stripe. Acceptable because everything a prune removes is below the retention floor.
- **Bucket allowlist** — an entry is a bucket only if it is a directory whose name parses as a `uint64` and formats back byte-identically. The blob index MDBX is a sibling of the buckets in the same directory, so this is what stops the pruner deleting the database.
- **Prune datum** — blocks keep cutting against `args.seenSlot`, blobs against the wall clock. Preserved and commented, not unified.
- **Flag semantics** — untouched; `ColumnKeepSlots == 0` still resolves to the spec window. Fixing that reading is #23411.
- **`RemoveAllColumnSidecars`** — deleted; no production callers.
- **`PeerDas.PruneBelow`** — keeps advancing `EarliestAvailableSlot`; the value is advertised to peers, so dropping it is a P2P defect. A zero floor sets 0 outright, bypassing the monotonic guard exactly as today. The floor advances even when the prune errored, which is a deliberate change from today's return-before-update.
- **Write rule** — OPEN, not ruled: overwrite always vs skip when the target exists. Leaning overwrite (last-write-wins self-heals, and a `capcli` re-import becomes a repair path) at the cost of a redundant encode and fsync per duplicate. Task 3 implements overwrite unless this is ruled the other way first.
- **Duplicate events** — `write` returns `created`, and the column façade emits `SendDataColumnSidecar` only on a create. Overwrite must not re-publish a sidecar to Beacon API subscribers, which today's `Stat` guard prevents by returning above the emit.

## Out of Scope

No freeze gate, no flag changes, no cold expiry. The directory layout is #23426, the column custody bitmap #23433, the index-row leak #23432.

## Implementation Steps

Every task lists its tests before the implementation they cover. In Go the red phase for a new symbol is a compile failure — that counts, and the runner must see it before writing the implementation bullet.

### Task 1: Filesystem test wrappers

**Files:**
- Create: `cl/persistence/blob_storage/fs_helpers_test.go`

- [x] write tests for a counting `afero.Fs` wrapper: each of `RemoveAll`, `Stat`, `Create`, `Rename` and `Open` is tallied, unrecorded methods still delegate, and the counters reset (red: helper does not exist yet)
- [x] write tests for a failing `afero.Fs` wrapper that makes a named file's writes or `Sync` return an error, so a caller can induce a failed write at a chosen path
- [x] write tests for a slow `afero.Fs` wrapper that delays inside `RemoveAll` by a settable duration, so a test can measure how long an unrelated operation waits during a prune
- [x] implement the three wrappers over `afero.NewMemMapFs()`
- [x] run `go test ./cl/persistence/blob_storage/...` — must pass before task 2

⚠️ `make lint` reports one pre-existing failure outside this change: `db/seg/decompress.go:212` field `residencyOnce is unused`. It is present on `origin/main` and untouched by this branch, so task 8's lint gate must treat it as inherited rather than chase it.

`failWritesAfter(path, budget, err)` takes a byte budget so a test can leave a genuinely truncated file rather than an empty one, which is what task 5's column repro needs. The budget is snapshotted per open file, so a retry after a failure starts from a fresh budget.

### Task 2: bucketStore with path, init and pruneBelow

**Files:**
- Create: `cl/persistence/blob_storage/bucket_store.go`
- Create: `cl/persistence/blob_storage/bucket_store_test.go`

- [x] write tests for `path` pinning the exact strings `blobSidecarFilePath` and `dataColumnFilePath` produce today (red: type does not exist)
- [x] write `pruneBelow` success tests: removes only buckets strictly below the cutoff, idempotent on a second call
- [x] write the database-safety test: a `chaindata` directory in the store root survives every prune, alongside entries like `0x`, `12a`, ` 7`, `007` and a plain file named `5` — name the test for the database it protects, not for "non-numeric entries"
- [x] write `pruneBelow` edge-case tests: `pruneBelow(0)` removes nothing, a readdir error propagates to the caller
- [x] write the syscall-count test with the task 1 counting wrapper: `RemoveAll` is called exactly once per existing expiring bucket and never for a bucket that does not exist
- [x] move `subdivisionSlot` and `rwLocksCount` into the new file from `blob_db.go:44-46` and `data_column_db.go:44`
- [x] add `bucketStore{fs}` with `init(fs)`, and `slotLocks` with `init()` and `forSlot(slot)`, so the stripe array has one implementation while the façades decide when to take it
- [x] add `path(slot, root, idx)` reproducing today's `<slot/subdivisionSlot>/<root>_<idx>` exactly
- [x] add `pruneBelow(slot)` taking no lock: readdir the root, accept an entry as a bucket only if it is a directory whose name parses as `uint64` and formats back byte-identically, and `RemoveAll` those below the cutoff
- [x] comment the allowlist with the reason — the blob index MDBX is a sibling of the buckets in this directory, so a looser filter deletes it
- [x] mutation-check each guard turns a test red: `<` to `<=`, relax the allowlist to "any directory", and revert `pruneBelow` to `for i := uint64(0); i < currentSlot; i += subdivisionSlot`
- [x] run tests — must pass before task 3

### Task 3: bucketStore write, read, exists, remove and stream

**Files:**
- Modify: `cl/persistence/blob_storage/bucket_store.go`
- Modify: `cl/persistence/blob_storage/bucket_store_test.go`

- [x] write success tests: write/read round trip, `exists` after a write, `stream` reproduces the written bytes, `remove` then `exists` is false
- [x] write tests that `write` reports `created == true` on a first write and `created == false` when it replaces an existing file
- [x] write error tests using the task 1 failing wrapper: a failed write leaves nothing at the target path and no stray temp, `remove` of a missing file returns nil, `read` of a missing file returns `found == false` with no error
- [x] add `write` returning `(created bool, err error)`: `Stat` the target to set `created`, `MkdirAll`, create `<file>.tmp`, `EncodeAndWrite`, `Sync`, `Close`, `Rename` onto the target, removing the temp on any error
- [x] add `read` returning `(found, err)`, mapping a missing file to `found == false` and leaving the not-found convention to callers
- [x] add `exists`, `remove` (ENOENT tolerated) and `stream`, none of them locking
- [x] comment on the type that it never locks and why — every method is one operation on a complete file, and callers own the granularity
- [x] mutation-check: revert `write` to writing in place, and `remove` to the erroring form
- [x] run tests — must pass before task 4

➕ `EncodeAndWrite` wraps the file in a `bufio.Writer` and flushes it in a defer whose error it discards, so on a value that fits the buffer it returns nil even when every byte failed to reach the file — the pre-existing path would then `Sync` a file it never wrote and rename the empty temp onto the target, which is problem 2 by another route. `write` therefore hands it an `errWriter` and checks the recorded error. Verified by mutation: dropping the `errWriter` leaves `TestBucketStoreFailedWriteLeavesNothingBehind` unable to induce a failure at all (`bucket_store_test.go:403` goes red on `got nil`).

### Task 4: BlobStore onto bucketStore

**Files:**
- Modify: `cl/persistence/blob_storage/blob_db.go`
- Modify: `cl/persistence/blob_storage/blob_db_test.go`

- [x] write a test that `RemoveBlobSidecars` succeeds when a file is already gone and still deletes the count row (red against today's first-ENOENT return)
- [x] write a test that a concurrent single-sidecar write cannot interleave with a `WriteBlobSidecars` batch for the same slot
- [x] write a test that an empty `blobSidecars` slice still records its zero count row and takes no slot lock
- [x] embed `bucketStore` and `slotLocks` in `BlobStore`, call both `init`s from `NewBlobStore`, delete `blobSidecarFilePath`
- [x] route `WriteBlobSidecars`, `ReadBlobSidecars`, `BlobSidecarExists`, `WriteStream` and `RemoveBlobSidecars` through the shared methods; `WriteBlobSidecars` takes `forSlot` on the first sidecar's slot and only when the batch is non-empty, `RemoveBlobSidecars` and `ReadBlobSidecars` take it around files-plus-row, and `WriteStream` takes nothing
- [x] keep `Prune()` and the `slotsKept` field, computing the floor inline from `slotsKept` and `ethClock` — `floorFor` belongs to package `stages` and does not exist yet, so it must not be referenced here
- [x] run `go test ./cl/persistence/blob_storage/...` — must pass before task 5

➕ The batch-lock guard needs the write order to be observable, so `blob_db_test.go` carries a fourth wrapper beside task 1's three: a `createOrderFs` that records every `Create` path and runs a hook inside the call, letting a test park the batch mid-write and see whether a concurrent write slips in.

### Task 5: dataColumnStorageImpl onto bucketStore

**Files:**
- Modify: `cl/persistence/blob_storage/data_column_db.go`
- Modify: `cl/persistence/blob_storage/data_column_db_test.go`
- Modify: `cl/persistence/blob_storage/mock_services/data_column_storage_mock.go`

- [x] write a test using the task 1 failing wrapper that a truncated file is never reported as held: `ColumnSidecarExists` and `GetSavedColumnIndex` must both say the column is absent, since a truncated file would otherwise count toward custody and be served over P2P while reads error
- [x] write a test that a duplicate `WriteColumnSidecars` for an already-stored `(root, index)` emits no second event, using a recording emitter
- [x] write a test that a concurrent write to the same slot cannot interleave with a `GetSavedColumnIndex` scan or a `RemoveColumnSidecars` loop
- [x] embed `bucketStore` and `slotLocks`, call both `init`s from `NewDataColumnStore`, delete the local `rwLocks` array and `acquireLock`, and delete `dataColumnFilePath` together with its three test call sites at `data_column_db_test.go:94,116,199`
- [x] route every per-file method through the shared ones, taking `forSlot(slot)` once around the whole loop in `GetSavedColumnIndex` and `RemoveColumnSidecars`, keeping the version-aware decode in the façade, and emitting `SendDataColumnSidecar` only when `write` reports `created`
- [x] delete `RemoveAllColumnSidecars` from the interface, the implementation and `TestRemoveAllColumnSidecars`
- [x] keep `Prune(keepSlotDistance)` computing its floor inline, for the same package reason as task 4
- [x] regenerate the mock with `go generate ./cl/persistence/blob_storage/...`
- [x] run `go test ./cl/persistence/blob_storage/... ./cl/das/...` — must pass before task 6

### Task 6: PruneBelow signature and the floor moving to the caller

**Files:**
- Modify: `cl/persistence/blob_storage/blob_db.go`
- Modify: `cl/persistence/blob_storage/data_column_db.go`
- Modify: `cl/das/peer_das.go`
- Modify: `cl/phase1/stages/cleanup_and_pruning.go`
- Modify: `cmd/caplin/caplin1/run.go`
- Modify: `cmd/capcli/cli.go`
- Modify: `cl/spectest/consensus_tests/fork_choice.go`
- Modify: `cl/persistence/blob_storage/mock_services/blob_storage_mock.go`
- Modify: `cl/persistence/blob_storage/mock_services/data_column_storage_mock.go`
- Modify: `cl/das/mock_services/peer_das_mock.go`
- Modify: `cl/persistence/blob_storage/blob_db_test.go`
- Modify: `cl/persistence/blob_storage/data_column_db_test.go`

- [x] write tests for `floorFor` in package `stages`: `head <= keep` gives 0, a `MaxUint64` keep gives 0, a normal window gives `head - keep`
- [x] write a test that `PeerDas.PruneBelow` advances `EarliestAvailableSlot` to the floor, refuses to move it backwards, sets 0 outright when the floor is 0, and still advances when the column prune returned an error
- [x] write a test that `PruneBelow(0)` removes nothing, and one that a `PruneBelow` error reaches the caller's log rather than being discarded
- [x] replace `Prune()` and `Prune(keepSlotDistance)` with `PruneBelow(slot uint64)` on `BlobStorage`, `DataColumnStorage` and `PeerDas`, keeping the `EarliestAvailableSlot` update in `peerdas.PruneBelow` (`peer_das.go:337-345`) with the floor in place of `curSlot - keepSlotDistance`
- [x] add `floorFor(head, keep uint64) uint64` to `cleanup_and_pruning.go`, and delete the inline floor code left in both stores by tasks 4 and 5
- [x] drop `slotsKept` and `ethClock` from `NewBlobStore` and `NewDataColumnStore` — the clock is read only by `Prune` — and drop the now-unused `blobPruneDistance` parameter from `OpenCaplinDatabase` (`run.go:86-94`), updating its 12 call sites in `cmd/capcli/cli.go` — lines 153, 296, 485, 526, 613, 664, 1053, 1129, 1170, 1233, 1306, 1365
- [x] move the `pruneBlobDistance` expression from `run.go:292` into `cleanupAndPruning`, and check and log both `PruneBelow` return values there
- [x] resolve `ColumnKeepSlots == 0` to the spec window exactly as `cleanup_and_pruning.go:30-33` does today, unchanged
- [x] comment which datum each pruner cuts against and why they differ — no forward reference to the freeze gate, which `.claude/rules/comments.md` bans as scope narration
- [x] update the three `Prune(N)` calls at `data_column_db_test.go:345,360,374`, delete the `impl.slotsKept` assertion at `data_column_db_test.go:86`, and update every direct constructor call: `data_column_db_test.go:54,77,384`, `blob_db_test.go:48`, `cl/spectest/consensus_tests/fork_choice.go:312-313`, `cl/beacon/handler/utils_test.go:107`, `cl/phase1/forkchoice/fork_choice_test.go:94,190`, `cl/phase1/forkchoice/weight_store_diff_test.go:87`, `cl/das/peer_das_download_test.go:139`, `cl/sentinel/handlers/blobs_test.go:99,223`, `cl/sentinel/handlers/data_column_sidecar_test.go:472`
- [x] regenerate all three mocks
- [x] run `go test ./cl/... ./cmd/caplin/... ./cmd/capcli/...` and `go build ./...` — must pass before task 7

### Task 7: Concurrency guards

**Files:**
- Modify: `cl/persistence/blob_storage/bucket_store_test.go`

- [x] write a test at façade level that a prune in progress does not delay unrelated work: with the task 1 slow wrapper delaying each `RemoveAll`, a write into a surviving bucket and a `stream` of an unrelated slot both complete in well under the prune's total delay — this goes red if any store-wide lock is reintroduced
- [x] write a test that a `write` racing the removal of its own bucket leaves the store consistent whichever order wins: no partial file at the target path, and any error is a clean failure
- [x] write a façade-level test that two concurrent writes to the same `(slot, root, index)` cannot corrupt each other through the shared `<file>.tmp` name — the guarantee is the façade's slot lock, so testing `bucketStore` directly would assert something it does not provide
- [x] run `go test ./cl/persistence/blob_storage/...` and `go test -race ./cl/persistence/blob_storage/...` — must pass before task 8
- [x] do not add a `-race` mutation check: `bucketStore` holds no mutable Go state and `MemMapFs` is internally synchronized, so the detector cannot observe these guards — the timing and consistency assertions above are what falsify them

### Task 8: Verify acceptance criteria

- [x] verify each of the three problems in the Overview is pinned by a test that fails when the fix is reverted
- [x] verify the only public API changes are `Prune` becoming `PruneBelow`, `RemoveAllColumnSidecars` being deleted, and the dropped `slotsKept`/`ethClock`/`blobPruneDistance` parameters on `NewBlobStore`, `NewDataColumnStore` and `OpenCaplinDatabase`
- [x] verify no test asserts that a façade lock excludes the pruner — it does not, and cannot
- [x] verify no flag semantics changed — same retention for the same flags on every network, `ColumnKeepSlots == 0` included
- [x] verify `EarliestAvailableSlot` still advances exactly as it does today for the same retention configuration
- [x] verify no duplicate `data_column_sidecar` event is published for a re-written sidecar
- [x] verify the prune allowlist against a store root containing `chaindata`, and confirm no code path can widen it
- [x] run the full suite: `go test ./cl/... ./cmd/caplin/... ./cmd/capcli/...`
- [x] run `go test -race ./cl/persistence/blob_storage/...` and `make lint` (lint reports only the pre-existing `db/seg/decompress.go:212` `residencyOnce` issue)

### Task 9: [Final] Update documentation

- [x] verify no comment in the diff narrates the change, cites an issue number, restates a signature, or forward-references work not in the tree
- [x] verify `bucket_store.go` carries 2026 in its license header
- [x] update `CLAUDE.md` only if a new pattern was established that future work should follow (not needed; no new repository-wide pattern was established)
- [x] mark every checkbox in this plan and record any `➕` or `⚠️` entries that arose (all discovered entries are recorded above)
- [x] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only.*

**Manual verification:**

- Run a node with a populated blob store and confirm the per-slot prune no longer scales with head. The syscall-count test proves the call count; it cannot show the wall-clock effect on a real datadir with hundreds of thousands of files.
- Confirm on a Fulu node that column write throughput is unchanged, given an extra `Stat` per write to set `created`. Blob writes newly take a slot lock they have never taken — `blob_db.go` locks nothing today — so blob write throughput needs the same check, not just columns.
- Exercise the first prune after a long backlog — a node restarted with pruning on after running `--caplin.blobs-archive` — and confirm gossip validation and P2P serving stay responsive while hundreds of buckets expire.
- On a node carrying files left truncated by the pre-fix write bug, confirm the repairing delivery is accepted and the column becomes readable, accepting that its `data_column_sidecar` event is suppressed once.

**Follow-ups this unblocks:**

- #23426 — the per-slot leaf directory becomes a change to `bucketStore.path` plus the pruner, rather than the same edit in two files.
- #23433 — the column custody bitmap puts its read-modify-write inside the shared write path.
- The freeze gate under epic #23024 item 4 edits one call site, `cleanupAndPruning`, to `min(floor, frozenTo)`.
