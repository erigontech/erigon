# Storage-level parallel trie fanout

Status: design + scaffolding (this branch). Implementation of the inner fold is follow-up work.

## Motivation

The benchmark `test_sstore_bloated[10GB-fork_Osaka-…NO_CACHE-existing_slots_True-write_new_value_True-30M]` writes ~4,200 cold SSTOREs per 30M-gas block, all targeting **one EOA's storage trie** (~150M leaves, depth 7–8). With erigon's current commitment design:

- `ConcurrentPatriciaHashed` (enabled by `--experimental.concurrent-commitment`) provides a 16-way fanout split on the **first nibble of `keccak256(plainKey)`**.
- For account updates that's `keccak256(address)[0]`. For storage updates it's *also* `keccak256(address)[0]` — every storage update for one account lands in the same subtrie.
- For our test workload, all 4,200 updates have `keccak256(0x87a6314d…)[0] == N` for one fixed `N`, so 1 of the 16 subtries does 100% of the work and the other 15 are idle.

We need a **second-level fanout**: when a single subtrie has many storage updates concentrated on one account, sub-fanout 16 ways on `keccak256(slot)[0]`.

For our test that's 4,200 / 16 ≈ 263 storage updates per sub-subtrie — both the cold-page reads (already parallelised by the trie warmuper) AND the trie hash CPU work get a 16× concurrency uplift.

## Design

### Detection

In `Updates.ParallelHashSort`, after the existing 16-way bucketing, scan each non-empty bucket. A bucket qualifies for sub-fanout when:

1. All updates in it share the same account prefix (first 64 nibbles of the hashed key), AND
2. The number of *storage* updates exceeds `STORAGE_PARALLEL_TRIE_THRESHOLD` (env var, default `0` = disabled).

Both conditions are needed:
- (1) means a second-level mount on the storage-key first nibble is well-defined (the parent account path is fixed for this subtrie).
- (2) ensures the goroutine spawn cost is amortised. Reasonable threshold values: 256–1024.

When neither condition holds, the subtrie processes sequentially (current behaviour).

### Fanout

For a qualifying subtrie:

1. Mount 16 *sub*-subtries off the existing subtrie at depth 64 (one nibble past the account address path), each owning a specific first-nibble of `keccak256(slot)`.
2. Each sub-subtrie does its own `followAndUpdate` loop over its slice of the storage updates. They run concurrently in an `errgroup` capped at 16.
3. After all sub-subtries fold, each contributes a `cell` covering its nibble of the storage trie row at depth 64. Merge them into the parent subtrie's storage row.
4. The parent subtrie continues with its account-leaf folding using the merged storage row.

The mount + fold mechanics mirror the existing `ConcurrentPatriciaHashed.foldNibble` but operate at depth 64 instead of 0. The depth-64 mount needs the parent subtrie's state to already be unfolded down to that depth — which it is by construction, because the parent subtrie's first action on processing a storage update is to unfold the account path.

### Why this is non-trivial

`HexPatriciaHashed.mountTo` was written assuming a depth-0 mount: it copies the parent's full grid and currentKey state up to `activeRows`. A depth-64 mount needs to:
- Copy the parent's state up to row 64 (the storage trie root row).
- Reset the storage-trie-and-below rows.
- Track which sub-subtrie owns which nibble of the storage row.

The fold side similarly needs a `foldMountedAtDepth` variant that stops folding at the storage root depth, returning a cell that the parent can merge into row 64.

The other complexity is **deferred branch updates**: sub-subtries should fold directly without buffering deferred updates (mirror `branchEncoder.SetDeferUpdates(false)` in `SpawnSubTrie`).

## Feature flag

`STORAGE_PARALLEL_TRIE_THRESHOLD` (int env var) — minimum storage-update count for the sub-fanout to trigger. Default `0` (disabled). Recommended: start at `1024` once implemented and rolled out.

## Phases

- **Phase 1** (this PR): doc + env flag + detection in `ParallelHashSort` (logs at Info when sub-fanout would trigger but defers to sequential). Lets us measure how often the condition holds in real workloads.
- **Phase 2**: actual sub-fanout fold. Mount-at-depth-64 + matching `foldMountedAtDepth` + concurrent inner errgroup. Tests against the bloated-SSTORE workload.
- **Phase 3**: tune the threshold based on measured speedup curves; potentially auto-tune.

## Risk

- Folding at non-zero depth interacts with the existing branch-encoder deferred-updates path; getting this wrong corrupts the storage root and therefore the account leaf.
- Cold-page race between sub-subtrie warmupers — the existing `Warmuper` is per-`HexPatriciaHashed`; we'd want one warmuper per sub-subtrie OR ensure the existing warmuper's `NumWorkers` is wide enough to cover the doubled concurrency.

## Out of scope

- Account-level fanout improvements (already optimal for non-degenerate workloads).
- Splitting one storage trie across multiple physical MDBX commits (would require domain-level changes).
