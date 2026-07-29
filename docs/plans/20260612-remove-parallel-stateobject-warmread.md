# Remove the parallel-exec stateObject — make the versionMap the single source of truth

## Problem

Warm account reads under parallel execution are the slowest among trie clients
(`warm-extcodehash` ~214 MGas/s vs geth 1816, reth 6691). EXTCODEHASH is
read-only, so the cost is execution, not commitment.

Mechanism: `IntraBlockState.getStateObject`'s warm-hit path, when
`versionMap != nil`, calls `refreshVersionedAccount(addr, &so.data, StorageRead,
UnknownVersion)` on **every** read. `UnknownVersion` means "treat any versionMap
entry as newer", i.e. it unconditionally re-reads the whole account
(balance + nonce + incarnation + codehash, each a synchronized `versionMap.Read`
plus a SelfDestruct probe) to return a single already-cached field. One warm
`GetCodeHash` costs ~8–10 synchronized cross-worker `versionMap.Read` calls plus
a full-account re-sync.

## Root cause: dual representation

An account is represented twice during a tx: the record-level `stateObject.data`
and the field-level versionMap entries. `refreshVersionedAccount` is the
reconciliation bridge between them — and it *is* the field-level arbitration
model already (`refreshBalance`/`refreshNonce`/`refreshIncarnation`/
`refreshCodeHash`, each comparing `TxIndex`/`Incarnation`), just run eagerly as a
bulk re-sync into `so.data` on every read.

In an optimistic/block-STM model this re-sync upholds no required invariant.
Cross-tx staleness is **validation's** responsibility: if a lower-indexed tx
rewrites a field this attempt read, the recorded read-source version stops
matching and the attempt re-executes. Re-reading the live versionMap mid-attempt
does not maintain correctness — it compensates for the record cache drifting from
the read-set. The imprecision (record-granular re-sync over field-granular state)
is the same reason it is slow.

The fix is therefore not to gate the refresh (that optimizes the hack) but to
remove the record cache from the parallel path: the field-specific versionMap is
already the authority, so let reads source values directly from it.

## Feasibility (already satisfied)

- **Value is in the versionMap.** Field reads return `fv.Value` per field
  (`versionmap.go`), so a read can return the value from the arbitrated entry,
  using the base DB read only on a miss.
- **Write-set already exists.** `versionedWrites WriteSet` with per-field getters
  (`GetAddress`/`GetBalance`/`GetNonce`/`GetIncarnation`/`GetSelfDestruct`/
  `GetCreateContract`/`GetCode`) is the write home that isn't `so.data`.

## The hard dependency: intra-tx revert (→ remove the journal)

`Snapshot`/`RevertToSnapshot` + the `journal` currently revert `so.data`
mutations (REVERT opcode, failed calls). In a field-based write-set with
versioned entries, revert stops being inverse-op replay and becomes
**checkpoint-and-truncate**: a snapshot is a per-field write-set marker, revert
discards entries newer than it. The write-set is the log, so the journal becomes
redundant rather than re-pointed.

Scope caveat: the journal reverts more than account fields — it also covers the
refund counter, the EIP-2929 access list, EIP-1153 transient storage, and logs.
A journal-free model needs each of those to be checkpoint-revertable too. This is
why this is the load-bearing step: it is not "re-point account reverts", it is
"make the entire revertable tx-context checkpoint-based."

## Staged plan

Each step gated by byte-equal-root-vs-serial over a real block range, `-race`
parallel exec, and the eest-spec parallel shards.

1. **Source field values from the versionMap entry** in `versionedReadCore`
   (today it calls `getStateObject` and reads `so.data`); base DB read only on a
   miss. `so.data` stops being the read authority, so **`refreshVersionedAccount`
   and the warm-hit refresh delete**. Captures the warm-read win; the stateObject
   still physically exists as a write buffer.
2. **Re-point remaining parallel-mode `so.data` reads** — code, storage, EIP-161
   empty-account check, incarnation/revival. (#21590's SD-revival handling lives
   in `refreshIncarnation`; it is the same arbitration applied to
   `Incarnation`/`CodeHash`, not a special case.)
3. **Move intra-tx revert off `so.data`** onto the write-set (journal reverts
   write-set entries). The hard part.
4. **Drop the stateObject from the parallel path** once nothing reads `so.data`.

Parallel-mode first; the serial path (`versionMap == nil`) keeps the stateObject.
Whether serial later unifies onto a single-version map is a separate question.

## Terminal state: field-based interpreter, no journal

The end of this trajectory is an interpreter that reads and writes fields
directly against the field-level read/write sets, with no journal — intra-tx
revert handled by write-set checkpoint/truncation (including refund, access list,
transient storage, logs). At that point the stateObject is completely redundant,
not merely bypassed on the read path. This is the horizon, not the next PR; the
steps above are the path to it.

## Relationship to #21536

#21536 (the typed-vio refactor) is the prior structural step on this same
trajectory, not merely a perf change. Before it, the versionMap held untyped
boxed cells that were effectively account-shaped proxies for the stateObject.
The typed per-path model (per-field `WriteCell[T]`, path-partitioned `WriteSet`/
`PerPathReadSet`) makes each field a first-class typed entry, so the structure
itself enforces field-granularity rather than carrying a loose record proxy. The
reduced alloc/boxing is a byproduct of that precision. This is what makes the
plan below reachable: once versionMap entries are genuine field-level state
rather than proxies, sourcing reads directly from them (step 1) is the natural
move and the record cache becomes redundant by construction.

## Sequencing

Strictly after #21536. Confirm with a warm-extcodehash CPU profile that
`refreshVersionedAccount`/`versionMap.Read` top the profile before starting
step 1.

## Superseded approach

An earlier idea — watermark-gated refresh (skip `refreshVersionedAccount` when no
write for `addr` is newer than the stateObject's load version) — is dropped. It
optimizes the hack while keeping it; removing the stateObject deletes the hack
and the dual-representation bug class with it.
