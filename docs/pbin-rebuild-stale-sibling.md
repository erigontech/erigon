# Bin commitment rebuild aborts on any range that inherits a base commitment file

`integration commitment rebuild --experimental.bin-commitment` fails on the second range
and every range after it. Range `0-8192` completes; range `8192-9216` dies 54 min into its
first shard.

```
[EROR] [08-16|10:10:01.119] pbin: process 8192-8320: loadIntoTable :
       pbin: record outlived its state:
       b3d68a67f7a903c5a4efa78a4245ee7e6cb85df4 63da688fe8ea636e3d15bec2b1d22d315339a1a5f15ab371947330bc90892ef2
```

Observed on snap-arb1, binary `df5156405f`, mainnet, `--no-history`, blake3. The trie sources
are byte-identical between `df5156405f` and `binary-trie` HEAD `dbe9a48c504`.

## Cause

`RebuildCommitmentFiles` slices a range's key stream in **plain-key order** — `stream.UnionKV`
over `AggregatorRoTx.FileStream` for the accounts and storage domains, cut every
`keysPerStep × shardSteps` keys. The trie is ordered by **tree key**. The two orders are
unrelated, so one shard's contiguous plain-key slice scatters across the whole trie, and any
shard can be forced to re-hash a branch whose sibling leaves belong to other shards.

Updates carry no value: `rebuildCommitmentShard` collects with `TouchKey` and a nil value, so
every value is read lazily. All shards in a range read at the **range's** end txNum —
`lastTxnumInShard` is assigned once from the range bound and feeds `NewFilesOnlyStateReader`.
That reader is files-only and capped at its limit: on a miss `FilesOnlyStateReader.Read`
returns nil with no fallback to history or DB.

Folding a branch inherited from the previous range's commitment file materializes its child
leaves. For a leaf whose slot was removed during this range, the lazy read returns nil at the
range-end point and `PBinPatriciaHashed.loadCellState` rejects it with
`errPBinDeleteUnsupported`. The removal is in the **range's** key stream but not in **this
shard's** slice, so nothing dropped the leaf first.

On the observed run the failing key sorts roughly 70% through the plain-key space — inferred
from its leading byte, not measured against the stream — while shard `8192-8320` covered the
first 38.6M of 308.5M keys. Its tombstone belonged to a later shard.

## Evidence

`erigon snapshots bt-search` against `/erigon-data/mainnet-caplin`, for
address `b3d68a67…cb85df4` slot `63da688f…90892ef2`:

| file | value |
|---|---|
| `v2.0-storage.0-8192.kv` | `0x2386f26fc10000` — live |
| `v2.0-storage.8192-9216.kv` | zero-length tombstone, exact key match |
| `v2.0-accounts.{0-8192,8192-9216}.kv` | non-empty both |

A plain `SSTORE`-to-zero, not a selfdestruct. The slot was live at step 8192, so the leaf
belongs in `0-8192.kv` — that output is correct and not implicated.

## Scope

Range `0-8192` cannot hit this: its tree only ever holds leaves inserted by an
already-processed key of the same range, each read live at the same point, and a key deleted
within the range is never inserted at all. Every range from the second on starts with a tree
populated from a file written at an earlier read point, so this recurs for every slot deleted
within the range whose parent branch is touched by another shard. First contact, not a
regression.

Hex has the same structural exposure — same rebuild path, same lazy `TouchKey` read, also
ordered by hashed key — but `HexPatriciaHashed.loadStateIfNeeded` has no `Deleted` check, so
it hashes the dead leaf as an empty account/storage leaf and continues. Under EIP-8297, where
absent and zero are the same state, that would be a root divergence rather than a cosmetic
one. Whether the hex rebuild hits this in practice is unverified;
`RebuildCommitmentFilesWithHistory` checks each root against the header root and would catch
it, the plain path does not.

`WithoutCommitmentSeek` is bin-only but only skips restoring the DB-persisted trie-state blob,
so it is not the difference.

## Fix

`touchRangeRemovals` streams the range's own accounts and storage `.kv` files and touches
every zero-length record into the update set. `rebuildCommitmentShard` runs it once per range,
in the first shard, and only where the range inherits commitment files — a range building its
own tree never inserts a leaf it later removes, so it skips the pass and keeps its cost and
output unchanged. Every removal is therefore applied before any shard folds, and no later
shard can meet a dead sibling. Cost is one extra sequential scan of the range's two `.kv`
files; on mainnet range `8192-9216` that was 51.18M removals out of 308.54M keys, scanned in
1m02s.

Alternatives considered: moving the check to unfold time and collapsing the parent there,
which the forward-only fold cannot do at hash time; and refusing to shard a range that
inherits a base file, which is correct but forfeits the memory win sharding exists for.

`TestRebuildCommitmentFilesBinTargetShardedRangeAppliesInheritedRemovals` pins it, asserting a
sharded range commits the same root as the same range rebuilt whole.
