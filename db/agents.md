# Database Layer

Erigon uses a temporal database architecture separating hot (mutable) from cold (immutable) data.

## Data Flow

1. New state changes → hot MDBX tables
2. Periodic snapshots freeze old data
3. Cold data compressed for long-term storage
4. `Unwind` beyond data in snapshots not allowed

## Storage Architecture

```
datadir/
├── chaindata/     # Hot state (MDBX) - recent blocks and state
└── snapshots/
    ├── domain/    # Latest state values
    ├── history/   # Historical value changes
    ├── idx/       # Inverted indices for search
    ├── accessor/  # Additional lookup indices
    └── caplin/    # Consensus-layer state-table snapshots
```

## Key Components

### MDBX (`kv/mdbx/`)
- Fork of LMDB optimized for Erigon's access patterns
- Hot database for recent, mutable state
- Tables defined in `kv/tables.go`

### Temporal Database (`kv/temporal/`)
- `kv_temporal.go` - `TemporalDB` wraps MDBX + Aggregator
- Enables time-travel queries via `GetAsOf(txNum)`
- Methods: `GetLatest()`, `HistorySeek()`, `RangeAsOf()`, `IndexRange()`

### State Aggregator (`state/`)
- `aggregator.go` - Central hub managing domains and indices
- `domain.go` - State domain with embedded history
- `history.go` - Historical value tracking
- `inverted_index.go` - Time-travel indices (key → [txNums])

## Four Domains

| Domain | Purpose | Content |
|--------|---------|---------|
| AccountsDomain | Account state | nonce, balance, code hash |
| StorageDomain | Contract storage | key-value slots |
| CodeDomain | Contract bytecode | deployed code |
| CommitmentDomain | Merkle proofs | state root commitments |

Each domain manages:
- Current values (hot, in MDBX)
- Historical snapshots (.seg files)
- Indices for lookups

## ETL Framework (`etl/`)

Sorts data before database insertion to reduce write amplification:
1. Collect changes in memory/temp files
2. Sort by key
3. Batch insert in order

## Snapshots (`seg/`, `downloader/`)

- `.seg` files store immutable historical data
- Downloaded via BitTorrent with WebSeed fallback
- Piece size: 2MB default
- Verification on download
- `Preverified.Typed` (`snapcfg/util.go`) filters block and `caplin/` entries to the binary's supported version window for their type and keeps the newest version per name. `domain/`, `history/`, `idx/`, `accessor/` state entries bypass both — they are kept unconditionally under their full name. `caplin/`-prefixed entries are the consensus state tables (`BlockRoot`, `EpochData`, …); they have no snaptype of their own and borrow `snaptype.BeaconBlocks`' window, so publishing a newer caplin state-table version requires bumping that type first, or existing binaries silently ignore the new files. Beacon blocks and blob sidecars live at the snapshots root, not under `caplin/`, and take the ordinary typed path with their own per-type windows
- Offline verification: `integrity.VerifyTorrentFiles` (`integrity/torrent_verify.go`) re-hashes data files against their `.torrent` piece hashes, walking `<datadir>/snapshots` recursively and following symlinked subtrees, so it covers `caplin/` and the state dirs rather than only the top-level block segments. `failFast` only decides whether the run stops at the first mismatch; a mismatch, an unreachable path or a cancelled context always fails the call
- Caplin's two snapshot collections do not share a lifecycle: `freezeblocks.CaplinSnapshots` (beacon blocks, blob sidecars) embeds `snapshotsync.BaseRoSnapshots` and reads through pinned generations, while `snapshotsync.CaplinStateSnapshots` (the `caplin/` state tables) still keeps its own `dirty`/`visible` copy. Three caplin accessors keep conventions the base does not, and consumers depend on each: `SegmentsMax` is dirty-backed (it counts a dumped-but-unindexed tail, which the archive backfill stop condition needs), `FrozenBlobs` is the exclusive `To` (consumers compare `slot < FrozenBlobs()`), and `OpenFolder` keeps caplin's own gap-filtered scan so a segment past a gap never reaches the dirty set

## Runtime settings (`snapshots/erigondb.toml`)

Per-datadir settings that travel with a snapshot set rather than the binary, resolved by `state.ResolveErigonDBSettings` (`state/erigondb_settings.go`). A downloaded `erigondb.toml` is synced snapshot metadata and is never rewritten, so a producer's published values survive on consumers.

| Field | Meaning |
|-------|---------|
| `step_size` | txs per step |
| `steps_in_frozen_file` | steps merged into a frozen (immutable) file |
| `references_in_commitment_branches` | whether new commitment merges write referenced (short-key, `v2.1`) `.kv` files; absent → default `true` |

`references_in_commitment_branches` governs *writes* only. Reads are version-aware (a commitment `.kv` is referenced iff its version `< v2.2` and its range ≥ the referencing threshold), so flipping the flag on a populated datadir stays correct in both directions and old referenced files convert to plain lazily through merges. To publish plain (`v2.2`) snapshots, a producer sets it `false` before running merges (see the `erigondb-sync-integration-test-plan` skill).
