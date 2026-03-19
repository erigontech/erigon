# Database Layer

Erigon uses a temporal database architecture separating hot (mutable) from cold (immutable) data.

## Storage Architecture

```
datadir/
├── chaindata/     # Hot state (MDBX) - recent blocks and state
└── snapshots/
    ├── domain/    # Latest state values
    ├── history/   # Historical value changes
    ├── idx/       # Inverted indices for search
    └── accessor/  # Additional lookup indices
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

## Data Flow

1. New state changes → hot MDBX tables
2. ETL sorts before insertion
3. Periodic snapshots freeze old data
4. Cold data compressed for long-term storage
