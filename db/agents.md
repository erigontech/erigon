# Database Layer

Erigon uses a temporal database architecture separating hot (mutable) from cold (immutable) data.

## Layering principle: txNum is the application unit; step is a db implementation detail

The application layer (execution, RPC, staged sync, SharedDomains and everything above
it) is **txNum-based**. It addresses state by transaction number and does not know — and
must not need to know — about *steps*.

A **step** is purely a db-layer concept: the collation/file granularity (`step =
txNum / stepSize`) by which the aggregator freezes hot MDBX data into immutable `.seg`/`.kv`
files. It exists so the aggregator can decide which file a value lives in and where the
hot/cold boundary sits.

Therefore **step must not leak above the aggregator/domain**:

- Temporal read interfaces (`TemporalGetter.GetLatest`, `TemporalMemBatch.GetLatest`, the
  `SharedDomains` getters) take and return **txNum**, never `step`.
- The aggregator/domain is the *single* place that converts between txNum and step — it
  receives a txNum (e.g. an unwind bound via `changeset.ReadContext`), converts it to a
  step for its on-disk comparisons, and converts the step a value was found at back to a
  txNum before returning it upward.
- Per-read state the db layer needs but the application does not care about (the unwind/
  file-search bound, read metrics) travels through `context.Context`
  (`changeset.WithReadContext`), not as interface parameters — which is why the read path
  is a single `GetLatest(ctx, name, k) (v, txNum, err)` rather than the accreted
  duck-typed metered/txN variants it replaced (issue #21739).

### Why this matters (the cost of leaking step)

A `step` leaked into application code is a *foreign concept* that layer has no control over
and no real use for — it only ever wants "the value as of this txNum". Carrying step upward
forces application code to reason about, and convert, a db-internal unit, which obscures
intent and couples the application to the freezing/collation scheme.

This is materially worse now that **stepSize is variable**. Converting between step and
txNum is no longer a constant `step << k`: it needs the *current* `stepSize`, so every
conversion in the application layer becomes a **function call** to fetch it plus a **data
dependency** on it. Keeping the application in txNum removes that call and that dependency
entirely — step↔txNum conversion happens once, inside the aggregator/domain, where the
stepSize already lives.

Today a read from the hot domain tables can only resolve to step granularity: each value
is keyed by its (inverted) **step**, not txNum (`domain.go` `stepBytes`). So at the read
boundary the aggregator/domain converts the found step to a txNum (`stepLastTxNum`, the
step's last txNum — an upper bound); only the mem overlay carries an exact txNum. This is
the current behaviour, not a defect; it is enough to keep step out of the application layer.

#### Future work this enables

With the application no longer seeing step, the hot DB could later be made txNum-native —
moving the txNum→step conversion into collation, where step (the snapshot-file bucket
granularity) belongs — which in turn enables a `(txNum, epoch)` DB unwind model. That is
future work, not part of this change; the point here is only that the interface no longer
stands in the way.

When adding a read path, prefer keeping step inside `state/` (aggregator, domain) and
exposing txNum.

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
