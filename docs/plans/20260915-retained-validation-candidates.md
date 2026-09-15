# Retain recent validated candidates

Base: `origin/main`, `396468d8ba7`.
Branch: `awskii/retain-validated-candidates`.

## Scope

Retain up to four completed candidate states in an LRU cache. Validate A,
insert and validate sibling B, then select A: adopt A's retained state instead
of executing A again. Validation and fork choice remain serialized.

This is bounded local reuse, not a distributed CRDT or a persistent artifact
format. Candidates remain eligible only until the next fork-choice operation,
a notified canonical-height change, an invalid-chain purge, or module shutdown.
Validity facts survive artifact eviction, as before. A validity-cache hit does
not reconstruct an evicted state.

## Ownership and adoption

- Each candidate owns its SharedDomains, state-change accumulation, and receipts.
- Completed overlays detach from the validation transaction before it rolls back.
- Cache hits refresh recency; eviction and invalidation close the retained state.
- Adoption consumes one candidate, transfers its accumulation, and closes its
  remaining resources. Other candidates are cleared by fork-choice cleanup.
- Later block insertion may allocate more physical transaction IDs. Adoption
  preserves the greatest existing EthTx sequence rather than restoring the
  candidate's older allocation frontier. This relies on serialized insertion;
  it is not a scheme for merging independently allocated ID ranges.

The limit bounds candidate count, not total process memory in bytes. Retaining
four candidates can use more memory than the former single-candidate design.
Cross-generation reuse and concurrent validation remain separate work.

## Regression evidence

The original A/B/A metrics regression changed from zero reused records to one
record for A. The stronger insertion-order test then exposed sequence rollback
from 8 to 5; preserving the allocation frontier fixes it. The sibling transfers
now succeed under Amsterdam rules and produce different state roots.

A second regression fills the cache, refreshes A, inserts a fifth candidate,
and verifies that B is evicted and closed in both flushing modes. B remains known-valid without a
retained state. Selecting A clears and closes all remaining candidates.

## Verification

- `go test ./execution/execmodule/... -count=1 -timeout=5m`
- `go test -race ./execution/execmodule/... -count=1 -timeout=10m`
- `make lint`
- `make erigon integration`

The focused benchmark is:

```sh
go test ./execution/execmodule -run '^$' \
  -bench '^BenchmarkValidatedCandidateReuse$' -benchtime=5x -count=3 -timeout=5m
```

It measures fork choice after validating two sibling blocks with 1,000 transfers.
The discarded arm clears retained state before selecting the older sibling;
the retained arm adopts it. Fixture construction and validation are outside the
timed region. This measures a candidate-reuse case, not overall node throughput.

## Local benchmark results

Apple M5 Max, darwin/arm64, five operations per sample, three samples per arm.
Median values from the isolated corrected run:

| Timed fork choice | Discarded state | Retained state |
|---|---:|---:|
| Latency | 19.28 ms | 8.30 ms |
| Allocated bytes | 32,355,734 | 10,247,172 |
| Allocations | 231,545 | 65,626 |

This case has about 57% lower latency (2.32x faster) and 68% fewer allocated
bytes during fork choice. The allocation measurement excludes setup and does
not measure the extra memory held by the candidate cache. These are small
local samples, not a forecast of production throughput.

The earlier FCU acknowledgement fix is a separate PR and is not included in this
branch's main-based changes.
