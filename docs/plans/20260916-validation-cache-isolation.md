# Validation cache isolation — item 6

Base: `84d5b18fc19` (`origin/main`). Worktree: `validation-session-reuse`.

## Scope

This change isolates speculative cache invalidation. It does not enable concurrent ValidateChain calls. The module semaphore, ForkValidator lock, pipeline Sync, notification accumulator, canonical adoption and persistence order remain unchanged.

Main already shares immutable code by hash. A second code cache would duplicate existing machinery.

## Problem and change

Validating a sibling fork calls SharedDomains.Unwind. Previously that immediately invalidated the shared account/storage/code bindings and commitment branch cache, even when validation was abandoned. It also revoked canonical read-ahead fill authority.

Validation SharedDomains now select local cache unwind. They record the lowest unwind boundary and reject shared entries at or above it without removing those entries. Local memory and changesets still take precedence. Branch lookups retain their step bound as well as the new transaction bound. Address-to-code-hash shortcuts go through the bounded account lookup after unwind. Content-addressed bytecode remains reusable.

After speculative unwind, reads do not populate shared address/prefix caches. This is conservative: it avoids publishing facts whose provenance belongs to the candidate. Canonical readers and read-ahead fills remain live. Adoption into canonical SharedDomains invalidates the discarded range; direct successful commit also publishes invalidation. Failed publication does not invalidate the shared cache.

No cache copy, per-key version chain, or new cache allocation is introduced. This is an ownership change, not a CRDT.

## Tests

The new account and branch preservation regressions failed against the original behavior before the implementation changed. Coverage includes canonical fills admitted after candidate unwind, adoption, successful and aborted commit, exact branch-boundary rejection, parent-memory bounds, and account-to-code binding bounds.

The affected execctx, commitment and execmodule package suites pass. Focused race tests pass. Five repeated race runs of side-fork switches and embedded RPC cache-view tests pass with USE_STATE_CACHE=true. These are regression checks; they do not establish that concurrent independent ValidateChain is safe.

## Remaining item 6 work

A bounded cache read is not a root-bound snapshot. Concurrent canonical publication and independent validations still require generation-bound read authority, independently owned pipeline/notifications, and explicit cancellation/retirement contracts. This change must not be used as justification to remove the module semaphore or enable concurrent validation.

## Measurement method

Matched baseline and updated binaries use the same benchmark sources. The baseline Go overlay restores production files from the base commit and makes the new constructor option inert. Runs alternate order. Allocation counts accompany timings; local CPU contention limits timing confidence.

`BenchmarkValidationCache` measures session construction/discard followed by 256 canonical reads, including the adverse case where the candidate reads the same keys. It uses cached absent accounts and is a cache lifecycle microbenchmark, not a transaction throughput benchmark. `BenchmarkGetLatestColdNegative` checks cold read overhead. `BenchmarkValidatePayload` checks actual warm/cold/hot transaction workloads.

Results are saved under `build/validation-session-measurements` when complete.

## Results

Four alternating rounds, three payload operations per workload per round, GOMAXPROCS=4, USE_STATE_CACHE=true, Apple M5 Max. Medians below exclude round 0, which overlapped the integration build.

| Workload | Main | Updated | Time change | Main → updated allocations |
|---|---:|---:|---:|---:|
| Discard candidate, 256 canonical cache reads | 64.92 µs | 13.65 µs | -79.0% | 883 → 111 |
| Candidate reads those keys, discard, canonical reads | 74.54 µs | 40.26 µs | -46.0% | 883 → 111 |
| Construct/close without unwind, canonical reads | 13.68 µs | 13.56 µs | -0.9% | 111 → 111 |
| Cold negative account read | 206.9 ns | 210.6 ns | +1.8% | 3 → 3 |
| Warm payload, 1,000 transactions | 23.40 ms | 22.80 ms | -2.6% | 267,101 → 267,188 |
| Cold payload, 1,000 transactions | 23.84 ms | 24.20 ms | +1.5% | 289,776 → 289,793 |
| Hot recipient payload, 1,000 transactions | 55.55 ms | 57.83 ms | +4.1% | 445,906 → 457,445 |

Discard/reuse bytes drop from 32,711 to 12,464 B/op. Candidate-read/discard bytes drop from 30,790 to 12,068 B/op. Cold reads remain 72 B/op. Cold payload bytes increase by 0.48%; warm by 1.41%. Hot-recipient retries vary widely: the timing and allocation differences in that workload are not evidence of an effect from this patch.

These results establish a synthetic cache-reuse improvement, not a general payload throughput improvement or an end-to-end sibling-fork speedup. The cold-path increases are included rather than dismissed. More representative sibling-fork replay is needed before claiming an application-level performance win.

The measured binaries precede the final cleanup that clears the local branch read bound after direct commit when no account cache is attached. That cleanup has a failing-then-passing regression. It does not change the measured discard path or the production merge-then-commit path. The final source snapshot includes it.
