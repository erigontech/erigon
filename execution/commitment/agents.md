# Commitment

Computes the Ethereum state root via a hex patricia trie. Driven by `Updates.TouchPlainKey` during execution; flushed via a `*PatriciaHashed.Process` variant at commit boundaries.

## Trie variants

- `HexPatriciaHashed` (`hex_patricia_hashed.go`) — single-threaded; the default for everything.
- `ConcurrentPatriciaHashed` (`hex_concurrent_patricia_hashed.go`) — depth-1 PoC; splits work by top nibble only.
- `ParallelPatriciaHashed` (`parallel_patricia_hashed.go`) — splits at arbitrary depths via a path-compressed prefix trie built during `TouchPlainKey`.

## ModeParallel

`ModeParallel` (in `commitment.go`) enables the `ParallelPatriciaHashed` codepath. It builds a prefix trie of touched hashed-keys, identifies fork points with subtree size ≥ `MinSplitKeys` (=32) as split-points, dispatches one worker per leaf-task, and uses a fold-time barrier (`splitMap` + `splitPoint.arrived atomic.Int32`) so the last sibling to arrive at a split-point continues folding upward through shared state. Use it for workloads where a single nibble bucket dominates runtime (e.g. bloatnet-shape: few accounts updating millions of storage slots) and leaves cores idle. For typical mainnet workloads the sequential variant is comparable or faster — parallel pays off when the workload has natural depth-N branching that exceeds the worker count.

Selected via the `--experimental.parallel-commitment` CLI flag (default OFF, registered in `cmd/utils/flags.go`). The flag sets `statecfg.ExperimentalParallelCommitment`, which `NewSharedDomains` reads to pick `VariantParallelHexPatricia`. `GenerateWitness` is not parallelized in v1 — it still runs through `HexPatriciaHashed`.

## Key files

- `commitment.go` — `Mode`, `Updates`, `TouchPlainKey`, `BranchData.ReplacePlainKeys`, deferred-branch machinery.
- `prefix_trie.go` — `prefixNode`, arena, insert (path-compressed nibble trie used only by ModeParallel).
- `parallel_update.go` — `splitPoint`, `leafTask`, `parallelUpdate.Prepare` (DFS, split-point emission, untouched-cell pre-population).
- `parallel_patricia_hashed.go` — worker pool, fold-time barrier, root publication via CAS.
- `verify_test.go` — equivalence harness (`assertEquivalentRoot`) and fuzz target (`FuzzParallelEquivalence`).
- `parallel_patricia_hashed_bench_test.go` — `Benchmark_Commitment_DirectVsParallel`: side-by-side `ModeDirect` vs `ModeParallel` sweep across worker counts on a 100K accounts-only corpus and a 500K storage-heavy corpus.

## Perf snapshot — Apple M2 Max (2026-05-17)

Hardware: Apple M2 Max, 12 cores, Go 1.25.7, darwin/arm64. Source: `go test -run=^$ -bench=Benchmark_Commitment_DirectVsParallel -benchtime=3x ./execution/commitment/`. Numbers are ratios vs `ModeDirect` on the same corpus; raw ns/op ages badly across hardware.

- **500K-StorageHeavy** (1000 accounts × 499 storage slots each — the bloatnet shape). `ModeParallel-w1` is ~1.06× faster than `ModeDirect` (parallel overhead is negligible here because the storage subtries dwarf coordination cost). `ModeParallel-w4` is ~2.0× faster than `ModeDirect` — the realistic speedup. Past 4 workers it plateaus and slightly regresses: w8 ≈ 1.93×, w12 ≈ 1.86×. Suggests the storage-heavy split tops out at ~4 effective workers on this corpus; beyond that, fold-time barrier contention and goroutine scheduling eat the gains.
- **100K-AccountsOnly** (uniform random account balances). `ModeParallel-w1` is ~2.0× *slower* than `ModeDirect` — the pure-overhead cost of the parallel codepath at zero concurrency on a workload where subtrees are shallow and uniform. `ModeParallel-w4` and `w8` only break even (~1.05-1.11× faster); `w12` regresses to ~0.82× of Direct. Confirms the expectation that uniform-balanced account workloads are not where ModeParallel pays off.
