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
