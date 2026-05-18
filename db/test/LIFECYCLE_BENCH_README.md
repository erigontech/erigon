# Lifecycle Benchmarks

`lifecycle_bench_test.go` provides a benchmark harness that drives data through
every phase of the aggregator pipeline with independently timed phases.
It complements the unit-level benchmarks in `db/state/` and
`execution/commitment/` by testing cross-component interactions at realistic
scale.

```
Execute (DomainPut) → Commit (ComputeCommitment) → Flush (mem→MDBX)
    → Collate (BuildFiles) → Prune (PruneSmallBatches) → Merge (MergeLoop)
```

### Key design choices

- **Reduced step size** (`stepSize = txsPerBlock * blocksPerStep`, typically 20)
  triggers collation frequently. Merge fires at 64 steps (default
  `stepsInFrozenFile`), so 200 steps produce 3+ merge cycles within a single
  benchmark run.

- **Realistic key distribution** via `keyGenerator`: hot accounts (Zipf head,
  ~70% of writes) model DeFi contracts like USDT/Uniswap; cold accounts (long
  tail, ~30%) model user wallets. Storage keys are composite
  (`addr + slotHash`, 52 bytes).

- **Accounts initialised before storage** — the commitment trie requires account
  nodes to exist before storage subtrees can be populated.

- **Per-phase timing** reported via `b.ReportMetric`: `ms/execute`,
  `ms/commit`, `ms/flush`, `ms/collate`, `ms/prune`.

### Benchmark index

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkLifecycle/Small` | 50 steps, no merge — baseline |
| `BenchmarkLifecycle/Medium` | 200 steps, 3+ merges — full lifecycle |
| `BenchmarkLifecycle/Large` | 500 steps, 7+ merges + prune cycles |
| `BenchmarkLifecycle/Bloatnet` | 200 steps, 500 keys/tx (10x density) |
| `BenchmarkLifecycle_PhaseIsolation/Execute_Only` | DomainPut throughput ceiling |
| `BenchmarkLifecycle_PhaseIsolation/Commit_Only` | ComputeCommitment ceiling |
| `BenchmarkLifecycle_PhaseIsolation/Flush_Only` | mem→MDBX write cost |
| `BenchmarkLifecycle_PhaseIsolation/Collate_Only` | BuildFiles (collate + compress) |
| `BenchmarkReadAfterLifecycle/GetLatest/*` | Domain reads after file build |
| `BenchmarkReadAfterLifecycle/HistorySeek/*` | Historical reads at varying depth |
| `BenchmarkLifecycle_KeyDensity/keys=N` | Commitment scaling curve |

### Running

```bash
# Quick validation (1 iteration each)
go test -run='^$' -bench='BenchmarkLifecycle/Small' -benchtime=1x ./db/test/

# Full lifecycle at medium scale
go test -run='^$' -bench='BenchmarkLifecycle/Medium' -benchtime=3x ./db/test/

# All phase isolation benchmarks
go test -run='^$' -bench='BenchmarkLifecycle_PhaseIsolation' -benchtime=5x ./db/test/

# Commitment scaling curve (bloatnet density analysis)
go test -run='^$' -bench='BenchmarkLifecycle_KeyDensity' -benchtime=1x ./db/test/

# Read performance after lifecycle builds files
go test -run='^$' -bench='BenchmarkReadAfterLifecycle' -benchtime=10x ./db/test/
```

### Bloatnet context

Bloatnet (EIP-7928 test network, 700M gas/block) produces ~33k commitment keys
per block vs mainnet's ~1.4k. The `Bloatnet` variant uses 500 keys/tx to
approximate this density, stressing commitment and merge paths without requiring
full EVM execution. This drives optimisation work for commitment parallelisation
(16-way root decomposition) and background DB scheduling (merge/prune impact on
execution throughput).

## Related benchmarks

| Package | Benchmarks | Focus |
|---------|-----------|-------|
| `db/state/` | `BenchmarkAggregator_Processing`, `BenchmarkInvertedIndexMergeFiles` | Aggregator throughput, merge cost |
| `execution/commitment/` | `Benchmark_HexPatriciaHashed_Process`, `BenchmarkBranchData_ReplacePlainKeys` | Trie hashing, branch encoding |
| `execution/vm/runtime/` | `BenchmarkSimpleLoop`, `BenchmarkEVM_CREATE2` | EVM opcode execution (in-memory state) |
| `cmd/scripts/exec_bench/` | `exec_bench.sh` | Integration: serial vs parallel block replay |
