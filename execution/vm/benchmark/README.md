# EVM Performance Benchmarks

Targeted EVM benchmarks based on mainnet block analysis (50 blocks, 14,886 txs, 1.53B gas)
and bloatnet (perfdevnet-3) comparison.

## Mainnet Gas Distribution

| Category | Gas % | Tx % | Pattern |
|----------|-------|------|---------|
| DeFi/swaps | 68.7% | 3.2% | CALL chains (Router->Pair->Token), SLOAD/SSTORE |
| Token transfers | 16.7% | 24.5% | SLOAD x2, SSTORE x2, LOG3 |
| ETH transfers | 8.3% | 42.1% | Trivial compute |
| Contract creates | 3.2% | 0.3% | INITCODE execution |
| Other | 3.1% | 29.9% | Mixed |

## DeFi Swap Opcode Profile (traced)

| Opcode | Gas % | Notes |
|--------|-------|-------|
| CALL/STATICCALL | 91.6% | Cross-contract call overhead dominates |
| SSTORE | 3.3% | Balance updates |
| SLOAD | 2.7% | Balance/reserve reads |
| LOG | 1.1% | Transfer events |
| Arithmetic+stack | 1.3% | Everything else |

## Bloatnet vs Mainnet

| Metric | Mainnet | Bloatnet |
|--------|---------|----------|
| Gas/block | ~30M | ~846M |
| Txs/block | ~200-300 | 51 |
| Gas/tx | ~100K avg | 16.7M |
| Contract diversity | Thousands | 1 contract |
| Pattern | Mixed workloads | Single function `0xc1926de5` |

Bloatnet is a single-contract stress test, not representative of mainnet workloads.

## Precompile Usage (50 blocks)

| Precompile | Calls | Notes |
|------------|-------|-------|
| identity (0x04) | 92 | Memory copy |
| ecrecover (0x01) | 54 | Signature verification |
| sha256 (0x02) | 4 | Rare |
| modexp (0x05) | 1 | Very rare |
| bn128/bls | 0 | Not seen in sample |

modexp and alt_bn128 precompiles have separate optimization tasks.

## Benchmark Categories

### A: Call Chains (`bench_call_chain_test.go`)

Targets the #1 gas consumer: cross-contract CALL overhead.

- `BenchmarkNestedStaticCalls` — depth 2/4/8/16 STATICCALL chains
- `BenchmarkDelegateCallProxy` — 1/2/4 DELEGATECALL proxy layers
- `BenchmarkCallWithValue` — CALL with/without ETH value transfer
- `BenchmarkDeFiSwapChain` — Router->Pair->TokenA+TokenB swap pattern

Hot paths: `evm.call()`, CallContext pool, snapshot push/pop, access list management.

### B: Storage Access (`bench_storage_test.go`)

Targets SLOAD/SSTORE which account for 6% of DeFi gas.

- `BenchmarkSLOADCold` — 10/50/100/500 cold SLOADs (2100 gas each, EIP-2929)
- `BenchmarkSLOADWarm` — warm SLOAD loops (100 gas each)
- `BenchmarkSSTORE` — zero-to-nonzero (20K), nonzero-to-nonzero (5K), nonzero-to-zero (refund)
- `BenchmarkTransientStorage` — TLOAD/TSTORE (EIP-1153)
- `BenchmarkStorageDiversity` — 100/1000 unique slot accesses

Hot paths: `IntraBlockState.GetState()`, dirty/origin/DB cache hierarchy.

### C: Token Transfers (`bench_token_transfer_test.go`)

Targets the 16.7% of mainnet gas from ERC-20 operations.

- `BenchmarkERC20Transfer` — SLOAD x2 + SSTORE x2 + LOG3
- `BenchmarkERC20TransferFrom` — + allowance SLOAD + SSTORE
- `BenchmarkERC20BalanceOf` — pure SLOAD read path
- `BenchmarkERC20BatchTransfers` — 5/10/50 sequential transfers (cold->warm amortization)

### D: Interpreter Loop (`bench_interpreter_test.go`)

Targets opcode dispatch overhead in `interpreter.go:Run()`.

- `BenchmarkPureArithmetic` — ADD/MUL at 1M/10M/100M gas
- `BenchmarkStackOps` — DUP/SWAP intensive
- `BenchmarkMemoryOps` — MSTORE/MLOAD fixed + growing memory
- `BenchmarkKeccak256` — SHA3 at 32B/256B/4KB input sizes
- `BenchmarkMixedCompute` — realistic opcode mix (60% stack, 20% arith, 10% mem, 10% control)

## Running

```bash
# Run all benchmarks
go test -bench=. -run=^$ -count=3 ./execution/vm/benchmark/ | tee bench.txt

# Run a specific category
go test -bench=BenchmarkDeFiSwapChain -run=^$ -count=5 ./execution/vm/benchmark/

# Profile a specific benchmark
go test -bench=BenchmarkDeFiSwapChain -run=^$ -cpuprofile=cpu.prof ./execution/vm/benchmark/
go tool pprof -http=:8080 cpu.prof

# Compare before/after
benchstat old.txt new.txt
```

## Engine X Coverage Gaps

The existing Engine X benchmarks (1064 variants) cover precompile regression well
but miss these real-world patterns:

1. **DeFi call chains** — cross-contract CALL overhead (68.7% of gas)
2. **Storage diversity** — many unique SLOAD/SSTORE slots per tx
3. **Compound patterns** — SLOAD+SSTORE+LOG in realistic combinations
4. **Cold vs warm** — EIP-2929 access list amortization across call depth
