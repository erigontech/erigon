# ModeDirect vs ModeParallel Comparison Benchmark

## Overview

Add a Go benchmark that compares sequential `HexPatriciaHashed` (ModeDirect) against `ParallelPatriciaHashed` (ModeParallel) on realistic corpora, sweeping worker counts. Produces hand-inspectable ns/op numbers to validate that the parallel-commitment work pays off and to see how throughput scales with worker count.

**Why this exists**: the existing `Benchmark_HexPatriciaHashed_Process` calls `Process` with only 5 updates per iteration — useless for measuring parallelism. There's no end-to-end side-by-side comparison of the two modes. The plan for parallel-hph listed this as optional/follow-up. This is the follow-up.

**Not a CI assertion**: speedup numbers are noisy enough that a "parallel must be faster than direct" check would flake. Output is for hand inspection.

## Context (from discovery)

- **Working dir**: `/Users/awskii/org/wrk/erigon-parallel-hph` (worktree on `awskii/parallel_hph`).
- **Reused helpers** (all in `execution/commitment/`):
  - `NewUpdateBuilder()` + `Balance(addr, val)` / `Storage(addr, loc, val)` — corpus builders.
  - `MockState` + `applyPlainUpdates(pk, updates)` — in-memory state backing.
  - `mockTrieCtxFactory(ms *MockState)` (hex_concurrent_patricia_hashed_test.go:73) — returns a `TrieContextFactory` for `ParallelPatriciaHashed`.
  - `NewHexPatriciaHashed`, `NewParallelPatriciaHashed` — trie constructors.
  - `WrapKeyUpdates(b, mode, hasher, pk, updates)` — wraps a corpus into an `*Updates`.
  - `KeyToHexNibbleHash`, `ModeDirect`, `ModeParallel`, `SetNumWorkers`.
- **Existing model**: `Benchmark_HexPatriciaHashed_Process` in `hex_patricia_hashed_bench_test.go` — different shape, kept untouched.

## Development Approach

- **testing approach**: Regular — write the benchmark file in one go, then smoke-run.
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include verification that the benchmark builds and runs** — this is the equivalent of "tests pass" for benchmark code. The smoke-run with `-benchtime=1x` is the verification step.
- **CRITICAL: all checks pass before next task** — no exceptions
- run `make lint` after each task; linter is non-deterministic, run until clean
- Copyright year 2026 on the new file

## Testing Strategy

This change is a benchmark, not production code, so the verification model differs:

- **No unit-test coverage required** for the benchmark itself — its correctness is "it compiles, all sub-benches run without error, ns/op numbers come out plausible (non-zero, non-NaN)".
- **Smoke run**: `go test -run=^$ -bench=Benchmark_Commitment_DirectVsParallel -benchtime=1x ./execution/commitment/` — runs every sub-bench exactly once, fast (a few minutes), confirms the matrix builds correctly.
- **Determinism check**: re-run smoke; the sub-bench names must be identical (corpus seeds are fixed). Numbers will vary slightly, that's expected.
- **Production code not modified** — no regression risk to the existing test suite, but `go test ./execution/commitment/...` is still run once in Task 2 to confirm the new file doesn't break the package build.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

One new file, `execution/commitment/parallel_patricia_hashed_bench_test.go`, containing:

1. Two corpus builders (deterministic, seeded).
2. Two helpers — `runDirectBench` and `runParallelBench` — that each encapsulate the StopTimer/StartTimer per-iteration loop.
3. One top-level `Benchmark_Commitment_DirectVsParallel` driver that sub-benches by corpus and (for parallel) by worker count.

Total: ~80 lines.

## Technical Details

### Corpus builders

```go
func build100KAccountsCorpus(b testing.TB) ([][]byte, []Update) {
    rnd := rand.New(rand.NewSource(133777))
    ub := NewUpdateBuilder()
    for i := 0; i < 100_000; i++ {
        addr := make([]byte, length.Addr)
        rnd.Read(addr)
        ub.Balance(hex.EncodeToString(addr), rnd.Uint64())
    }
    return ub.Build()
}

func build500KStorageHeavyCorpus(b testing.TB) ([][]byte, []Update) {
    rnd := rand.New(rand.NewSource(244888))
    ub := NewUpdateBuilder()

    addrs := make([]string, 1000)
    for i := range addrs {
        addr := make([]byte, length.Addr)
        rnd.Read(addr)
        addrs[i] = hex.EncodeToString(addr)
        ub.Balance(addrs[i], rnd.Uint64())
    }

    const slotsPerAccount = 499 // 1000 * 499 = 499_000; plus 1000 accounts = 500_000 total updates
    for _, addr := range addrs {
        for j := 0; j < slotsPerAccount; j++ {
            loc := make([]byte, length.Hash)
            rnd.Read(loc)
            val := make([]byte, 32)
            rnd.Read(val)
            ub.Storage(addr, hex.EncodeToString(loc), hex.EncodeToString(val))
        }
    }
    return ub.Build()
}
```

### Bench helpers

```go
func runDirectBench(b *testing.B, pk [][]byte, updates []Update) {
    ctx := context.Background()
    for b.Loop() {
        // StopTimer/StartTimer brackets per-iteration setup so we measure only
        // Process(). Existing `commitment_bench_test.go` uses the same pattern.
        b.StopTimer()
        ms := NewMockState(b)
        require.NoError(b, ms.applyPlainUpdates(pk, updates))
        hph := NewHexPatriciaHashed(length.Addr, ms)
        upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, updates)
        b.StartTimer()

        _, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})

        b.StopTimer()
        require.NoError(b, err)
        upds.Close()
    }
}

func runParallelBench(b *testing.B, pk [][]byte, updates []Update, workers int) {
    ctx := context.Background()
    for b.Loop() {
        b.StopTimer()
        ms := NewMockState(b)
        require.NoError(b, ms.applyPlainUpdates(pk, updates))
        pph := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr)
        pph.SetNumWorkers(workers)
        upds := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, updates)
        b.StartTimer()

        _, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

        b.StopTimer()
        require.NoError(b, err)
        upds.Close()
    }
}
```

### Driver

```go
func Benchmark_Commitment_DirectVsParallel(b *testing.B) {
    workers := []int{1, 4, 8, runtime.NumCPU()}
    slices.Sort(workers)
    workers = slices.Compact(workers)

    b.Run("100K-AccountsOnly", func(b *testing.B) {
        pk, updates := build100KAccountsCorpus(b)
        b.ResetTimer()

        b.Run("ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
        for _, w := range workers {
            b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) {
                runParallelBench(b, pk, updates, w)
            })
        }
    })

    b.Run("500K-StorageHeavy", func(b *testing.B) {
        pk, updates := build500KStorageHeavyCorpus(b)
        b.ResetTimer()

        b.Run("ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
        for _, w := range workers {
            b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) {
                runParallelBench(b, pk, updates, w)
            })
        }
    })
}
```

Loop variable capture (`w := w`) is not needed — Go 1.22+ scopes range vars per iteration, and the worktree is on Go 1.25.7.

### Knobs

- `WarmupConfig{}` (Enabled=false) — matches existing benchmark, deterministic, no warmup overhead noise.
- `MinSplitKeys = 32` default — no override.
- `accountKeyLen = length.Addr`.

### Expected output shape

```
Benchmark_Commitment_DirectVsParallel/100K-AccountsOnly/ModeDirect-12             1   <X>ns/op
Benchmark_Commitment_DirectVsParallel/100K-AccountsOnly/ModeParallel-w1-12        1   <X>ns/op
Benchmark_Commitment_DirectVsParallel/100K-AccountsOnly/ModeParallel-w4-12        1   <X>ns/op
Benchmark_Commitment_DirectVsParallel/100K-AccountsOnly/ModeParallel-w8-12        1   <X>ns/op
Benchmark_Commitment_DirectVsParallel/100K-AccountsOnly/ModeParallel-w12-12       1   <X>ns/op
Benchmark_Commitment_DirectVsParallel/500K-StorageHeavy/ModeDirect-12             1   <X>ns/op
Benchmark_Commitment_DirectVsParallel/500K-StorageHeavy/ModeParallel-w1-12        1   <X>ns/op
...
```

On an 8-core or 4-core machine the matrix is correspondingly smaller (dedup).

### Constraints

- Per-iteration setup (`MockState` + `applyPlainUpdates` on 500K updates) is non-trivial — `b.StopTimer/StartTimer` keeps it out of the measurement.
- Memory peak: one `MockState` alive at a time per iteration (~hundreds of MB for 500K). Released between iterations.
- `-benchtime=1s` default → `b.N` likely 1-3 for 500K. User can pass `-benchtime=5x` to force a fixed iteration count.

## What Goes Where

- **Implementation Steps** (checkboxes below): the new benchmark file, smoke run, lint, commit.
- **Post-Completion** (no checkboxes): manual perf runs on representative hardware, sharing/recording results.

## Implementation Steps

### Task 1: Write the benchmark file

**Files:**
- Create: `execution/commitment/parallel_patricia_hashed_bench_test.go`

- [x] create `execution/commitment/parallel_patricia_hashed_bench_test.go` with copyright header (year 2026) and package declaration matching other files
- [x] implement `build100KAccountsCorpus(b testing.TB) ([][]byte, []Update)` with seed 133777 (signature follows `UpdateBuilder.Build()` return, not `[]plainKey, []*Update` from the original plan text)
- [x] implement `build500KStorageHeavyCorpus(b testing.TB) ([][]byte, []Update)` with seed 244888 — 1000 accounts × 499 storage slots = 499K storage + 1K account updates = 500K total
- [x] implement `runDirectBench(b *testing.B, pk [][]byte, updates []Update)` with `b.Loop()` + `b.StopTimer/StartTimer` bracketing, fresh `MockState` per iteration. Note: `b.Loop()` requires the timer running on entry, so each iteration ends with `b.StartTimer()` after teardown.
- [x] implement `runParallelBench(b *testing.B, pk [][]byte, updates []Update, workers int)` parallel to runDirectBench, using `NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr)` + `SetNumWorkers(workers)`. Calls `ms.SetConcurrentCommitment(true)` so the MockState's map access is locked for parallel workers, and `pph.Release()` after each iteration.
- [x] implement `Benchmark_Commitment_DirectVsParallel(b *testing.B)` driver with two corpus sub-benches; for each, one `ModeDirect` sub-bench and one `ModeParallel-w%d` per worker count. Dedupe the `{1, 4, 8, runtime.NumCPU()}` worker list inline via `slices.Sort` + `slices.Compact` (no custom helper needed).
- [x] confirm package builds: `go build ./execution/commitment/` from worktree root

### Task 2: Smoke-run and verify

**Files:**
- (none — verification only)

- [ ] run smoke bench: `go test -run=^$ -bench=Benchmark_Commitment_DirectVsParallel -benchtime=1x ./execution/commitment/` from worktree root
- [ ] verify all sub-benches in the matrix execute without error and report non-zero ns/op
- [ ] verify ModeDirect sub-benches produce a successful root hash (no error)
- [ ] verify each ModeParallel sub-bench at every worker count produces a successful root hash (no error). If any worker count trips multi-bucket-no-split rejection, ⚠️ note it in the plan and consider lowering `MinSplitKeys` for the 100K corpus or skipping that sub-bench.
- [ ] run package tests: `go test ./execution/commitment/...` — confirm new file does not break existing tests
- [ ] run `make lint` from worktree root; iterate until clean (linter is non-deterministic)

### Task 3: Capture sample output and commit

**Files:**
- Modify: `execution/commitment/agents.md` (add a short perf snapshot)

- [ ] run a real bench: `go test -run=^$ -bench=Benchmark_Commitment_DirectVsParallel -benchtime=3x ./execution/commitment/ | tee /tmp/parallel-bench-sample.txt` (3 iterations to smooth noise without taking forever)
- [ ] append a short perf-snapshot section to `execution/commitment/agents.md` — date, machine description (CPU + cores), Go version, and a one-paragraph summary expressed as ratios (e.g. "ModeParallel-w<NumCPU> on 500K-StorageHeavy is ~Nx faster than ModeDirect; w1 is ~Mx slower than ModeDirect due to coordination overhead"). Raw ns/op numbers age badly across hardware and Go versions, but ratios on one machine convey the shape and resist rot.
- [ ] `git add execution/commitment/parallel_patricia_hashed_bench_test.go execution/commitment/agents.md`
- [ ] `git commit -m "commitment: add ModeDirect vs ModeParallel benchmark"` per CLAUDE.md commit-prefix convention

### Task 4: Final — move plan to completed/

- [ ] verify all checkboxes above are `[x]`
- [ ] `mkdir -p docs/plans/completed`
- [ ] `git mv docs/plans/20260517-direct-vs-parallel-bench.md docs/plans/completed/`
- [ ] commit with `docs: archive direct-vs-parallel-bench plan`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only*

**Manual performance runs:**
- run the benchmark on a representative production-class machine (e.g. a 16-core box) and on a smaller machine (4-core) — record both sets of ns/op for the agents.md or follow-up perf write-up
- compare ModeDirect ns/op against ModeParallel-w1 to quantify the pure-overhead cost of the parallel codepath at zero concurrency
- compare ModeParallel-w<NumCPU> against ModeDirect to quantify the realistic speedup
- check for plateau: ratio of ModeParallel-w4 vs ModeParallel-w8 vs ModeParallel-w<NumCPU> on the 500K-StorageHeavy corpus

**Followups (not in this plan):**
- warmup-enabled variant of the same matrix (matches production wiring closer)
- MinSplitKeys sweep (would expose tradeoff between split-overhead and parallelism)
- bloatnet-shape corpus (250 accounts × 36K storage slots = 9M total) — `testing.Short()` gated; large RAM
- automated regression check that flags >20% slowdown vs a baseline snapshot
