// these tests have cleanup issues for mdbx on windows
package stagedsync

import (
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkLessConflicts runs the full low-contention parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkLessConflicts -benchtime=1x
func BenchmarkLessConflicts(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						tasks, serialDuration := taskFactory(numTx, lessConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkLessConflictsWithMetadata runs the low-contention parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkLessConflictsWithMetadata -benchtime=1x
func BenchmarkLessConflictsWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		tasks, serialDuration := taskFactory(numTx, lessConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkMoreConflicts runs the full high-contention parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkMoreConflicts -benchtime=1x
func BenchmarkMoreConflicts(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkMoreConflictsWithMetadata runs the high-contention parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkMoreConflictsWithMetadata -benchtime=1x
func BenchmarkMoreConflictsWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkRandomTx runs the full random-sender parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkRandomTx -benchtime=1x
func BenchmarkRandomTx(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						tasks, serialDuration := taskFactory(numTx, randomSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkRandomTxWithMetadata runs the random-sender parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkRandomTxWithMetadata -benchtime=1x
func BenchmarkRandomTxWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		tasks, serialDuration := taskFactory(numTx, randomSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkTxWithLongTailRead runs the full parameter sweep with occasional 100x read latency spikes.
// Run with: go test -run='^$' -bench=BenchmarkTxWithLongTailRead -benchtime=1x
func BenchmarkTxWithLongTailRead(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}
	if testing.Short() {
		totalTxs = totalTxs[:1]
		numReads = numReads[:1]
		numWrites = numWrites[:1]
		numNonIO = numNonIO[:1]
	}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)
						tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkTxWithLongTailReadWithMetadata runs the long-tail-read parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkTxWithLongTailReadWithMetadata -benchtime=1x
func BenchmarkTxWithLongTailReadWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)
		tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkAlternatingTx runs the alternating-sender parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkAlternatingTx -benchtime=1x
func BenchmarkAlternatingTx(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) accounts.Address {
			return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % 2))))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		return runParallel(b, tasks, defaultChecks, false, logger), serialDuration
	}

	testExecutorComb(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkAlternatingTxWithMetadata runs the alternating-sender parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkAlternatingTxWithMetadata -benchtime=1x
func BenchmarkAlternatingTxWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) accounts.Address {
			return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % 2))))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkDexScenario runs the full DEX parameter sweep (5×3×3×2 = 90 combinations) and
// reports parallel speedup over expected serial duration.
// Run with: go test -run='^$' -bench=BenchmarkDexScenario -benchtime=1x
func BenchmarkDexScenario(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, dexPostValidation, checkNoDroppedTx})

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						sender := func(i int) accounts.Address {
							return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i))))
						}
						tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, dexPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, checks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkDexScenarioWithMetadata runs the full DEX+metadata parameter sweep and reports
// speedup with and without pre-computed dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkDexScenarioWithMetadata -benchtime=1x
func BenchmarkDexScenarioWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, dexPostValidation, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) accounts.Address { return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i)))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, dexPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(b, tasks, checks, false, logger)

		allDeps := runParallelGetMetadata(b, tasks, checks)
		newTasks := make([]exec.Task, 0, len(tasks))
		for _, task := range tasks {
			temp := task.(*testExecTask)
			keys := make([]int, 0, len(allDeps[temp.Version().TxIndex]))
			for k := range allDeps[temp.Version().TxIndex] {
				keys = append(keys, k)
			}
			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}
		return parallelDuration, runParallel(b, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkDispatchPendingCompaction holds back and dispatches a fixed prefix in
// front of a growing suffix. The compaction never touches the suffix, so the
// time stays flat as the suffix grows.
func BenchmarkDispatchPendingCompaction(b *testing.B) {
	const held, consumed = 8, 8
	prefix := held + consumed
	decide := func(tx int) dispatchAction {
		switch {
		case tx < held:
			return dispatchHold
		case tx < prefix:
			return dispatchConsume
		default:
			return dispatchStop
		}
	}
	for _, suffix := range []int{1024, 4096, 16384, 65536} {
		total := prefix + suffix
		base := make([]int, total)
		for i := range base {
			base[i] = i
		}
		buf := make([]int, total)
		b.Run(fmt.Sprintf("suffix=%d", suffix), func(b *testing.B) {
			m := &execStatusList{}
			m.ensureLen(total)
			copy(buf, base) // dispatchPending only rewrites the prefix, so the suffix stays valid
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(buf[:prefix], base[:prefix])
				m.pending = buf
				m.dispatchPending(decide)
			}
		})
	}
}
