// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// benchCPUSink keeps burnCPU's result observable so the compiler cannot elide
// the synthetic work.
var benchCPUSink atomic.Uint64

// burnCPU spins a tunable arithmetic loop standing in for the per-touch block
// execution cost the streaming committer overlaps its background folds with.
func burnCPU(iters int) {
	var x uint64 = 1469598103934665603
	for i := range iters {
		x = (x ^ uint64(i)) * 1099511628211
	}
	benchCPUSink.Add(x)
}

// buildStreamingWhaleCorpus is a bench-scale whale: one big-storage account
// (deep fan-out) plus accounts spread across the top nibbles so the background
// scheduler has multiple splits to fold in parallel under execution.
func buildStreamingWhaleCorpus() ([][]byte, []Update) {
	return buildBigAccountCorpus(40_000)
}

// streamingBenchCorpora is the (whale, mixed) pair Task 9 measures.
func streamingBenchCorpora() []struct {
	name string
	pk   [][]byte
	upds []Update
} {
	wk, wu := buildStreamingWhaleCorpus()
	mk, mu := buildMixedCorpus(99, 20_000)
	return []struct {
		name string
		pk   [][]byte
		upds []Update
	}{
		{"whale", wk, wu},
		{"mixed", mk, mu},
	}
}

// runStreamingOverlapBench drives one full touch→Process cycle per b.Loop
// iteration: a synthetic CPU cost is burned before every touch, then Process
// merges. With scheduler=true the background pool folds splits during the burn
// (overlap); with scheduler=false everything folds at Process (batch baseline).
// It reports Process-only time and per-op re-fold count alongside wall-clock.
func runStreamingOverlapBench(b *testing.B, pk [][]byte, upds []Update, cpuIters int, scheduler bool) {
	ctx := context.Background()
	b.ReportAllocs()
	var (
		totalProcess time.Duration
		totalRefold  uint64
		iters        int
	)
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		ms.SetConcurrentCommitment(true)
		require.NoError(b, ms.applyPlainUpdates(pk, upds))
		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(runtime.NumCPU())
		if scheduler {
			require.NoError(b, sc.StartScheduler(ctx))
		}
		b.StartTimer()

		for _, k := range pk {
			burnCPU(cpuIters)
			sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
		}
		procStart := time.Now()
		_, err := sc.Process(ctx)
		procDur := time.Since(procStart)

		b.StopTimer()
		require.NoError(b, err)
		totalProcess += procDur
		totalRefold += sc.RefoldCount()
		iters++
		sc.Release()
		b.StartTimer()
	}
	if iters > 0 {
		b.ReportMetric(float64(totalProcess.Nanoseconds())/float64(iters), "process-ns/op")
		b.ReportMetric(float64(totalRefold)/float64(iters), "refolds/op")
	}
}

// Benchmark_StreamingOverlap is a MECHANISM SANITY-CHECK, not a performance
// claim. It interleaves a tunable synthetic CPU cost per touch (standing in for
// block execution) with background folds and compares overlap (scheduler) vs
// batch (touch-all-then-Process) total wall-clock plus Process-only time and
// re-fold count. The synthetic number must NOT be cited as a headline win — the
// real measurement is the live-node run (Post-Completion).
func Benchmark_StreamingOverlap(b *testing.B) {
	for _, c := range streamingBenchCorpora() {
		for _, cpu := range []int{0, 500, 5000} {
			b.Run(fmt.Sprintf("%s/cpu=%d/overlap", c.name, cpu), func(b *testing.B) {
				runStreamingOverlapBench(b, c.pk, c.upds, cpu, true)
			})
			b.Run(fmt.Sprintf("%s/cpu=%d/batch", c.name, cpu), func(b *testing.B) {
				runStreamingOverlapBench(b, c.pk, c.upds, cpu, false)
			})
		}
	}
}

// streamingCycle runs one full overlap cycle and returns total wall-clock,
// Process-only time, and the re-fold count — the metric primitive the bench and
// the metrics test share.
func streamingCycle(tb testing.TB, pk [][]byte, upds []Update, cpuIters int, scheduler bool) (total, process time.Duration, refolds uint64) {
	ctx := context.Background()
	ms := NewMockState(tb)
	ms.SetConcurrentCommitment(true)
	require.NoError(tb, ms.applyPlainUpdates(pk, upds))
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(runtime.NumCPU())
	if scheduler {
		require.NoError(tb, sc.StartScheduler(ctx))
	}

	start := time.Now()
	for _, k := range pk {
		burnCPU(cpuIters)
		sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	procStart := time.Now()
	_, err := sc.Process(ctx)
	process = time.Since(procStart)
	total = time.Since(start)
	require.NoError(tb, err)
	return total, process, sc.RefoldCount()
}

// TestStreaming_Metrics reports re-fold count and Process-only time for the
// whale and mixed corpora, overlap vs batch, at a fixed synthetic CPU cost. It
// is the reproducible source for the numbers documented in the plan — a
// mechanism sanity-check, NOT a perf claim. Skipped under -short.
func TestStreaming_Metrics(t *testing.T) {
	if testing.Short() {
		t.Skip("metrics report is heavy; skipped under -short")
	}
	const cpu = 2000
	for _, c := range streamingBenchCorpora() {
		bTotal, bProc, _ := streamingCycle(t, c.pk, c.upds, cpu, false)
		oTotal, oProc, oRefold := streamingCycle(t, c.pk, c.upds, cpu, true)
		t.Logf("%-6s keys=%d cpu=%d | batch:   total=%-12v process=%-12v",
			c.name, len(c.pk), cpu, bTotal, bProc)
		t.Logf("%-6s keys=%d cpu=%d | overlap: total=%-12v process=%-12v refolds=%d",
			c.name, len(c.pk), cpu, oTotal, oProc, oRefold)
	}
}
