// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-only

package commitment

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

// Benchmark_ProcessCPUUtil reports whole-Process CPU utilization of the parallel commitment engine:
// average cores busy (utime+stime over the Process window / wall), plus per-op wall and CPU. Unlike
// Benchmark_FoldUtilization (which measures only the fold scheduler's internal busy-integral), this
// includes the serial phases around the fold — prefix build, seeding, the deferred write drain, and
// concurrent GC — so avg-cores << numWorkers quantifies the serial-phase headroom directly.
func Benchmark_ProcessCPUUtil(b *testing.B) {
	ncpu := runtime.NumCPU()
	workers := slices.Compact(slices.Sorted(slices.Values([]int{1, ncpu, ncpu * 2})))
	wpk, wupds := buildWhaleCorpus(whale1M())

	b.Run("1MWhales", func(b *testing.B) {
		b.Logf("corpus keys=%d ncpu=%d", len(wpk), ncpu)
		for _, w := range workers {
			b.Run(fmt.Sprintf("flag-off/w%d", w), func(b *testing.B) {
				benchForkWholeFresh(b, false)
				runProcessCPUUtilBench(b, wpk, wupds, w)
			})
			b.Run(fmt.Sprintf("flag-on/w%d", w), func(b *testing.B) {
				benchForkWholeFresh(b, true)
				runProcessCPUUtilBench(b, wpk, wupds, w)
			})
		}
	})
}

func runProcessCPUUtilBench(b *testing.B, pk [][]byte, updates []Update, workers int) {
	ctx := context.Background()
	b.ReportAllocs()

	var pph *ParallelPatriciaHashed
	defer func() {
		if pph != nil {
			pph.Release()
		}
	}()

	var (
		wallSum time.Duration
		cpuSum  time.Duration
		iters   int
	)
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		ms.SetConcurrentCommitment(true)
		require.NoError(b, ms.applyPlainUpdates(pk, updates))
		if pph == nil {
			pph = NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
			pph.SetNumWorkers(workers)
		} else {
			pph.SetTrieContextFactory(mockTrieCtxFactory(ms))
			pph.ResetContext(ms)
		}
		pph.RootTrie().Reset()
		upds := wrapCarriedUpdates(b, pk, updates)

		var ru0, ru1 syscall.Rusage
		require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &ru0))
		wall0 := time.Now()
		b.StartTimer()

		_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		wall := time.Since(wall0)
		require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &ru1))
		require.NoError(b, err)

		wallSum += wall
		cpuSum += rusageCPU(&ru1) - rusageCPU(&ru0)
		iters++
		upds.Close()
		b.StartTimer()
	}

	if iters == 0 || wallSum == 0 {
		return
	}
	b.ReportMetric(float64(cpuSum)/float64(wallSum), "avg-cores")
	b.ReportMetric(float64(wallSum.Nanoseconds())/float64(iters)/1e6, "wall-ms/op")
	b.ReportMetric(float64(cpuSum.Nanoseconds())/float64(iters)/1e6, "cpu-ms/op")
}

// Benchmark_SerialVsForkCPU puts the golden serial (ModeDirect HexPatriciaHashed) and the shipped
// fresh-build fork (flag-on, NumCPU workers) on the same 1M-whale corpus, reporting wall + avg-cores
// so the parallel speedup and remaining serial ceiling are visible against the true serial baseline.
func Benchmark_SerialVsForkCPU(b *testing.B) {
	wpk, wupds := buildWhaleCorpus(whale1M())
	b.Run("1MWhales/serial-direct", func(b *testing.B) {
		runSerialCPUUtilBench(b, wpk, wupds)
	})
	b.Run("1MWhales/fork-w18", func(b *testing.B) {
		benchForkWholeFresh(b, true)
		runProcessCPUUtilBench(b, wpk, wupds, runtime.NumCPU())
	})
}

func runSerialCPUUtilBench(b *testing.B, pk [][]byte, updates []Update) {
	ctx := context.Background()
	b.ReportAllocs()

	var (
		wallSum time.Duration
		cpuSum  time.Duration
		iters   int
	)
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		require.NoError(b, ms.applyPlainUpdates(pk, updates))
		trie := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, updates)

		var ru0, ru1 syscall.Rusage
		require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &ru0))
		wall0 := time.Now()
		b.StartTimer()

		_, err := trie.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		wall := time.Since(wall0)
		require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &ru1))
		require.NoError(b, err)

		wallSum += wall
		cpuSum += rusageCPU(&ru1) - rusageCPU(&ru0)
		iters++
		upds.Close()
		trie.Release()
		b.StartTimer()
	}

	if iters == 0 || wallSum == 0 {
		return
	}
	b.ReportMetric(float64(cpuSum)/float64(wallSum), "avg-cores")
	b.ReportMetric(float64(wallSum.Nanoseconds())/float64(iters)/1e6, "wall-ms/op")
	b.ReportMetric(float64(cpuSum.Nanoseconds())/float64(iters)/1e6, "cpu-ms/op")
}

type nibTiming struct {
	nib int
	cnt uint32
	d   time.Duration
}

// Benchmark_FreshNibTimeline captures the per-top-nibble fold wall time of the serial dispatch loop
// in dispatchWholeFresh over one fresh 1M-whale Process, to locate the critical path: if the 16
// durations sum to ~the total Process wall, the top-nibble subtrees run serially (no overlap); a
// single dominant duration is the whale nibble carrying the tail.
func Benchmark_FreshNibTimeline(b *testing.B) {
	wpk, wupds := buildWhaleCorpus(whale1M())
	benchForkWholeFresh(b, true)

	var mu sync.Mutex
	var timings []nibTiming
	onNibFold = func(nib int, cnt uint32, d time.Duration) {
		mu.Lock()
		timings = append(timings, nibTiming{nib, cnt, d})
		mu.Unlock()
	}
	defer func() { onNibFold = nil }()

	ctx := context.Background()
	ms := NewMockState(b)
	ms.SetConcurrentCommitment(true)
	require.NoError(b, ms.applyPlainUpdates(wpk, wupds))
	pph := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	pph.SetNumWorkers(runtime.NumCPU())
	defer pph.Release()
	pph.RootTrie().Reset()
	upds := wrapCarriedUpdates(b, wpk, wupds)

	t0 := time.Now()
	_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})
	total := time.Since(t0)
	require.NoError(b, err)
	upds.Close()

	slices.SortFunc(timings, func(a, c nibTiming) int { return int(c.d - a.d) })
	var sum time.Duration
	for _, t := range timings {
		sum += t.d
	}
	b.Logf("Process wall=%v  sum(nib folds)=%v  ratio=%.2f  nibs=%d",
		total.Round(time.Millisecond), sum.Round(time.Millisecond),
		float64(sum)/float64(total), len(timings))
	for _, t := range timings {
		b.Logf("  nib=%x subtreeCount=%-8d fold=%-8v %.1f%% of Process",
			t.nib, t.cnt, t.d.Round(time.Millisecond), 100*float64(t.d)/float64(total))
	}
}

func rusageCPU(ru *syscall.Rusage) time.Duration {
	u := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	s := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return u + s
}
