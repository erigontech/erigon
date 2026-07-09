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
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func runDirectBench(b *testing.B, pk [][]byte, updates []Update) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		require.NoError(b, ms.applyPlainUpdates(pk, updates))
		hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, updates)
		b.StartTimer()

		_, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		upds.Close()
		b.StartTimer()
	}
}

func runParallelBench(b *testing.B, pk [][]byte, updates []Update, workers int) {
	ctx := context.Background()
	b.ReportAllocs()
	// pph is reused across iterations so the worker pool amortizes.
	var pph *ParallelPatriciaHashed
	defer func() {
		if pph != nil {
			pph.Release()
		}
	}()
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		ms.SetConcurrentCommitment(true)
		require.NoError(b, ms.applyPlainUpdates(pk, updates))
		if pph == nil {
			pph = NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
			pph.SetNumWorkers(workers)
		} else {
			// Rewire MockState without Reset()/Release(), which would drop the worker pool.
			pph.SetTrieContextFactory(mockTrieCtxFactory(ms))
			pph.ResetContext(ms)
		}
		pph.RootTrie().Reset()
		upds := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, updates)
		b.StartTimer()

		_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		upds.Close()
		b.StartTimer()
	}
}

func benchWorkerCounts() []int {
	w := []int{1, 4, 8, runtime.NumCPU()}
	slices.Sort(w)
	return slices.Compact(w)
}

func Benchmark_Commitment_SmallCounts(b *testing.B) {
	workers := benchWorkerCounts()
	for _, nKeys := range []int{10, 20, 241, 1546} {
		pk, updates := buildMixedCorpus(int64(nKeys)*1_000_003+7, nKeys)
		b.Run(fmt.Sprintf("keys=%d", nKeys), func(b *testing.B) {
			b.Run("ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
			for _, w := range workers {
				b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) { runParallelBench(b, pk, updates, w) })
			}
		})
	}
}

// ModeParallel is the frontier-pool engine; ModeDirect is the sequential baseline. The former
// static top-nibble partition arm was deleted with the whale fan-out (plan Task 6), so it is no
// longer an in-tree comparison point — frontier-pool vs sequential is the gate.
func Benchmark_Commitment_1MWhales(b *testing.B) {
	pk, updates := buildWhaleCorpus(whale1M())
	b.Logf("corpus keys=%d", len(pk))
	ncpu := runtime.NumCPU()
	workers := []int{ncpu, ncpu * 2, ncpu * 4}
	slices.Sort(workers)
	workers = slices.Compact(workers)
	b.Run("ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
	for _, w := range workers {
		b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) { runParallelBench(b, pk, updates, w) })
	}
}

func Benchmark_Commitment_DirectVsParallel(b *testing.B) {
	workers := []int{1, 4, 8, runtime.NumCPU()}
	slices.Sort(workers)
	workers = slices.Compact(workers)

	b.Run("100K-AccountsOnly", func(b *testing.B) {
		pk, updates := build100KAccountsCorpus(b)

		b.Run("ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
		for _, w := range workers {
			b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) {
				runParallelBench(b, pk, updates, w)
			})
		}
	})

	b.Run("500K-StorageHeavy", func(b *testing.B) {
		pk, updates := build500KStorageHeavyCorpus(b)

		b.Run("ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
		for _, w := range workers {
			b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) {
				runParallelBench(b, pk, updates, w)
			})
		}
	})
}

// Accounts are pinned to distinct top nibbles so their sub-tries don't share branches.
func buildClusteredStorageCorpus(b testing.TB, numAccounts, slotsPerAccount int) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(99001))
	ub := NewUpdateBuilder()
	for i := 0; i < numAccounts; i++ {
		addNibbleAccount(ub, rnd, i%16, i, slotsPerAccount)
	}
	return ub.Build()
}

func Benchmark_Commitment_Clustered(b *testing.B) {
	for _, c := range []struct {
		name     string
		accounts int
		slots    int
	}{
		{"4acct-500K", 4, 125_000},
		{"8acct-500K", 8, 62_500},
	} {
		pk, updates := buildClusteredStorageCorpus(b, c.accounts, c.slots)
		b.Run(c.name+"/ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
		for _, w := range []int{1, 4, 8, 18} {
			b.Run(fmt.Sprintf("%s/ModeParallel-w%d", c.name, w), func(b *testing.B) {
				runParallelBench(b, pk, updates, w)
			})
		}
	}
}

type storageGroup struct {
	pk      [][]byte
	updates []Update
}

// Splits one whale account's slots into disjoint, independently processable sub-tries.
func buildWhaleStorageGroups(slots, groups int) []storageGroup {
	rnd := rand.New(rand.NewSource(919273))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)

	ubs := make([]*UpdateBuilder, groups)
	for i := range ubs {
		ubs[i] = NewUpdateBuilder()
		ubs[i].Balance(a, rnd.Uint64()+1)
	}
	for i := 0; i < slots; i++ {
		addRandomSlot(ubs[i%groups], rnd, a)
	}

	out := make([]storageGroup, groups)
	for i := range ubs {
		pk, upd := ubs[i].Build()
		out[i] = storageGroup{pk: pk, updates: upd}
	}
	return out
}

type groupRun struct {
	hph  *HexPatriciaHashed
	upds *Updates
}

// Must run on the test goroutine (uses require); each group gets its own MockState so concurrent process() shares no state.
func setupGroup(tb testing.TB, g storageGroup) groupRun {
	ms := NewMockState(tb)
	require.NoError(tb, ms.applyPlainUpdates(g.pk, g.updates))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	upds := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, g.pk, g.updates)
	return groupRun{hph: hph, upds: upds}
}

// process is safe to call from any goroutine (no require/FailNow).
func (r groupRun) process() error {
	_, err := r.hph.Process(context.Background(), r.upds, "", nil, WarmupConfig{})
	return err
}

func setupGroups(tb testing.TB, gs []storageGroup) []groupRun {
	rs := make([]groupRun, len(gs))
	for i := range gs {
		rs[i] = setupGroup(tb, gs[i])
	}
	return rs
}

func closeGroups(rs []groupRun) {
	for _, r := range rs {
		r.upds.Close()
	}
}

func Benchmark_StorageConcurrency(b *testing.B) {
	ncpu := runtime.NumCPU()
	poolWorkers := slices.Compact(slices.Sorted(slices.Values([]int{ncpu, ncpu * 2, ncpu * 4})))
	for _, slots := range []int{750_000} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			single := buildWhaleStorageGroups(slots, 1)
			b.Run("Single", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					r := setupGroup(b, single[0])
					b.StartTimer()
					require.NoError(b, r.process())
					b.StopTimer()
					r.upds.Close()
					b.StartTimer()
				}
			})
			// The real frontier-pool engine over the same single whale: Single/Groups-* above are the
			// synthetic independent-trie ceiling; this is what the engine actually achieves.
			for _, w := range poolWorkers {
				b.Run(fmt.Sprintf("FrontierPool-w%d", w), func(b *testing.B) {
					runParallelBench(b, single[0].pk, single[0].updates, w)
				})
			}

			for _, groups := range []int{4, 8, 16} {
				gs := buildWhaleStorageGroups(slots, groups)
				b.Run(fmt.Sprintf("Groups%d-Serial", groups), func(b *testing.B) {
					for b.Loop() {
						b.StopTimer()
						rs := setupGroups(b, gs)
						b.StartTimer()
						for _, r := range rs {
							require.NoError(b, r.process())
						}
						b.StopTimer()
						closeGroups(rs)
						b.StartTimer()
					}
				})
				b.Run(fmt.Sprintf("Groups%d-Parallel", groups), func(b *testing.B) {
					for b.Loop() {
						b.StopTimer()
						rs := setupGroups(b, gs)
						b.StartTimer()
						var eg errgroup.Group
						for _, r := range rs {
							r := r
							eg.Go(r.process)
						}
						require.NoError(b, eg.Wait())
						b.StopTimer()
						closeGroups(rs)
						b.StartTimer()
					}
				})
			}
		})
	}
}

// Keeps burnCPU's result observable so the compiler cannot elide the synthetic work.
var benchCPUSink atomic.Uint64

// Synthetic per-touch CPU cost standing in for block execution.
func burnCPU(iters int) {
	var x uint64 = 1469598103934665603
	for i := range iters {
		x = (x ^ uint64(i)) * 1099511628211
	}
	benchCPUSink.Add(x)
}

func streamingBenchCorpora() []struct {
	name string
	pk   [][]byte
	upds []Update
} {
	wk, wu := buildWhaleCorpus(bigAccountWhale(40_000))
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

// scheduler=true overlaps folds with the per-touch CPU burn; false defers all folds to Process.
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

// Mechanism sanity-check with synthetic CPU cost; numbers are not a performance claim.
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

// The real frontier-pool engine (ModeParallel) is compared against the sequential baseline
// (Sequential) and the synthetic per-nibble ceiling (ConcurrentStorage-*). numWorkers sweeps
// {NumCPU, 2×, 4×} so the >fan-out utilization behavior on a mega-whale is visible.
func Benchmark_DeepStorageWhale(b *testing.B) {
	ncpu := runtime.NumCPU()
	poolWorkers := slices.Compact(slices.Sorted(slices.Values([]int{ncpu, ncpu * 2, ncpu * 4})))
	for _, slots := range []int{750_000} {
		addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(slots)
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			b.Run("Sequential", func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					ms := NewMockState(b)
					require.NoError(b, ms.applyPlainUpdates(pk, upds))
					hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
					upd := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, upds)
					b.StartTimer()
					_, err := hph.Process(context.Background(), upd, "", nil, WarmupConfig{})
					b.StopTimer()
					require.NoError(b, err)
					upd.Close()
					b.StartTimer()
				}
			})
			for _, w := range poolWorkers {
				b.Run(fmt.Sprintf("ModeParallel-w%d", w), func(b *testing.B) { runParallelBench(b, pk, upds, w) })
			}
			for _, parallel := range []bool{false, true} {
				name := "ConcurrentStorage-serial"
				if parallel {
					name = "ConcurrentStorage-parallel"
				}
				b.Run(name, func(b *testing.B) {
					for b.Loop() {
						b.StopTimer()
						ms := NewMockState(b)
						require.NoError(b, ms.applyPlainUpdates(pk, upds))
						b.StartTimer()
						_, err := concurrentAccountRoot(ms, addr, accHash, accNib, accUpd, groups, parallel)
						b.StopTimer()
						require.NoError(b, err)
						b.StartTimer()
					}
				})
			}
		})
	}
}

// foldKForC sizes the leaf/merge boundary for an oversubscription factor c, mirroring the
// production foldK (which pins c=1) so the c-sweep can measure alternative constants without
// mutating the shipped policy.
func foldKForC(total uint32, numWorkers, c int) uint32 {
	if numWorkers < 1 {
		numWorkers = 1
	}
	if c < 1 {
		c = 1
	}
	k := total / (uint32(c) * uint32(numWorkers))
	if k < foldKMin {
		return foldKMin
	}
	return k
}

// busyInterval is one task's fold window relative to the dispatch start.
type busyInterval struct {
	start, end time.Duration
}

// utilStats summarizes how busy the worker pool stayed over one dispatch: busy is the summed
// per-task fold time, span the wall-clock of the dispatch, and tailFrac the fraction of span
// after concurrency last fell below 2 — the serial-spine collapse the frontier design trades
// idle-tail against static 16-way ownership.
type utilStats struct {
	tasks       int
	span        time.Duration
	busy        time.Duration
	maxParallel int
	tailFrac    float64
}

// foldUtilStats reconstructs the concurrency profile from the per-task intervals via an event
// sweep and derives the busy integral, peak parallelism, and serial-collapse tail.
func foldUtilStats(iv []busyInterval, span time.Duration) utilStats {
	st := utilStats{tasks: len(iv), span: span}
	if len(iv) == 0 {
		return st
	}
	type ev struct {
		t time.Duration
		d int
	}
	evs := make([]ev, 0, 2*len(iv))
	for _, x := range iv {
		st.busy += x.end - x.start
		evs = append(evs, ev{x.start, +1}, ev{x.end, -1})
	}
	// Ends (-1) sort before starts (+1) at the same instant so a hand-off is not counted as an
	// extra concurrent worker.
	slices.SortFunc(evs, func(a, b ev) int {
		if a.t != b.t {
			return int(a.t - b.t)
		}
		return a.d - b.d
	})
	cur := 0
	var lastGE2End time.Duration
	prevGE2 := false
	for _, e := range evs {
		if prevGE2 {
			lastGE2End = e.t
		}
		cur += e.d
		if cur > st.maxParallel {
			st.maxParallel = cur
		}
		prevGE2 = cur >= 2
	}
	if span > 0 {
		st.tailFrac = float64(span-lastGE2End) / float64(span)
	}
	return st
}

func (s *utilStats) add(o utilStats) {
	s.tasks += o.tasks
	s.span += o.span
	s.busy += o.busy
	if o.maxParallel > s.maxParallel {
		s.maxParallel = o.maxParallel
	}
	s.tailFrac += o.tailFrac
}

func (s utilStats) report(b *testing.B, iters, workers int, k, total uint32) {
	if iters == 0 {
		return
	}
	span := float64(s.span.Nanoseconds()) / float64(iters)
	busy := float64(s.busy.Nanoseconds()) / float64(iters)
	b.ReportMetric(span, "dispatch-ns/op")
	if span > 0 {
		b.ReportMetric(busy/span, "avg-parallel")
		b.ReportMetric(100*busy/(span*float64(workers)), "util-%")
	}
	b.ReportMetric(float64(s.maxParallel), "max-parallel")
	b.ReportMetric(100*s.tailFrac/float64(iters), "serial-tail-%")
	b.ReportMetric(float64(s.tasks)/float64(iters), "tasks/op")
	b.Logf("k=%d total=%d workers=%d", k, total, workers)
}

// buildRetouchedWhale seeds a whale (top nibble 0xd) with slots random storage slots plus a few
// spread accounts (batch1), then re-touches every slot and the balance (batch2). After batch1 the
// depth-64 account prefix carries an on-disk branch, so batch2's fold derives a seedable storage
// merge that fans out across the first-storage-nibble subtries — the mega-whale utilization case.
func buildRetouchedWhale(seed int64, slots int) (batch1, batch2 engineBatch) {
	rnd := rand.New(rand.NewSource(seed))
	waddr := findAddressForNibble(0xd, int(seed))
	whale := addrHex(waddr)

	locs := make([]string, slots)
	ub1 := NewUpdateBuilder()
	ub1.Balance(whale, 12345)
	for i := range slots {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		val := make([]byte, 32)
		rnd.Read(val)
		locs[i] = hex.EncodeToString(loc)
		ub1.Storage(whale, locs[i], hex.EncodeToString(val))
	}
	for _, nib := range []int{2, 6, 0xa} {
		ub1.Balance(addrHex(findAddressForNibble(nib, int(seed)+nib)), uint64(8000+nib))
	}
	k1, u1 := ub1.Build()

	ub2 := NewUpdateBuilder()
	ub2.Balance(whale, 55555)
	for _, l := range locs {
		val := make([]byte, 32)
		rnd.Read(val)
		ub2.Storage(whale, l, hex.EncodeToString(val))
	}
	k2, u2 := ub2.Build()
	return engineBatch{k1, u1}, engineBatch{k2, u2}
}

// newBenchFoldBase builds the finale root base the top-nibble tasks stitch into — unfolded one
// level at the on-disk root branch, then walled at the top nibble, exactly StreamingCommitter's
// buildBase. Returns the base and a cleanup that releases it and its factory context.
func newBenchFoldBase(b *testing.B, ctx context.Context, factory TrieContextFactory) (*HexPatriciaHashed, func()) {
	base := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	bctx, bclean := factory()
	base.ResetContext(bctx)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)
	require.NoError(b, unfoldRootWall(ctx, base))
	seedRootBase(base)
	return base, func() {
		base.Release()
		if bclean != nil {
			bclean()
		}
	}
}

// dispatchFoldInstrumented seeds every merge base and folds the DAG below rootTask through the real
// worker pool, bracketing each task's foldOne with wall-clock timestamps so the caller gets a
// worker-busy-over-time profile. It reproduces foldPool.run's seeding and cleanup but wraps the
// fold callback dispatchFoldTasks already accepts as a parameter, so no production code is touched.
// The finale root fold (the serial spine) is intentionally excluded — the utilization window is the
// parallel dispatch itself.
func dispatchFoldInstrumented(ctx context.Context, pool *foldPool, rootTask *foldTask) (utilStats, error) {
	subTasks := collectFoldTasks(rootTask, nil)
	defer func() {
		for _, t := range subTasks {
			if t.baseCleanup != nil {
				t.baseCleanup()
				t.baseCleanup = nil
			}
		}
	}()
	for _, t := range subTasks {
		if t.kind != foldMerge {
			continue
		}
		if err := pool.seedMerge(t); err != nil {
			return utilStats{}, err
		}
	}

	var (
		mu sync.Mutex
		iv []busyInterval
	)
	t0 := time.Now()
	fold := func(ctx context.Context, t *foldTask) error {
		s := time.Since(t0)
		err := pool.foldOne(ctx, t)
		e := time.Since(t0)
		mu.Lock()
		iv = append(iv, busyInterval{s, e})
		mu.Unlock()
		return err
	}
	derr := dispatchFoldTasks(ctx, pool.numWorkers, rootTask, subTasks, fold)
	span := time.Since(t0)
	if derr != nil {
		return utilStats{}, derr
	}
	stats := foldUtilStats(iv, span)
	recycleTaskDeferred(subTasks)
	return stats, nil
}

// runFoldUtilBench measures pool utilization on batch2's fold after batch1 has seeded the on-disk
// branch store, at the given worker count and oversubscription factor c. The corpus build and
// batch1 seeding happen once; each iteration rebuilds only the per-Process DAG and root base.
func runFoldUtilBench(b *testing.B, batch1, batch2 engineBatch, workers, c int) {
	ctx := context.Background()
	b.ReportAllocs()

	ms := NewMockState(b)
	ms.SetConcurrentCommitment(true)
	seqTrie := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	processBatch(b, ms, seqTrie, batch1.keys, batch1.upds)
	seqTrie.Release()
	require.NoError(b, ms.applyPlainUpdates(batch2.keys, batch2.upds))

	factory := mockTrieCtxFactory(ms)
	seedable := func(prefix []byte) bool {
		bb, _, err := ms.Branch(nibbles.HexToCompact(prefix))
		require.NoError(b, err)
		return len(bb) > 0
	}
	buildTrie := func() *prefixTrie {
		tr := newPrefixTrie()
		for i, k := range batch2.keys {
			tr.Insert(KeyToHexNibbleHash(k), k, &batch2.upds[i])
		}
		return tr
	}
	total := buildTrie().root.subtreeCount
	k := foldKForC(total, workers, c)

	var (
		agg   utilStats
		iters int
	)
	for b.Loop() {
		b.StopTimer()
		rootTask := deriveFoldFrontier(buildTrie().root, k, seedable)
		require.NotNil(b, rootTask)
		base, cleanupBase := newBenchFoldBase(b, ctx, factory)
		rootTask.base = base
		pool := &foldPool{
			numWorkers: workers,
			ctxFactory: factory,
			workerPool: &sync.Pool{New: func() any { return NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()) }},
		}
		b.StartTimer()

		stats, err := dispatchFoldInstrumented(ctx, pool, rootTask)

		b.StopTimer()
		require.NoError(b, err)
		agg.add(stats)
		iters++
		cleanupBase()
		b.StartTimer()
	}
	agg.report(b, iters, workers, k, total)
}

func foldUtilCorpora() []struct {
	name           string
	batch1, batch2 engineBatch
} {
	mk, mu := buildMixedCorpus(99, 20_000)
	w1, w2 := buildRetouchedWhale(717, 120_000)
	return []struct {
		name           string
		batch1, batch2 engineBatch
	}{
		{"mixed20k", engineBatch{mk, mu}, engineBatch{mk, mu}},
		{"whale120k", w1, w2},
	}
}

// Benchmark_FoldUtilization reports the worker-busy-over-time profile (util-%, avg/max parallelism,
// serial-tail-%) of the frontier dispatch across worker counts — the idle-tail metric the perf gate
// needs beyond aggregate ns/op. c is pinned to the shipped value (1).
func Benchmark_FoldUtilization(b *testing.B) {
	ncpu := runtime.NumCPU()
	workers := slices.Compact(slices.Sorted(slices.Values([]int{ncpu, ncpu * 2, ncpu * 4})))
	for _, corpus := range foldUtilCorpora() {
		for _, w := range workers {
			b.Run(fmt.Sprintf("%s/w%d", corpus.name, w), func(b *testing.B) {
				runFoldUtilBench(b, corpus.batch1, corpus.batch2, w, 1)
			})
		}
	}
}

// Benchmark_FoldKSweep sweeps the oversubscription factor c at numWorkers=NumCPU. Raising c shrinks
// K and multiplies tasks (each paying ctx+pin+seed); the sweep records whether that buys utilization
// or just adds overhead, fixing the shipped constant with evidence rather than assertion.
func Benchmark_FoldKSweep(b *testing.B) {
	ncpu := runtime.NumCPU()
	for _, corpus := range foldUtilCorpora() {
		for _, c := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("%s/c%d", corpus.name, c), func(b *testing.B) {
				runFoldUtilBench(b, corpus.batch1, corpus.batch2, ncpu, c)
			})
		}
	}
}
