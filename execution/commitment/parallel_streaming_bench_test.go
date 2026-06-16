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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/length"
)

func runDirectBench(b *testing.B, pk [][]byte, updates []Update) {
	ctx := context.Background()
	b.ReportAllocs()
	// b.Loop requires the timer to be running on entry, so each iteration ends
	// with StartTimer to bracket the (untimed) teardown before the next check.
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
	// pph lives across iterations: production usage is a long-lived instance
	// servicing many blocks, so the worker pool gets to actually reuse hph
	// instances. Re-creating pph per iteration (with .Release() dropping the
	// pool) would amortize zero work.
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
			// Re-wire MockState dependencies without dropping the worker pool.
			// pph.Reset()/Release() would call resetPool() and defeat the experiment.
			pph.SetTrieContextFactory(mockTrieCtxFactory(ms))
			pph.ResetContext(ms)
		}
		// Each iteration rebuilds the trie from a fresh MockState, so the
		// long-lived template must not carry the previous iteration's root.
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

// buildClusteredStorageCorpus pins numAccounts to distinct top nibbles (so only
// numAccounts of the 16 top-nibble buckets are populated) and gives each many
// storage slots. The old per-nibble dispatch serializes each bucket on one
// goroutine; the DFS dispatch fans each account's storage across workers.
func buildClusteredStorageCorpus(b testing.TB, numAccounts, slotsPerAccount int) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(99001))
	ub := NewUpdateBuilder()
	for i := 0; i < numAccounts; i++ {
		addr := findAddressForNibble(i%16, i)
		ah := hex.EncodeToString(addr)
		ub.Balance(ah, rnd.Uint64())
		for range slotsPerAccount {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			val := make([]byte, 32)
			rnd.Read(val)
			ub.Storage(ah, hex.EncodeToString(loc), hex.EncodeToString(val))
		}
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

// storageGroup is one whale account plus a disjoint subset of its storage
// slots — the shape a single concurrent storage-fold worker would own.
type storageGroup struct {
	pk      [][]byte
	updates []Update
}

// buildWhaleStorageGroups splits one whale account's `slots` storage entries
// into `groups` disjoint sub-tries (account + subset). Each group is a valid,
// independent account+storage trie whose per-key processing cost matches a real
// storage-nibble subtree at depth 64; the only difference from the production
// mount is the (negligible) repeated single-account prefix.
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
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		val := make([]byte, 32)
		rnd.Read(val)
		ubs[i%groups].Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
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

// setupGroup builds the (untimed) per-iteration state for one group. Must run on
// the test goroutine (uses require). Each group gets its own MockState/trie so
// concurrent process() calls share no mutable state.
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

// Benchmark_StorageConcurrency measures whether processing one whale account's
// storage concurrently (split into N disjoint sub-tries) beats the single
// serial pass — the headline question for concurrent storage-subtree folding.
//
//	Single            one trie, all slots (the current whale-worker cost)
//	GroupsN-Serial    N sub-tries one after another (isolates split overhead)
//	GroupsN-Parallel  N sub-tries concurrently (the win)
//
// All timed regions cover only Process; MockState/Updates construction is under
// StopTimer, matching runDirectBench.
func Benchmark_StorageConcurrency(b *testing.B) {
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

// streamingBenchCorpora is the (whale, mixed) pair Task 9 measures. The whale is
// a bench-scale big-storage account (deep fan-out) plus accounts spread across
// the top nibbles so the background scheduler has multiple splits to fold in
// parallel under execution.
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

func Benchmark_DeepStorageWhale(b *testing.B) {
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
