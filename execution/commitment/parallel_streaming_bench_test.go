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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/length"
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
	runParallelBenchWith(b, pk, updates, workers,
		func(ms *MockState) (TrieContextFactory, func()) { return mockTrieCtxFactory(ms), func() {} }, false)
}

func runCollectingParallelBench(b *testing.B, pk [][]byte, updates []Update, workers int) {
	runParallelBenchWith(b, pk, updates, workers, collectingTrieCtxFactory, false)
}

func runCollectAndProcessBench(b *testing.B, pk [][]byte, updates []Update, workers int) {
	runParallelBenchWith(b, pk, updates, workers,
		func(ms *MockState) (TrieContextFactory, func()) { return mockTrieCtxFactory(ms), func() {} }, true)
}

func runParallelBenchWith(b *testing.B, pk [][]byte, updates []Update, workers int,
	newFactory func(*MockState) (TrieContextFactory, func()), timeCollect bool) {
	ctx := context.Background()
	b.ReportAllocs()
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
		factory, drain := newFactory(ms)
		if pph == nil {
			pph = NewParallelPatriciaHashed(factory, length.Addr, DefaultTrieConfig())
			pph.SetNumWorkers(workers)
		} else {
			pph.SetTrieContextFactory(factory)
			pph.ResetContext(ms)
		}
		pph.RootTrie().Reset()
		var upds *Updates
		if timeCollect {
			upds = NewUpdates(ModeParallel, "", KeyToHexNibbleHash)
		} else {
			upds = WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, updates)
		}
		b.StartTimer()

		if timeCollect {
			WrapKeyUpdatesInto(b, upds, pk, updates)
		}
		_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		upds.Close()
		drain()
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
	b.Run(fmt.Sprintf("collecting-w%d", ncpu), func(b *testing.B) { runCollectingParallelBench(b, pk, updates, ncpu) })
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
		b.Run(fmt.Sprintf("collecting-w%d", runtime.NumCPU()), func(b *testing.B) {
			runCollectingParallelBench(b, pk, updates, runtime.NumCPU())
		})
	})
}

func buildClusteredStorageCorpus(b testing.TB, numAccounts, slotsPerAccount int) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(99001))
	ub := NewUpdateBuilder()
	for i := range numAccounts {
		addNibbleAccount(ub, rnd, i%16, i, slotsPerAccount)
	}
	return ub.Build()
}

func buildStragglerCorpus(seed int64, accounts, stragglerSlots int) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	ub := NewUpdateBuilder()
	for range accounts {
		addRandomAccount(ub, rnd, 0)
	}
	addRandomAccount(ub, rnd, stragglerSlots)
	return ub.Build()
}

func Benchmark_Commitment_HotContractStraggler(b *testing.B) {
	for _, c := range []struct {
		name           string
		accounts       int
		stragglerSlots int
	}{
		{"4k-accounts-200-slots", 4_000, 200},
		{"20k-accounts-250-slots", 20_000, 250},
		{"20k-accounts-500-slots", 20_000, 500},
		{"20k-accounts-1000-slots", 20_000, 1_000},
	} {
		pk, updates := buildStragglerCorpus(int64(c.accounts)*7+int64(c.stragglerSlots), c.accounts, c.stragglerSlots)
		b.Run(c.name+"/ModeDirect", func(b *testing.B) { runDirectBench(b, pk, updates) })
		workers := []int{4, runtime.NumCPU()}
		slices.Sort(workers)
		workers = slices.Compact(workers)
		for _, w := range workers {
			b.Run(fmt.Sprintf("%s/ModeParallel-w%d", c.name, w), func(b *testing.B) {
				runParallelBench(b, pk, updates, w)
			})
		}
	}
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
	for i := range slots {
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

func setupGroup(tb testing.TB, g storageGroup) groupRun {
	ms := NewMockState(tb)
	require.NoError(tb, ms.applyPlainUpdates(g.pk, g.updates))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	upds := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, g.pk, g.updates)
	return groupRun{hph: hph, upds: upds}
}

// Returns err, not require: FailNow is unsafe off the test goroutine.
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

func runParallelBenchGrain(b *testing.B, pk [][]byte, updates []Update, workers int, grain uint32) {
	ctx := context.Background()
	b.ReportAllocs()
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
			pph.SetTrieContextFactory(mockTrieCtxFactory(ms))
			pph.ResetContext(ms)
		}
		pph.SetForkGrain(grain)
		pph.RootTrie().Reset()
		upds := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, updates)
		b.StartTimer()

		_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		upds.Close()
		b.StartTimer()
	}
	if pph != nil {
		b.ReportMetric(float64(pph.Forks()), "forks/op")
	}
}

func runIncrementalParallelBenchGrain(b *testing.B, batch1, batch2 engineBatch, workers int, grain uint32) {
	ctx := context.Background()
	b.ReportAllocs()
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
		if pph == nil {
			pph = NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
			pph.SetNumWorkers(workers)
		} else {
			pph.SetTrieContextFactory(mockTrieCtxFactory(ms))
			pph.ResetContext(ms)
		}
		pph.SetForkGrain(grain)
		pph.RootTrie().Reset()

		require.NoError(b, ms.applyPlainUpdates(batch1.keys, batch1.upds))
		u1 := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, batch1.keys, batch1.upds)
		_, err := pph.Process(ctx, u1, "", nil, WarmupConfig{})
		require.NoError(b, err)
		u1.Close()

		require.NoError(b, ms.applyPlainUpdates(batch2.keys, batch2.upds))
		u2 := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, batch2.keys, batch2.upds)
		b.StartTimer()

		_, err = pph.Process(ctx, u2, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		u2.Close()
		b.StartTimer()
	}
	if pph != nil {
		b.ReportMetric(float64(pph.Forks()), "forks/op")
	}
}

func Benchmark_Commitment_GrainSweep(b *testing.B) {
	w := runtime.NumCPU()
	grains := []struct {
		name  string
		grain uint32
	}{{"G2", 2}, {"Gauto", 0}, {"Ginf", ForkGrainNever}}

	b.Run("straggler-20k-500", func(b *testing.B) {
		pk, upds := buildStragglerCorpus(20_000*7+500, 20_000, 500)
		for _, g := range grains {
			b.Run(g.name, func(b *testing.B) { runParallelBenchGrain(b, pk, upds, w, g.grain) })
		}
	})
	b.Run("500K-StorageHeavy", func(b *testing.B) {
		pk, upds := build500KStorageHeavyCorpus(b)
		for _, g := range grains {
			b.Run(g.name, func(b *testing.B) { runParallelBenchGrain(b, pk, upds, w, g.grain) })
		}
	})
	b.Run("1MWhales", func(b *testing.B) {
		pk, upds := buildWhaleCorpus(whale1M())
		for _, g := range grains {
			b.Run(g.name, func(b *testing.B) { runParallelBenchGrain(b, pk, upds, w, g.grain) })
		}
	})
	b.Run("Clustered-8acct-500K", func(b *testing.B) {
		pk, upds := buildClusteredStorageCorpus(b, 8, 62_500)
		for _, g := range grains {
			b.Run(g.name, func(b *testing.B) { runParallelBenchGrain(b, pk, upds, w, g.grain) })
		}
	})
	b.Run("incremental-whale120k", func(b *testing.B) {
		inc1, inc2 := buildRetouchedWhale(717, 120_000)
		for _, g := range grains {
			b.Run(g.name, func(b *testing.B) { runIncrementalParallelBenchGrain(b, inc1, inc2, w, g.grain) })
		}
	})
}

func buildRetouchedWhale(seed int64, slots int) (batch1, batch2 engineBatch) {
	rnd := rand.New(rand.NewSource(seed))
	whale := addrHex(findAddressForNibble(0xd, int(seed)))

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
	for _, loc := range locs {
		val := make([]byte, 32)
		rnd.Read(val)
		ub2.Storage(whale, loc, hex.EncodeToString(val))
	}
	k2, u2 := ub2.Build()
	return engineBatch{k1, u1}, engineBatch{k2, u2}
}

func runIncrementalParallelBench(b *testing.B, batch1, batch2 engineBatch, workers int, newFactory func(*MockState) (TrieContextFactory, func())) {
	ctx := context.Background()
	b.ReportAllocs()
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
		factory, drain := newFactory(ms)
		if pph == nil {
			pph = NewParallelPatriciaHashed(factory, length.Addr, DefaultTrieConfig())
			pph.SetNumWorkers(workers)
		} else {
			pph.SetTrieContextFactory(factory)
			pph.ResetContext(ms)
		}
		pph.RootTrie().Reset()

		require.NoError(b, ms.applyPlainUpdates(batch1.keys, batch1.upds))
		u1 := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, batch1.keys, batch1.upds)
		_, err := pph.Process(ctx, u1, "", nil, WarmupConfig{})
		require.NoError(b, err)
		u1.Close()
		drain()

		require.NoError(b, ms.applyPlainUpdates(batch2.keys, batch2.upds))
		u2 := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, batch2.keys, batch2.upds)
		b.StartTimer()

		_, err = pph.Process(ctx, u2, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		u2.Close()
		drain()
		b.StartTimer()
	}
}

func Benchmark_Commitment_IncrementalWhale(b *testing.B) {
	batch1, batch2 := buildRetouchedWhale(717, 120_000)
	mock := func(ms *MockState) (TrieContextFactory, func()) { return mockTrieCtxFactory(ms), func() {} }
	workers := []int{4, runtime.NumCPU()}
	slices.Sort(workers)
	workers = slices.Compact(workers)
	for _, w := range workers {
		b.Run(fmt.Sprintf("incremental-whale120k/ModeParallel-w%d", w), func(b *testing.B) {
			runIncrementalParallelBench(b, batch1, batch2, w, mock)
		})
	}
	b.Run(fmt.Sprintf("incremental-whale120k/collecting-w%d", runtime.NumCPU()), func(b *testing.B) {
		runIncrementalParallelBench(b, batch1, batch2, runtime.NumCPU(), collectingTrieCtxFactory)
	})
}

type collectingTrieCtx struct {
	*MockState
	local map[string]BranchData
}

func (c *collectingTrieCtx) PutBranch(prefix, data, _ []byte) error {
	c.local[string(prefix)] = bytes.Clone(data)
	return nil
}

func collectingTrieCtxFactory(ms *MockState) (TrieContextFactory, func()) {
	var mu sync.Mutex
	var made []*collectingTrieCtx
	f := func(context.Context) (PatriciaContext, func()) {
		c := &collectingTrieCtx{MockState: ms, local: make(map[string]BranchData)}
		mu.Lock()
		made = append(made, c)
		mu.Unlock()
		return c, func() {}
	}
	drain := func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range made {
			maps.Copy(ms.cm, c.local)
		}
		made = nil
	}
	return f, drain
}

func Benchmark_ModeParallel_TouchAndProcess(b *testing.B) {
	ncpu := runtime.NumCPU()
	for _, c := range []struct {
		name  string
		build func(testing.TB) ([][]byte, []Update)
	}{
		{"100K-AccountsOnly", build100KAccountsCorpus},
		{"500K-StorageHeavy", build500KStorageHeavyCorpus},
	} {
		pk, updates := c.build(b)
		for _, w := range []int{4, ncpu} {
			b.Run(fmt.Sprintf("%s/w%d", c.name, w), func(b *testing.B) {
				runCollectAndProcessBench(b, pk, updates, w)
			})
		}
	}
}
