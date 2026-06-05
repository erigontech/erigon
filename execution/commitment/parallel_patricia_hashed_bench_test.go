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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func build100KAccountsCorpus(b testing.TB) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(133777))
	ub := NewUpdateBuilder()
	for range 100_000 {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		ub.Balance(hex.EncodeToString(addr), rnd.Uint64())
	}
	return ub.Build()
}

func build500KStorageHeavyCorpus(b testing.TB) ([][]byte, []Update) {
	b.Helper()
	rnd := rand.New(rand.NewSource(244888))
	ub := NewUpdateBuilder()

	addrs := make([]string, 1000)
	for i := range addrs {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		addrs[i] = hex.EncodeToString(addr)
		ub.Balance(addrs[i], rnd.Uint64())
	}

	const slotsPerAccount = 499 // 1000 * 499 = 499_000 storage + 1000 accounts = 500_000 total
	for _, addr := range addrs {
		for range slotsPerAccount {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			val := make([]byte, 32)
			rnd.Read(val)
			ub.Storage(addr, hex.EncodeToString(loc), hex.EncodeToString(val))
		}
	}
	return ub.Build()
}

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
		upds := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, updates)
		b.StartTimer()

		_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		upds.Close()
		b.StartTimer()
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
