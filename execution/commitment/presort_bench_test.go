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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func benchTouchAndProcess(b *testing.B, pk [][]byte, updates []Update, workers int) {
	ctx := context.Background()
	tmp := b.TempDir()
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
		factory := mockTrieCtxFactory(ms)
		if pph == nil {
			pph = NewParallelPatriciaHashed(factory, length.Addr, DefaultTrieConfig())
			pph.SetNumWorkers(workers)
		} else {
			pph.SetTrieContextFactory(factory)
			pph.ResetContext(ms)
		}
		pph.RootTrie().Reset()
		upds := NewUpdates(ModeParallel, tmp, KeyToHexNibbleHash)
		b.StartTimer()

		WrapKeyUpdatesInto(b, upds, pk, updates)
		_, err := pph.Process(ctx, upds, "", nil, WarmupConfig{})

		b.StopTimer()
		require.NoError(b, err)
		upds.Close()
		b.StartTimer()
	}
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
				benchTouchAndProcess(b, pk, updates, w)
			})
		}
	}
}
