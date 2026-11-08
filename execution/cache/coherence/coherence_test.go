// Copyright 2024 The Erigon Authors
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

package coherence

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGen_FloorAndEpoch(t *testing.T) {
	var g Gen
	require.Equal(t, uint32(0), g.Epoch())

	// Before any unwind nothing is stale.
	require.False(t, g.IsStale(100, 0))

	// Unwind to txN=50: entries at/above 50 from the old epoch are stale.
	g.Unwind(50)
	require.Equal(t, uint32(1), g.Epoch())
	require.True(t, g.IsStale(50, 0), "txN>=floor, old epoch -> stale")
	require.True(t, g.IsStale(80, 0))
	require.False(t, g.IsStale(49, 0), "txN<floor -> survives")
	require.False(t, g.IsStale(50, 1), "current epoch -> not stale")

	// A shallower later unwind must not raise the floor.
	g.Unwind(70)
	require.Equal(t, uint32(2), g.Epoch())
	require.True(t, g.IsStale(50, 0), "floor stays at the deeper 50")
	require.False(t, g.IsStale(49, 1))
}

// TestGen_ConcurrentUnwindNoTear runs Unwind against concurrent readers; the
// race detector must see no torn (epoch, floor) read and the epoch must equal
// the number of Unwinds.
func TestGen_ConcurrentUnwindNoTear(t *testing.T) {
	var g Gen

	const unwinds = 200
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 1000 {
				_ = g.IsStale(uint64(i), 0)
				_ = g.Epoch()
			}
		})
	}
	wg.Go(func() {
		for i := range unwinds {
			g.Unwind(uint64(unwinds - i)) // descending -> floor keeps dropping
		}
	})
	wg.Wait()
	require.Equal(t, uint32(unwinds), g.Epoch())
	require.True(t, g.IsStale(1, 0), "deepest floor reached 1")
}

func TestGen_ResetAdvancesEpochAndLiftsFloor(t *testing.T) {
	var g Gen
	g.Unwind(50)

	g.Reset()

	s := g.Snapshot()
	require.Equal(t, uint32(2), s.epoch)
	require.Equal(t, uint64(math.MaxUint64), s.floor)
}

func TestGen_SnapshotKeepsDeadEntryStaleAcrossReset(t *testing.T) {
	var g Gen
	entryEpoch := g.Epoch()
	g.Unwind(50)
	read := g.Snapshot()

	g.Reset()

	require.True(t, read.IsStale(60, entryEpoch))
	require.False(t, g.IsStale(60, entryEpoch))
}

func TestGen_ConcurrentResetAndUnwindPreserveEveryEpochBump(t *testing.T) {
	var g Gen

	const operations = 200
	var wg sync.WaitGroup
	for range operations {
		wg.Go(g.Reset)
		wg.Go(func() { g.Unwind(50) })
	}
	wg.Wait()

	require.Equal(t, uint32(operations*2), g.Epoch())
}
