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
	"testing"

	"github.com/stretchr/testify/require"
)

// Benchmark_NestedCacheWhaleRefold measures the whale STORAGE RE-FOLD — the bulk
// path the nested cache targets, NOT steady-state per-block commitment (on the live
// frontier the deep fan-out never fires and the cache is bypassed; see the plan's
// Scope reality note). A big account's storage is folded, then an incremental block
// adds slots to a single first-storage nibble and the storage root is re-folded.
//
// Three directly-comparable, droppable configurations of the same re-fold:
//   - ModeParallel-fullfold: the parallel_mount fold model (concurrentAccountRoot,
//     re-folds all 16 nibbles fresh, concurrently) — the untouched baseline the
//     whole strategy must beat and stays comparable against.
//   - NestedCache-coldfold: the streaming nested cache with a COLD cache
//     (gate-only / cache-miss) — re-folds every present nibble, then aggregates.
//   - NestedCache-incremental: the streaming nested cache WARM — only the dirtied
//     nibble re-folds, the rest reuse their cached depth-65 cells, then aggregate.
//
// Prototype targets on a 750k whale (Benchmark_NestedStorageRefold): ~656 ms full
// re-fold vs ~49 ms incremental (~13×). The streaming committer's per-block Process
// is NOT a valid stand-in here: a sub-threshold incremental block never crosses
// deepStorageThreshold, so it folds the few touched keys against the persisted trie
// and the deep cache is bypassed entirely — the cache only pays off when a single
// commitment re-folds a whale's storage, which this benchmark isolates.
func Benchmark_NestedCacheWhaleRefold(b *testing.B) {
	const slots = 750_000
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(slots)

	ms := NewMockState(b)
	ms.SetConcurrentCommitment(true)
	require.NoError(b, ms.applyPlainUpdates(pk, upds))

	// Incremental block: add slots to the most-populated first-storage nibble so the
	// re-fold has a meaningful single-nibble subtree to rebuild.
	tx := 0
	for x := 1; x < 16; x++ {
		if len(groups[x]) > len(groups[tx]) {
			tx = x
		}
	}
	extra := extraSlotsInNibble(addr, byte(tx), 50, 9)
	extraPk := make([][]byte, len(extra))
	extraUpds := make([]Update, len(extra))
	for i := range extra {
		extraPk[i] = extra[i].pk
		extraUpds[i] = extra[i].upd
	}
	require.NoError(b, ms.applyPlainUpdates(extraPk, extraUpds))
	groups[tx] = append(append([]storKV{}, groups[tx]...), extra...)
	tg := touchedGroups(&groups)
	factory := newWhaleWorker(ms)

	b.Run("ModeParallel-fullfold", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := concurrentAccountRoot(ms, addr, accHash, accNib, accUpd, groups, true)
			require.NoError(b, err)
		}
	})

	b.Run("NestedCache-coldfold", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			cache := &accountStorageCache{prefix: append([]byte(nil), accHash[:64]...)}
			b.StartTimer()
			_, folded, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil)
			require.NoError(b, err)
			require.Equal(b, bitsCount(cache.present), folded)
		}
	})

	b.Run("NestedCache-incremental", func(b *testing.B) {
		b.ReportAllocs()
		cache := &accountStorageCache{prefix: append([]byte(nil), accHash[:64]...)}
		_, _, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil)
		require.NoError(b, err)
		for b.Loop() {
			b.StopTimer()
			cache.perNibble[tx].dirty = true
			b.StartTimer()
			_, folded, _, err := foldStorageRootCached(factory, cache, accNib, tg, nil)
			require.NoError(b, err)
			require.Equal(b, 1, folded)
		}
	})
}
