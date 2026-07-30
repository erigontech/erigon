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

package snapshotsync

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

// A View reports the generation it pinned, not whatever is current. Serving the live
// set instead would hand back segments whose files the View's RoTxs never pinned, so
// a concurrent recalc could unlink them under the reader.
func TestCaplinStateViewServesPinnedGeneration(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	view := s.View()
	defer view.Close()
	require.Len(t, view.VisibleSegments(table), 1)

	// Publish a second generation while the view is alive.
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 200_000, logger)
	require.NoError(t, s.OpenFolder())
	require.Len(t, s.visible.Load().segments[table], 2, "recalc must publish the new generation")

	require.Len(t, view.VisibleSegments(table), 1, "view must keep serving the generation it pinned")
	_, served := view.VisibleSegment(150_000, table)
	require.False(t, served, "a slot only in the newer generation must not be served by an older view")
}

// idxMax must travel with the generation it was computed from: a reader that observes
// the new segment set must never pair it with the previous set's height.
func TestCaplinStateIdxMaxMatchesItsGeneration(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.Equal(t, uint64(100_000), s.IndicesMax())

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 200_000, logger)
	require.NoError(t, s.OpenFolder())

	v := s.visible.Load()
	require.Equal(t, uint64(200_000), v.idxMax)
	require.Equal(t, caplinStateIdxAvailability(v.segments), v.idxMax)
}

// Readers run lock-free against recalc, so every read path must go through one
// pointer load. Under -race this fails on any unguarded field read.
func TestCaplinStateConcurrentRecalcAndReaders(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	const rounds = 200
	var wg sync.WaitGroup
	wg.Go(func() {
		for range rounds {
			s.recalcVisibleFiles()
		}
	})
	wg.Go(func() {
		for range rounds {
			view := s.View()
			require.Len(t, view.VisibleSegments(table), 1)
			view.Close()
		}
	})
	wg.Go(func() {
		for range rounds {
			require.Equal(t, uint64(100_000), s.IndicesMax())
			require.NotEmpty(t, s.coveredRangesForType(table))
			s.BlocksAvailable()
		}
	})
	wg.Wait()
}
