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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

// An un-indexed .seg (published before its .idx, e.g. a dump interrupted between the
// two writes) must never enter the visible set: it has no index to serve a read yet
// would shadow the DB for its slot range. Pins the isIndexed gate in recalcVisibleFiles
// that keeps the publish-.seg-before-.idx window safe.
func TestCaplinStateUnindexedSegmentInvisible(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	// Indexed control that must be visible, plus an abutting segment whose .idx is
	// withheld and must stay invisible.
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)
	_, unindexedIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)
	require.NoError(t, dir2.RemoveFile(unindexedIdx))

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	typ := mustCaplinStateType(t, table)
	require.Equal(t, []Range{{from: 0, to: 100_000}}, s.coveredRangesForType(typ),
		"un-indexed segment must not be a covered (visible) range")

	view := s.View()
	defer view.Close()
	_, servedIndexed := view.VisibleSegment(50_000, typ)
	require.True(t, servedIndexed, "indexed range must serve from the snapshot")
	_, servedUnindexed := view.VisibleSegment(120_000, typ)
	require.False(t, servedUnindexed, "un-indexed range must fall through to the DB, not the snapshot")
}

// A CaplinStateView takes a real pin on the current generation and keeps reading its own
// payload after a concurrent republish supersedes it; the pin drops only on Close, after
// which the generation chain collapses. Pins the Task-5 acquire/release contract.
func TestCaplinStateViewPinsGeneration(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	typ := mustCaplinStateType(t, table)

	require.Equal(t, s._visibleFiles.visible.Load(), s._visibleFiles.oldest, "chain must be collapsed before opening a view")

	view := s.View()
	pinned := s._visibleFiles.visible.Load()
	require.Equal(t, int32(1), pinned.refcnt.Load(), "View must pin the current generation")
	require.Len(t, view.VisibleSegments(typ), 1, "view sees the generation it acquired")

	// Republish a larger visible set (an abutting indexed segment) while the view is open.
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)
	require.NoError(t, s.OpenFolder())

	require.NotEqual(t, pinned, s._visibleFiles.visible.Load(), "OpenFolder must publish a new generation")
	require.Equal(t, int32(1), pinned.refcnt.Load(), "the open view keeps its generation pinned across republish")
	seg, ok := view.VisibleSegment(50_000, typ)
	require.True(t, ok)
	require.Equal(t, Range{from: 0, to: 100_000}, seg.Range, "pinned view still reads its own payload, not the new set")
	require.Len(t, view.VisibleSegments(typ), 1, "pinned view is unaffected by the republish")

	view.Close()
	require.Equal(t, int32(0), pinned.refcnt.Load(), "Close releases the pin")
	require.Equal(t, s._visibleFiles.visible.Load(), s._visibleFiles.oldest, "chain collapses once the view drains")

	view2 := s.View()
	defer view2.Close()
	require.Len(t, view2.VisibleSegments(typ), 2, "a fresh view sees the republished set")
}

// The .idx path must be derived from the .seg path by replacing only the trailing
// extension. A datadir whose path itself contains ".seg" (here the base dir) must not
// cause the index lookup to hit a wrong path, silently leaving an indexed segment
// un-indexed and permanently shadowed by the DB.
func TestCaplinStateIndexFoundWhenDatadirPathContainsSeg(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(filepath.Join(t.TempDir(), "erigon.seg"))
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	require.Equal(t, []Range{{from: 0, to: 100_000}}, s.coveredRangesForType(mustCaplinStateType(t, table)),
		`index in a datadir whose path contains ".seg" must still be found`)
}
