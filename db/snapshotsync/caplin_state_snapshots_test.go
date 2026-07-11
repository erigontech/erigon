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
	"github.com/tidwall/btree"
)

func TestCaplinStateCloseWhatNotInListDropsUnopenedStub(t *testing.T) {
	tree := btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 4, NoLocks: false})
	kept := &DirtySegment{Range: Range{0, 1000}, filePath: filepath.Join("snapshots", "keep.seg")}
	stale := &DirtySegment{Range: Range{1000, 2000}, filePath: filepath.Join("snapshots", "drop.seg")}
	tree.Set(kept)
	tree.Set(stale)
	s := &CaplinStateSnapshots{dirty: map[CaplinStateType]*btree.BTreeG[*DirtySegment]{CaplinBlockRoot: tree}}

	s.closeWhatNotInList([]string{"keep.seg"})

	require.Equal(t, 1, tree.Len(), "unopened stale stub must be dropped")
	survivor, ok := tree.Min()
	require.True(t, ok)
	require.Same(t, kept, survivor)
}
