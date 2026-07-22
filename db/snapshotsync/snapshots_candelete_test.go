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
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
)

// The last releaser of a retired segment consults canDelete: segments retired
// for deletion are unlinked, segments that merely left the visible set are only closed.
func TestReclaimSegmentsRespectsCanDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger := log.New()

	newSeg := func(from, to uint64) *DirtySegment {
		createTestSegmentFile(t, from, to, snaptype2.Enums.Headers, dir, version.V1_0, logger)
		s := NewDirtySegment(snaptype2.Headers, version.V1_0, from, to, false)
		require.NoError(t, s.Open(dir))
		return s
	}

	keep := newSeg(0, 1000)
	del := newSeg(1000, 2000)
	del.canDelete.Store(true)

	keepPath := filepath.Join(dir, keep.FileName())
	delPath := filepath.Join(dir, del.FileName())

	reclaimSegments([]*DirtySegment{keep, del})

	require.Nil(t, keep.Decompressor)
	require.Nil(t, del.Decompressor)

	exists, err := dir2.FileExist(keepPath)
	require.NoError(t, err)
	require.True(t, exists, "canDelete=false: close only, file stays on disk")

	exists, err = dir2.FileExist(delPath)
	require.NoError(t, err)
	require.False(t, exists, "canDelete=true: file is deleted")
}
