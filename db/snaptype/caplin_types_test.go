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

package snaptype_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func TestEnumeration(t *testing.T) {

	if snaptype.BlobSidecars.Enum() != snaptype.CaplinEnums.BlobSidecars {
		t.Fatal("enum mismatch", snaptype.BlobSidecars, snaptype.BlobSidecars.Enum(), snaptype.CaplinEnums.BlobSidecars)
	}

	if snaptype.BeaconBlocks.Enum() != snaptype.CaplinEnums.BeaconBlocks {
		t.Fatal("enum mismatch", snaptype.BeaconBlocks, snaptype.BeaconBlocks.Enum(), snaptype.CaplinEnums.BeaconBlocks)
	}
}

func TestNames(t *testing.T) {

	if snaptype.BeaconBlocks.Name() != snaptype.CaplinEnums.BeaconBlocks.String() {
		t.Fatal("name mismatch", snaptype.BeaconBlocks, snaptype.BeaconBlocks.Name(), snaptype.CaplinEnums.BeaconBlocks.String())
	}

	if snaptype.BlobSidecars.Name() != snaptype.CaplinEnums.BlobSidecars.String() {
		t.Fatal("name mismatch", snaptype.BlobSidecars, snaptype.BlobSidecars.Name(), snaptype.CaplinEnums.BlobSidecars.String())
	}

}

func readDirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

func TestHasIndexFiles(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()

	from, to := uint64(0), uint64(500_000)
	ver := version.V1_1
	snapType := snaptype.BeaconBlocks
	idx := snaptype.CaplinIndexes.BeaconBlockSlot

	segPath := filepath.Join(tmpDir, snaptype.SegmentFileName(ver, from, to, snaptype.CaplinEnums.BeaconBlocks))
	info := snaptype.FileInfo{
		Version: ver,
		From:    from,
		To:      to,
		Path:    segPath,
	}

	t.Run("no seg file returns false", func(t *testing.T) {
		require.False(t, snapType.HasIndexFiles(info, readDirNames(t, tmpDir), logger))
	})

	f, err := os.Create(segPath)
	require.NoError(t, err)
	f.Close()

	t.Run("seg exists but no idx returns false", func(t *testing.T) {
		require.False(t, snapType.HasIndexFiles(info, readDirNames(t, tmpDir), logger))
	})

	idxPath := filepath.Join(tmpDir, snaptype.IdxFileName(ver, from, to, idx.Name))
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     tmpDir,
		IndexFile:  idxPath,
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer rs.Close()
	rs.DisableFsync()
	require.NoError(t, rs.AddKey([]byte{1}, 0))
	require.NoError(t, rs.Build(context.Background()))

	t.Run("both seg and idx exist returns true", func(t *testing.T) {
		require.True(t, snapType.HasIndexFiles(info, readDirNames(t, tmpDir), logger))
	})

	require.NoError(t, os.Remove(idxPath))

	t.Run("idx removed returns false again", func(t *testing.T) {
		require.False(t, snapType.HasIndexFiles(info, readDirNames(t, tmpDir), logger))
	})
}

func TestIndexHasFileFromEntries(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()

	from, to := uint64(0), uint64(500_000)
	ver := version.V1_1
	idx := snaptype.CaplinIndexes.BeaconBlockSlot

	segPath := filepath.Join(tmpDir, snaptype.SegmentFileName(ver, from, to, snaptype.CaplinEnums.BeaconBlocks))
	info := snaptype.FileInfo{
		Version: ver,
		From:    from,
		To:      to,
		Path:    segPath,
	}

	t.Run("no seg file returns false", func(t *testing.T) {
		require.False(t, idx.HasFileFromEntries(info, readDirNames(t, tmpDir), logger))
	})

	f, err := os.Create(segPath)
	require.NoError(t, err)
	f.Close()

	t.Run("seg exists but no idx returns false", func(t *testing.T) {
		require.False(t, idx.HasFileFromEntries(info, readDirNames(t, tmpDir), logger))
	})

	idxPath := filepath.Join(tmpDir, snaptype.IdxFileName(ver, from, to, idx.Name))
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     tmpDir,
		IndexFile:  idxPath,
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer rs.Close()
	rs.DisableFsync()
	require.NoError(t, rs.AddKey([]byte{1}, 0))
	require.NoError(t, rs.Build(context.Background()))

	entries := readDirNames(t, tmpDir) // scan once, reuse
	t.Run("both exist with pre-scanned entries returns true", func(t *testing.T) {
		require.True(t, idx.HasFileFromEntries(info, entries, logger))
	})

	t.Run("idx removed returns false even with stale entries", func(t *testing.T) {
		// MatchVersionedFile finds the name in entries, but OpenIndex still hits disk
		require.NoError(t, os.Remove(idxPath))
		require.False(t, idx.HasFileFromEntries(info, entries, logger))
	})
}
