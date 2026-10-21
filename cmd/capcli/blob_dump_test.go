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

package main

import (
	"cmp"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func blobSegment(t *testing.T, name string) snaptype.FileInfo {
	t.Helper()
	f, _, ok := snaptype.ParseFileName("", name)
	require.True(t, ok, name)
	return f
}

func TestBlobDumpPlan(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 20_000 / cfg.SlotsPerEpoch
	v10 := snaptype.BlobSidecars.FileName(version.V1_0, 30_000, 40_000)
	v11 := snaptype.BlobSidecars.FileName(version.V1_1, 30_000, 40_000)
	v12 := snaptype.BlobSidecars.FileName(version.V1_2, 30_000, 40_000)

	tests := []struct {
		name         string
		from, to     uint64
		frozenBlocks uint64
		onDisk       []string
		pinned       []string
		overwrite    bool
		want         []uint64
		wantErr      string
	}{
		{name: "starts at the Deneb segment", to: 50_000, want: []uint64{20_000, 30_000, 40_000}},
		{name: "drops a trailing partial segment", to: 45_000, want: []uint64{20_000, 30_000}},
		{name: "stops at the frozen block frontier", to: 50_000, frozenBlocks: 40_000, want: []uint64{20_000, 30_000}},
		{name: "plans nothing for a --from within a segment of the slot limit", from: math.MaxUint64 / snaptype.CaplinMergeLimit * snaptype.CaplinMergeLimit, to: 30_000},
		{name: "starts at --from", from: 30_000, to: 50_000, want: []uint64{30_000, 40_000}},
		{name: "--from off a segment boundary", from: 25_000, to: 50_000, wantErr: "multiple of"},
		{name: "--from below the Deneb segment", from: 10_000, to: 50_000, wantErr: "below the Deneb"},
		{name: "skips a segment on disk", to: 50_000, onDisk: []string{v10}, want: []uint64{20_000, 40_000}},
		{name: "skips a pinned segment", to: 50_000, pinned: []string{v11}, want: []uint64{20_000, 40_000}},
		{name: "--overwrite rewrites a current-version segment", to: 50_000, onDisk: []string{v11}, overwrite: true, want: []uint64{20_000, 30_000, 40_000}},
		{name: "--overwrite refuses a pinned segment", to: 50_000, onDisk: []string{v11}, pinned: []string{v11}, overwrite: true, wantErr: datadir.PreverifiedFileName},
		{name: "--overwrite refuses a segment at a lower version", to: 50_000, onDisk: []string{v11, v10}, overwrite: true, wantErr: v10},
		{name: "--overwrite refuses a segment at a newer version", to: 50_000, onDisk: []string{v12}, overwrite: true, wantErr: v12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var onDisk, pinned []snaptype.FileInfo
			for _, name := range tt.onDisk {
				onDisk = append(onDisk, blobSegment(t, name))
			}
			for _, name := range tt.pinned {
				pinned = append(pinned, blobSegment(t, name))
			}
			got, err := blobDumpPlan(&cfg, tt.from, tt.to, cmp.Or(tt.frozenBlocks, math.MaxUint64), onDisk, pinned, tt.overwrite)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func writeSegment(t *testing.T, dirs datadir.Dirs, name string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), []byte{1}, 0o644))
}

func TestFrozenBlobSegmentsReadsDiskAndPreverifiedLock(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeSegment(t, dirs, snaptype.BlobSidecars.FileName(version.V1_1, 20_000, 30_000))
	writeSegment(t, dirs, snaptype.BeaconBlocks.FileName(version.V1_1, 20_000, 30_000))
	lock := fmt.Sprintf("'%s' = '3fe3c8bedc2c60447ce4080b685f13db2d4b0d53'\n'%s' = '4e1e712cb8ab49263438938af130f4406bdb6d73'\n",
		snaptype.BlobSidecars.FileName(version.V1_0, 30_000, 40_000), snaptype.BeaconBlocks.FileName(version.V1_1, 30_000, 40_000))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte(lock), 0o444))

	onDisk, pinned, err := frozenBlobSegments(dirs)
	require.NoError(t, err)

	type seg struct {
		from, to uint64
		v        version.Version
	}
	segs := func(files []snaptype.FileInfo) (out []seg) {
		for _, f := range files {
			out = append(out, seg{f.From, f.To, f.Version})
		}
		return out
	}
	require.Equal(t, []seg{{20_000, 30_000, version.V1_1}}, segs(onDisk))
	require.Equal(t, []seg{{30_000, 40_000, version.V1_0}}, segs(pinned))
}

func TestFrozenBlobSegmentsWithoutPreverifiedLock(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeSegment(t, dirs, snaptype.BlobSidecars.FileName(version.V1_1, 20_000, 30_000))

	onDisk, pinned, err := frozenBlobSegments(dirs)
	require.NoError(t, err)
	require.Len(t, onDisk, 1)
	require.Empty(t, pinned)
}

func TestFrozenBlobSegmentsRejectsAMalformedPreverifiedLock(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("not toml ="), 0o444))

	_, _, err := frozenBlobSegments(dirs)
	require.ErrorContains(t, err, datadir.PreverifiedFileName)
}

func TestRemoveBlobIndexAndTorrents(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	idx := func(v version.Version, from, to uint64) string {
		return snaptype.IdxFileName(v, from, to, snaptype.CaplinIndexes.BlobSidecarSlot.Name)
	}
	seg := snaptype.BlobSidecars.FileName(version.V1_1, 30_000, 40_000)
	removed := []string{seg + ".torrent", idx(version.V1_1, 30_000, 40_000), idx(version.V1_1, 30_000, 40_000) + ".torrent", idx(version.V1_0, 30_000, 40_000)}
	kept := []string{
		seg,
		snaptype.BlobSidecars.FileName(version.V1_1, 40_000, 50_000) + ".torrent",
		idx(version.V1_1, 40_000, 50_000),
		snaptype.BeaconBlocks.FileName(version.V1_1, 30_000, 40_000) + ".torrent",
	}
	for _, name := range append(removed, kept...) {
		writeSegment(t, dirs, name)
	}

	require.NoError(t, removeBlobIndexAndTorrents(dirs.Snap, 30_000, 40_000))

	entries, err := os.ReadDir(dirs.Snap)
	require.NoError(t, err)
	var left []string
	for _, e := range entries {
		if !e.IsDir() {
			left = append(left, e.Name())
		}
	}
	require.ElementsMatch(t, kept, left)
}
