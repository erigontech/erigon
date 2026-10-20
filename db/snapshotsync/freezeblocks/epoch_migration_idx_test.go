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

package freezeblocks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
)

// removeDecimalSegFiles must take the decimal indexes with the segment. They carry their own file
// version, so a mask derived from the .seg name cannot reach them — and a decimal index left
// behind is not merely garbage: its name encodes the same block pair as the epoch segment that
// replaces it, so it can be mistaken for that segment's index.
func TestRemoveSegFilesDeletesDecimalIndexes(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
		return p
	}

	segPath := write(snaptype.SegmentFileName(version.V1_1, false, 0, 1000, snaptype2.Headers.Enum()))
	decIdx := write(snaptype.IdxFileName(version.V2_0, false, 0, 1000, snaptype2.Headers.Name()))
	torrent := write(snaptype.SegmentFileName(version.V1_1, false, 0, 1000, snaptype2.Headers.Enum()) + ".torrent")
	// the epoch segment's own index encodes to the same 000000-000001 pair and must survive
	epochInfo := snaptype2.Headers.FileInfo(dir, true, 0, 1024)
	epochSeg := write(epochInfo.Name())
	var epochIdxs []string
	for _, n := range epochInfo.Type.IdxFileNames(epochInfo.Epoch, epochInfo.From, epochInfo.To) {
		epochIdxs = append(epochIdxs, write(n))
	}

	info, _, ok := snaptype.ParseFileName(dir, filepath.Base(segPath))
	require.True(t, ok)
	require.NoError(t, removeDecimalSegFiles([]snaptype.FileInfo{info}))

	require.NoFileExists(t, segPath)
	require.NoFileExists(t, decIdx)
	require.NoFileExists(t, torrent)
	require.FileExists(t, epochSeg)
	for _, p := range epochIdxs {
		require.FileExists(t, p, p)
	}
}

// The transactions type owns two indexes, one of which (transactions-to-block) is only a prefix
// match away from the other, so removing a decimal transactions segment must take both.
func TestRemoveSegFilesDeletesAllIndexesOfType(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
		return p
	}

	segPath := write(snaptype.SegmentFileName(version.V1_1, false, 1000, 2000, snaptype2.Transactions.Enum()))
	var decIdx []string
	for _, idx := range snaptype2.Transactions.Indexes() {
		decIdx = append(decIdx, write(snaptype.IdxFileName(version.V2_0, false, 1000, 2000, idx.Name)))
	}
	require.Len(t, decIdx, 2)
	// epoch [1024,2048) prints the same 000001-000002 digits as decimal [1000,2000)
	epochInfo := snaptype2.Transactions.FileInfo(dir, true, 1024, 2048)
	epochKeep := []string{write(epochInfo.Name())}
	for _, n := range epochInfo.Type.IdxFileNames(epochInfo.Epoch, epochInfo.From, epochInfo.To) {
		epochKeep = append(epochKeep, write(n))
	}

	info, _, ok := snaptype.ParseFileName(dir, filepath.Base(segPath))
	require.True(t, ok)
	require.NoError(t, removeDecimalSegFiles([]snaptype.FileInfo{info}))

	require.NoFileExists(t, segPath)
	for _, p := range decIdx {
		require.NoFileExists(t, p, p)
	}
	for _, p := range epochKeep {
		require.FileExists(t, p, p)
	}
}

// hasEpochIndexes decides whether buildEpochIndexes may skip a segment, so it must answer only
// about the epoch-named index. snaptype.HasIndexFiles cannot: its lookup wildcards the version and
// encodes the range as block/1000, and decimal [1000k,1000(k+1)) collides with epoch
// [1024k,1024(k+1)) for every k below ~42 — accepting the decimal index there makes the migration
// skip building the real one and then delete the decimal segments.
func TestHasEpochIndexesIgnoresDecimalIndex(t *testing.T) {
	const from, to = uint64(0), uint64(1024)
	epochInfo := snaptype2.Headers.FileInfo("", true, from, to)
	epochSeg := epochInfo.Name()
	decIdx := snaptype.IdxFileName(version.V2_0, false, 0, 1000, snaptype2.Headers.Name())
	epochIdxs := epochInfo.Type.IdxFileNames(epochInfo.Epoch, epochInfo.From, epochInfo.To)

	// The "ep" marker is what keeps an epoch index distinct from a decimal one that would otherwise
	// wildcard-match the same range+type: the epoch index carries the "-ep" marker, the decimal one does not.
	require.NotContains(t, decIdx, "-ep.")
	require.Len(t, epochIdxs, 1)
	require.Contains(t, epochIdxs[0], "-ep.")

	require.False(t, hasEpochIndexes(epochInfo, []string{epochSeg, decIdx}),
		"a leftover decimal index must not count as the epoch segment's index")
	require.True(t, hasEpochIndexes(epochInfo, append([]string{epochSeg}, epochIdxs...)))
}
