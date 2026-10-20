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

package snaptype_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
)

// The regime is a filename marker ("ep"), independent of the content version: decimal names encode
// block/1000 with no marker, epoch names encode block/1024 with the marker. The public FileName is
// always decimal; epoch names are built through FileInfo.
func TestEpochNameRoundTrip(t *testing.T) {
	// decimal round-trips (public FileName, no marker)
	name := snaptype.FileName(version.V1_0, false, 21_000_000, 21_100_000, "headers")
	require.Equal(t, "v1.0-021000-021100-headers", name)
	res, _, ok := snaptype.ParseFileName("", name+".seg")
	require.True(t, ok)
	require.False(t, res.Epoch)
	require.Equal(t, uint64(21_000_000), res.From)
	require.Equal(t, uint64(21_100_000), res.To)

	// epoch round-trips: marker + block/1024, at a range that is not 1000-aligned (top tier)
	epoch := snaptype2.Headers.FileInfo("", true, 20_971_520, 21_495_808)
	require.Equal(t, "ep", epochMarkerOf(t, epoch.Name()))
	res, _, ok = snaptype.ParseFileName("", epoch.Name())
	require.True(t, ok)
	require.True(t, res.Epoch)
	require.Equal(t, uint64(20_971_520), res.From)
	require.Equal(t, uint64(21_495_808), res.To)
}

func epochMarkerOf(t *testing.T, name string) string {
	t.Helper()
	fi, _, ok := snaptype.ParseFileName("", name)
	require.True(t, ok)
	if fi.Epoch {
		return "ep"
	}
	return ""
}

// A segment's idx mask must encode the same range the segment name does, or it addresses the wrong
// file. For epoch (block/1024) segments this bites from block 43008 up, where from/1000 != from/1024
// and consecutive segments are one printed digit apart — a /1000 mask would land on the neighbour.
func TestEpochIdxMaskMatchesName(t *testing.T) {
	seg := snaptype2.Headers.FileInfo("", true, 43_008, 44_032)
	idxName := seg.Type.IdxFileNames(seg.Epoch, seg.From, seg.To)[0]
	mask := snaptype.IdxFileMask(seg.Epoch, seg.From, seg.To, snaptype2.Indexes.HeaderHash.Name)

	ok, err := filepath.Match(mask, idxName)
	require.NoError(t, err)
	require.True(t, ok, "mask %q does not match its own idx name %q", mask, idxName)

	neighbour := snaptype2.Headers.IdxFileNames(true, 44_032, 45_056)[0]
	ok, err = filepath.Match(mask, neighbour)
	require.NoError(t, err)
	require.False(t, ok, "mask %q wrongly matches the next segment %q", mask, neighbour)
}

// FileInfo must carry the exact block/1024 range: building the name decimally and re-parsing would
// truncate (1024 -> 1000, 524288 -> 524000), which corrupts From/To and, downstream, the segment
// length the transactions index is built against.
func TestFileInfoNoTruncation(t *testing.T) {
	for _, tc := range []struct{ from, to uint64 }{
		{0, 1024},
		{0, 524_288},
		{524_288, 589_824},
		{43_008, 44_032},
	} {
		f := snaptype2.Headers.FileInfo("", true, tc.from, tc.to)
		require.True(t, f.Epoch)
		require.Equal(t, tc.from, f.From, "[%d,%d)", tc.from, tc.to)
		require.Equal(t, tc.to, f.To, "[%d,%d)", tc.from, tc.to)

		res, _, ok := snaptype.ParseFileName("", f.Name())
		require.True(t, ok)
		require.Equal(t, tc.from, res.From)
		require.Equal(t, tc.to, res.To)
	}
}

// The download filter uses EpochRegimeMismatch to keep an epoch chain from pulling decimal block
// files (and vice versa), keyed on the marker. It also covers block indexes — including the
// transactions-to-block index, which resolves to the transactions type. Non-block files never match.
func TestEpochRegimeMismatch(t *testing.T) {
	decHeaders := snaptype.FileName(version.V1_0, false, 0, 500_000, "headers") + ".seg"
	epochHeaders := snaptype2.Headers.FileInfo("", true, 0, 524_288).Name()
	epochBodies := snaptype2.Bodies.FileInfo("", true, 0, 524_288).Name()

	// epoch chain: keep epoch block files, skip decimal ones
	require.False(t, snaptype.EpochRegimeMismatch(epochHeaders, true))
	require.False(t, snaptype.EpochRegimeMismatch(epochBodies, true))
	require.True(t, snaptype.EpochRegimeMismatch(decHeaders, true))

	// decimal chain: keep decimal block files, skip epoch ones
	require.False(t, snaptype.EpochRegimeMismatch(decHeaders, false))
	require.True(t, snaptype.EpochRegimeMismatch(epochHeaders, false))

	// indexes are classified by their own marker, same as segments
	require.True(t, snaptype.EpochRegimeMismatch("v2.0-000000-000500-headers.idx", true))
	require.True(t, snaptype.EpochRegimeMismatch("v2.0-000000-000500-transactions-to-block.idx", true))
	require.False(t, snaptype.EpochRegimeMismatch("v2.0-000000-000512-headers-ep.idx", true))
	require.True(t, snaptype.EpochRegimeMismatch("v2.0-000000-000512-transactions-to-block-ep.idx", false))
	require.False(t, snaptype.EpochRegimeMismatch("v2.0-000000-000500-headers.idx", false))

	// non-block files are never a mismatch, regardless of regime
	for _, epochOn := range []bool{true, false} {
		require.False(t, snaptype.EpochRegimeMismatch("v1.0-accounts.0-2048.kv", epochOn))
		require.False(t, snaptype.EpochRegimeMismatch("salt-blocks.txt", epochOn))
		require.False(t, snaptype.EpochRegimeMismatch("not-a-snapshot", epochOn))
	}
}

// FileInfo.As must keep the source regime and range: the transactions index builder resolves the
// bodies segment through As, and an epoch segment resolved with a decimal /1000 range would look for
// the wrong file (at the 524288 tier, -000524- instead of -000512-).
func TestFileInfoAsKeepsEpoch(t *testing.T) {
	for _, tc := range []struct{ from, to uint64 }{
		{0, 1024},
		{0, 524_288},
		{524_288, 589_824},
		{43_008, 44_032},
	} {
		txInfo := snaptype2.Transactions.FileInfo("/snap", true, tc.from, tc.to)
		got := txInfo.As(snaptype2.Bodies)
		want := snaptype2.Bodies.FileInfo("/snap", true, tc.from, tc.to).Name()
		require.Equal(t, want, got.Name(), "As() name for [%d,%d)", tc.from, tc.to)
		require.True(t, got.Epoch)
		require.Equal(t, tc.from, got.From)
		require.Equal(t, tc.to, got.To)
	}
}

// A decimal mask's leading version wildcard also matches an "ep"-marked name that prints the same
// digits: decimal [1000,2000) and epoch [1024,2048) are both "000001-000002". The two address
// different block ranges, so a lookup in one regime must never settle for the other's file.
func TestLookupDoesNotCrossRegime(t *testing.T) {
	dir := t.TempDir()
	decimal := snaptype2.Headers.FileInfo(dir, false, 1_000, 2_000)
	epoch := snaptype2.Headers.FileInfo(dir, true, 1_024, 2_048)
	for _, f := range []snaptype.FileInfo{decimal, epoch} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f.Name()), []byte{}, 0o644))
	}

	// the marker has to stay clear of the mask's version wildcard: a mask is version-wildcarded because
	// an index's version may differ from its segment's, and `*` spans dashes.
	decMask := snaptype.SegmentFileMask(false, 1_000, 2_000, snaptype2.Enums.Headers)
	crosses, err := filepath.Match(decMask, epoch.Name())
	require.NoError(t, err)
	require.False(t, crosses, "decimal mask %q matches the epoch name %q", decMask, epoch.Name())

	found := snaptype2.Headers.FileInfoByMask(dir, false, 1_000, 2_000)
	require.False(t, found.Epoch)
	require.Equal(t, [2]uint64{1_000, 2_000}, [2]uint64{found.From, found.To})

	found = snaptype2.Headers.FileInfoByMask(dir, true, 1_024, 2_048)
	require.True(t, found.Epoch)
	require.Equal(t, [2]uint64{1_024, 2_048}, [2]uint64{found.From, found.To})
}

func TestEpochSubTierNameSpellsOutBlocks(t *testing.T) {
	small := snaptype2.Headers.FileInfo("", true, 8, 16)
	require.Equal(t, "v1.1-000000008-000000016-headers-ep.seg", small.Name())

	res, _, ok := snaptype.ParseFileName("", small.Name())
	require.True(t, ok)
	require.True(t, res.Epoch)
	require.Equal(t, [2]uint64{8, 16}, [2]uint64{res.From, res.To})

	// a full tier is unaffected
	full := snaptype2.Headers.FileInfo("", true, 0, 1024)
	require.Equal(t, "v1.1-000000-000001-headers-ep.seg", full.Name())
}
