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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
)

// retireSegments drives the real produce path: the retire loop calls CanRetire to pick one
// tier at a time, and DumpBlocks carves that range with chooseSegmentEnd. It returns the
// [from,to) of every segment file that would be produced for [0, available). The regime is
// selected by snapType (a block type is epoch, a Bor type is decimal), not by any switch.
func retireSegments(available uint64, snapType snaptype.Enum, epoch bool, cfg *snapcfg.Cfg) [][2]uint64 {
	var segs [][2]uint64
	for from := uint64(0); ; {
		bf, bt, can := snapshotsync.CanRetire(epoch, from, available, snapType, cfg, 0)
		if !can {
			return segs
		}
		for i := bf; i < bt; {
			end := chooseSegmentEnd(epoch, i, bt, snapType, cfg)
			segs = append(segs, [2]uint64{i, end})
			i = end
		}
		from = bt
	}
}

// The epoch produce path (block stream) decomposes a range into clean 1024/8192/65536/524288
// tiers, and chooseSegmentEnd preserves each tier boundary (the 1024 round-down never corrupts it).
func TestEpochRetirePipeline(t *testing.T) {
	require.Equal(t, [][2]uint64{
		{0, 524_288},       // 64 era1 (top tier)
		{524_288, 589_824}, // 8 era1
		{589_824, 598_016}, // 1 era1
		{598_016, 599_040}, // min tier (1024)
	}, retireSegments(600_000, snaptype.Unknown, true, &snapcfg.Cfg{}))
	// tail [599_040, 600_000) is < 1024 and stays in the DB.
}

// A retire step below the produce tier hands DumpBlocks ranges shorter than the tier, like
// [1024,1032); rounding their end down would give an empty range and the dump loop would stand still.
func TestChooseSegmentEndAlwaysAdvances(t *testing.T) {
	cfg := &snapcfg.Cfg{}
	require.Equal(t, uint64(1_032), chooseSegmentEnd(true, 1_024, 1_032, snaptype.Unknown, cfg))
	require.Equal(t, uint64(1_008), chooseSegmentEnd(false, 1_000, 1_008, snaptype.Unknown, cfg))
	require.Equal(t, uint64(599_040), chooseSegmentEnd(true, 598_016, 600_000, snaptype.Unknown, cfg), "a tier-long range still rounds down")
	require.Equal(t, uint64(900), chooseSegmentEnd(true, 0, 900, snaptype.Unknown, cfg), "a chain shorter than the tier is one file")

	for _, epoch := range []bool{true, false} {
		for _, from := range []uint64{0, 8, 1_000, 1_024, 9_216, 524_288} {
			for _, to := range []uint64{from + 1, from + 8, from + 1_000, from + 1_024, from + 5_000} {
				end := chooseSegmentEnd(epoch, from, to, snaptype.Unknown, cfg)
				require.Greater(t, end, from, "epoch=%v [%d,%d)", epoch, from, to)
				require.LessOrEqual(t, end, to, "epoch=%v [%d,%d)", epoch, from, to)
			}
		}
	}
}

// The decimal tier decomposition (1000/10000/100000) is what a non-epoch chain gets; the regime
// comes from the explicit epoch argument, not from the type.
func TestDecimalRetirePipeline(t *testing.T) {
	require.Equal(t, [][2]uint64{
		{0, 100_000},
		{100_000, 110_000},
		{110_000, 120_000},
	}, retireSegments(120_000, snaptype.Unknown, false, &snapcfg.Cfg{}))
}

// A decimal segment is compressed only at the frozen (merge-limit) size, smaller tiers are left for
// merge to compress. An epoch segment is left uncompressed only at the 1024 produce tier: the merger
// compresses every tier above it, and a migrated or dumped 8192 or 65536 file must match.
func TestSegmentNoCompress(t *testing.T) {
	require.False(t, segmentNoCompress(false, snaptype.Erigon2MergeLimit)) // decimal frozen -> compress
	require.True(t, segmentNoCompress(false, 10_000))                      // small tier -> uncompressed

	require.False(t, segmentNoCompress(true, snaptype.EpochMergeLimit))
	require.False(t, segmentNoCompress(true, 65_536))
	require.False(t, segmentNoCompress(true, 8_192))
	require.True(t, segmentNoCompress(true, 1_024))
}
