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
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
)

// chooseSegmentEnd has two modes selected by snCfg.BlockAlignedBoundaries:
//   - Legacy (default false): clamp to next merge boundary, then round
//     DOWN to the nearest Erigon2MinSegmentSize (1000) — every emitted
//     file's To is a multiple of 1000, which is the convention every
//     existing preverified.toml uses.
//   - Aligned (true): clamp to next merge boundary; emit literal block
//     coordinates without rounding. Eliminates partial-block straddles
//     by construction. See
//     memory/block-slot-aligned-storage-model-2026-05-24.

func newTestCfg(aligned bool) *snapcfg.Cfg {
	// Empty PreverifiedParsed — MergeLimit falls through to defaults.
	return &snapcfg.Cfg{
		BlockAlignedBoundaries: aligned,
	}
}

func TestChooseSegmentEnd_LegacyRoundsDownToNearest1k(t *testing.T) {
	// Mid-merge-segment request: from=99_000, to=99_500 (within the
	// default 100_000 merge segment, not on a 1k boundary). Legacy
	// mode rounds DOWN to 99_000; aligned mode preserves 99_500.
	cfg := newTestCfg(false)
	got := chooseSegmentEnd(99_000, 99_500, snaptype2.Enums.Headers, cfg)
	require.Equal(t, uint64(99_000), got,
		"legacy mode rounds DOWN to nearest 1k")
}

func TestChooseSegmentEnd_AlignedReturnsLiteralBlock(t *testing.T) {
	cfg := newTestCfg(true)
	got := chooseSegmentEnd(99_000, 99_500, snaptype2.Enums.Headers, cfg)
	require.Equal(t, uint64(99_500), got,
		"aligned mode preserves the literal block coordinate")
}

func TestChooseSegmentEnd_LegacyAtMergeBoundaryNoRoundingChange(t *testing.T) {
	// At the merge boundary itself (default Erigon2MergeLimit=100_000),
	// the rounding has no effect: 100_000 % 1000 == 0. Both modes
	// return 100_000.
	cfgLegacy := newTestCfg(false)
	cfgAligned := newTestCfg(true)
	require.Equal(t, uint64(100_000), chooseSegmentEnd(0, 100_000, snaptype2.Enums.Headers, cfgLegacy))
	require.Equal(t, uint64(100_000), chooseSegmentEnd(0, 100_000, snaptype2.Enums.Headers, cfgAligned))
}

func TestChooseSegmentEnd_LegacyBelowMinSegmentSizeReturnsTo(t *testing.T) {
	// Below Erigon2MinSegmentSize (1000), legacy returns the literal
	// to without rounding (rounding to 0 would be wrong). Aligned
	// returns the literal too — both modes agree here.
	cfgLegacy := newTestCfg(false)
	cfgAligned := newTestCfg(true)
	require.Equal(t, uint64(500), chooseSegmentEnd(0, 500, snaptype2.Enums.Headers, cfgLegacy))
	require.Equal(t, uint64(500), chooseSegmentEnd(0, 500, snaptype2.Enums.Headers, cfgAligned))
}

func TestChooseSegmentEnd_AlignedClampsToMergeBoundary(t *testing.T) {
	// If the request crosses a merge boundary, aligned mode still
	// clamps to the boundary (one file per merge segment is the
	// fundamental invariant; alignment only affects rounding within
	// a segment). from=0, to=150_000 with default 100_000 merge limit:
	// next merge boundary = 100_000; clamp = 100_000; aligned mode
	// preserves the boundary (no rounding needed since 100_000 is
	// already aligned).
	cfg := newTestCfg(true)
	got := chooseSegmentEnd(0, 150_000, snaptype2.Enums.Headers, cfg)
	require.Equal(t, uint64(100_000), got,
		"aligned mode still clamps to merge boundary")
}

func TestChooseSegmentEnd_AlignedSecondSegmentPreservesLiteral(t *testing.T) {
	// Continuing from a merge boundary: from=100_000, to=199_500.
	// next merge boundary = 200_000; clamp = 199_500.
	// Aligned: return 199_500 literal.
	// Legacy: round 199_500 down to 199_000.
	cfgAligned := newTestCfg(true)
	cfgLegacy := newTestCfg(false)
	require.Equal(t, uint64(199_500), chooseSegmentEnd(100_000, 199_500, snaptype2.Enums.Headers, cfgAligned))
	require.Equal(t, uint64(199_000), chooseSegmentEnd(100_000, 199_500, snaptype2.Enums.Headers, cfgLegacy))
}

func TestChooseSegmentEnd_DefaultCfgPreservesLegacyBehavior(t *testing.T) {
	// A freshly-constructed Cfg with BlockAlignedBoundaries unset
	// (zero value false) behaves as legacy. Guards the
	// "no behaviour change" property on existing chains.
	cfg := &snapcfg.Cfg{}
	require.Equal(t, uint64(99_000), chooseSegmentEnd(99_000, 99_500, snaptype.Unknown, cfg))
}
