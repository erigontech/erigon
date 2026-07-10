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

package prune

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBlocksDistance(t *testing.T) {
	cases := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "0", want: 0},
		{in: "262144", want: 262_144},
		{in: "  100  ", want: 100},
		{in: "keep-post-merge", want: uint64(KeepPostMergeBlocksPruneMode)},
		{in: "KEEP-POST-MERGE", want: uint64(KeepPostMergeBlocksPruneMode)},
		{in: "keep-all", want: uint64(KeepAllBlocksPruneMode)},
		{in: "Keep-All", want: uint64(KeepAllBlocksPruneMode)},
		// Raw sentinel numbers must keep working for backward compatibility.
		{in: "18446744073709551615", want: uint64(KeepPostMergeBlocksPruneMode)},
		{in: "18446744073709551614", want: uint64(KeepAllBlocksPruneMode)},
		// Prefix-aware bases must parse as they did under the previous
		// cli.Uint64Flag (stdlib base 0), including leading-zero octal.
		{in: "0x40000", want: 262_144},
		{in: "0o20", want: 16},
		{in: "0b1000", want: 8},
		{in: "010", want: 8},
		// Only keep-post-merge and keep-all are recognized.
		{in: "post-merge", wantErr: true},
		{in: "history-expiry", wantErr: true},
		{in: "all", wantErr: true},
		{in: "garbage", wantErr: true},
		{in: "-1", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseBlocksDistance(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseHistoryDistance(t *testing.T) {
	cases := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "100000", want: 100_000},
		{in: "keep-all", want: uint64(math.MaxUint64)},
		{in: "Keep-All", want: uint64(math.MaxUint64)},
		{in: "18446744073709551615", want: uint64(math.MaxUint64)},
		// Prefix-aware bases keep working (stdlib base-0 parity).
		{in: "0x186a0", want: 100_000},
		{in: "010", want: 8},
		// keep-post-merge is block-only; everything else is invalid.
		{in: "all", wantErr: true},
		{in: "keep-post-merge", wantErr: true},
		{in: "garbage", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseHistoryDistance(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBlocksDistanceCLIValue(t *testing.T) {
	assert.Equal(t, "keep-post-merge", blocksDistanceCLIValue(uint64(KeepPostMergeBlocksPruneMode)))
	assert.Equal(t, "keep-all", blocksDistanceCLIValue(uint64(KeepAllBlocksPruneMode)))
	assert.Equal(t, "262144", blocksDistanceCLIValue(262_144))
}

// A keep-post-merge Blocks sentinel must render with its readable alias rather
// than the raw MaxUint64 magic number.
func TestModeString_BlocksSentinelAlias(t *testing.T) {
	blockDist, err := ParseBlocksDistance("keep-post-merge")
	require.NoError(t, err)

	mode, err := FromCli(archiveModeStr, 0, blockDist, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, KeepPostMergeBlocksPruneMode, mode.Blocks)
	assert.Equal(t, "archive --prune.distance.blocks=keep-post-merge", mode.String())
}

func TestParseCommitmentHistoryDistance(t *testing.T) {
	cases := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "100000", want: 100_000},
		{in: "keep-all", want: uint64(KeepAllBlocksPruneMode)},
		{in: "Keep-All", want: uint64(KeepAllBlocksPruneMode)},
		{in: "0x186a0", want: 100_000},
		{in: "keep-post-merge", wantErr: true},
		{in: "garbage", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseCommitmentHistoryDistance(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseReceiptsDistance(t *testing.T) {
	cases := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "100000", want: 100_000},
		// keep-all maps to KeepAllReceiptsPruneMode, distinct from the
		// KeepAllBlocksPruneMode follow-history default.
		{in: "keep-all", want: uint64(KeepAllReceiptsPruneMode)},
		{in: "Keep-All", want: uint64(KeepAllReceiptsPruneMode)},
		{in: "keep-post-merge", wantErr: true},
		{in: "garbage", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseReceiptsDistance(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Explicit receipts keep-all must round-trip through FromCli, be distinct from
// the follow-history default, and render with its alias (not the magic number).
func TestModeString_ReceiptsKeepAll(t *testing.T) {
	dist, err := ParseReceiptsDistance("keep-all")
	require.NoError(t, err)

	mode, err := FromCli(minimalModeStr, 0, 0, 0, dist)
	require.NoError(t, err)
	assert.Equal(t, KeepAllReceiptsPruneMode, mode.Receipts)
	assert.False(t, mode.ReceiptsFollowHistory(), "explicit keep-all is not the follow-history default")
	assert.Contains(t, mode.String(), "--prune.receipts.distance=keep-all")

	def, err := FromCli(minimalModeStr, 0, 0, 0, 0)
	require.NoError(t, err)
	assert.True(t, def.ReceiptsFollowHistory(), "unset receipts follows history")
	assert.NotContains(t, def.String(), "prune.receipts.distance")
}
