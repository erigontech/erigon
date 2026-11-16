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

package composer

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/types"
)

func TestGridCap(t *testing.T) {
	for _, c := range []struct{ max, k, want uint64 }{
		{300, 5, 300},
		{300, 6, 300},
		{300, 4, 300},
		{300, 7, 294},
		{102, 5, 100},
		{3, 5, 5},
		{5, 5, 5},
	} {
		require.Equal(t, c.want, GridCap(c.max, c.k), "%d over K %d", c.max, c.k)
	}
	require.Equal(t, uint64(5), GridCap(5, 0))
}

// TestChunkBoundary: a chunk ends on the Sync grid, above the cursor and
// below the Sync block, within the cap when one fits it; one K below the
// Sync block when the range fits the cap whole.
func TestChunkBoundary(t *testing.T) {
	const k, sync = 6, uint64(174_198)
	for cursor := sync - 500; cursor < sync-10; cursor++ {
		for _, limit := range []uint64{6, 12, 60, 300} {
			b, err := ChunkBoundary(cursor, sync, k, limit)
			require.NoError(t, err, "cursor %d cap %d", cursor, limit)
			require.Zero(t, (sync-b)%k, "boundary %d off the grid", b)
			require.Greater(t, b, cursor)
			require.Less(t, b, sync)
			if sync-cursor > limit {
				require.LessOrEqual(t, b-cursor, limit, "cursor %d cap %d", cursor, limit)
			}
		}
	}
	// The highest grid height within the cap.
	for _, c := range []struct{ cursor, sync, want uint64 }{
		{0, 6005, 299},
		{137, 6000, 432},
		{100, 406, 400},
	} {
		b, err := ChunkBoundary(c.cursor, c.sync, k, 300)
		require.NoError(t, err)
		require.Equal(t, c.want, b, "cursor %d Sync block %d", c.cursor, c.sync)
	}
	b, err := ChunkBoundary(100, 112, k, 300)
	require.NoError(t, err)
	require.Equal(t, uint64(106), b, "a range within the cap steps one K down")
	_, err = ChunkBoundary(100, 106, k, 300)
	require.Error(t, err, "no grid height between the cursor and the Sync block")
}

// TestEmptyBoundaries: the grid heights from the highest down whose blocks
// carry no transactions, highest first.
func TestEmptyBoundaries(t *testing.T) {
	down := func(top uint64, flags []bool) []*types.Header {
		hs := make([]*types.Header, len(flags))
		for i, e := range flags {
			h := &types.Header{Number: *uint256.NewInt(top - uint64(i)), TxHash: common.Hash{1}}
			if e {
				h.TxHash = empty.RootHash
			}
			hs[i] = h
		}
		return hs
	}
	all := []bool{true, true, true, true, true, true, true, true, true, true, true, true}
	require.Equal(t, []uint64{100, 95, 90}, EmptyBoundaries(down(100, all), 5))
	first := append([]bool{false}, all[1:]...)
	require.Equal(t, []uint64{95, 90}, EmptyBoundaries(down(100, first), 5))
	off := make([]bool, 12)
	off[3] = true
	require.Empty(t, EmptyBoundaries(down(100, off), 5))
	require.Equal(t, []uint64{100}, EmptyBoundaries(down(100, all[:3]), 5))
}
