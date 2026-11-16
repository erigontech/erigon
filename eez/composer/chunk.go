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
	"fmt"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/types"
)

// GridCap is the largest multiple of k not above maxBlocks, at least k: a
// chunk ends on the Sync grid, so the cap's remainder is never spent.
func GridCap(maxBlocks, k uint64) uint64 {
	if k == 0 {
		return maxBlocks
	}
	return max(maxBlocks-maxBlocks%k, k)
}

// ChunkBoundary is the terminal of a bounded historical chunk: the highest
// height within limit blocks of the cursor that shares sync's place on the
// grid of k, since derivation rebuilds a terminal by its position, when it
// lies between the cursor and sync; else the one k below sync. It fails when
// that is not above the cursor.
func ChunkBoundary(cursor, sync, k, limit uint64) (uint64, error) {
	over := sync - min(sync, cursor+limit)
	b := sync - min(sync, (over+k-1)/k*k)
	if b <= cursor || b >= sync {
		b = sync - min(sync, k)
	}
	if b <= cursor {
		return 0, fmt.Errorf("no grid height above cursor %d below Sync block %d", cursor, sync)
	}
	return b, nil
}

// EmptyBoundaries returns, highest first, the grid heights of down whose
// blocks carry no transactions. down holds consecutive headers descending
// from a grid height, so every k-th one is on the grid.
func EmptyBoundaries(down []*types.Header, k uint64) []uint64 {
	var out []uint64
	for i := uint64(0); i < uint64(len(down)); i += k {
		if down[i].TxHash == empty.RootHash {
			out = append(out, down[i].Number.Uint64())
		}
	}
	return out
}
