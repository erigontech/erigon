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

package commitment

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// BranchMaps captures the three branch-level bitmasks decoded alongside
// the per-cell payload.
type BranchMaps struct {
	Bitmap   uint16 // present-children bitmap (canonical encoded map)
	TouchMap uint16 // children touched in this branch (per the deleted flag)
	AfterMap uint16 // children present after this commitment step
}

// DecodeBranchInto parses the on-disk encoded form of a branch into cells.
// branchData must already have the leading 2-byte touch-map prefix stripped.
//
// deleted=true  → touchMap=bitmap, afterMap=0 (touched-but-not-present-after).
// deleted=false → touchMap=0, afterMap=bitmap (present-after).
//
// Pure decode — does NOT call deriveHashedKeys on the cells. Trie callers do
// that themselves (they have the keccak scratch buffer); cache callers can
// defer it until the cell is consumed by the trie.
func DecodeBranchInto(
	branchData []byte,
	deleted bool,
	cells *[16]cell,
) (BranchMaps, error) {
	if len(branchData) < 2 {
		return BranchMaps{}, fmt.Errorf("branch data too short for bitmap: %d bytes", len(branchData))
	}
	bitmap := binary.BigEndian.Uint16(branchData[0:])
	maps := BranchMaps{Bitmap: bitmap}
	if deleted {
		maps.TouchMap, maps.AfterMap = bitmap, 0
	} else {
		maps.TouchMap, maps.AfterMap = 0, bitmap
	}

	pos := 2
	for bitset := bitmap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		c := &cells[nibble]
		if pos >= len(branchData) {
			return BranchMaps{}, fmt.Errorf("branch data truncated before cell at nibble %d", nibble)
		}
		fieldBits := branchData[pos]
		pos++
		newPos, err := c.fillFromFields(branchData, pos, cellFields(fieldBits))
		if err != nil {
			return BranchMaps{}, fmt.Errorf("fillFromFields nibble %d: %w", nibble, err)
		}
		pos = newPos
		bitset ^= bit
	}
	return maps, nil
}
