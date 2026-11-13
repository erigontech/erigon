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

type BranchMaps struct {
	Bitmap   uint16
	TouchMap uint16
	AfterMap uint16
}

// branchData must have its leading 2-byte touch-map prefix already stripped by the caller.
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
