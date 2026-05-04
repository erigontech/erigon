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

// BranchMaps captures the three branch-level bitmasks decoded alongside the
// per-cell payload. Held separately from the cells slice so callers can apply
// them into trie state without struct-assigning the whole [16]cell array.
type BranchMaps struct {
	Bitmap   uint16 // present-children bitmap (the canonical map encoded in the branch)
	TouchMap uint16 // children that were touched in this branch (set by caller per deleted flag)
	AfterMap uint16 // children present after this commitment step
}

// DecodeBranchInto parses the on-disk encoded form of a branch (as returned
// by ctx.Branch / readBranchAndCheckForFlushing with the leading 2-byte
// touch-map stripped) and populates the supplied cells array.
//
// branchData must already have the leading touch-map prefix stripped — the
// raw bytes returned from Branch() include it; callers strip it before
// invoking this function (matching the unfoldBranchNode call pattern).
//
// deleted controls the touchMap/afterMap convention:
//   - deleted=true  → all decoded cells are touched-but-not-present-after
//     (touchMap = bitmap, afterMap = 0)
//   - deleted=false → all decoded cells are present-after (touchMap = 0,
//     afterMap = bitmap)
//
// Returns the three branch-level maps for the caller to apply into their
// own state machine. The cells array is mutated in place.
//
// This is a PURE decode — it does NOT call deriveHashedKeys on the cells.
// Trie callers (HexPatriciaHashed.unfoldBranchNode) follow this with their
// own loop calling deriveHashedKeys per cell, since they have the keccak
// scratch buffer. Cache callers can skip deriveHashedKeys entirely until
// the cell is consumed by the trie.
//
// This function is the canonical decoder for branch data. Both
// unfoldBranchNode (which feeds the in-memory grid for fold/unfold) and
// future cache populators consume branches via this same path so the
// encoded→cells transformation lives in one place.
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
