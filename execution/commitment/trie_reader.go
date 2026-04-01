// Copyright 2024 The Erigon Authors
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

// TrieReader navigates the Patricia trie by hashed key without any mutable grid
// state. Each Lookup call starts from the root and descends independently.
type TrieReader struct {
	ctx PatriciaContext
}

// NewTrieReader creates a TrieReader that uses ctx for branch lookups.
func NewTrieReader(ctx PatriciaContext) *TrieReader {
	return &TrieReader{ctx: ctx}
}

// parseCellAt parses exactly one cell from branch cell data at position nibble.
// data is the cell data portion of branch data (after the 4-byte touchMap+afterMap header).
// bitmap is the afterMap indicating which nibbles are present.
func parseCellAt(data []byte, bitmap uint16, nibble int) (c cell, err error) {
	if bitmap&(uint16(1)<<nibble) == 0 {
		return c, fmt.Errorf("nibble %d not set in bitmap %016b", nibble, bitmap)
	}

	pos := 0
	for bitset := bitmap; bitset != 0; {
		bit := bitset & -bitset
		n := bits.TrailingZeros16(bit)

		if n == nibble {
			if pos >= len(data) {
				return c, fmt.Errorf("branch data truncated at nibble %d", nibble)
			}
			fieldBits := data[pos]
			pos++
			if _, err = c.fillFromFields(data, pos, cellFields(fieldBits)); err != nil {
				return c, fmt.Errorf("fillFromFields nibble %d: %w", nibble, err)
			}
			return c, nil
		}

		// Skip this cell's fields to advance pos.
		if pos >= len(data) {
			return c, fmt.Errorf("branch data truncated before nibble %d", nibble)
		}
		fieldBits := data[pos]
		pos = skipCellFields(data, pos+1, fieldBits)
		bitset ^= bit
	}

	return c, fmt.Errorf("nibble %d not reached in bitmap iteration", nibble)
}

// Lookup descends the trie for hashedKey and returns the leaf cell if found.
// hashedKey is a slice of nibbles (one nibble per byte, values 0-15).
// Returns (cell, true, nil) on hit, (zero cell, false, nil) on miss, or an error.
func (tr *TrieReader) Lookup(hashedKey []byte) (c cell, found bool, err error) {
	depth := 0
	for {
		if depth >= len(hashedKey) {
			return c, false, nil
		}

		prefix := HexNibblesToCompactBytes(hashedKey[:depth])
		branchData, _, err := tr.ctx.Branch(prefix)
		if err != nil {
			return c, false, fmt.Errorf("Branch at depth %d: %w", depth, err)
		}
		// Branch data layout: [touchMap(2)][afterMap(2)][cell_data...]
		if len(branchData) < 4 {
			return c, false, nil
		}

		afterMap := binary.BigEndian.Uint16(branchData[2:4])
		cellData := branchData[4:]
		nibble := int(hashedKey[depth])

		if afterMap&(uint16(1)<<nibble) == 0 {
			return c, false, nil
		}

		c, err = parseCellAt(cellData, afterMap, nibble)
		if err != nil {
			return c, false, err
		}

		// Advance past the nibble we matched in the bitmap.
		depth++

		// If the cell has an extension, verify it matches and advance depth.
		if c.hashedExtLen > 0 {
			extEnd := depth + int(c.hashedExtLen)
			if extEnd > len(hashedKey) {
				return c, false, nil
			}
			for i := int16(0); i < c.hashedExtLen; i++ {
				if hashedKey[depth+int(i)] != c.hashedExtension[i] {
					return c, false, nil
				}
			}
			depth = extEnd
		}

		// Leaf: account or storage address present.
		if c.accountAddrLen > 0 || c.storageAddrLen > 0 {
			return c, true, nil
		}

		// Branch hash: the cell references a deeper subtree, continue.
		if c.hashLen > 0 {
			continue
		}

		// No useful data in this cell — key not in trie.
		return c, false, nil
	}
}
