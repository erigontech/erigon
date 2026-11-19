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

type BranchCell struct {
	Extension   []byte
	AccountAddr []byte
	StorageAddr []byte
	Hash        []byte
}

func (branchData BranchData) ForEachCell(fn func(nibble int, c BranchCell) error) error {
	if len(branchData) < 4 {
		return fmt.Errorf("branch data too short: %d bytes", len(branchData))
	}
	if !branchData.IsComplete() {
		return fmt.Errorf("branch data is a partial update: touchMap %04x afterMap %04x",
			binary.BigEndian.Uint16(branchData[0:]), binary.BigEndian.Uint16(branchData[2:]))
	}
	pos := 4
	for bitset := binary.BigEndian.Uint16(branchData[2:]); bitset != 0; bitset &= bitset - 1 {
		nibble := bits.TrailingZeros16(bitset)
		if pos >= len(branchData) {
			return fmt.Errorf("branch data truncated before cell at nibble %d", nibble)
		}
		fields := cellFields(branchData[pos])
		pos++
		if fields&^(fieldExtension|fieldAccountAddr|fieldStorageAddr|fieldHash|fieldStateHash) != 0 {
			return fmt.Errorf("unknown cell fields %08b at nibble %d", fields, nibble)
		}
		var c BranchCell
		for _, f := range [...]struct {
			flag cellFields
			dst  *[]byte
		}{
			{fieldExtension, &c.Extension},
			{fieldAccountAddr, &c.AccountAddr},
			{fieldStorageAddr, &c.StorageAddr},
			{fieldHash, &c.Hash},
			{fieldStateHash, nil},
		} {
			if fields&f.flag == 0 {
				continue
			}
			l, n, err := readUvarint(branchData[pos:])
			if err != nil {
				return fmt.Errorf("cell %v at nibble %d: %w", f.flag, nibble, err)
			}
			pos += n
			if uint64(len(branchData)-pos) < l {
				return fmt.Errorf("cell %v at nibble %d: %d bytes past the end", f.flag, nibble, uint64(pos)+l-uint64(len(branchData)))
			}
			if f.dst != nil {
				*f.dst = branchData[pos : pos+int(l)]
			}
			pos += int(l)
		}
		if err := fn(nibble, c); err != nil {
			return err
		}
	}
	if pos != len(branchData) {
		return fmt.Errorf("branch data has %d trailing bytes", len(branchData)-pos)
	}
	return nil
}
