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
	"errors"
	"fmt"
	"math/bits"
)

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
		fields := CellFields(branchData[pos])
		pos++
		if fields&^(FieldExtension|FieldAccountAddr|FieldStorageAddr|FieldHash|FieldStateHash) != 0 {
			return fmt.Errorf("unknown cell fields %08b at nibble %d", fields, nibble)
		}
		var c BranchCell
		for _, f := range [...]struct {
			flag CellFields
			dst  *[]byte
		}{
			{FieldExtension, &c.Extension},
			{FieldAccountAddr, &c.AccountAddr},
			{FieldStorageAddr, &c.StorageAddr},
			{FieldHash, &c.Hash},
			{FieldStateHash, nil},
		} {
			if fields&f.flag == 0 {
				continue
			}
			l, n, err := ReadUvarint(branchData[pos:])
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

// MaxCompactKeyLen bounds a compact-encoded (hex-prefix) key or branch prefix,
// derived from the 128-nibble key path rather than from the shorter keys today's depth
// arithmetic yields, so a caller slicing deeper cannot overflow the buffer.
const MaxCompactKeyLen = 128/2 + 1

func ReadUvarint(data []byte) (uint64, int, error) {
	l, n := binary.Uvarint(data)
	if n == 0 {
		return 0, 0, errors.New("buffer too small for length")
	} else if n < 0 {
		return 0, 0, errors.New("value overflow for length")
	}
	return l, n, nil
}

// skipCellFields skips over the fields in branch data based on fieldBits.
// Used during warmup traversal to find the right child's data.
func skipCellFields(data []byte, pos int, fieldBits byte) int {
	// Field flags: extension=1, accountAddr=2, storageAddr=4, hash=8, stateHash=16
	for bit := byte(1); bit <= 16; bit <<= 1 {
		if fieldBits&bit != 0 {
			if pos >= len(data) {
				return pos
			}
			// Read length varint
			l, n := binary.Uvarint(data[pos:])
			if n <= 0 {
				return pos
			}
			pos += n + int(l) // Skip length + data
		}
	}
	return pos
}
