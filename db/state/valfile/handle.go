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

package valfile

import (
	"encoding/binary"
	"fmt"
)

// Handle is the offset and length of a value inside a step's value-file, stored
// in MDBX in place of the value bytes.
type Handle struct {
	Offset uint64
	Len    uint32
}

// AppendTo appends the varint-encoded handle to b and returns the extended slice.
func (h Handle) AppendTo(b []byte) []byte {
	b = binary.AppendUvarint(b, h.Offset)
	b = binary.AppendUvarint(b, uint64(h.Len))
	return b
}

// DecodeHandle reads a handle from the front of b, returning it and the number of
// bytes consumed.
func DecodeHandle(b []byte) (Handle, int, error) {
	offset, n1 := binary.Uvarint(b)
	if n1 <= 0 {
		return Handle{}, 0, fmt.Errorf("valfile: bad handle offset varint")
	}
	length, n2 := binary.Uvarint(b[n1:])
	if n2 <= 0 {
		return Handle{}, 0, fmt.Errorf("valfile: bad handle len varint")
	}
	return Handle{Offset: offset, Len: uint32(length)}, n1 + n2, nil
}
