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

// HandleSize is the fixed encoded width of a Handle: an 8-byte offset. The value
// length is not stored in MDBX; it is length-prefixed in the value-file record so
// the reader recovers it on read. Fixed width keeps an external DupSort record a
// constant size so it can be rewritten in place rather than delete+insert.
const HandleSize = 8

// Handle is the offset of a value's record inside a step's value-file, stored in
// MDBX in place of the value bytes. The record at Offset is `uvarint(len) || value`.
type Handle struct {
	Offset uint64
}

// AppendTo appends the fixed-width big-endian offset to b and returns the extended slice.
func (h Handle) AppendTo(b []byte) []byte {
	var buf [HandleSize]byte
	binary.BigEndian.PutUint64(buf[:], h.Offset)
	return append(b, buf[:]...)
}

// DecodeHandle reads a handle from the front of b, returning it and the number of
// bytes consumed.
func DecodeHandle(b []byte) (Handle, int, error) {
	if len(b) < HandleSize {
		return Handle{}, 0, fmt.Errorf("valfile: short handle: have %d bytes, need %d", len(b), HandleSize)
	}
	return Handle{Offset: binary.BigEndian.Uint64(b[:HandleSize])}, HandleSize, nil
}
