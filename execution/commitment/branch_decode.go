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
)

// MaxCompactKeyLen bounds a compact-encoded (hex-prefix) key or branch prefix,
// derived from the 128-nibble key path rather than from the shorter keys today's depth
// arithmetic yields, so a caller slicing deeper cannot overflow the buffer.
const MaxCompactKeyLen = 128/2 + 1

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
