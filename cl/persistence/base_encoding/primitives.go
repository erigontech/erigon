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

package base_encoding

import (
	"encoding/binary"
)

func Encode64ToBytes4(x uint64) (out []byte) {
	// little endian
	out = make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(x))
	return
}

func Decode64FromBytes4(buf []byte) (x uint64) {
	// little endian
	return uint64(binary.BigEndian.Uint32(buf))
}
