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

	"github.com/erigontech/erigon-lib/common"
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

// IndexAndPeriodKey encodes index and period (can be epoch/slot/epoch period) into 8 bytes
func IndexAndPeriodKey(index, timeframe uint64) (out []byte) {
	out = make([]byte, 8)
	binary.BigEndian.PutUint32(out[:4], uint32(index))
	binary.BigEndian.PutUint32(out[4:], uint32(timeframe))
	return
}

// Encode a number with least amount of bytes
func EncodeCompactUint64(x uint64) (out []byte) {
	for x >= 0x80 {
		out = append(out, byte(x)|0x80)
		x >>= 7
	}
	out = append(out, byte(x))
	return
}

// DecodeCompactUint64 decodes a number encoded with EncodeCompactUint64
func DecodeCompactUint64(buf []byte) (x uint64) {
	for i := 0; i < len(buf); i++ {
		x |= uint64(buf[i]&0x7f) << (7 * uint(i))
		if buf[i]&0x80 == 0 {
			return
		}
	}
	return
}

func EncodePeriodAndRoot(period uint32, root common.Hash) []byte {
	out := make([]byte, 36)
	binary.BigEndian.PutUint32(out[:4], period)
	copy(out[4:], root[:])
	return out
}
