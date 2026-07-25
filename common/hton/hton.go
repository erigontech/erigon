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

package hton

// U64 writes value to buffer at index with big endian byte order
func U64(buffer []byte, index int, value uint64) []byte {
	buffer[index] = byte(value >> 56)
	buffer[index+1] = byte(value >> 48)
	buffer[index+2] = byte(value >> 40)
	buffer[index+3] = byte(value >> 32)
	buffer[index+4] = byte(value >> 24)
	buffer[index+5] = byte(value >> 16)
	buffer[index+6] = byte(value >> 8)
	buffer[index+7] = byte(value)
	return buffer
}

// U32 writes value to buffer at index with big endian byte order
func U32(buffer []byte, index int, value uint32) []byte {
	buffer[index] = byte(value >> 24)
	buffer[index+1] = byte(value >> 16)
	buffer[index+2] = byte(value >> 8)
	buffer[index+3] = byte(value)
	return buffer
}

// U16 writes value to buffer at index with big endian byte order
func U16(buffer []byte, index int, value uint16) []byte {
	buffer[index] = byte(value >> 8)
	buffer[index+1] = byte(value)
	return buffer
}

// U8 writes value to buffer at index with big endian byte order
func U8(buffer []byte, index int, value uint8) []byte {
	buffer[index] = value
	return buffer
}

// UInt writes value to buffer at index with big endian byte
// order, using the least number of bytes needed to represent value
func UInt(buffer []byte, index int, value uint64) (int, []byte) {
	switch {
	case value < (1 << 8):
		buffer[index+0] = byte(value)
		return 1, buffer[index : index+1]
	case value < (1 << 16):
		buffer[index+0] = byte(value >> 8)
		buffer[index+1] = byte(value)
		return 2, buffer[index : index+2]
	case value < (1 << 24):
		buffer[index+0] = byte(value >> 16)
		buffer[index+1] = byte(value >> 8)
		buffer[index+2] = byte(value)
		return 3, buffer[index : index+3]
	case value < (1 << 32):
		buffer[index+0] = byte(value >> 24)
		buffer[index+1] = byte(value >> 16)
		buffer[index+2] = byte(value >> 8)
		buffer[index+3] = byte(value)
		return 4, buffer[index : index+4]
	case value < (1 << 40):
		buffer[index+0] = byte(value >> 32)
		buffer[index+1] = byte(value >> 24)
		buffer[index+2] = byte(value >> 16)
		buffer[index+3] = byte(value >> 8)
		buffer[index+4] = byte(value)
		return 5, buffer[index : index+5]
	case value < (1 << 48):
		buffer[index+0] = byte(value >> 40)
		buffer[index+1] = byte(value >> 32)
		buffer[index+2] = byte(value >> 24)
		buffer[index+3] = byte(value >> 16)
		buffer[index+4] = byte(value >> 8)
		buffer[index+5] = byte(value)
		return 6, buffer[index : index+6]
	case value < (1 << 56):
		buffer[index+0] = byte(value >> 48)
		buffer[index+1] = byte(value >> 40)
		buffer[index+2] = byte(value >> 32)
		buffer[index+3] = byte(value >> 24)
		buffer[index+4] = byte(value >> 16)
		buffer[index+5] = byte(value >> 8)
		buffer[index+6] = byte(value)
		return 7, buffer[index : index+7]
	default:
		buffer[index+0] = byte(value >> 56)
		buffer[index+1] = byte(value >> 48)
		buffer[index+2] = byte(value >> 40)
		buffer[index+3] = byte(value >> 32)
		buffer[index+4] = byte(value >> 24)
		buffer[index+5] = byte(value >> 16)
		buffer[index+6] = byte(value >> 8)
		buffer[index+7] = byte(value)
		return 8, buffer[index : index+8]
	}
}

// UIntLen computes the minimum number of bytes required to store i.
func UIntLen(value uint64) (len int) {
	for len = 1; ; len++ {
		if value >>= 8; value == 0 {
			return len
		}
	}
}
