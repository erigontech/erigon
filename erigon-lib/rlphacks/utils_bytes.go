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

package rlphacks

func multiByteHeaderPrefixOfLen(l int) byte {
	// > If a string is more than 55 bytes long, the
	// > RLP encoding consists of a single byte with value 0xB7 plus the length
	// > of the length of the string in binary form, followed by the length of
	// > the string, followed by the string. For example, a length-1024 string
	// > would be encoded as 0xB90400 followed by the string. The range of
	// > the first byte is thus [0xB8, 0xBF].
	//
	// see package rlp/decode.go:887
	return byte(0xB7 + l)
}

func generateByteArrayLen(buffer []byte, pos int, l int) int {
	if l < 56 {
		buffer[pos] = byte(0x80 + l)
		pos++
	} else if l < 256 {
		// len(vn) can be encoded as 1 byte
		buffer[pos] = multiByteHeaderPrefixOfLen(1)
		pos++
		buffer[pos] = byte(l)
		pos++
	} else if l < 65536 {
		// len(vn) is encoded as two bytes
		buffer[pos] = multiByteHeaderPrefixOfLen(2)
		pos++
		buffer[pos] = byte(l >> 8)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	} else {
		// len(vn) is encoded as three bytes
		buffer[pos] = multiByteHeaderPrefixOfLen(3)
		pos++
		buffer[pos] = byte(l >> 16)
		pos++
		buffer[pos] = byte((l >> 8) & 255)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	}
	return pos
}

func generateRlpPrefixLen(l int) int {
	if l < 2 {
		return 0
	}
	if l < 56 {
		return 1
	}
	if l < 256 {
		return 2
	}
	if l < 65536 {
		return 3
	}
	return 4
}

func generateRlpPrefixLenDouble(l int, firstByte byte) int {
	if l < 2 {
		if firstByte >= 0x80 {
			return 2
		}
		return 0
	}
	if l < 55 {
		return 2
	}
	if l < 56 { // 2 + 1
		return 3
	}
	if l < 254 {
		return 4
	}
	if l < 256 {
		return 5
	}
	if l < 65533 {
		return 6
	}
	if l < 65536 {
		return 7
	}
	return 8
}

func generateByteArrayLenDouble(buffer []byte, pos int, l int) int {
	if l < 55 {
		// After first wrapping, the length will be l + 1 < 56
		buffer[pos] = byte(0x80 + l + 1)
		pos++
		buffer[pos] = byte(0x80 + l)
		pos++
	} else if l < 56 {
		buffer[pos] = multiByteHeaderPrefixOfLen(1)
		pos++
		buffer[pos] = byte(l + 1)
		pos++
		buffer[pos] = byte(0x80 + l)
		pos++
	} else if l < 254 {
		// After first wrapping, the length will be l + 2 < 256
		buffer[pos] = multiByteHeaderPrefixOfLen(1)
		pos++
		buffer[pos] = byte(l + 2)
		pos++
		buffer[pos] = multiByteHeaderPrefixOfLen(1)
		pos++
		buffer[pos] = byte(l)
		pos++
	} else if l < 256 {
		// First wrapping is 2 bytes, second wrapping 3 bytes
		buffer[pos] = multiByteHeaderPrefixOfLen(2)
		pos++
		buffer[pos] = byte((l + 2) >> 8)
		pos++
		buffer[pos] = byte((l + 2) & 255)
		pos++
		buffer[pos] = multiByteHeaderPrefixOfLen(1)
		pos++
		buffer[pos] = byte(l)
		pos++
	} else if l < 65533 {
		// Both wrappings are 3 bytes
		buffer[pos] = multiByteHeaderPrefixOfLen(2)
		pos++
		buffer[pos] = byte((l + 3) >> 8)
		pos++
		buffer[pos] = byte((l + 3) & 255)
		pos++
		buffer[pos] = multiByteHeaderPrefixOfLen(2)
		pos++
		buffer[pos] = byte(l >> 8)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	} else if l < 65536 {
		// First wrapping is 3 bytes, second wrapping is 4 bytes
		buffer[pos] = multiByteHeaderPrefixOfLen(3)
		pos++
		buffer[pos] = byte((l + 3) >> 16)
		pos++
		buffer[pos] = byte(((l + 3) >> 8) & 255)
		pos++
		buffer[pos] = byte((l + 3) & 255)
		pos++
		buffer[pos] = multiByteHeaderPrefixOfLen(2)
		pos++
		buffer[pos] = byte((l >> 8) & 255)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	} else {
		// Both wrappings are 4 bytes
		buffer[pos] = multiByteHeaderPrefixOfLen(3)
		pos++
		buffer[pos] = byte((l + 4) >> 16)
		pos++
		buffer[pos] = byte(((l + 4) >> 8) & 255)
		pos++
		buffer[pos] = byte((l + 4) & 255)
		pos++
		buffer[pos] = multiByteHeaderPrefixOfLen(3)
		pos++
		buffer[pos] = byte(l >> 16)
		pos++
		buffer[pos] = byte((l >> 8) & 255)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	}
	return pos
}
