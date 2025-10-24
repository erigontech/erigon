// Copyright 2022 The Erigon Authors
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

package rlp

import "io"

// RLP-related utilities necessary for computing commitments for state root hash

// generateRlpPrefixLenDouble calculates the length of RLP prefix to encode a string of bytes of length l "twice",
// meaning that it is the prefix for rlp(rlp(data))
func generateRlpPrefixLenDouble(l int, firstByte byte) int {
	if l < 2 {
		// firstByte only matters when there is 1 byte to encode
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

// RlpSerializable is a value that can be double-RLP coded.
type RlpSerializable interface {
	ToDoubleRLP(io.Writer, []byte) error
	DoubleRLPLen() int
	RawBytes() []byte
}

type RlpSerializableBytes []byte

func (b RlpSerializableBytes) ToDoubleRLP(w io.Writer, prefixBuf []byte) error {
	return encodeBytesAsRlpToWriter(b, w, generateByteArrayLenDouble, prefixBuf)
}

func (b RlpSerializableBytes) RawBytes() []byte {
	return b
}

func (b RlpSerializableBytes) DoubleRLPLen() int {
	if len(b) < 1 {
		return 0
	}
	return generateRlpPrefixLenDouble(len(b), b[0]) + len(b)
}

type RlpEncodedBytes []byte

func (b RlpEncodedBytes) ToDoubleRLP(w io.Writer, prefixBuf []byte) error {
	return encodeBytesAsRlpToWriter(b, w, generateByteArrayLen, prefixBuf)
}

func (b RlpEncodedBytes) RawBytes() []byte {
	return b
}

func (b RlpEncodedBytes) DoubleRLPLen() int {
	return generateRlpPrefixLen(len(b)) + len(b)
}

func encodeBytesAsRlpToWriter(source []byte, w io.Writer, prefixGenFunc func([]byte, int, int) int, prefixBuf []byte) error {
	// > 1 byte, write a prefix or prefixes first
	if len(source) > 1 || (len(source) == 1 && source[0] >= 0x80) {
		prefixLen := prefixGenFunc(prefixBuf, 0, len(source))

		if _, err := w.Write(prefixBuf[:prefixLen]); err != nil {
			return err
		}
	}

	_, err := w.Write(source)
	return err
}

func EncodeByteArrayAsRlp(raw []byte, w io.Writer, prefixBuf []byte) (int, error) {
	err := encodeBytesAsRlpToWriter(raw, w, generateByteArrayLen, prefixBuf)
	if err != nil {
		return 0, err
	}
	return generateRlpPrefixLen(len(raw)) + len(raw), nil
}

func GenerateStructLen(buffer []byte, l int) int {
	if l < 56 {
		buffer[0] = byte(192 + l)
		return 1
	}
	if l < 256 {
		// l can be encoded as 1 byte
		buffer[1] = byte(l)
		buffer[0] = byte(247 + 1)
		return 2
	}
	if l < 65536 {
		buffer[2] = byte(l & 255)
		buffer[1] = byte(l >> 8)
		buffer[0] = byte(247 + 2)
		return 3
	}
	buffer[3] = byte(l & 255)
	buffer[2] = byte((l >> 8) & 255)
	buffer[1] = byte(l >> 16)
	buffer[0] = byte(247 + 3)
	return 4
}
