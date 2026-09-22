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

package v4

func packedLen(nibbleCount int) int {
	if nibbleCount < 0 {
		panic("negative nibble count")
	}
	return (nibbleCount + 1) / 2
}

func packPath(nibbles []byte, dst []byte) []byte {
	packed := packedLen(len(nibbles))
	if cap(dst) < packed {
		dst = make([]byte, packed)
	} else {
		dst = dst[:packed]
	}
	for i := range dst {
		dst[i] = 0
	}
	for i, nibble := range nibbles {
		if nibble > 0x0f {
			panic("nibble out of range")
		}
		if i&1 == 0 {
			dst[i/2] = nibble << 4
		} else {
			dst[i/2] |= nibble
		}
	}
	return dst
}

func packedMatches(packed []byte, nibbles []byte) bool {
	if len(packed) != packedLen(len(nibbles)) {
		return false
	}
	for i, nib := range nibbles {
		b := packed[i/2]
		if i&1 == 0 {
			b >>= 4
		} else {
			b &= 0x0f
		}
		if b != nib {
			return false
		}
	}
	return true
}

func unpackPath(packed []byte, count int, dst []byte) []byte {
	need := packedLen(count)
	if len(packed) < need {
		panic("packed path is shorter than nibble count")
	}
	if cap(dst) < count {
		dst = make([]byte, count)
	} else {
		dst = dst[:count]
	}
	for i := range dst {
		if i&1 == 0 {
			dst[i] = packed[i/2] >> 4
		} else {
			dst[i] = packed[i/2] & 0x0f
		}
	}
	return dst
}
