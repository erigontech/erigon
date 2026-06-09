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

package nibbles

import (
	"errors"
	"fmt"
)

const MaxPathNibbles = 128

var (
	ErrV2KeyLength       = errors.New("nibbles v2: key length out of range")
	ErrV2KeyParity       = errors.New("nibbles v2: parity byte must be 0x00 or 0x01")
	ErrV2KeyShape        = errors.New("nibbles v2: parity=1 with no packed byte")
	ErrV2NonCanonicalPad = errors.New("nibbles v2: non-zero pad nibble in odd encoding")
)

// EncodeKeyV2 packs a nibble path into a V2 DB key: nibbles are packed 2-per-byte
// high-first, then a single trailing parity byte (0x00 even, 0x01 odd) marks the
// length parity. Suffix-parity preserves prefix-sort locality across the trie.
func EncodeKeyV2(nibbles []byte) []byte {
	n := len(nibbles)
	if n > MaxPathNibbles {
		panic(fmt.Sprintf("nibbles v2: path length %d exceeds MaxPathNibbles=%d", n, MaxPathNibbles))
	}
	for i, b := range nibbles {
		if b > 0x0F {
			panic(fmt.Sprintf("nibbles v2: nibble at index %d is 0x%02x, must be in [0x00, 0x0F]", i, b))
		}
	}

	odd := n & 1
	out := make([]byte, n/2+odd+1)
	for i := 0; i < n/2; i++ {
		out[i] = (nibbles[2*i] << 4) | (nibbles[2*i+1] & 0x0F)
	}
	if odd == 1 {
		out[n/2] = nibbles[n-1] << 4
	}
	out[len(out)-1] = byte(odd)
	return out
}

// DecodeKeyV2 reverses EncodeKeyV2. Returns one of the four sentinel errors
// (ErrV2KeyLength, ErrV2KeyParity, ErrV2KeyShape, ErrV2NonCanonicalPad) when
// the input is not a canonical V2 key.
func DecodeKeyV2(key []byte) ([]byte, error) {
	// max key = 128 nibbles → 64 packed bytes + 1 parity byte = 65 bytes
	if len(key) < 1 || len(key) > MaxPathNibbles/2+1 {
		return nil, ErrV2KeyLength
	}
	parity := key[len(key)-1]
	if parity > 1 {
		return nil, ErrV2KeyParity
	}
	packed := key[:len(key)-1]

	var n int
	if parity == 0 {
		n = len(packed) * 2
	} else {
		if len(packed) == 0 {
			return nil, ErrV2KeyShape
		}
		// strict canonicality: the pad nibble in the last byte must be zero so that
		// each logical path has exactly one valid V2 byte sequence
		if packed[len(packed)-1]&0x0F != 0 {
			return nil, ErrV2NonCanonicalPad
		}
		n = len(packed)*2 - 1
	}

	out := make([]byte, n)
	for i := 0; i < n/2; i++ {
		out[2*i] = packed[i] >> 4
		out[2*i+1] = packed[i] & 0x0F
	}
	if parity == 1 {
		out[n-1] = packed[len(packed)-1] >> 4
	}
	return out, nil
}
