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
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrV3KeyLength   = errors.New("nibbles v3: key length out of range")
	ErrV3KeyTerminal = errors.New("nibbles v3: illegal terminal byte")
)

// EncodeKeyV3 packs a nibble path into a V3 node key. The final byte is 0x00
// for an even path and 0xf0|lastNibble for an odd path.
func EncodeKeyV3(nibbles []byte) []byte {
	n := len(nibbles)
	if n > MaxPathNibbles {
		panic(fmt.Sprintf("nibbles v3: path length %d exceeds MaxPathNibbles=%d", n, MaxPathNibbles))
	}
	for i, nibble := range nibbles {
		if nibble > 0x0f {
			panic(fmt.Sprintf("nibbles v3: nibble at index %d is 0x%02x, must be in [0x00, 0x0F]", i, nibble))
		}
	}

	out := make([]byte, n/2+1)
	for i := 0; i < n/2; i++ {
		out[i] = nibbles[2*i]<<4 | nibbles[2*i+1]
	}
	if n&1 == 1 {
		out[n/2] = 0xf0 | nibbles[n-1]
	}
	return out
}

// DecodeKeyV3 reverses EncodeKeyV3.
func DecodeKeyV3(key []byte) ([]byte, error) {
	if len(key) == 0 || len(key) > MaxPathNibbles/2+1 {
		return nil, ErrV3KeyLength
	}

	term := key[len(key)-1]
	var n int
	switch {
	case term == 0:
		n = (len(key) - 1) * 2
	case term >= 0xf0:
		n = (len(key)-1)*2 + 1
	default:
		return nil, ErrV3KeyTerminal
	}
	if n > MaxPathNibbles {
		return nil, ErrV3KeyLength
	}

	out := make([]byte, n)
	for i, packed := range key[:len(key)-1] {
		out[2*i] = packed >> 4
		out[2*i+1] = packed & 0x0f
	}
	if term >= 0xf0 {
		out[n-1] = term & 0x0f
	}
	return out, nil
}

// ChildKeyV3 returns the record key for a child of nodeKey.
func ChildKeyV3(nodeKey []byte, nibble byte) []byte {
	if nibble > 0x0f {
		panic(fmt.Sprintf("nibbles v3: child nibble 0x%02x is out of range", nibble))
	}
	key := make([]byte, len(nodeKey)+1)
	copy(key, nodeKey)
	key[len(nodeKey)] = 0x80 | nibble
	return key
}

// IsChildKeyV3 reports whether key is a canonical V3 child key.
func IsChildKeyV3(key []byte) bool {
	if len(key) < 2 || key[len(key)-1] < 0x80 || key[len(key)-1] > 0x8f {
		return false
	}
	_, err := DecodeKeyV3(key[:len(key)-1])
	return err == nil
}

// ChildNibbleV3 returns the child nibble encoded in a V3 child key.
func ChildNibbleV3(key []byte) byte {
	if len(key) == 0 {
		panic("nibbles v3: empty child key")
	}
	return key[len(key)-1] & 0x0f
}

// ChildKeyLenForDepth returns the V3 child-key length for a node at depth d.
func ChildKeyLenForDepth(d int) int {
	return d/2 + 2
}

// IsChildKeyAtDepthV3 reports whether key has the exact child-key length for depth.
func IsChildKeyAtDepthV3(key []byte, depth int) bool {
	return depth >= 0 && depth <= MaxPathNibbles && len(key) == ChildKeyLenForDepth(depth) && IsChildKeyV3(key)
}

// IsChildKeyForNodeV3 reports whether key is a direct child record of nodeKey.
func IsChildKeyForNodeV3(nodeKey, key []byte) bool {
	return len(key) == len(nodeKey)+1 && bytes.Equal(key[:len(nodeKey)], nodeKey) && IsChildKeyV3(key)
}

// ChildRangeBoundsV3 returns the half-open range containing the sixteen direct
// child records of nodeKey. The upper bound is not itself a child key.
func ChildRangeBoundsV3(nodeKey []byte) (lo, hi []byte) {
	lo = ChildKeyV3(nodeKey, 0)
	hi = make([]byte, len(nodeKey)+1)
	copy(hi, nodeKey)
	hi[len(nodeKey)] = 0x90
	return lo, hi
}
