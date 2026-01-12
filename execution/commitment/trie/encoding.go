// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package trie

import (
	"io"

	"github.com/erigontech/erigon/execution/rlp"
)

// Trie keys are dealt with in three distinct encodings:
//
// KEYBYTES encoding contains the actual key and nothing else. This encoding is the
// input to most API functions. It is a packed encoding of hex sequences
// with 2 nibbles per byte.
//
// HEX encoding contains one byte for each nibble of the key and an optional trailing
// 'terminator' byte of value 0x10 which indicates whether or not the node at the key
// contains a value. Hex key encoding is used for nodes loaded in memory because it's
// convenient to access.
//
// COMPACT encoding is defined by the Ethereum Yellow Paper (it's called "hex prefix
// encoding" there) and contains the bytes of the key and a flag. The high nibble of the
// first byte contains the flag; the lowest bit encoding the oddness of the length and
// the second-lowest encoding whether the node at the key is a value node. The low nibble
// of the first byte is zero in the case of an even number of nibbles and the first nibble
// in the case of an odd number. All remaining nibbles (now an even number) fit properly
// into the remaining bytes. Compact encoding is used for nodes stored on disk.

func hexToCompact(hex []byte) []byte {
	terminator := byte(0)
	if hasTerm(hex) {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[0] = terminator << 5 // the flag byte
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4 // odd flag
		buf[0] |= hex[0] // first nibble is contained in the first byte
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

func compactToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return compact
	}
	base := keybytesToHex(compact)
	// delete terminator flag
	if base[0] < 2 {
		base = base[:len(base)-1]
	}
	// apply odd flag
	chop := 2 - base[0]&1
	return base[chop:]
}

// Keybytes represent a packed encoding of hex sequences
// where 2 nibbles per byte are stored in Data
// + an additional flag for terminating nodes.
type Keybytes struct {
	Data        []byte
	Odd         bool
	Terminating bool
}

// Nibbles returns the number of nibbles.
func (x *Keybytes) Nibbles() int {
	n := len(x.Data) * 2
	if x.Odd {
		n--
	}
	return n
}

// ToHex translates from KEYBYTES to HEX encoding.
func (x *Keybytes) ToHex() []byte {
	return compactToHex(x.ToCompact())
}

// ToCompact translates from KEYBYTES to COMPACT encoding.
func (x *Keybytes) ToCompact() []byte {
	l := len(x.Data)
	if !x.Odd {
		l++
	}

	var compact = make([]byte, l)

	if x.Terminating {
		compact[0] = 0x20
	}

	if x.Odd {
		compact[0] += 0x10
		compact[0] += x.Data[0] >> 4
		for i := 1; i < len(x.Data); i++ {
			compact[i] = (x.Data[i-1] << 4) + (x.Data[i] >> 4)
		}
	} else {
		copy(compact[1:], x.Data)
	}

	return compact
}

// CompactToKeybytes translates from COMPACT to KEYBYTES encoding.
func CompactToKeybytes(c []byte) Keybytes {
	var k Keybytes
	k.Odd = (c[0] & 0x10) != 0
	k.Terminating = (c[0] & 0x20) != 0

	if k.Odd {
		k.Data = make([]byte, len(c))
		for i := 1; i < len(c); i++ {
			k.Data[i-1] = (c[i-1] << 4) + (c[i] >> 4)
		}
		k.Data[len(c)-1] = c[len(c)-1] << 4
	} else {
		k.Data = c[1:]
	}

	return k
}

// EncodeRLP implements rlp.Encoder and encodes Keybytes in the COMPACT encoding.
func (x *Keybytes) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, x.ToCompact())
}

// DecodeRLP implements rlp.Decoder and decodes Keybytes from the COMPACT encoding.
func (x *Keybytes) DecodeRLP(s *rlp.Stream) error {
	var compact []byte
	if err := s.Decode(&compact); err != nil {
		return err
	}
	*x = CompactToKeybytes(compact)
	return nil
}

func keybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

// hexToKeybytes turns hex nibbles into key bytes.
// This can only be used for keys of even length.
func hexToKeybytes(hex []byte) []byte {
	if hasTerm(hex) {
		hex = hex[:len(hex)-1]
	}
	if len(hex)&1 != 0 {
		panic("can't convert hex key of odd length")
	}
	key := make([]byte, len(hex)/2)
	decodeNibbles(hex, key)
	return key
}

func decodeNibbles(nibbles []byte, bytes []byte) {
	if hasTerm(nibbles) {
		nibbles = nibbles[:len(nibbles)-1]
	}

	nl := len(nibbles)
	for bi, ni := 0, 0; ni < nl; bi, ni = bi+1, ni+2 {
		if ni == nl-1 {
			bytes[bi] = (bytes[bi] &^ 0xf0) | nibbles[ni]<<4
		} else {
			bytes[bi] = nibbles[ni]<<4 | nibbles[ni+1]
		}
	}
}

// prefixLen returns the length of the common prefix of a and b.
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

// hasTerm returns whether a hex key has the terminator flag.
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}
