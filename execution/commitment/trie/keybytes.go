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
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// Keybytes represent a packed encoding of hex sequences
// where 2 nibbles per byte are stored in Data
// + an additional flag for terminating nodes.
type Keybytes struct {
	Data        []byte
	Odd         bool
	Terminating bool
}

// ToHex translates from KEYBYTES to HEX encoding.
func (x *Keybytes) ToHex() []byte {
	return nibbles.CompactToHex(x.ToCompact())
}

// ToCompact translates from KEYBYTES to COMPACT encoding.
func (x *Keybytes) ToCompact() []byte {
	l := len(x.Data)
	if !x.Odd {
		l++
	}

	compact := make([]byte, l)

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
