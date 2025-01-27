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

package txpool

import (
	"bytes"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

type Announcements struct {
	ts     []byte
	sizes  []uint32
	hashes []byte
}

func (a *Announcements) Append(t byte, size uint32, hash []byte) {
	a.ts = append(a.ts, t)
	a.sizes = append(a.sizes, size)
	a.hashes = append(a.hashes, hash...)
}

func (a *Announcements) AppendOther(other Announcements) {
	a.ts = append(a.ts, other.ts...)
	a.sizes = append(a.sizes, other.sizes...)
	a.hashes = append(a.hashes, other.hashes...)
}

func (a *Announcements) Reset() {
	a.ts = a.ts[:0]
	a.sizes = a.sizes[:0]
	a.hashes = a.hashes[:0]
}

func (a Announcements) At(i int) (byte, uint32, []byte) {
	return a.ts[i], a.sizes[i], a.hashes[i*length.Hash : (i+1)*length.Hash]
}

func (a Announcements) Len() int {
	return len(a.ts)
}

func (a Announcements) Less(i, j int) bool {
	return bytes.Compare(a.hashes[i*length.Hash:(i+1)*length.Hash], a.hashes[j*length.Hash:(j+1)*length.Hash]) < 0
}
func (a Announcements) Swap(i, j int) {
	a.ts[i], a.ts[j] = a.ts[j], a.ts[i]
	a.sizes[i], a.sizes[j] = a.sizes[j], a.sizes[i]
	ii := i * length.Hash
	jj := j * length.Hash
	for k := 0; k < length.Hash; k++ {
		a.hashes[ii], a.hashes[jj] = a.hashes[jj], a.hashes[ii]
		ii++
		jj++
	}
}

// DedupCopy sorts hashes, and creates deduplicated copy
func (a Announcements) DedupCopy() Announcements {
	if len(a.ts) == 0 {
		return a
	}
	sort.Sort(a)
	unique := 1
	for i := length.Hash; i < len(a.hashes); i += length.Hash {
		if !bytes.Equal(a.hashes[i:i+length.Hash], a.hashes[i-length.Hash:i]) {
			unique++
		}
	}
	c := Announcements{
		ts:     make([]byte, unique),
		sizes:  make([]uint32, unique),
		hashes: make([]byte, unique*length.Hash),
	}
	copy(c.hashes, a.hashes[0:length.Hash])
	c.ts[0] = a.ts[0]
	c.sizes[0] = a.sizes[0]
	dest := length.Hash
	j := 1
	origin := length.Hash
	for i := 1; i < len(a.ts); i++ {
		if !bytes.Equal(a.hashes[origin:origin+length.Hash], a.hashes[origin-length.Hash:origin]) {
			copy(c.hashes[dest:dest+length.Hash], a.hashes[origin:origin+length.Hash])
			c.ts[j] = a.ts[i]
			c.sizes[j] = a.sizes[i]
			dest += length.Hash
			j++
		}
		origin += length.Hash
	}
	return c
}

func (a Announcements) DedupHashes() Hashes {
	if len(a.ts) == 0 {
		return Hashes{}
	}
	sort.Sort(a)
	unique := 1
	for i := length.Hash; i < len(a.hashes); i += length.Hash {
		if !bytes.Equal(a.hashes[i:i+length.Hash], a.hashes[i-length.Hash:i]) {
			unique++
		}
	}
	c := make(Hashes, unique*length.Hash)
	copy(c[:], a.hashes[0:length.Hash])
	dest := length.Hash
	j := 1
	origin := length.Hash
	for i := 1; i < len(a.ts); i++ {
		if !bytes.Equal(a.hashes[origin:origin+length.Hash], a.hashes[origin-length.Hash:origin]) {
			copy(c[dest:dest+length.Hash], a.hashes[origin:origin+length.Hash])
			dest += length.Hash
			j++
		}
		origin += length.Hash
	}
	return c
}

func (a Announcements) Hashes() Hashes {
	return a.hashes
}

func (a Announcements) Copy() Announcements {
	if len(a.ts) == 0 {
		return a
	}
	c := Announcements{
		ts:     common.Copy(a.ts),
		sizes:  make([]uint32, len(a.sizes)),
		hashes: common.Copy(a.hashes),
	}
	copy(c.sizes, a.sizes)
	return c
}

type Hashes []byte // flatten list of 32-byte hashes

func (h Hashes) At(i int) []byte {
	return h[i*length.Hash : (i+1)*length.Hash]
}

func (h Hashes) Len() int {
	return len(h) / length.Hash
}

func (h Hashes) Less(i, j int) bool {
	return bytes.Compare(h[i*length.Hash:(i+1)*length.Hash], h[j*length.Hash:(j+1)*length.Hash]) < 0
}

func (h Hashes) Swap(i, j int) {
	ii := i * length.Hash
	jj := j * length.Hash
	for k := 0; k < length.Hash; k++ {
		h[ii], h[jj] = h[jj], h[ii]
		ii++
		jj++
	}
}

// DedupCopy sorts hashes, and creates deduplicated copy
func (h Hashes) DedupCopy() Hashes {
	if len(h) == 0 {
		return h
	}
	sort.Sort(h)
	unique := 1
	for i := length.Hash; i < len(h); i += length.Hash {
		if !bytes.Equal(h[i:i+length.Hash], h[i-length.Hash:i]) {
			unique++
		}
	}
	c := make(Hashes, unique*length.Hash)
	copy(c[:], h[0:length.Hash])
	dest := length.Hash
	for i := dest; i < len(h); i += length.Hash {
		if !bytes.Equal(h[i:i+length.Hash], h[i-length.Hash:i]) {
			copy(c[dest:dest+length.Hash], h[i:i+length.Hash])
			dest += length.Hash
		}
	}
	return c
}
