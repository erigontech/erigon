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

package commitment

import (
	"bytes"
	"cmp"
	"slices"
)

type presortEntry struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
	seq       uint32
}

type presorter struct {
	entries []presortEntry
	seq     uint32
}

func (p *presorter) collect(hashedKey, plainKey []byte, update *Update) {
	p.entries = append(p.entries, presortEntry{
		hashedKey: hashedKey,
		plainKey:  plainKey,
		update:    update,
		seq:       p.seq,
	})
	p.seq++
}

func (p *presorter) count() int { return len(p.entries) }

func presortLess(a, b presortEntry) int {
	if c := bytes.Compare(a.hashedKey, b.hashedKey); c != 0 {
		return c
	}
	return cmp.Compare(a.seq, b.seq)
}

func (p *presorter) sort() {
	if len(p.entries) > 1 {
		slices.SortFunc(p.entries, presortLess)
	}
}

func (p *presorter) reset() {
	clear(p.entries)
	p.entries = p.entries[:0]
	p.seq = 0
}
