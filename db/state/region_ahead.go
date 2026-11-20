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

package state

import (
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
)

const (
	regionAheadHits     = 64
	regionAheadMaxBytes = 64 << 20
)

type regionKey struct {
	addr [length.Addr]byte
	file int
}

func (dt *DomainRoTx) regionAhead(i int, k []byte) bool {
	if len(k) <= length.Addr || dt.probe != nil {
		return false
	}
	if dt.regionHits == nil {
		dt.regionHits = make(map[regionKey]uint32)
	} else if len(dt.regionHits) > 1024 {
		clear(dt.regionHits)
	}
	key := regionKey{addr: [length.Addr]byte(k[:length.Addr]), file: i}
	dt.regionHits[key]++
	if dt.regionHits[key] != regionAheadHits {
		return false
	}
	from, to, ok := dt.regionWords(i, key.addr[:])
	if !ok || to-from > regionAheadMaxBytes {
		return false
	}
	dt.files[i].src.decompressor.MadvWillNeedWords(from, to)
	return true
}

func (dt *DomainRoTx) regionWords(i int, prefix []byte) (from, to uint64, ok bool) {
	bt := dt.statelessBtree(i)
	if bt == nil || bt.Empty() {
		return 0, 0, false
	}
	g := dt.dataReader(dt.files[i].src.decompressor)
	c, err := bt.Seek(g, prefix)
	if err != nil || c == nil {
		return 0, 0, false
	}
	first, end := c.Di(), bt.KeyCount()
	c.Close()
	if next, more := kv.NextSubtree(prefix); more {
		c, err := bt.Seek(g, next)
		if err != nil {
			return 0, 0, false
		}
		if c != nil {
			end = c.Di()
			c.Close()
		}
	}
	if first >= end {
		return 0, 0, false
	}
	from, to = bt.Offsets().Get(first), uint64(g.Size())
	if end < bt.KeyCount() {
		to = bt.Offsets().Get(end)
	}
	return from, to, true
}
