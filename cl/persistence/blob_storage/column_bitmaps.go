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

package blob_storage

import (
	"math/bits"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
)

// columnBitmaps remembers, per block root, which columns have a file on disk, so
// GetSavedColumnIndex and ColumnSidecarExists answer without a stat per column. It lives in
// memory only: after a restart the files decide again.
//
// An entry describes the files of one root in one bucket, at one column count. A caller
// asking about another bucket or count gets no answer and goes to the files; a write or
// remove for another bucket or count drops the entry.
//
// The caller keeps every set bit backed by a file:
//   - a bit is set after its file is in place and cleared before its file is removed, both
//     under the root's lock;
//   - an entry is seeded only from a full scan of the root's files under the root's lock, so
//     no write or remove of that root, at any slot, interleaves with the scan;
//   - entries are used and seeded only for slots at or above the prune floor, read under
//     pruneMutex. pruneBelow removes only buckets under the floor's, so it never removes a
//     file a used entry claims.
type columnBitmaps struct {
	mu      sync.Mutex
	entries map[common.Hash]*columnBitmap
	dropped uint64
}

type columnBitmap struct {
	bucket uint64
	width  uint64
	bits   []byte
}

// columnKey names what an entry describes: the root's files in bucket, for width columns.
type columnKey struct {
	root          common.Hash
	bucket, width uint64
}

func newColumnBitmaps() *columnBitmaps {
	return &columnBitmaps{entries: map[common.Hash]*columnBitmap{}}
}

// entry returns the root's entry when it describes k. Callers hold c.mu.
func (c *columnBitmaps) entry(k columnKey) (*columnBitmap, bool) {
	e, ok := c.entries[k.root]
	if !ok || e.bucket != k.bucket || e.width != k.width {
		return nil, false
	}
	return e, true
}

// has reports whether column is stored; known is false when no entry describes k or the
// column is outside it, and then only the files can answer.
func (c *columnBitmaps) has(k columnKey, column uint64) (stored, known bool) {
	if column >= k.width {
		return false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entry(k)
	if !ok {
		return false, false
	}
	return e.bits[column/8]&(1<<(column%8)) != 0, true
}

func (c *columnBitmaps) columns(k columnKey) ([]uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entry(k)
	if !ok {
		return nil, false
	}
	var columns []uint64
	for i, b := range e.bits {
		for ; b != 0; b &= b - 1 {
			columns = append(columns, uint64(i*8+bits.TrailingZeros8(b)))
		}
	}
	return columns, true
}

// seed records the columns a scan of the root's files in k.bucket found. A scan that found
// no file leaves the map as it is, so lookups of roots this node never stored, or of a
// stored root at a bucket it is not in, cannot grow the map or displace a real entry.
func (c *columnBitmaps) seed(k columnKey, columns []uint64) {
	if len(columns) == 0 {
		return
	}
	e := &columnBitmap{bucket: k.bucket, width: k.width, bits: make([]byte, (k.width+7)/8)}
	for _, column := range columns {
		if column < k.width {
			e.bits[column/8] |= 1 << (column % 8)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[k.root] = e
}

// set marks a column whose file was just written. A root without an entry stays without
// one: its next scan finds the file.
func (c *columnBitmaps) set(k columnKey, column uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[k.root]; !ok {
		return
	}
	e, ok := c.entry(k)
	if !ok || column >= k.width {
		delete(c.entries, k.root)
		return
	}
	e.bits[column/8] |= 1 << (column % 8)
}

// clear unmarks columns about to be removed and drops an entry left with none.
func (c *columnBitmaps) clear(k columnKey, columns []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[k.root]; !ok {
		return
	}
	e, ok := c.entry(k)
	if !ok {
		delete(c.entries, k.root)
		return
	}
	for _, column := range columns {
		if column < k.width {
			e.bits[column/8] &^= 1 << (column % 8)
		}
	}
	if !slices.ContainsFunc(e.bits, func(b byte) bool { return b != 0 }) {
		delete(c.entries, k.root)
	}
}

// forget drops the root's entry, so its files decide again.
func (c *columnBitmaps) forget(root common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, root)
}

// dropBelow frees the entries of buckets under bucket. They are never consulted once the
// prune floor has passed them, so this only bounds memory.
func (c *columnBitmaps) dropBelow(bucket uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bucket <= c.dropped {
		return
	}
	c.dropped = bucket
	for root, e := range c.entries {
		if e.bucket < bucket {
			delete(c.entries, root)
		}
	}
}
