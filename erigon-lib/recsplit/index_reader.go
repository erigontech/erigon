// Copyright 2021 The Erigon Authors
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

package recsplit

import (
	"sync"

	"github.com/spaolacci/murmur3"
)

// IndexReader encapsulates Hash128 to allow concurrent access to Index
type IndexReader struct {
	index *Index
	salt  uint32

	bufLock sync.RWMutex
	buf     []byte
}

// NewIndexReader creates new IndexReader
func NewIndexReader(index *Index) *IndexReader {
	return &IndexReader{
		salt:  index.salt,
		index: index,
	}
}

func (r *IndexReader) Sum(key []byte) (uint64, uint64) {
	// this inlinable alloc-free version, it's faster than pre-allocated `hasher` object
	// because `hasher` object is interface and need call many methods on it
	return murmur3.Sum128WithSeed(key, r.salt)
}

// Lookup wraps index Lookup
func (r *IndexReader) Lookup(key []byte) (uint64, bool) {
	bucketHash, fingerprint := r.Sum(key)
	return r.index.Lookup(bucketHash, fingerprint)
}

func (r *IndexReader) Lookup2(key1, key2 []byte) (uint64, bool) {
	r.bufLock.Lock()
	// hash of 2 concatenated keys is equal to 2 separated calls of `.Write`
	r.buf = append(append(r.buf[:0], key1...), key2...)
	bucketHash, fingerprint := murmur3.Sum128WithSeed(r.buf, r.salt)
	r.bufLock.Unlock()
	return r.index.Lookup(bucketHash, fingerprint)
}

func (r *IndexReader) Empty() bool {
	return r.index.Empty()
}

func (r *IndexReader) Close() {
	if r == nil || r.index == nil {
		return
	}
	r.index.readers.Put(r)
}

func (r *IndexReader) OrdinalLookup(id uint64) uint64 { return r.index.OrdinalLookup(id) }
func (r *IndexReader) twoLayerLookup(key []byte) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	bucketHash, fingerprint := r.Sum(key)
	id, ok := r.index.Lookup(bucketHash, fingerprint)
	if !ok {
		return 0, false
	}
	return r.OrdinalLookup(id), true
}
func (r *IndexReader) twoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	id, ok := r.index.Lookup(hi, lo)
	if !ok {
		return 0, false
	}
	return r.index.OrdinalLookup(id), true
}
func (r *IndexReader) BaseDataID() uint64 { return r.index.BaseDataID() }

// TwoLayerLookupByHash high-level methods. allow to turn-off `enum` in future
func (r *IndexReader) TwoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	enums := r.index.Enums()
	if enums {
		return r.twoLayerLookupByHash(hi, lo)
	}
	return r.index.Lookup(hi, lo)
}

// TwoLayerLookup high-level methods. allow to turn-off `enum` in future
func (r *IndexReader) TwoLayerLookup(key []byte) (uint64, bool) {
	enums := r.index.Enums()
	if enums {
		return r.twoLayerLookup(key)
	}
	return r.Lookup(key)
}
