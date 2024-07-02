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
	hasher murmur3.Hash128
	index  *Index
	mu     sync.RWMutex
}

// NewIndexReader creates new IndexReader
func NewIndexReader(index *Index) *IndexReader {
	return &IndexReader{
		hasher: murmur3.New128WithSeed(index.salt),
		index:  index,
	}
}

func (r *IndexReader) sum(key []byte) (hi uint64, lo uint64) {
	r.mu.Lock()
	r.hasher.Reset()
	r.hasher.Write(key) //nolint:errcheck
	hi, lo = r.hasher.Sum128()
	r.mu.Unlock()
	return hi, lo
}

func (r *IndexReader) sum2(key1, key2 []byte) (hi uint64, lo uint64) {
	r.mu.Lock()
	r.hasher.Reset()
	r.hasher.Write(key1) //nolint:errcheck
	r.hasher.Write(key2) //nolint:errcheck
	hi, lo = r.hasher.Sum128()
	r.mu.Unlock()
	return hi, lo
}

// Lookup wraps index Lookup
func (r *IndexReader) Lookup(key []byte) (uint64, bool) {
	bucketHash, fingerprint := r.sum(key)
	return r.index.Lookup(bucketHash, fingerprint)
}

func (r *IndexReader) Lookup2(key1, key2 []byte) (uint64, bool) {
	bucketHash, fingerprint := r.sum2(key1, key2)
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

func (r *IndexReader) Sum(key []byte) (uint64, uint64)         { return r.sum(key) }
func (r *IndexReader) LookupHash(hi, lo uint64) (uint64, bool) { return r.index.Lookup(hi, lo) }
func (r *IndexReader) OrdinalLookup(id uint64) uint64          { return r.index.OrdinalLookup(id) }
func (r *IndexReader) TwoLayerLookup(key []byte) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	bucketHash, fingerprint := r.sum(key)
	id, ok := r.index.Lookup(bucketHash, fingerprint)
	if !ok {
		return 0, false
	}
	return r.OrdinalLookup(id), true
}
func (r *IndexReader) TwoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	id, ok := r.index.Lookup(hi, lo)
	if !ok {
		return 0, false
	}
	return r.index.OrdinalLookup(id), true
}
