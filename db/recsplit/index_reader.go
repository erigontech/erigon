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

// IndexReader encapsulates Hash128 to allow concurrent access to Index
type IndexReader struct {
	index *Index
	salt  uint32
}

// NewIndexReader creates new IndexReader
func NewIndexReader(index *Index) *IndexReader {
	return &IndexReader{
		salt:  index.salt,
		index: index,
	}
}

func (r *IndexReader) Sum(key []byte) (uint64, uint64) {
	// in-package murmur3 port is bit-identical to the library but faster for short keys
	return murmur128WithSeed(key, r.salt)
}

// Lookup wraps index Lookup
func (r *IndexReader) Lookup(key []byte) (uint64, bool) {
	bucketHash, fingerprint := r.Sum(key)
	return r.index.Lookup(bucketHash, fingerprint)
}

// Lookup2 looks up the concatenation key1||key2 without materializing it
func (r *IndexReader) Lookup2(key1, key2 []byte) (uint64, bool) {
	bucketHash, fingerprint := murmur128PairWithSeed(key1, key2, r.salt)
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
