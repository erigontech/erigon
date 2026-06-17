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
	"github.com/erigontech/erigon/common/murmur3"
)

// IndexShardReader encapsulates Hash128 to allow concurrent access to IndexShard
type IndexShardReader struct {
	index *IndexShard
	salt  uint32
}

// NewIndexShardReader creates new IndexShardReader
func NewIndexShardReader(index *IndexShard) *IndexShardReader {
	return &IndexShardReader{
		salt:  index.salt,
		index: index,
	}
}

func (r *IndexShardReader) Sum(key []byte) (uint64, uint64) {
	return murmur3.Sum128WithSeed(key, r.salt)
}

// Lookup wraps index Lookup
func (r *IndexShardReader) Lookup(key []byte) (uint64, bool) {
	bucketHash, fingerprint := r.Sum(key)
	return r.index.Lookup(bucketHash, fingerprint)
}

// Lookup2 looks up the concatenation key1||key2 without materializing it
func (r *IndexShardReader) Lookup2(key1, key2 []byte) (uint64, bool) {
	bucketHash, fingerprint := murmur3.Sum128PairWithSeed(key1, key2, r.salt)
	return r.index.Lookup(bucketHash, fingerprint)
}

func (r *IndexShardReader) Empty() bool {
	return r.index.Empty()
}

// Close is a no-op kept for API compatibility: readers are stateless and shared
func (r *IndexShardReader) Close() {}

func (r *IndexShardReader) OrdinalLookup(id uint64) uint64 { return r.index.OrdinalLookup(id) }
func (r *IndexShardReader) twoLayerLookup(key []byte) (uint64, bool) {
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
func (r *IndexShardReader) twoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	if r.index.Empty() {
		return 0, false
	}
	id, ok := r.index.Lookup(hi, lo)
	if !ok {
		return 0, false
	}
	return r.index.OrdinalLookup(id), true
}
func (r *IndexShardReader) BaseDataID() uint64 { return r.index.BaseDataID() }

// TwoLayerLookupByHash high-level methods. allow to turn-off `enum` in future
func (r *IndexShardReader) TwoLayerLookupByHash(hi, lo uint64) (uint64, bool) {
	enums := r.index.Enums()
	if enums {
		return r.twoLayerLookupByHash(hi, lo)
	}
	return r.index.Lookup(hi, lo)
}

// TwoLayerLookup high-level methods. allow to turn-off `enum` in future
func (r *IndexShardReader) TwoLayerLookup(key []byte) (uint64, bool) {
	enums := r.index.Enums()
	if enums {
		return r.twoLayerLookup(key)
	}
	return r.Lookup(key)
}
