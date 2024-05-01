/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
