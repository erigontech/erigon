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

func (r *IndexReader) sum(key []byte) (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hasher.Reset()
	r.hasher.Write(key) //nolint:errcheck
	return r.hasher.Sum128()
}

func (r *IndexReader) sum2(key1, key2 []byte) (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hasher.Reset()
	r.hasher.Write(key1) //nolint:errcheck
	r.hasher.Write(key2) //nolint:errcheck
	return r.hasher.Sum128()
}

// Lookup wraps index Lookup
func (r *IndexReader) Lookup(key []byte) uint64 {
	bucketHash, fingerprint := r.sum(key)
	if r.index != nil {
		return r.index.Lookup(bucketHash, fingerprint)
	}
	return 0
}

func (r *IndexReader) Lookup2(key1, key2 []byte) uint64 {
	bucketHash, fingerprint := r.sum2(key1, key2)
	if r.index != nil {
		return r.index.Lookup(bucketHash, fingerprint)
	}
	return 0
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
