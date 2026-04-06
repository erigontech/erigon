// Copyright 2025 The Erigon Authors
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

package integrity

import (
	"sync/atomic"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// cachingCommitmentReader wraps a StateReader and caches commitment domain
// reads in memory across blocks within a window. Between consecutive blocks,
// most branch nodes are identical — only branches on trie paths of changed
// keys differ. The caller must call InvalidateChangedKeys between blocks to
// evict stale entries.
type cachingCommitmentReader struct {
	inner commitmentdb.StateReader
	cache map[string]cachedEntry

	// stats for debugging — shared across clones via pointer
	hits        *atomic.Uint64
	misses      *atomic.Uint64
	invalidated *atomic.Uint64
}

type cachedEntry struct {
	enc  []byte
	step kv.Step
}

func newCachingCommitmentReader(inner commitmentdb.StateReader) *cachingCommitmentReader {
	return &cachingCommitmentReader{
		inner:       inner,
		cache:       make(map[string]cachedEntry, 4096),
		hits:        &atomic.Uint64{},
		misses:      &atomic.Uint64{},
		invalidated: &atomic.Uint64{},
	}
}

func (r *cachingCommitmentReader) WithHistory() bool { return r.inner.WithHistory() }

func (r *cachingCommitmentReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	return r.inner.CheckDataAvailable(d, step)
}

func (r *cachingCommitmentReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if d != kv.CommitmentDomain {
		return r.inner.Read(d, plainKey, stepSize)
	}
	k := string(plainKey)
	if e, ok := r.cache[k]; ok {
		r.hits.Add(1)
		return e.enc, e.step, nil
	}
	r.misses.Add(1)
	enc, step, err := r.inner.Read(d, plainKey, stepSize)
	if err != nil {
		return nil, 0, err
	}
	// Copy enc — the underlying buffer may be reused by the decompressor.
	encCopy := make([]byte, len(enc))
	copy(encCopy, enc)
	r.cache[k] = cachedEntry{enc: encCopy, step: step}
	return encCopy, step, nil
}

func (r *cachingCommitmentReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	// Clone shares the cache and stats — the cloned reader is used within the
	// same block's ComputeCommitment call.
	return &cachingCommitmentReader{
		inner:       r.inner.Clone(tx),
		cache:       r.cache,
		hits:        r.hits,
		misses:      r.misses,
		invalidated: r.invalidated,
	}
}

// SetInner replaces the underlying reader (e.g. when commitmentAsOf changes
// between blocks).
func (r *cachingCommitmentReader) SetInner(inner commitmentdb.StateReader) {
	r.inner = inner
}

// InvalidateChangedKeys scans the commitment domain history for keys that
// changed between prevCommitmentAsOf and newCommitmentAsOf, and removes them
// from the cache. All other entries remain valid because GetAsOf returns the
// same value when no history entry exists between the two txNums.
func (r *cachingCommitmentReader) InvalidateChangedKeys(tx kv.TemporalTx, prevCommitmentAsOf, newCommitmentAsOf uint64) error {
	if prevCommitmentAsOf >= newCommitmentAsOf {
		return nil
	}
	it, err := tx.Debug().HistoryKeyTxNumRange(kv.CommitmentDomain, int(prevCommitmentAsOf), int(newCommitmentAsOf), order.Asc, -1)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return err
		}
		delete(r.cache, string(k))
		r.invalidated.Add(1)
	}
	return nil
}

// Stats returns (hits, misses, invalidated, cacheSize).
func (r *cachingCommitmentReader) Stats() (uint64, uint64, uint64, int) {
	return r.hits.Load(), r.misses.Load(), r.invalidated.Load(), len(r.cache)
}

// ResetStats zeroes the hit/miss/invalidated counters.
func (r *cachingCommitmentReader) ResetStats() {
	r.hits.Store(0)
	r.misses.Store(0)
	r.invalidated.Store(0)
}

// Reset clears all cached entries.
func (r *cachingCommitmentReader) Reset() {
	clear(r.cache)
}
