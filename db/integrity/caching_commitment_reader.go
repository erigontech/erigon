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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// cachingCommitmentReader wraps a StateReader and caches commitment domain
// reads. During Process, the trie unfolds/folds/unfolds for keys sharing
// prefixes — the same branch nodes are read multiple times. This cache
// eliminates those redundant disk reads, giving ~50% within-block hit rate.
//
// The cache is NOT reset between blocks: since data comes from immutable
// frozen snapshots, most branch values remain valid across consecutive blocks.
// If a stale entry causes a root mismatch, the caller retries with Reset().
type cachingCommitmentReader struct {
	inner commitmentdb.StateReader
	cache map[string]cachedEntry
}

type cachedEntry struct {
	enc  []byte
	step kv.Step
}

func newCachingCommitmentReader(inner commitmentdb.StateReader) *cachingCommitmentReader {
	return &cachingCommitmentReader{
		inner: inner,
		cache: make(map[string]cachedEntry, 4096),
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
		return e.enc, e.step, nil
	}
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
	// Clone shares the cache — the cloned reader is used within the same block's
	// ComputeCommitment call, so cache coherence is guaranteed.
	return &cachingCommitmentReader{
		inner: r.inner.Clone(tx),
		cache: r.cache,
	}
}

// SetInner replaces the underlying reader.
func (r *cachingCommitmentReader) SetInner(inner commitmentdb.StateReader) {
	r.inner = inner
}

// Reset clears all cached entries.
func (r *cachingCommitmentReader) Reset() {
	clear(r.cache)
}
