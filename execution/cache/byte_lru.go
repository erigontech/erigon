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

package cache

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
	"github.com/maypok86/otter/v2"

	"github.com/erigontech/erigon/common/cachebudget"
)

// byteLRU is a uint64-keyed cache bounded by the bytes it holds, weighed by the
// caller's weigher, instead of by an entry count over an assumed average size.
// The ceiling rises one chunk at a time out of the shared cachebudget envelope
// and stops for good once the envelope refuses a chunk.
//
// The synchronous executor orders InvalidateAll but not eviction against Add,
// so a layer can sit over its ceiling until the next write; CleanUp forces a
// drain. Eviction is W-TinyLFU, so a newcomer can be rejected outright.
type byteLRU[V any] struct {
	c        *otter.Cache[uint64, V]
	weigh    func(uint64, V) int64
	maxBytes int64

	// Indirect so Close can clear it: the cache outlives Close via its runtime
	// cleanup, and a callback capturing the owner would keep the owner too.
	onEvict atomic.Pointer[func(uint64, V)]

	resident atomic.Int64
	limit    atomic.Int64 // bytes reserved from the envelope; also otter's maximum
	ceiling  atomic.Int64 // limit may still grow to this; drops to limit when the envelope refuses

	growMu sync.Mutex
	closed bool
}

const (
	// Taken unconditionally, so it must stay small: every short-lived cache pays
	// it whatever the envelope has left.
	byteLRUFloorBytes = int64(1 * datasize.MB)
	byteLRUChunkBytes = int64(32 * datasize.MB)
)

func newByteLRU[V any](maxBytes datasize.ByteSize, weigh func(uint64, V) int64, onEvict func(uint64, V)) *byteLRU[V] {
	b := &byteLRU[V]{weigh: weigh, maxBytes: max(int64(maxBytes), 1)}
	if onEvict != nil {
		b.onEvict.Store(&onEvict)
	}
	floor := min(byteLRUFloorBytes, b.maxBytes)
	cachebudget.Global.Take(floor)
	b.limit.Store(floor)
	b.ceiling.Store(b.maxBytes)
	b.c = otter.Must(&otter.Options[uint64, V]{
		MaximumWeight: uint64(floor),
		Weigher:       func(k uint64, v V) uint32 { return uint32(min(weigh(k, v), math.MaxUint32)) },
		OnDeletion: func(e otter.DeletionEvent[uint64, V]) {
			b.resident.Add(-weigh(e.Key, e.Value))
			if fn := b.onEvict.Load(); fn != nil {
				(*fn)(e.Key, e.Value)
			}
		},
		Executor: func(fn func()) { fn() },
	})
	return b
}

func (b *byteLRU[V]) Get(key uint64) (V, bool) { return b.c.GetIfPresent(key) }

// Add reports that the value could be offered, not that it is resident: a
// W-TinyLFU rejection also returns true. False means it exceeds the whole
// budget, and onEvict never fires for it, so the caller must refund its charge.
func (b *byteLRU[V]) Add(key uint64, value V) bool {
	w := b.weigh(key, value)
	if w > b.maxBytes {
		return false
	}
	if lim := b.limit.Load(); b.resident.Load()+w > lim && lim < b.ceiling.Load() {
		b.grow(w)
	}
	b.resident.Add(w)
	b.c.Set(key, value)
	return true
}

// grow re-tests the caller's condition under the lock: without it every writer
// piled up at a full limit reserves its own chunk, draining the envelope at once.
func (b *byteLRU[V]) grow(w int64) {
	b.growMu.Lock()
	defer b.growMu.Unlock()
	lim, ceil := b.limit.Load(), b.ceiling.Load()
	if b.closed || lim >= ceil || b.resident.Load()+w <= lim {
		return
	}
	chunk := min(byteLRUChunkBytes, ceil-lim)
	if !cachebudget.Global.Reserve(chunk) {
		b.ceiling.Store(lim) // envelope full: stop asking until Purge/Close frees it
		return
	}
	b.limit.Store(lim + chunk)
	b.c.SetMaximum(uint64(lim + chunk))
}

func (b *byteLRU[V]) Remove(key uint64) { b.c.Invalidate(key) }
func (b *byteLRU[V]) Len() int          { return b.c.EstimatedSize() }

// Purge empties the cache and returns the grown reservation, keeping the floor
// so the cache is never left disabled.
func (b *byteLRU[V]) Purge() {
	b.growMu.Lock()
	defer b.growMu.Unlock()
	b.c.InvalidateAll()
	b.resident.Store(0)
	if b.closed {
		return
	}
	floor := min(byteLRUFloorBytes, b.maxBytes)
	cachebudget.Global.Release(b.limit.Load() - floor)
	b.limit.Store(floor)
	b.ceiling.Store(b.maxBytes)
	b.c.SetMaximum(uint64(floor))
}

// Close returns this cache's envelope reservation. Idempotent.
func (b *byteLRU[V]) Close() {
	b.growMu.Lock()
	defer b.growMu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	// Entries, not just the reservation: the cache stays reachable after Close.
	b.c.InvalidateAll()
	b.onEvict.Store(nil)
	b.resident.Store(0)
	cachebudget.Global.Release(b.limit.Load())
	b.limit.Store(0)
	b.ceiling.Store(0)
}
