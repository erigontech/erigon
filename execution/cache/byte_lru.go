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
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
	"github.com/maypok86/otter/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/length"
)

// ByteLRU is a uint64-keyed cache bounded by the bytes it holds, weighed by the
// caller's weigher, instead of by an entry count over an assumed average size.
// A budgeted cache (newByteLRU) raises its limit one chunk at a time out of the shared
// cachebudget envelope, up to maxBytes, and stops asking once the envelope refuses a chunk,
// until Purge or Close; an unbudgeted one (NewByteLRU) holds maxBytes from the start.
//
// The synchronous executor orders InvalidateAll but not eviction against Add,
// so a layer can sit over its ceiling until the next write; CleanUp forces a
// drain. Eviction is W-TinyLFU, so a newcomer can be rejected outright.
type ByteLRU[V any] struct {
	c        *otter.Cache[uint64, V]
	weigh    func(uint64, V) int64
	maxBytes int64

	*byteLRUEvictions[V]

	limit atomic.Int64 // for a budgeted cache, the bytes reserved from the envelope
	// ceiling is otter's maximum: limit may grow to it, and it drops to limit when the envelope
	// refuses. otter runs its frequency sketch from half its maximum, so a maximum that tracked
	// limit would charge every write of a growing cache for it.
	ceiling atomic.Int64

	growMu sync.Mutex
	closed bool
	// unbudgeted holds maxBytes from the start and never touches the envelope.
	unbudgeted bool
}

// byteLRUEvictions is the state otter's deletion handler updates.
type byteLRUEvictions[V any] struct {
	// resident is the weight of the entries present: every write charges its net
	// change and otter's deletion handler refunds each removal, so it needs no
	// reset on Purge.
	resident atomic.Int64
	// Indirect so Close can clear it: a callback capturing the owner would keep
	// the owner alive as long as the cache.
	onEvict atomic.Pointer[func(uint64, V)]
}

// otterEntryOverheadBytes is otter's own per-entry cost beyond the key and the
// value. Omitting it undercounts a cache of small entries ~1.6x.
const otterEntryOverheadBytes = 64

// ByteLRUEntryOverheadBytes is what an entry costs beyond its value's own bytes:
// the uint64 key and otter's bookkeeping. Weighers add it to every entry.
const ByteLRUEntryOverheadBytes = 8 + otterEntryOverheadBytes

const (
	// Taken unconditionally, so it must stay small: every short-lived cache pays
	// it whatever the envelope has left.
	byteLRUFloorBytes = int64(1 * datasize.MB)
	byteLRUChunkBytes = int64(32 * datasize.MB)
)

func newByteLRU[V any](maxBytes datasize.ByteSize, weigh func(uint64, V) int64, onEvict func(uint64, V)) *ByteLRU[V] {
	ev := &byteLRUEvictions[V]{}
	b := &ByteLRU[V]{weigh: weigh, maxBytes: max(int64(maxBytes), 1), byteLRUEvictions: ev}
	if onEvict != nil {
		b.onEvict.Store(&onEvict)
	}
	floor := min(byteLRUFloorBytes, b.maxBytes)
	cachebudget.Global.Take(floor)
	b.limit.Store(floor)
	b.ceiling.Store(b.maxBytes)
	b.c = newOtter(b.maxBytes, weigh, ev.onDeletion(weigh))
	return b
}

// onDeletion refunds a removal and forwards it to the owner's hook. It closes
// over the evictions object, not the ByteLRU: otter's runtime cleanup keeps the
// handler alive as long as the cache, and a handler reaching the ByteLRU would
// make it uncollectable.
func (ev *byteLRUEvictions[V]) onDeletion(weigh func(uint64, V) int64) func(otter.DeletionEvent[uint64, V]) {
	return func(e otter.DeletionEvent[uint64, V]) {
		// A replacement is refunded by the write, from the value Set displaced.
		if e.Cause != otter.CauseReplacement {
			ev.resident.Add(-weigh(e.Key, e.Value))
		}
		if fn := ev.onEvict.Load(); fn != nil {
			(*fn)(e.Key, e.Value)
		}
	}
}

// NewByteLRU is a byte-bounded cache outside the shared cachebudget envelope. It may hold maxBytes
// from the start and needs no Close, for owners that have no teardown.
func NewByteLRU[V any](maxBytes datasize.ByteSize, weigh func(uint64, V) int64) *ByteLRU[V] {
	ev := &byteLRUEvictions[V]{}
	b := &ByteLRU[V]{weigh: weigh, maxBytes: max(int64(maxBytes), 1), byteLRUEvictions: ev, unbudgeted: true}
	b.limit.Store(b.maxBytes)
	b.ceiling.Store(b.maxBytes)
	b.c = newOtter(b.maxBytes, weigh, ev.onDeletion(weigh))
	return b
}

func newOtter[V any](maxWeight int64, weigh func(uint64, V) int64, onDeletion func(otter.DeletionEvent[uint64, V])) *otter.Cache[uint64, V] {
	return otter.Must(&otter.Options[uint64, V]{
		MaximumWeight: uint64(maxWeight),
		Weigher:       func(k uint64, v V) uint32 { return uint32(min(weigh(k, v), math.MaxUint32)) },
		// Atomic: called inside the delete itself, so otter allocates no event per
		// eviction to hand to the executor.
		OnAtomicDeletion: onDeletion,
	})
}

func (b *ByteLRU[V]) Get(key uint64) (V, bool) { return b.c.GetIfPresent(key) }

// Add reports that the value could be offered, not that it is resident: a
// W-TinyLFU rejection also returns true. False means it was too big for the
// whole budget, and no deletion fires for it.
func (b *ByteLRU[V]) Add(key uint64, value V) bool {
	w := b.weigh(key, value)
	if w > b.maxBytes {
		return false
	}
	// Set returns the displaced value for a present key, so the charge is the net change.
	prev, inserted := b.c.Set(key, value)
	if !inserted {
		w -= b.weigh(key, prev)
	}
	b.charge(w)
	return true
}

// compute is Add for a write that depends on the key's current value: fn runs
// under otter's bucket lock, so its check and its write are one step against
// other writers of key. fn must not call the cache.
func (b *ByteLRU[V]) compute(key uint64, fn func(prev V, found bool) (V, otter.ComputeOp)) {
	var delta int64
	b.c.Compute(key, func(prev V, found bool) (V, otter.ComputeOp) {
		v, op := fn(prev, found)
		if op != otter.WriteOp {
			return prev, op
		}
		w := b.weigh(key, v)
		if w > b.maxBytes {
			if found {
				return prev, otter.InvalidateOp // the write supersedes prev, so prev must not stay live
			}
			return prev, otter.CancelOp
		}
		if found {
			w -= b.weigh(key, prev)
		}
		delta = w
		return v, otter.WriteOp
	})
	b.charge(delta)
}

// removeIf deletes key's value when cond holds for it, as one step against writers of key.
func (b *ByteLRU[V]) removeIf(key uint64, cond func(V) bool) {
	b.c.ComputeIfPresent(key, func(v V) (V, otter.ComputeOp) {
		if cond(v) {
			return v, otter.InvalidateOp
		}
		return v, otter.CancelOp
	})
}

// charge books a net weight change and takes the next envelope chunk once
// residency passes the reserved limit.
func (b *ByteLRU[V]) charge(w int64) {
	if n := b.resident.Add(w); n > b.limit.Load() && b.limit.Load() < b.ceiling.Load() {
		b.grow()
	}
}

// grow re-tests the caller's condition under the lock: without it every writer
// piled up at a full limit reserves its own chunk, draining the envelope at once.
func (b *ByteLRU[V]) grow() {
	b.growMu.Lock()
	defer b.growMu.Unlock()
	b.growLocked()
}

func (b *ByteLRU[V]) growLocked() {
	if b.closed {
		return
	}
	lim, ceil := b.limit.Load(), b.ceiling.Load()
	for lim < ceil && b.resident.Load() > lim {
		chunk := min(byteLRUChunkBytes, ceil-lim)
		if !cachebudget.Global.Reserve(chunk) {
			b.ceiling.Store(lim) // envelope full: stop asking until Purge/Close frees it
			b.c.SetMaximum(uint64(lim))
			return
		}
		lim += chunk
		b.limit.Store(lim)
	}
}

func (b *ByteLRU[V]) Remove(key uint64) { b.c.Invalidate(key) }
func (b *ByteLRU[V]) Len() int          { return b.c.EstimatedSize() }

// SizeBytes is the weighed residency the bound is applied against.
func (b *ByteLRU[V]) SizeBytes() int64 { return b.resident.Load() }

// Purge empties the cache. A budgeted cache also returns its grown reservation, keeping
// the floor so it is never left disabled.
func (b *ByteLRU[V]) Purge() {
	b.growMu.Lock()
	defer b.growMu.Unlock()
	b.c.InvalidateAll()
	if b.closed || b.unbudgeted {
		return
	}
	floor := min(byteLRUFloorBytes, b.maxBytes)
	cachebudget.Global.Release(b.limit.Load() - floor)
	b.limit.Store(floor)
	b.ceiling.Store(b.maxBytes)
	b.c.SetMaximum(uint64(b.maxBytes))
	// A write that raced InvalidateAll was charged against the old limit and did not grow.
	b.growLocked()
}

// Close empties the cache and returns a budgeted cache's envelope reservation. Idempotent.
func (b *ByteLRU[V]) Close() {
	b.growMu.Lock()
	defer b.growMu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	// Entries, not just the reservation: the owner may keep the cache after Close.
	b.c.InvalidateAll()
	b.onEvict.Store(nil)
	if !b.unbudgeted {
		cachebudget.Global.Release(b.limit.Load())
	}
	b.limit.Store(0)
	b.ceiling.Store(0)
}

// HashByteLRU is an unbudgeted ByteLRU keyed by a hash: the first 8 bytes pick the slot, Get compares the whole hash.
type HashByteLRU[V any] struct {
	c     *ByteLRU[hashEntry[V]]
	weigh func(V) int64
}

type hashEntry[V any] struct {
	hash   common.Hash
	value  V
	weight int64 // weighed once in Add: ByteLRU asks for the weight on Add, on insert and on eviction
}

func NewHashByteLRU[V any](maxBytes datasize.ByteSize, weigh func(V) int64) *HashByteLRU[V] {
	return &HashByteLRU[V]{weigh: weigh, c: NewByteLRU(maxBytes, func(_ uint64, e hashEntry[V]) int64 { return e.weight })}
}

func (l *HashByteLRU[V]) Get(hash common.Hash) (value V, ok bool) {
	e, ok := l.c.Get(binary.BigEndian.Uint64(hash[:]))
	if !ok || e.hash != hash {
		return value, false
	}
	return e.value, true
}

func (l *HashByteLRU[V]) Add(hash common.Hash, value V) {
	l.c.Add(binary.BigEndian.Uint64(hash[:]), hashEntry[V]{hash: hash, value: value, weight: l.weigh(value) + length.Hash + ByteLRUEntryOverheadBytes})
}
