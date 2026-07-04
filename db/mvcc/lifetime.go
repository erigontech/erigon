// Copyright 2024 The Erigon Authors
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

// Package mvcc provides Lifetime: a lock-free-read, single-writer container that
// publishes immutable versions of a value and reclaims a superseded version's
// payload once no reader still pins it. Readers pin the current version and read
// without locking (RCU-style publication, reference-counted reclamation); only
// the writer takes the lock, and only on rare background events.
//
// It is biz-logic-free: it never inspects the values it carries. Callers supply
// the type parameters V (the published payload) and R (both the dirty items it
// sweeps and a superseded version's retired payload, freed once drained) plus a
// reclaim callback.
package mvcc

import (
	"sync"
	"sync/atomic"
)

// Dirty is a mutable collection of R that Lifetime sweeps under its lock
// (RegisterDirty + ForEachDirtyItem). The structural Scan requirement is
// satisfied by e.g. *btree2.BTreeG[R], so Lifetime holds the registry without
// depending on any container type.
type Dirty[R any] interface {
	Scan(iter func(item R) bool)
}

// Version is one immutable, published generation. Its V payload lives inline
// (one allocation per publish); readers pin it via Lifetime.Acquire so it stays
// alive while they read. Once a newer Version supersedes it and its last reader
// drains, its retired R payload is reclaimed.
type Version[V, R any] struct {
	Value V

	refcnt  atomic.Int32   // live readers
	retired []R            // payload to reclaim once drained
	next    *Version[V, R] // oldest→newest link (set under lock)
}

// Lifetime publishes immutable Versions under a single writer lock and lets
// readers pin the current one without locking. When a superseded Version's last
// reader drains, its retired payload is handed to the reclaim callback, out of
// the lock. It also owns the dirty registry guarded by the same lock.
type Lifetime[V, R any] struct {
	lock    sync.Mutex
	dirty   []Dirty[R]
	visible atomic.Pointer[Version[V, R]]
	oldest  *Version[V, R] // chain head; mutated only under lock
	reclaim func([]R)
}

// Init installs the reclaim callback and publishes an initial zero-value
// Version. Call once before any Acquire.
func (lt *Lifetime[V, R]) Init(reclaim func([]R)) {
	lt.reclaim = reclaim
	v := &Version[V, R]{}
	lt.visible.Store(v)
	lt.oldest = v
}

// RegisterDirty adds mutable collections to the lock-guarded dirty registry.
// Not safe to call concurrently with Recalc/ForEachDirtyItem.
func (lt *Lifetime[V, R]) RegisterDirty(stores ...Dirty[R]) {
	lt.dirty = append(lt.dirty, stores...)
}

// ForEachDirtyItem calls fn for every item in every registered dirty collection,
// under the writer lock. Slow, background-only (uniform all-dirty sweeps).
func (lt *Lifetime[V, R]) ForEachDirtyItem(fn func(item R)) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	for _, store := range lt.dirty {
		store.Scan(func(item R) bool { fn(item); return true })
	}
}

// Visible returns the current published payload, unpinned: safe only for
// lock-free reads of immutable fields, never retain it across a publish.
func (lt *Lifetime[V, R]) Visible() *V {
	return &lt.visible.Load().Value
}

// Acquire pins the current Version for a reader. Hot path: lock-free (atomic
// load + refcnt bump, re-validated against a concurrent publish).
func (lt *Lifetime[V, R]) Acquire() *Version[V, R] {
	// Load+increment is not atomic: between them the last reader of v may drain
	// and reclaim it. So re-load and retry if a publish swapped visible.
	// Hazard-pointer concept: https://github.com/facebook/folly/blob/main/folly/synchronization/Hazptr.h
	for {
		v := lt.visible.Load()
		v.refcnt.Add(1)
		if lt.visible.Load() == v {
			return v
		}
		lt.Release(v) // mis-pinned a superseded generation; drop and retry
	}
}

// Release drops a reader's pin. Hot path: lock-free unless this is the last
// reader of an already-superseded Version, which then reclaims under the lock.
func (lt *Lifetime[V, R]) Release(v *Version[V, R]) {
	if v.refcnt.Add(-1) == 0 {
		lt.reclaimRetired()
	}
}

// Recalc is the only way to mutate dirty state and/or publish a new version.
// Pass nil mutate to publish only; nil recalc to mutate without publishing.
// Slow, background-only: it holds the writer lock. For reads use the lock-free
// Acquire/Release/Visible.
//
// mutate runs under the lock and returns the files the publish retires; recalc
// returns the new payload to publish (wrapped into a Version here), or nil to
// decline publishing (mutate must then have retired nothing).
func (lt *Lifetime[V, R]) Recalc(
	mutate func() (retired []R, err error),
	recalc func() *V,
) error {
	var toDelete []R
	err := func() error {
		lt.lock.Lock()
		defer lt.lock.Unlock()

		var retired []R
		if mutate != nil {
			var err error
			if retired, err = mutate(); err != nil {
				return err
			}
		}
		if recalc != nil {
			// Publish the new generation, hand the outgoing one its retired
			// payload, and collect whatever is now reclaimable (freed after unlock).
			if v := recalc(); v != nil {
				next := &Version[V, R]{Value: *v}
				old := lt.visible.Load()
				old.retired = retired
				old.next = next
				lt.visible.Store(next)
			}
		}
		toDelete = lt.reclaimRetiredLocked()
		return nil
	}()
	lt.runReclaim(toDelete)
	return err
}

// reclaimRetiredLocked walks the oldest→newest chain while refcnt == 0,
// collecting drained generations' retired payloads for reclamation out of lock.
func (lt *Lifetime[V, R]) reclaimRetiredLocked() (toDelete []R) {
	cur := lt.visible.Load()
	for h := lt.oldest; h != cur && h.refcnt.Load() == 0; h = h.next {
		toDelete = append(toDelete, h.retired...)
		h.retired = nil
		lt.oldest = h.next
	}
	return toDelete
}

func (lt *Lifetime[V, R]) reclaimRetired() {
	lt.lock.Lock()
	toDelete := lt.reclaimRetiredLocked()
	lt.lock.Unlock()
	lt.runReclaim(toDelete)
}

func (lt *Lifetime[V, R]) runReclaim(toDelete []R) {
	if len(toDelete) > 0 && lt.reclaim != nil {
		lt.reclaim(toDelete)
	}
}
