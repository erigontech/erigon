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
// publishes immutable generations of a value and reclaims a superseded
// generation's payload once no reader still pins it. Readers pin the current
// generation and read without locking (RCU-style publication, reference-counted
// reclamation); only the writer takes the lock, and only on rare background events.
//
// It is biz-logic-free: it never inspects the values it carries. Callers supply
// the type parameters visibleFiles (the published payload), dirtyFiles (the source
// state recalcVisibleFiles reads to build each payload) and dirtyFile (a superseded
// version's retired payload, freed once no reader pins the version) plus a
// closeAndPhysicalRemove callback.
package mvcc

import (
	"sync"
	"sync/atomic"
)

// Generation is one immutable, published snapshot of the payload. It lives inline
// (one allocation per publish); readers pin it via Lifetime.Acquire so it stays
// alive while they read. Once a newer Generation supersedes it and its last reader
// releases it (refcnt reaches 0), its retired dirtyFile payload is reclaimed.
type Generation[visibleFiles, dirtyFile any] struct {
	Value visibleFiles

	refcnt  atomic.Int32                         // live readers
	retired []dirtyFile                          // reclaimed once refcnt reaches 0
	next    *Generation[visibleFiles, dirtyFile] // oldest→newest link (set under lock)
}

// Lifetime publishes immutable Generations under a single writer lock and lets
// readers pin the current one without locking. When a superseded Generation's last
// reader releases it (refcnt reaches 0), its retired payload is handed to
// closeAndPhysicalRemove, out of the lock.
//
// RCU:
// - Read the current version,
// - Copy it and mutate the copy (so in-flight readers keep seeing a stable old snapshot),
// - Update the pointer to publish the new copy atomically; free the old one later.
type Lifetime[visibleFiles, dirtyFiles, dirtyFile any] struct {
	lock                   sync.Mutex
	visible                atomic.Pointer[Generation[visibleFiles, dirtyFile]]
	oldest                 *Generation[visibleFiles, dirtyFile] // chain head; mutated only under lock
	dirty                  dirtyFiles                           // source state recalcVisibleFiles reads to build each payload
	closeAndPhysicalRemove func([]dirtyFile)
	recalcVisibleFiles     func(dirtyFiles) *visibleFiles // builds the payload each publish installs
}

// Init stores dirty (the source state passed to recalcVisibleFiles on each publish)
// and installs the callbacks — closeAndPhysicalRemove reclaims a superseded
// generation's retired files once no reader pins it, recalcVisibleFiles builds
// the payload each publish installs — and publishes an initial zero-value
// Generation. Both callbacks are required. Call once before any Acquire.
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) Init(dirty dirtyFiles, closeAndPhysicalRemove func([]dirtyFile), recalcVisibleFiles func(dirtyFiles) *visibleFiles) {
	if closeAndPhysicalRemove == nil || recalcVisibleFiles == nil {
		panic("mvcc.Lifetime.Init: closeAndPhysicalRemove and recalcVisibleFiles must be non-nil")
	}
	lt.dirty = dirty
	lt.closeAndPhysicalRemove = closeAndPhysicalRemove
	lt.recalcVisibleFiles = recalcVisibleFiles
	v := &Generation[visibleFiles, dirtyFile]{}
	lt.visible.Store(v)
	lt.oldest = v
}

// Visible returns the current published payload, unpinned: safe only for
// lock-free reads of immutable fields, never retain it across a publish.
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) Visible() *visibleFiles {
	return &lt.visible.Load().Value
}

// Acquire pins the current Generation for a reader. Hot path: lock-free (atomic
// load + refcnt bump, re-validated against a concurrent publish).
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) Acquire() *Generation[visibleFiles, dirtyFile] {
	// Load+increment is not atomic: between them the last reader of v may release
	// it (refcnt to 0) and reclaim it. So re-load and retry if a publish swapped visible.
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
// reader of an already-superseded Generation, which then reclaims under the lock.
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) Release(v *Generation[visibleFiles, dirtyFile]) {
	if v.refcnt.Add(-1) != 0 {
		return
	}
	lt.lock.Lock()
	toDelete := lt.reclaimRetiredLocked()
	lt.lock.Unlock()
	lt.runReclaim(toDelete)
}

// SlowReadDirtyFiles runs fn under the writer lock. Lifetime holds no dirty state
// of its own, so this merely lends its lock for a consistent read or sweep of the
// caller's dirty files — which the publish path mutates under this same lock.
// Slow, background-only: it contends with publishes, so hot-path readers must use
// the lock-free Acquire/Release instead.
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) SlowReadDirtyFiles(fn func()) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	fn()
}

// UpdateDirtyFiles runs mutate (nil = none) under the writer lock, then, if
// mutate asks to, publishes a new generation. mutate returns the files the
// publish retires and whether to publish — only the caller knows whether it
// changed the visible set (a removal-only pass can decline when it retired
// nothing; anything additive must publish). nil mutate republishes.
//
// Slow, background-only: for reads use the lock-free Acquire/Release/Visible; to
// run under the lock without publishing, use SlowReadDirtyFiles.
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) UpdateDirtyFiles(mutate func() (retired []dirtyFile, publish bool, err error)) error {
	var toDelete []dirtyFile
	err := func() error {
		lt.lock.Lock()
		defer lt.lock.Unlock()

		publish := true // nil mutate = republish
		var retired []dirtyFile
		if mutate != nil {
			var err error
			if retired, publish, err = mutate(); err != nil {
				return err
			}
		}
		if publish {
			// Publish the new generation, hand the outgoing one its retired payload,
			// and collect whatever is now reclaimable (freed after unlock).
			next := &Generation[visibleFiles, dirtyFile]{Value: *lt.recalcVisibleFiles(lt.dirty)}
			old := lt.visible.Load()
			old.retired = retired
			old.next = next
			lt.visible.Store(next)
		}
		toDelete = lt.reclaimRetiredLocked()
		return nil
	}()
	lt.runReclaim(toDelete)
	return err
}

// reclaimRetiredLocked frees retired payloads oldest-first, stopping at the first
// still-pinned generation, and collects them for reclamation out of lock. Oldest-first
// is a safety rule, not just chain bookkeeping: a file retired at one generation may
// still be present in an older, still-pinned generation's visible set, so it is unsafe
// to remove until every older generation has drained.
func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) reclaimRetiredLocked() (toDelete []dirtyFile) {
	cur := lt.visible.Load()
	for h := lt.oldest; h != cur && h.refcnt.Load() == 0; h = h.next {
		toDelete = append(toDelete, h.retired...)
		h.retired = nil
		lt.oldest = h.next
	}
	return toDelete
}

func (lt *Lifetime[visibleFiles, dirtyFiles, dirtyFile]) runReclaim(toDelete []dirtyFile) {
	if len(toDelete) > 0 {
		lt.closeAndPhysicalRemove(toDelete)
	}
}
