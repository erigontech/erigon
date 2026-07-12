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

package snapshotsync

import (
	"sync"
	"sync/atomic"
)

// generation is one published, immutable view of the dirty file set. Readers pin a generation
// via acquire/release; its retired files are closed and unlinked only once every reader that
// pinned it has drained (refcnt hits 0).
type generation[P any] struct {
	payload P
	refcnt  atomic.Int32    // live readers pinning this generation
	retired []*DirtySegment // files this generation was the last to reference; unlinked on drain
	next    *generation[P]  // oldest->newest chain link (set under lock)
}

// visibleGenerations is the payload-opaque refcounted-generation core shared by the EL
// block-snapshot and CL caplin-state readers. Keying, payload iteration, and watermark
// computation live entirely in the consumer's P and its recalc; the core touches only refcnt,
// retired, next, and oldest. lock is shared with the consumer's dirty-tree mutex so a dirty
// mutation and its publish are one atomic step. Mirrors the Aggregator refcnt model
// (visible/oldestVisible/reclaimRetiredLocked).
type visibleGenerations[P any] struct {
	lock    *sync.RWMutex
	visible atomic.Pointer[generation[P]]
	oldest  *generation[P] // chain head; reclamation walks oldest->visible from here
}

func (g *visibleGenerations[P]) init(lock *sync.RWMutex, initial P) {
	g.lock = lock
	empty := &generation[P]{payload: initial}
	g.visible.Store(empty)
	g.oldest = empty
}

func (g *visibleGenerations[P]) current() P {
	return g.visible.Load().payload
}

// drained reports whether no generation is pinned: after a publish, the chain has collapsed to
// the single current generation, so no reader can still reach an older generation's segments.
// Caller must hold lock (oldest is lock-guarded).
func (g *visibleGenerations[P]) drained() bool {
	return g.oldest == g.visible.Load()
}

// acquire pins the current generation. Load and increment are not atomic together, so after
// incrementing we re-check the generation is still current; if superseded mid-pin we drop
// the stale pin and retry (hazard-pointer style).
func (g *visibleGenerations[P]) acquire() *generation[P] {
	for {
		v := g.visible.Load()
		v.refcnt.Add(1)
		if g.visible.Load() == v {
			return v
		}
		g.release(v)
	}
}

// release drops a pin taken by acquire; the last reader of a superseded generation triggers
// reclamation of drained generations' retired files.
func (g *visibleGenerations[P]) release(v *generation[P]) {
	if v.refcnt.Add(-1) == 0 {
		g.reclaim()
	}
}

// publish stores newPayload as the current generation, retiring `retired` (files removed from
// dirty during the outgoing generation's tenure) onto the outgoing generation, then eagerly
// reclaims the drained older generations inline under lock. Caller must hold lock.
func (g *visibleGenerations[P]) publish(newPayload P, retired []*DirtySegment) {
	next := &generation[P]{payload: newPayload}
	old := g.visible.Load()
	old.retired = retired
	old.next = next
	g.visible.Store(next)
	closeAndRemoveSegments(g.reclaimRetiredLocked())
}

// reclaimRetiredLocked walks the oldest->visible chain from the head, collecting the retired
// files of every fully-drained generation older than the current one. Caller must hold lock;
// the returned files are closed and unlinked by the caller off-lock.
func (g *visibleGenerations[P]) reclaimRetiredLocked() (toRemove []*DirtySegment) {
	cur := g.visible.Load()
	for h := g.oldest; h != cur && h.refcnt.Load() == 0; h = h.next {
		toRemove = append(toRemove, h.retired...)
		h.retired = nil
		g.oldest = h.next
	}
	return toRemove
}

func (g *visibleGenerations[P]) reclaim() {
	g.lock.Lock()
	toRemove := g.reclaimRetiredLocked()
	g.lock.Unlock()
	closeAndRemoveSegments(toRemove)
}
