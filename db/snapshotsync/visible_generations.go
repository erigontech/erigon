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

// generation is one published, immutable view of the dirty file set. Readers pin a
// generation via acquire/release; the files it is the last to reference are disposed only
// once every reader pinning it has drained (refcnt hits 0). retired files are unlinked from
// disk; detached files only have their fds closed (the file stays on disk) — the split keeps
// permanent removal (RemoveOverlaps, merge cleanup) apart from re-open/shutdown fd cleanup.
type generation[P any] struct {
	payload  P
	refcnt   atomic.Int32    // live readers pinning this generation
	retired  []*DirtySegment // last-referenced files to unlink on head-drain (permanent removal)
	detached []*DirtySegment // last-referenced files to close fds-only on head-drain (no unlink)
	next     *generation[P]  // oldest->newest chain link (set under lock)
}

// visibleGenerations is the payload-opaque refcounted-generation core shared by the EL
// block-snapshot and CL caplin-state readers. Keying, payload iteration, and watermark
// computation live entirely in the consumer's P and its recalc; the core touches only
// refcnt, retired, next, and oldest. lock is shared with the consumer's dirty-tree mutex so
// a dirty mutation and its publish are one atomic step.
type visibleGenerations[P any] struct {
	lock    *sync.RWMutex
	current atomic.Pointer[generation[P]]
	oldest  *generation[P] // chain head; reclamation walks oldest->newest from here
}

func (g *visibleGenerations[P]) init(lock *sync.RWMutex, initial P) {
	g.lock = lock
	first := &generation[P]{payload: initial}
	g.current.Store(first)
	g.oldest = first
}

func (g *visibleGenerations[P]) currentPayload() P {
	return g.current.Load().payload
}

// acquire pins the current generation. Load and increment are not atomic together, so after
// incrementing we re-check the generation is still current; if superseded mid-pin we drop
// the stale pin and retry (hazard-pointer style).
func (g *visibleGenerations[P]) acquire() *generation[P] {
	for {
		v := g.current.Load()
		v.refcnt.Add(1)
		if g.current.Load() == v {
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

// publish stores newPayload as the current generation, retiring `retired` (files permanently
// removed from dirty during the outgoing generation's tenure) onto the outgoing generation,
// then eagerly reclaims the drained older generations inline under lock. Caller must hold
// lock. Publish is publish-and-eager-reclaim, never store-only.
func (g *visibleGenerations[P]) publish(newPayload P, retired []*DirtySegment) {
	g.publishDisposing(newPayload, retired, nil)
}

// publishDisposing is publish with both disposition lists: retired files are unlinked on
// head-drain, detached files only have their fds closed (kept on disk). Caller must hold lock.
func (g *visibleGenerations[P]) publishDisposing(newPayload P, retired, detached []*DirtySegment) {
	next := &generation[P]{payload: newPayload}
	old := g.current.Load()
	old.retired = retired
	old.detached = detached
	old.next = next
	g.current.Store(next)
	disposeReclaimed(g.reclaimLocked())
}

// reclaimed holds the files of drained generations, split by disposition.
type reclaimed struct {
	toUnlink []*DirtySegment // closeAndRemoveFiles: close fds and unlink from disk
	toClose  []*DirtySegment // close fds only; the file stays on disk
}

// reclaimLocked walks the oldest->newest chain from the head, collecting the disposable files
// of every fully-drained generation older than the current one. Caller must hold lock; the
// returned files are disposed by the caller off-lock.
func (g *visibleGenerations[P]) reclaimLocked() (r reclaimed) {
	cur := g.current.Load()
	for h := g.oldest; h != cur && h.refcnt.Load() == 0; h = h.next {
		r.toUnlink = append(r.toUnlink, h.retired...)
		r.toClose = append(r.toClose, h.detached...)
		h.retired = nil
		h.detached = nil
		g.oldest = h.next
	}
	return r
}

func disposeReclaimed(r reclaimed) {
	closeAndRemoveSegments(r.toUnlink)
	for _, sn := range r.toClose {
		sn.close()
	}
}

func (g *visibleGenerations[P]) reclaim() {
	g.lock.Lock()
	r := g.reclaimLocked()
	g.lock.Unlock()
	disposeReclaimed(r)
}
