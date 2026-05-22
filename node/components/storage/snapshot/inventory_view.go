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

package snapshot

import "sync"

// ChangeSet describes an atomic transition emitted by Inventory.
// Subscribers receive ChangeSet on every mutation (AddFile, RemoveFile,
// ReplaceWithMerge, MarkAdvertisable, PromoteTrust, MarkLocal,
// MarkNotLocal) and use it primarily as a wake signal — re-check the
// entries you care about against the live Inventory.
//
// Files names every entry the transition touched. The slice is a
// notification, not a delta replay: it tells subscribers WHICH names
// might have changed, not WHAT changed about them.
type ChangeSet struct {
	Files []string
}

// HeldView is an immutable snapshot of Inventory state. Mutations to
// Inventory after View() returns do not affect a held view. Eviction of
// files referenced by held views is deferred until the last referencing
// view closes — analogous to db/snapshotsync.RoSnapshots' segment
// refcount discipline.
type HeldView interface {
	// Files returns the FileEntry slice for the given domain captured at
	// View() time. The returned entries are clones — mutating them does
	// not affect the inventory or other held views.
	Files(domain Domain) []*FileEntry

	// BlockFiles returns the captured block-file slice (entries with
	// empty Domain and Kind=KindKV).
	BlockFiles() []*FileEntry

	// Get returns the captured FileEntry for the named file, or
	// (nil, false) if the view did not reference it at construction.
	Get(name string) (*FileEntry, bool)

	// RefCount returns the current held-view refcount for the named
	// file. Tests use this to verify deferred-eviction; production code
	// does not rely on it.
	RefCount(name string) int

	// Close releases the view. Decrements refcount on each captured
	// file; any deferred-eviction state for files dropping to zero
	// refcount clears. Multi-call safe.
	Close()
}

// InventoryView is the concrete held-view returned by Inventory.View().
// It satisfies HeldView structurally; consumers that want the interface
// can assign to a HeldView variable.
type InventoryView struct {
	inv *Inventory

	mu       sync.Mutex
	captured map[string]*FileEntry
	closed   bool
}

// View captures the current Inventory state and returns a held view.
// Refcount on every captured entry is incremented; the view's Close
// decrements them and clears any pending-deletes that drop to zero.
//
// Tests and short-lived consumers can defer view.Close() at construction;
// long-running consumers must close explicitly when their unit of work
// completes.
func (inv *Inventory) View() *InventoryView {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	captured := make(map[string]*FileEntry)
	for _, entries := range inv.domains {
		for _, e := range entries {
			captured[e.Name] = e.Clone()
			inv.refcount[e.Name]++
		}
	}
	for _, e := range inv.blocks {
		captured[e.Name] = e.Clone()
		inv.refcount[e.Name]++
	}
	for _, e := range inv.caplin {
		captured[e.Name] = e.Clone()
		inv.refcount[e.Name]++
	}
	for _, e := range inv.meta {
		captured[e.Name] = e.Clone()
		inv.refcount[e.Name]++
	}
	for _, e := range inv.salt {
		captured[e.Name] = e.Clone()
		inv.refcount[e.Name]++
	}
	return &InventoryView{inv: inv, captured: captured}
}

// Files implements HeldView.
func (v *InventoryView) Files(domain Domain) []*FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Domain == domain {
			out = append(out, e)
		}
	}
	return out
}

// BlockFiles implements HeldView. It mirrors the inv.blocks bucket
// (AddFile's default branch with Domain==""): block primaries (.seg,
// KindKV) AND block accessors (.idx, KindAccessor). Meta/salt/caplin
// also carry Domain=="" but live in their own buckets with their own
// dispatch paths, so they are excluded here. Filtering to KindKV alone
// dropped block .idx accessors — the lifecycle Sweep never dispatched
// them, they never reached LifecycleIndexed, and InitialStateReady (which
// waits on every phase-1 file, accessors included) hung forever.
func (v *InventoryView) BlockFiles() []*FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Domain == "" && e.Kind != KindMeta && e.Kind != KindSalt && e.Kind != KindCaplin {
			out = append(out, e)
		}
	}
	return out
}

// MetaFiles returns the captured meta-kind entries (e.g. erigondb.toml).
// Meta files have their own lifecycle path that's distinct from
// indexing: they're the precursor input to BuildMissedIndices for other
// files, not a target of indexing themselves. The lifecycle driver
// dispatches them via a meta-specific transition rather than mixing
// them into OnIndexing.
func (v *InventoryView) MetaFiles() []*FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Kind == KindMeta {
			out = append(out, e)
		}
	}
	return out
}

// SaltFiles returns the captured salt-kind entries (salt-blocks.txt,
// salt-state.txt). Same dispatch shape as MetaFiles — salt is a
// precursor to indexing (consumed by GetIndexSalt), not a target.
func (v *InventoryView) SaltFiles() []*FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Kind == KindSalt {
			out = append(out, e)
		}
	}
	return out
}

// CaplinFiles returns the captured caplin-kind entries. Caplin beacon
// snapshots have no indexing or validation step in the EL lifecycle —
// they're consumed by Caplin itself, not by execution. Same dispatch
// shape as Meta/Salt.
func (v *InventoryView) CaplinFiles() []*FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Kind == KindCaplin {
			out = append(out, e)
		}
	}
	return out
}

// Get implements HeldView.
func (v *InventoryView) Get(name string) (*FileEntry, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.captured[name]
	return e, ok
}

// RefCount implements HeldView.
func (v *InventoryView) RefCount(name string) int {
	v.inv.mu.RLock()
	defer v.inv.mu.RUnlock()
	return v.inv.refcount[name]
}

// Close implements HeldView. Multi-call safe.
func (v *InventoryView) Close() {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return
	}
	v.closed = true
	captured := v.captured
	v.mu.Unlock()

	v.inv.mu.Lock()
	defer v.inv.mu.Unlock()
	for name := range captured {
		v.inv.refcount[name]--
		if v.inv.refcount[name] <= 0 {
			delete(v.inv.refcount, name)
			// Only drop timings if the file was actually evicted
			// (had a pending delete). Re-added files keep their
			// timings — the orchestrator wants the original
			// EnqueuedAt, not the re-add time.
			if _, evicted := v.inv.pendingDeletes[name]; evicted {
				delete(v.inv.timings, name)
			}
			delete(v.inv.pendingDeletes, name)
		}
	}
}

// Subscribe registers a subscriber to receive ChangeSet on every
// mutation. Returns the receive channel and an unsubscribe function;
// calling unsubscribe closes the channel.
//
// The channel is buffered (size 16); subscribers must drain promptly.
// On a full buffer, the notification is dropped — consumers needing
// guaranteed delivery should layer their own bounded-queue/back-pressure
// strategy. The integration with the framework event bus
// (flow.InventoryChanged) lands in a follow-on commit and replaces this
// direct path for production consumers.
func (inv *Inventory) Subscribe() (<-chan ChangeSet, func()) {
	ch := make(chan ChangeSet, 16)
	inv.mu.Lock()
	inv.subs = append(inv.subs, ch)
	inv.mu.Unlock()
	unsubscribe := func() {
		inv.mu.Lock()
		defer inv.mu.Unlock()
		for i, c := range inv.subs {
			if c == ch {
				inv.subs = append(inv.subs[:i], inv.subs[i+1:]...)
				close(c)
				return
			}
		}
	}
	return ch, unsubscribe
}

// PendingDeletes returns names of entries that have been removed but are
// still referenced by at least one held view. Tests inspect this to
// verify deferred-eviction behavior; production consumers do not depend
// on it.
func (inv *Inventory) PendingDeletes() []string {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	out := make([]string, 0, len(inv.pendingDeletes))
	for name := range inv.pendingDeletes {
		out = append(out, name)
	}
	return out
}

// GetByName scans every category and returns a clone of the first entry
// matching the name, or (nil, false) if no entry matches.
//
// The result is a clone — callers can safely read fields (Local,
// Advertisable, Trust, etc.) without holding any lock, and concurrent
// mutators (MarkLocal, MarkAdvertisable, etc.) will not race against
// the caller. Mutating the returned clone does not affect the
// inventory.
func (inv *Inventory) GetByName(name string) (*FileEntry, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if e := inv.findByNameLocked(name); e != nil {
		return e.Clone(), true
	}
	return nil, false
}

// MarkLocal flips Local=true on the named entry and notifies
// subscribers. Returns true if the flag was actually changed (false if
// already local, or no entry with that name exists).
//
// Internally advances LifecycleState to LifecycleDownloaded (the
// minimum state for Local=true). If the entry was already past
// LifecycleDownloaded (e.g. Indexed or Advertisable), this is a no-op.
func (inv *Inventory) MarkLocal(name string) bool {
	inv.mu.Lock()
	e := inv.findByNameLocked(name)
	if e == nil || e.Local {
		inv.mu.Unlock()
		return false
	}
	applyStateToFlags(e, LifecycleDownloaded)
	inv.mu.Unlock()
	inv.notify(ChangeSet{Files: []string{name}})
	return true
}

// MarkNotLocal flips Local=false on the named entry and notifies
// subscribers. Returns true if the flag was actually changed. Used by
// callers that detect corruption and trigger a re-download.
//
// Internally resets LifecycleState to LifecycleDeclared — corruption
// means re-fetch from scratch, not just back-tracking one step.
func (inv *Inventory) MarkNotLocal(name string) bool {
	inv.mu.Lock()
	e := inv.findByNameLocked(name)
	if e == nil || !e.Local {
		inv.mu.Unlock()
		return false
	}
	applyStateToFlags(e, LifecycleDeclared)
	inv.mu.Unlock()
	inv.notify(ChangeSet{Files: []string{name}})
	return true
}

// findByNameLocked scans every category for an entry by name. Caller
// must hold inv.mu (read or write).
func (inv *Inventory) findByNameLocked(name string) *FileEntry {
	for _, slice := range [][]*FileEntry{inv.blocks, inv.caplin, inv.meta, inv.salt} {
		if e := findByName(slice, name); e != nil {
			return e
		}
	}
	for _, entries := range inv.domains {
		if e := findByName(entries, name); e != nil {
			return e
		}
	}
	return nil
}

// notify sends ChangeSet to every subscriber. Drops to subscribers with
// full buffers — see Subscribe's docstring.
func (inv *Inventory) notify(cs ChangeSet) {
	inv.mu.RLock()
	subs := append([]chan ChangeSet(nil), inv.subs...)
	inv.mu.RUnlock()
	for _, c := range subs {
		select {
		case c <- cs:
		default:
		}
	}
}
