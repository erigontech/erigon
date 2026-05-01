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

// BlockFiles implements HeldView.
func (v *InventoryView) BlockFiles() []*FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Domain == "" && e.Kind == KindKV {
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
func (inv *Inventory) MarkLocal(name string) bool {
	inv.mu.Lock()
	e := inv.findByNameLocked(name)
	if e == nil || e.Local {
		inv.mu.Unlock()
		return false
	}
	e.Local = true
	inv.mu.Unlock()
	inv.notify(ChangeSet{Files: []string{name}})
	return true
}

// MarkNotLocal flips Local=false on the named entry and notifies
// subscribers. Returns true if the flag was actually changed. Used by
// callers that detect corruption and trigger a re-download.
func (inv *Inventory) MarkNotLocal(name string) bool {
	inv.mu.Lock()
	e := inv.findByNameLocked(name)
	if e == nil || !e.Local {
		inv.mu.Unlock()
		return false
	}
	e.Local = false
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
