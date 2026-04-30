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

package harness

import (
	"context"
	"errors"
	"sync"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/views"
)

// ErrNotFound — the not-declared (Missing) outcome from a stub read.
// Real read handles return their package-specific not-found error class;
// the stub uses this sentinel so scenarios can assert with errors.Is.
var ErrNotFound = errors.New("stub read: file not declared")

// StubInventory wraps snapshot.Inventory with the held-view + ChangeSet
// surface the contract requires. Item #1 will fold these methods into
// snapshot.Inventory itself; this stub exists so the harness can validate
// the spec against passing scenarios before that lands.
//
// The wrapper preserves snapshot.Inventory's existing API for mutation
// (AddFile, RemoveFile, ReplaceWithMerge) and accessors (LocalFiles,
// Coverage, etc.) — the test surface adds View, Subscribe, and a small
// set of explicit-state mutations (MarkLocal, MarkAdvertisable,
// MarkNotLocal) that scenarios drive.
type StubInventory struct {
	inv *snapshot.Inventory

	mu             sync.Mutex
	refcount       map[string]int                 // name → number of held views referencing
	pendingDeletes map[string]*snapshot.FileEntry // entries removed-while-held; preserved view-side
	subs           []chan views.ChangeSet
}

// NewStubInventory returns an empty StubInventory.
func NewStubInventory() *StubInventory {
	return &StubInventory{
		inv:            snapshot.NewInventory(),
		refcount:       map[string]int{},
		pendingDeletes: map[string]*snapshot.FileEntry{},
	}
}

// AddFile declares a file in the inventory and emits a ChangeSet.
func (s *StubInventory) AddFile(entry *snapshot.FileEntry) {
	s.inv.AddFile(entry)
	s.notify(views.ChangeSet{Files: []string{entry.Name}})
}

// MarkLocal flips Local=true on the named entry and emits a ChangeSet.
// No-op (no notify) if the entry doesn't exist.
func (s *StubInventory) MarkLocal(name string) {
	orig := s.find(name)
	if orig == nil {
		return
	}
	updated := orig.Clone()
	updated.Local = true
	s.inv.AddFile(updated)
	s.notify(views.ChangeSet{Files: []string{name}})
}

// MarkNotLocal flips Local=false on the named entry and emits a ChangeSet.
// Used in scenario 10 (corruption-detected re-download).
func (s *StubInventory) MarkNotLocal(name string) {
	orig := s.find(name)
	if orig == nil {
		return
	}
	updated := orig.Clone()
	updated.Local = false
	s.inv.AddFile(updated)
	s.notify(views.ChangeSet{Files: []string{name}})
}

// MarkAdvertisable flips Advertisable=true on the named entry and emits a
// ChangeSet. Returns the underlying Inventory.MarkAdvertisable result.
func (s *StubInventory) MarkAdvertisable(name string) bool {
	ok := s.inv.MarkAdvertisable(name)
	if ok {
		s.notify(views.ChangeSet{Files: []string{name}})
	}
	return ok
}

// RemoveFile removes the named entry. If a held view still references it,
// the entry is recorded in pendingDeletes (and held views continue to
// see their captured copy). Emits a ChangeSet.
func (s *StubInventory) RemoveFile(name string) {
	s.mu.Lock()
	if rc := s.refcount[name]; rc > 0 {
		if e := s.find(name); e != nil {
			s.pendingDeletes[name] = e.Clone()
		}
	}
	s.mu.Unlock()
	s.inv.RemoveFile(name)
	s.notify(views.ChangeSet{Files: []string{name}})
}

// ReplaceWithMerge atomically replaces constituents with a merged file.
// Constituents referenced by held views go into pendingDeletes for
// deferred eviction. Emits a ChangeSet on success.
func (s *StubInventory) ReplaceWithMerge(merged *snapshot.FileEntry, replaced []string) bool {
	s.mu.Lock()
	for _, name := range replaced {
		if rc := s.refcount[name]; rc > 0 {
			if e := s.find(name); e != nil {
				s.pendingDeletes[name] = e.Clone()
			}
		}
	}
	s.mu.Unlock()
	ok := s.inv.ReplaceWithMerge(merged, replaced)
	if ok {
		files := append([]string{merged.Name}, replaced...)
		s.notify(views.ChangeSet{Files: files})
	}
	return ok
}

// View captures the current Inventory state and returns a HeldView.
// Refcount on every captured entry is incremented; the view's Close
// decrements them and clears any pending-deletes that drop to zero.
func (s *StubInventory) View() views.HeldView {
	s.mu.Lock()
	defer s.mu.Unlock()

	captured := map[string]*snapshot.FileEntry{}
	for _, d := range s.inv.Domains() {
		for _, e := range s.inv.AllDomainFiles(d) {
			captured[e.Name] = e.Clone()
			s.refcount[e.Name]++
		}
	}
	for _, e := range s.inv.BlockFiles() {
		captured[e.Name] = e.Clone()
		s.refcount[e.Name]++
	}
	for _, e := range s.inv.CaplinFiles() {
		captured[e.Name] = e.Clone()
		s.refcount[e.Name]++
	}
	return &stubView{inv: s, captured: captured}
}

// Subscribe returns a buffered channel that receives ChangeSet on every
// inventory mutation. Unsubscribe closes the channel.
func (s *StubInventory) Subscribe() (<-chan views.ChangeSet, func()) {
	ch := make(chan views.ChangeSet, 16)
	s.mu.Lock()
	s.subs = append(s.subs, ch)
	s.mu.Unlock()
	unsubscribe := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, c := range s.subs {
			if c == ch {
				s.subs = append(s.subs[:i], s.subs[i+1:]...)
				close(c)
				return
			}
		}
	}
	return ch, unsubscribe
}

// PendingDeletes returns the names of entries that have been removed but
// are still referenced by at least one held view. Tests inspect this to
// verify deferred-eviction behavior.
func (s *StubInventory) PendingDeletes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.pendingDeletes))
	for name := range s.pendingDeletes {
		out = append(out, name)
	}
	return out
}

// Inventory exposes the underlying snapshot.Inventory for tests that need
// to assert against its accessor surface (Coverage, LocalFiles, etc.).
func (s *StubInventory) Inventory() *snapshot.Inventory { return s.inv }

func (s *StubInventory) notify(cs views.ChangeSet) {
	s.mu.Lock()
	subs := append([]chan views.ChangeSet(nil), s.subs...)
	s.mu.Unlock()
	for _, c := range subs {
		select {
		case c <- cs:
		default:
			// Drop if subscriber's buffer is full; harness tests are
			// expected to drain promptly. Real implementations need a
			// firmer back-pressure story.
		}
	}
}

// find locates the entry by name across domains, blocks, caplin, meta,
// salt. Returns nil if not present. Internal helper for the stub.
func (s *StubInventory) find(name string) *snapshot.FileEntry {
	for _, d := range s.inv.Domains() {
		for _, e := range s.inv.AllDomainFiles(d) {
			if e.Name == name {
				return e
			}
		}
	}
	for _, e := range s.inv.BlockFiles() {
		if e.Name == name {
			return e
		}
	}
	for _, e := range s.inv.CaplinFiles() {
		if e.Name == name {
			return e
		}
	}
	return nil
}

// stubView implements views.HeldView. Captured entries are clones; mutations
// to the underlying inventory after View() do not affect this view.
type stubView struct {
	inv      *StubInventory
	mu       sync.Mutex
	captured map[string]*snapshot.FileEntry
	closed   bool
}

func (v *stubView) Files(domain snapshot.Domain) []*snapshot.FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*snapshot.FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Domain == domain {
			out = append(out, e)
		}
	}
	return out
}

func (v *stubView) BlockFiles() []*snapshot.FileEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*snapshot.FileEntry, 0, len(v.captured))
	for _, e := range v.captured {
		if e.Domain == "" && e.Kind == snapshot.KindKV {
			out = append(out, e)
		}
	}
	return out
}

func (v *stubView) Get(name string) (*snapshot.FileEntry, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.captured[name]
	return e, ok
}

func (v *stubView) RefCount(name string) int {
	v.inv.mu.Lock()
	defer v.inv.mu.Unlock()
	return v.inv.refcount[name]
}

func (v *stubView) Close() {
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

// StubReadHandle simulates a read against (Inventory + optional held view)
// embodying the wait-or-pending contract:
//
//   - File present in held view AND ready → return Ready immediately.
//   - File declared in live inventory AND ready → return Ready immediately.
//   - File declared but not ready → wait on ChangeSet until ready or ctx
//     deadline. On deadline → ErrPending.
//   - File not declared anywhere → ErrNotFound.
//
// "Ready" means Local=true and (when RequireAdvertisable is set)
// Advertisable=true.
type StubReadHandle struct {
	Inventory *StubInventory

	// RequireAdvertisable: when true, files with Local=true but
	// Advertisable=false count as Pending (mirrors scenario 9).
	RequireAdvertisable bool
}

// Read attempts to read the named file. view may be nil for tests not
// exercising held-view semantics.
func (h *StubReadHandle) Read(ctx context.Context, view views.HeldView, name string) (string, error) {
	if view != nil {
		if e, ok := view.Get(name); ok && h.ready(e) {
			return e.Name, nil
		}
	}

	// Subscribe before re-checking live state to avoid lost-update races
	// between "file just landed" and "we just started waiting".
	sub, unsub := h.Inventory.Subscribe()
	defer unsub()

	if e := h.Inventory.find(name); e != nil && h.ready(e) {
		return e.Name, nil
	}
	if h.Inventory.find(name) == nil && (view == nil || !hasInView(view, name)) {
		return "", ErrNotFound
	}

	for {
		select {
		case <-ctx.Done():
			return "", views.ErrPending
		case _, ok := <-sub:
			if !ok {
				return "", views.ErrPending
			}
			if e := h.Inventory.find(name); e != nil && h.ready(e) {
				return e.Name, nil
			}
		}
	}
}

func (h *StubReadHandle) ready(e *snapshot.FileEntry) bool {
	if !e.Local {
		return false
	}
	if h.RequireAdvertisable && !e.Advertisable {
		return false
	}
	return true
}

func hasInView(view views.HeldView, name string) bool {
	_, ok := view.Get(name)
	return ok
}
