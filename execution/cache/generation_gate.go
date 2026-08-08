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
	"sync"
	"sync/atomic"
)

// FilesView records the immutable value files relevant to one cache. Equal
// exclusive ends mean that two pinned file views provide compatible latest
// state, even when the physical files were merged or repacked.
type FilesView struct {
	accountsEnd   uint64
	storageEnd    uint64
	codeEnd       uint64
	commitmentEnd uint64
}

func stateFilesView(accountsEnd, storageEnd, codeEnd uint64) FilesView {
	return FilesView{accountsEnd: accountsEnd, storageEnd: storageEnd, codeEnd: codeEnd}
}

// BranchFilesView identifies the files relevant to BranchCache.
func BranchFilesView(commitmentEnd uint64) FilesView {
	return FilesView{commitmentEnd: commitmentEnd}
}

// Generation identifies both parts of a cache snapshot: durable database state
// and the relevant immutable files pinned by its reader.
type Generation struct {
	stateVersion uint64
	files        FilesView
}

// StateGeneration returns a StateCache identity for one pinned transaction.
func StateGeneration(stateVersion, accountsEnd, storageEnd, codeEnd uint64) Generation {
	return Generation{stateVersion: stateVersion, files: stateFilesView(accountsEnd, storageEnd, codeEnd)}
}

// BranchGeneration returns a BranchCache identity for one pinned transaction.
func BranchGeneration(stateVersion, commitmentEnd uint64) Generation {
	return Generation{stateVersion: stateVersion, files: BranchFilesView(commitmentEnd)}
}

// WithStateVersion returns the same files identity at another durable version.
func (g Generation) WithStateVersion(stateVersion uint64) Generation {
	g.stateVersion = stateVersion
	return g
}

// publishedGeneration is immutable after publication. Pointer identity
// prevents a revoked view from becoming valid if the same Generation is
// published again.
type publishedGeneration struct {
	identity Generation
	active   bool
}

// GenerationGate binds lock-free cache reads and serialized fills to one
// durable database state over one compatible files view.
type GenerationGate struct {
	current     atomic.Pointer[publishedGeneration]
	admissionMu sync.RWMutex
	// publicationMu orders durable cache publication with independent changes
	// to the backing-file view. Begin holds it until Publish or Abort.
	publicationMu sync.Mutex
	// files remembers the latest publication even while no durable generation
	// is active, so a later commit cannot restore an older transaction's view.
	files      FilesView
	filesKnown bool
}

// GenerationView is the immutable validity token held by one cache view.
type GenerationView struct {
	gate       *GenerationGate
	generation *publishedGeneration
}

// View returns an inert token unless identity is currently published.
func (g *GenerationGate) View(identity Generation) GenerationView {
	if g == nil {
		return GenerationView{}
	}
	generation := g.current.Load()
	if generation == nil || !generation.active || generation.identity != identity {
		return GenerationView{}
	}
	return GenerationView{gate: g, generation: generation}
}

// Current reports whether the generation is still published.
func (v GenerationView) Current() bool {
	return v.gate != nil && v.generation != nil && v.gate.current.Load() == v.generation
}

// Admit runs fill only if the view remains current while serialized against
// publication. The early check avoids taking the read lock for stale views.
func (v GenerationView) Admit(fill func()) bool {
	if !v.Current() {
		return false
	}
	v.gate.admissionMu.RLock()
	defer v.gate.admissionMu.RUnlock()
	if v.gate.current.Load() != v.generation {
		return false
	}
	fill()
	return true
}

// CurrentStateVersion reports the durable database version of the active
// generation. It returns false before initialization and during publication.
func (g *GenerationGate) CurrentStateVersion() (uint64, bool) {
	if g == nil {
		return 0, false
	}
	generation := g.current.Load()
	if generation == nil || !generation.active {
		return 0, false
	}
	return generation.identity.stateVersion, true
}

// GenerationPublisher is the mutation capability for one generation gate.
type GenerationPublisher struct {
	gate *GenerationGate
}

// Publisher returns a handle that can initialize and publish the gate.
func (g *GenerationGate) Publisher() GenerationPublisher {
	if g == nil {
		return GenerationPublisher{}
	}
	return GenerationPublisher{gate: g}
}

func (p GenerationPublisher) Enabled() bool { return p.gate != nil }

// Initialize binds the gate to identity's state version and the newest files
// view already reported by the backing store. A mismatch clears entries while
// fills are blocked because their origin cannot be proven compatible.
func (p GenerationPublisher) Initialize(identity Generation, clear func()) {
	if p.gate == nil {
		return
	}
	gate := p.gate
	gate.publicationMu.Lock()
	defer gate.publicationMu.Unlock()
	gate.admissionMu.Lock()
	defer gate.admissionMu.Unlock()

	if gate.filesKnown {
		identity.files = gate.files
	} else {
		gate.files = identity.files
		gate.filesKnown = true
	}
	current := gate.current.Load()
	if current != nil && current.active && current.identity == identity {
		return
	}

	gate.current.Store(nil)
	if clear != nil {
		clear()
	}
	gate.current.Store(&publishedGeneration{identity: identity, active: true})
}

// GenerationPublication represents one pending durable transition.
type GenerationPublication struct {
	gate       *GenerationGate
	previous   *publishedGeneration
	transition *publishedGeneration
	files      FilesView
	filesKnown bool
}

// Begin revokes all existing views without changing cache entries. It also
// blocks backing-file changes until Publish or Abort completes the durable
// transition.
func (p GenerationPublisher) Begin() *GenerationPublication {
	if p.gate == nil {
		return nil
	}
	gate := p.gate
	gate.publicationMu.Lock()
	gate.admissionMu.Lock()
	defer gate.admissionMu.Unlock()

	previous := gate.current.Load()
	if previous != nil && !previous.active {
		gate.publicationMu.Unlock()
		panic("cache generation publication already in progress")
	}
	transition := &publishedGeneration{}
	gate.current.Store(transition)
	return &GenerationPublication{
		gate:       gate,
		previous:   previous,
		transition: transition,
		files:      gate.files,
		filesKnown: gate.filesKnown,
	}
}

// StartedFrom reports whether view was the live token revoked by Begin.
func (p *GenerationPublication) StartedFrom(view GenerationView) bool {
	return p != nil &&
		p.gate != nil &&
		p.previous != nil &&
		view.gate == p.gate &&
		view.generation == p.previous
}

// Abort restores the previous generation when no cache entries were changed.
func (p *GenerationPublication) Abort() {
	if p == nil || p.gate == nil {
		return
	}
	gate := p.gate
	gate.admissionMu.Lock()
	defer gate.publicationMu.Unlock()
	defer gate.admissionMu.Unlock()
	if gate.current.Load() != p.transition {
		panic("cache generation publication changed before abort")
	}
	gate.current.Store(p.previous)
	p.gate = nil
}

// Publish applies the committed cache transition before exposing identity.
func (p *GenerationPublication) Publish(identity Generation, apply func()) {
	if p == nil || p.gate == nil {
		return
	}
	gate := p.gate
	gate.admissionMu.Lock()
	defer gate.publicationMu.Unlock()
	defer gate.admissionMu.Unlock()
	if gate.current.Load() != p.transition {
		panic("cache generation publication changed before publish")
	}
	if apply != nil {
		apply()
	}
	if p.filesKnown {
		identity.files = p.files
	} else {
		gate.files = identity.files
		gate.filesKnown = true
	}
	gate.current.Store(&publishedGeneration{identity: identity, active: true})
	p.gate = nil
}

// Reset revokes all views, clears the cache, and leaves it unpublished. The
// next durable publication can start from this empty state.
func (g *GenerationGate) Reset(clear func()) {
	if g == nil {
		return
	}
	g.publicationMu.Lock()
	defer g.publicationMu.Unlock()
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	g.current.Store(nil)
	g.files = FilesView{}
	g.filesKnown = false
	if clear != nil {
		clear()
	}
}

// BackingChange keeps cache publication blocked while a new files view becomes
// visible.
type BackingChange struct {
	gate       *GenerationGate
	transition *publishedGeneration
	next       *publishedGeneration
}

// BeginBackingChange runs reconcile while publications and fills are blocked.
// It always revokes an active generation when its files identity changes, but
// clears entries only when reconcile reports foreign state. The returned
// handle keeps publication blocked until Finish makes both the new files and
// their matching cache generation observable.
func (p GenerationPublisher) BeginBackingChange(files FilesView, reconcile func() bool, clear func()) *BackingChange {
	if p.gate == nil {
		return nil
	}
	gate := p.gate
	gate.publicationMu.Lock()
	gate.admissionMu.Lock()
	keepPublicationLocked := false
	defer func() {
		gate.admissionMu.Unlock()
		if !keepPublicationLocked {
			gate.publicationMu.Unlock()
		}
	}()

	incompatible := reconcile != nil && reconcile()
	current := gate.current.Load()
	if current != nil && !current.active {
		panic("cache generation publication already in progress")
	}
	gate.files = files
	gate.filesKnown = true
	if current != nil && current.identity.files == files && !incompatible {
		return nil
	}
	var transition, next *publishedGeneration
	if current != nil {
		transition = &publishedGeneration{}
		next = &publishedGeneration{
			identity: Generation{stateVersion: current.identity.stateVersion, files: files},
			active:   true,
		}
		gate.current.Store(transition)
	}
	if incompatible && clear != nil {
		clear()
	}
	keepPublicationLocked = true
	return &BackingChange{gate: gate, transition: transition, next: next}
}

// Finish publishes the matching cache identity after the files view is visible.
func (c *BackingChange) Finish() {
	if c == nil || c.gate == nil {
		return
	}
	gate := c.gate
	gate.admissionMu.Lock()
	defer gate.publicationMu.Unlock()
	defer gate.admissionMu.Unlock()
	if c.transition != nil && gate.current.Load() != c.transition {
		panic("cache generation changed during files publication")
	}
	gate.current.Store(c.next)
	c.gate = nil
}

// Close permanently revokes current views. The owner may then close its cache
// storage without admitting new fills.
func (g *GenerationGate) Close() {
	if g == nil {
		return
	}
	g.publicationMu.Lock()
	defer g.publicationMu.Unlock()
	g.admissionMu.Lock()
	g.current.Store(nil)
	g.admissionMu.Unlock()
}
