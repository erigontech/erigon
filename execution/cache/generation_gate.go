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

func (v FilesView) lowerThan(previous FilesView) bool {
	return v.accountsEnd < previous.accountsEnd ||
		v.storageEnd < previous.storageEnd ||
		v.codeEnd < previous.codeEnd ||
		v.commitmentEnd < previous.commitmentEnd
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
}

// GenerationGate binds lock-free cache reads and serialized fills to one
// durable database state over one compatible files view.
type GenerationGate struct {
	// current is nil until initialization and while publication is in progress.
	current atomic.Pointer[publishedGeneration]
	// resetLineage invalidates publisher handles captured before a full reset.
	// Publisher reads it without publicationMu, so the counter must be atomic.
	resetLineage atomic.Uint64
	admissionMu  sync.RWMutex
	// publicationMu orders durable cache publication with independent changes
	// to the backing-file view. Begin holds it until Publish or Abort.
	publicationMu sync.Mutex
	// closed is permanent and protected by publicationMu. Publisher operations
	// become inert after Close, including through handles created beforehand.
	closed bool
	// files remembers the latest publication even while no durable generation
	// is active, so a later commit cannot restore an older transaction's view.
	files      FilesView
	filesKnown bool
}

// GenerationView is the immutable validity token held by one cache view. View
// constructs it with both fields set or returns the inert zero value.
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
	if generation == nil || generation.identity != identity {
		return GenerationView{}
	}
	return GenerationView{gate: g, generation: generation}
}

// Current reports whether the generation is still published.
func (v GenerationView) Current() bool {
	return v.gate != nil && v.generation != nil && v.gate.current.Load() == v.generation
}

// ReadCurrent returns a cache lookup only if the view remains published for
// the whole read. A concurrent publication therefore turns the result into a
// miss instead of exposing an entry from a mixed generation.
func ReadCurrent[T any](view GenerationView, read func() (T, bool)) (T, bool) {
	var zero T
	if view.gate == nil || view.gate.current.Load() != view.generation {
		return zero, false
	}
	value, ok := read()
	if view.gate.current.Load() != view.generation {
		return zero, false
	}
	return value, ok
}

// ReadCurrentWithStep applies the same guard to a lookup that also returns
// source metadata such as a state step.
func ReadCurrentWithStep[T any](view GenerationView, read func() (T, uint64, bool)) (T, uint64, bool) {
	var zeroValue T
	if view.gate == nil || view.gate.current.Load() != view.generation {
		return zeroValue, 0, false
	}
	value, step, ok := read()
	if view.gate.current.Load() != view.generation {
		return zeroValue, 0, false
	}
	return value, step, ok
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

// GenerationPublisher is the mutation capability for one reset lineage.
type GenerationPublisher struct {
	gate         *GenerationGate
	resetLineage uint64
}

// Publisher returns a handle bound to the current reset lineage. Reset makes
// existing handles inert so older work cannot re-establish a cleared generation.
func (g *GenerationGate) Publisher() GenerationPublisher {
	if g == nil {
		return GenerationPublisher{}
	}
	return GenerationPublisher{gate: g, resetLineage: g.resetLineage.Load()}
}

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
	if gate.closed || p.resetLineage != gate.resetLineage.Load() {
		return
	}
	gate.admissionMu.Lock()
	defer gate.admissionMu.Unlock()

	if gate.filesKnown {
		identity.files = gate.files
	} else {
		gate.files = identity.files
		gate.filesKnown = true
	}
	current := gate.current.Load()
	if current != nil && current.identity == identity {
		return
	}

	gate.current.Store(nil)
	if clear != nil {
		clear()
	}
	gate.current.Store(&publishedGeneration{identity: identity})
}

// GenerationPublication represents one pending durable transition.
type GenerationPublication struct {
	gate     *GenerationGate
	previous *publishedGeneration
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
	if gate.closed || p.resetLineage != gate.resetLineage.Load() {
		gate.publicationMu.Unlock()
		return nil
	}
	gate.admissionMu.Lock()
	defer gate.admissionMu.Unlock()

	previous := gate.current.Load()
	gate.current.Store(nil)
	return &GenerationPublication{
		gate:     gate,
		previous: previous,
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
	defer func() { p.gate = nil }()
	gate.admissionMu.Lock()
	defer gate.publicationMu.Unlock()
	defer gate.admissionMu.Unlock()
	if gate.current.Load() != nil {
		panic("cache generation publication changed before abort")
	}
	gate.current.Store(p.previous)
}

// Publish applies the committed cache transition before exposing identity. The
// state version comes from identity, while the files view is replaced by the
// newest backing view known to the gate. If publication fails, clear runs while
// fills remain blocked and the gate stays unpublished.
func (p *GenerationPublication) Publish(identity Generation, apply, clear func()) {
	if p == nil || p.gate == nil {
		return
	}
	gate := p.gate
	defer func() { p.gate = nil }()
	gate.admissionMu.Lock()
	defer gate.publicationMu.Unlock()
	defer gate.admissionMu.Unlock()
	if gate.current.Load() != nil {
		panic("cache generation publication changed before publish")
	}
	// Database commits are serialized, but their post-commit cache publications
	// may arrive out of order. Keep a newer generation instead of applying an
	// older transaction's partial update set to it.
	if p.previous != nil && identity.stateVersion < p.previous.identity.stateVersion {
		gate.current.Store(p.previous)
		return
	}
	completed := false
	defer func() {
		if !completed && clear != nil {
			clear()
		}
	}()
	if apply != nil {
		apply()
	}
	if gate.filesKnown {
		identity.files = gate.files
	} else {
		gate.files = identity.files
		gate.filesKnown = true
	}
	gate.current.Store(&publishedGeneration{identity: identity})
	completed = true
}

// Reset revokes all views, clears the cache, and leaves it unpublished. It also
// invalidates existing publisher handles; only a handle acquired afterwards can
// establish the next durable generation.
func (g *GenerationGate) Reset(clear func()) {
	if g == nil {
		return
	}
	g.publicationMu.Lock()
	defer g.publicationMu.Unlock()
	if g.closed {
		return
	}
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	g.resetLineage.Add(1)
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
	gate *GenerationGate
	next *publishedGeneration
}

// BeginBackingChange runs reconcile while publications and fills are blocked.
// It revokes the active generation when the files identity changes and clears
// entries when reconcile reports incompatibility. The callback receives whether
// a file end moved backwards, which also requires resetting forward provenance.
// Finish publishes the matching generation after the new files become visible.
func (p GenerationPublisher) BeginBackingChange(files FilesView, reconcile func(lowered bool) bool, clear func()) *BackingChange {
	if p.gate == nil {
		return nil
	}
	gate := p.gate
	gate.publicationMu.Lock()
	if gate.closed || p.resetLineage != gate.resetLineage.Load() {
		gate.publicationMu.Unlock()
		return nil
	}
	gate.admissionMu.Lock()
	keepPublicationLocked := false
	defer func() {
		gate.admissionMu.Unlock()
		if !keepPublicationLocked {
			gate.publicationMu.Unlock()
		}
	}()

	lowered := gate.filesKnown && files.lowerThan(gate.files)
	incompatible := reconcile != nil && reconcile(lowered)
	current := gate.current.Load()
	gate.files = files
	gate.filesKnown = true
	if current != nil && current.identity.files == files && !incompatible {
		return nil
	}
	var next *publishedGeneration
	if current != nil {
		next = &publishedGeneration{
			identity: Generation{stateVersion: current.identity.stateVersion, files: files},
		}
		gate.current.Store(nil)
	}
	if incompatible && clear != nil {
		clear()
	}
	keepPublicationLocked = true
	return &BackingChange{gate: gate, next: next}
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
	if gate.current.Load() != nil {
		panic("cache generation changed during files publication")
	}
	gate.current.Store(c.next)
	c.gate = nil
}

// Close permanently revokes publication and runs clear while publications and
// fills remain blocked. It reports whether this call closed the gate.
func (g *GenerationGate) Close(clear func()) bool {
	if g == nil {
		return false
	}
	g.publicationMu.Lock()
	defer g.publicationMu.Unlock()
	if g.closed {
		return false
	}
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	g.closed = true
	g.current.Store(nil)
	if clear != nil {
		clear()
	}
	return true
}
