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

// versionGeneration is immutable after publication. Pointer identity prevents
// a view revoked by one publication from becoming valid when the same
// PlainStateVersion is published again.
type versionGeneration struct {
	stateVersion uint64
	active       bool
}

// PlainStateVersionGate binds lock-free cache reads and serialized fills to one
// durable PlainStateVersion. It controls visibility only; each cache remains
// responsible for storing and applying its own entries.
type PlainStateVersionGate struct {
	current     atomic.Pointer[versionGeneration]
	admissionMu sync.RWMutex
}

// PlainStateVersionView is the immutable validity token held by one cache view.
type PlainStateVersionView struct {
	gate       *PlainStateVersionGate
	generation *versionGeneration
}

// View returns an inert token unless stateVersion is currently published.
func (g *PlainStateVersionGate) View(stateVersion uint64) PlainStateVersionView {
	if g == nil {
		return PlainStateVersionView{}
	}
	generation := g.current.Load()
	if generation == nil || !generation.active || generation.stateVersion != stateVersion {
		return PlainStateVersionView{}
	}
	return PlainStateVersionView{gate: g, generation: generation}
}

// Current reports whether the generation is still published.
func (v PlainStateVersionView) Current() bool {
	return v.gate != nil && v.generation != nil && v.gate.current.Load() == v.generation
}

// Admit runs fill only if the view remains current while serialized against
// publication. The early check avoids taking the read lock for stale views.
func (v PlainStateVersionView) Admit(fill func()) bool {
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

// CurrentStateVersion reports the active durable version. It returns false
// before initialization and while a publication is in progress.
func (g *PlainStateVersionGate) CurrentStateVersion() (uint64, bool) {
	if g == nil {
		return 0, false
	}
	generation := g.current.Load()
	if generation == nil || !generation.active {
		return 0, false
	}
	return generation.stateVersion, true
}

// PlainStateVersionPublisher is the mutation capability for one version gate.
type PlainStateVersionPublisher struct {
	gate *PlainStateVersionGate
}

// Publisher returns a handle that can initialize and publish the gate.
func (g *PlainStateVersionGate) Publisher() PlainStateVersionPublisher {
	if g == nil {
		return PlainStateVersionPublisher{}
	}
	return PlainStateVersionPublisher{gate: g}
}

func (p PlainStateVersionPublisher) Enabled() bool { return p.gate != nil }

// Initialize binds the gate to stateVersion. A version mismatch runs clear
// while all fills are blocked because existing entries have an unknown origin
// relative to the requested database snapshot.
func (p PlainStateVersionPublisher) Initialize(stateVersion uint64, clear func()) {
	if p.gate == nil {
		return
	}
	gate := p.gate
	gate.admissionMu.Lock()
	defer gate.admissionMu.Unlock()

	current := gate.current.Load()
	if current != nil && current.active {
		if current.stateVersion == stateVersion {
			return
		}
	} else if current != nil {
		// The owner already revoked the old version and will publish the
		// transaction's version after its database commit. A concurrent owner
		// cannot initialize from this in-between state, so it stays inert.
		return
	}

	gate.current.Store(&versionGeneration{})
	if clear != nil {
		clear()
	}
	gate.current.Store(&versionGeneration{stateVersion: stateVersion, active: true})
}

// PlainStateVersionPublication represents one pending durable transition.
type PlainStateVersionPublication struct {
	gate       *PlainStateVersionGate
	previous   *versionGeneration
	transition *versionGeneration
}

// Begin revokes all existing views without changing cache entries.
func (p PlainStateVersionPublisher) Begin() *PlainStateVersionPublication {
	if p.gate == nil {
		return nil
	}
	gate := p.gate
	gate.admissionMu.Lock()
	defer gate.admissionMu.Unlock()

	previous := gate.current.Load()
	if previous != nil && !previous.active {
		panic("cache version publication already in progress")
	}
	transition := &versionGeneration{}
	gate.current.Store(transition)
	return &PlainStateVersionPublication{gate: gate, previous: previous, transition: transition}
}

// Abort restores the previous generation when no cache entries were changed.
func (p *PlainStateVersionPublication) Abort() {
	if p == nil || p.gate == nil {
		return
	}
	p.gate.admissionMu.Lock()
	defer p.gate.admissionMu.Unlock()
	if p.gate.current.Load() != p.transition {
		panic("cache version publication changed before abort")
	}
	p.gate.current.Store(p.previous)
	p.gate = nil
}

// Publish applies the committed cache transition before exposing stateVersion.
func (p *PlainStateVersionPublication) Publish(stateVersion uint64, apply func()) {
	if p == nil || p.gate == nil {
		return
	}
	p.gate.admissionMu.Lock()
	defer p.gate.admissionMu.Unlock()
	if p.gate.current.Load() != p.transition {
		panic("cache version publication changed before publish")
	}
	if apply != nil {
		apply()
	}
	p.gate.current.Store(&versionGeneration{stateVersion: stateVersion, active: true})
	p.gate = nil
}

// Reset revokes all views, clears the cache, and leaves it unpublished. The
// next durable publication can start from this empty state.
func (g *PlainStateVersionGate) Reset(clear func()) {
	if g == nil {
		return
	}
	g.admissionMu.Lock()
	defer g.admissionMu.Unlock()
	current := g.current.Load()
	if current != nil && !current.active {
		panic("cannot reset cache during version publication")
	}
	g.current.Store(&versionGeneration{})
	if clear != nil {
		clear()
	}
	g.current.Store(nil)
}

// Close permanently revokes current views. The owner may then close its cache
// storage without admitting new fills.
func (g *PlainStateVersionGate) Close() {
	if g == nil {
		return
	}
	g.admissionMu.Lock()
	g.current.Store(&versionGeneration{})
	g.admissionMu.Unlock()
}
