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

// CanonicalPublisher owns the common lifecycle for a generation-bound cache.
// Cache-specific publishers retain their typed update APIs while delegating
// initialization, publication locking, abort, and failure cleanup here.
type CanonicalPublisher struct {
	generation GenerationPublisher
	clear      func()
}

// NewCanonicalPublisher binds a cache's lifecycle to its generation gate.
func NewCanonicalPublisher(gate *GenerationGate, clear func()) CanonicalPublisher {
	return CanonicalPublisher{generation: gate.Publisher(), clear: clear}
}

func (p CanonicalPublisher) Enabled() bool {
	return p.generation.gate != nil
}

// Initialize binds the cache to generation. A mismatch clears entries because
// their origin cannot be proven compatible with the requested snapshot.
func (p CanonicalPublisher) Initialize(generation Generation) {
	p.generation.Initialize(generation, p.clear)
}

// CanonicalPublication is one pending durable cache transition.
type CanonicalPublication struct {
	generation *GenerationPublication
	clear      func()
}

// Begin revokes current read views without changing entries, allowing Abort to
// restore the previous generation if the database transaction fails.
func (p CanonicalPublisher) Begin() *CanonicalPublication {
	generation := p.generation.Begin()
	if generation == nil {
		return nil
	}
	return &CanonicalPublication{generation: generation, clear: p.clear}
}

// Abort restores the generation revoked by Begin.
func (p *CanonicalPublication) Abort() {
	if p == nil || p.generation == nil {
		return
	}
	generation := p.generation
	p.generation = nil
	generation.Abort()
}

// Publish applies a transition after its database commit and exposes the new
// generation. clear is required when retained entries cannot be proven to
// belong to the new canonical lineage. If apply panics, the cache is cleared
// and remains unpublished.
func (p *CanonicalPublication) Publish(generation Generation, clear bool, apply func(*GenerationPublication)) {
	if p == nil || p.generation == nil {
		return
	}
	publication := p.generation
	defer func() { p.generation = nil }()
	publication.Publish(generation, func() {
		if clear && p.clear != nil {
			p.clear()
		}
		if apply != nil {
			apply(publication)
		}
	}, p.clear)
}
