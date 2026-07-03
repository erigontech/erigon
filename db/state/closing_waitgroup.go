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

package state

import "sync"

// closingWaitGroup is a sync.WaitGroup with a close latch: TryAdd refuses to
// register once BeginClose has run, so an Add from a goroutine the owner did not
// spawn can't race Wait (WaitGroup reuse). Plain Add remains for goroutines
// spawned from an already-registered one, whose Add can't observe a zero counter.
type closingWaitGroup struct {
	sync.WaitGroup
	mu      sync.Mutex
	closing bool
}

// TryAdd registers the caller on the WaitGroup unless BeginClose has run.
func (g *closingWaitGroup) TryAdd() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.WaitGroup.Add(1)
	return true
}

// BeginClose latches the group closed; reports whether this call did the latching.
func (g *closingWaitGroup) BeginClose() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.closing = true
	return true
}
