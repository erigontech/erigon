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

package concurrent

import "sync"

// ClosingWaitGroup is a sync.WaitGroup with a close latch. Every goroutine
// registers via TryAdd, which refuses once BeginClose has latched, so an Add
// can't race Wait on a zero counter (WaitGroup reuse). The WaitGroup is a named
// field, not embedded, so no bare Add can bypass the latch.
type ClosingWaitGroup struct {
	wg      sync.WaitGroup
	mu      sync.Mutex
	closing bool
}

// TryAdd registers the caller on the group unless BeginClose has latched.
func (g *ClosingWaitGroup) TryAdd() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.wg.Add(1)
	return true
}

func (g *ClosingWaitGroup) Done() { g.wg.Done() }

func (g *ClosingWaitGroup) Wait() { g.wg.Wait() }

// BeginClose latches the group closed, returning false if it was already latched
// so Close is idempotent. Latching before Close's Wait makes every TryAdd either
// register before Wait or get refused.
func (g *ClosingWaitGroup) BeginClose() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.closing = true
	return true
}
