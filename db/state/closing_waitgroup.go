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

// closingWaitGroup is a sync.WaitGroup with a close latch. A goroutine the owner
// did not spawn must register via TryAdd, which refuses once BeginClose latches;
// this keeps its Add from racing Wait on a zero counter (WaitGroup reuse). The
// WaitGroup is a named field, not embedded, so no bare Add can bypass the latch.
type closingWaitGroup struct {
	wg      sync.WaitGroup
	mu      sync.Mutex
	closing bool
	closed  chan struct{}
}

// TryAdd registers the caller on the group unless BeginClose has latched.
func (g *closingWaitGroup) TryAdd() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.wg.Add(1)
	return true
}

// AddFromRegistered registers a goroutine spawned from one already registered on
// the group. The parent's outstanding count keeps Wait from returning, so this
// Add can't race it and needs no latch check — unlike TryAdd.
func (g *closingWaitGroup) AddFromRegistered() { g.wg.Add(1) }

func (g *closingWaitGroup) Done() { g.wg.Done() }

func (g *closingWaitGroup) Wait() { g.wg.Wait() }

// BeginClose latches the group closed and reports whether this call is the one
// that must run teardown. Callers that lose the latch block until the winner
// calls MarkClosed, so Close blocks until teardown is complete for every caller.
func (g *closingWaitGroup) BeginClose() bool {
	g.mu.Lock()
	if g.closing {
		closed := g.closed
		g.mu.Unlock()
		<-closed
		return false
	}
	g.closing = true
	g.closed = make(chan struct{})
	g.mu.Unlock()
	return true
}

// MarkClosed releases callers blocked in BeginClose; the caller that won the
// latch must call it exactly once, once teardown is complete.
func (g *closingWaitGroup) MarkClosed() { close(g.closed) }
