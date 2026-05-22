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

package snapshotsync

import (
	"sync"
	"time"
)

// AdoptionGraceGate defers an adoption trigger until a minority verdict
// has persisted for a grace window, so a transient swarm disagreement —
// a brief quorum flap, or this node's own fresh publish not yet observed
// by peers — settles instead of kicking off a network fetch + cutover. A
// Clear within the window cancels the pending trigger.
//
// Concurrency: Arm and Clear may be called from any goroutine; fire runs
// on its own goroutine when the window elapses.
type AdoptionGraceGate struct {
	grace time.Duration
	fire  func(*MinorityVerdict)

	mu      sync.Mutex
	timer   *time.Timer
	pending *MinorityVerdict
}

// NewAdoptionGraceGate returns a gate that calls fire once a verdict has
// persisted for grace. grace <= 0 makes Arm call fire synchronously —
// i.e. adopt on first minority detection, no wait.
func NewAdoptionGraceGate(grace time.Duration, fire func(*MinorityVerdict)) *AdoptionGraceGate {
	return &AdoptionGraceGate{grace: grace, fire: fire}
}

// Arm records a minority verdict. The first Arm starts the grace window
// and returns true; further Arms within the window update the verdict
// fire will receive but keep the original deadline and return false.
// With grace <= 0 Arm fires synchronously and returns false.
func (g *AdoptionGraceGate) Arm(verdict *MinorityVerdict) bool {
	if g.grace <= 0 {
		g.fire(verdict)
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pending = verdict // latest verdict wins
	if g.timer != nil {
		return false // window already running — keep its original deadline
	}
	g.timer = time.AfterFunc(g.grace, func() {
		g.mu.Lock()
		v := g.pending
		g.timer = nil
		g.pending = nil
		g.mu.Unlock()
		if v != nil {
			g.fire(v)
		}
	})
	return true
}

// Clear cancels a pending trigger — the minority condition resolved
// within the grace window. Returns true if a running window was
// cancelled, false if there was nothing pending.
func (g *AdoptionGraceGate) Clear() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.timer == nil {
		return false
	}
	g.timer.Stop()
	g.timer = nil
	g.pending = nil
	return true
}
