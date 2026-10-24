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

package execmodule

import "sync/atomic"

// frozenBlocksGate reports whether a frozen-block pass is safe to insert
// alongside. A pass holds the module semaphore throughout, so anything
// inserting during one is starved for its whole duration.
//
// Level, not edge: an unwind sends the module back through a pass, so
// "quiescent" has to be able to become false again. The signal this replaces
// was a channel closed on the first pass, which could only ever answer once.
type frozenBlocksGate struct {
	completed atomic.Bool
	running   atomic.Bool
}

func (g *frozenBlocksGate) begin() { g.running.Store(true) }

func (g *frozenBlocksGate) end() {
	g.running.Store(false)
	g.completed.Store(true)
}

// quiescent is true once a pass has finished and none is in progress. Before
// the first pass it is false: the frozen tip is not yet readable, which is the
// condition the original one-shot signal existed to cover.
func (g *frozenBlocksGate) quiescent() bool {
	return g.completed.Load() && !g.running.Load()
}
