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

package sentinel

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// handshakeGate admits one status handshake per peer at a time. Connection events are
// handled on their own goroutine, so without this a burst of events for one peer starts a
// handshake for each, and the three-strike ban cannot intervene because every attempt
// completes before any of them raises the failure count to its threshold.
type handshakeGate struct {
	mu       sync.Mutex
	inflight map[peer.ID]struct{}
}

func newHandshakeGate() *handshakeGate {
	return &handshakeGate{inflight: make(map[peer.ID]struct{})}
}

// tryAcquire reports whether the caller may handshake this peer. A false return means
// another attempt is already running and this event should be dropped, not queued: the
// peer is about to be judged by the attempt already in flight.
func (g *handshakeGate) tryAcquire(pid peer.ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, busy := g.inflight[pid]; busy {
		return false
	}
	g.inflight[pid] = struct{}{}
	return true
}

func (g *handshakeGate) release(pid peer.ID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.inflight, pid)
}

func (g *handshakeGate) inFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.inflight)
}
