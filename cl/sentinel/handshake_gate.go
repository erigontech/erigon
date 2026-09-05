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

// handshakeGate admits one status handshake per peer at a time, so a burst of connection
// events cannot outrun the pool's failure count before it reaches the ban threshold.
type handshakeGate struct {
	mu       sync.Mutex
	inflight map[peer.ID]struct{}
}

func newHandshakeGate() *handshakeGate {
	return &handshakeGate{inflight: make(map[peer.ID]struct{})}
}

// tryAcquire reports whether the caller may handshake this peer. A false return means the
// event should be dropped, not queued: the attempt in flight will judge the peer.
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

// holds reports whether a handshake slot for pid is currently taken.
func (g *handshakeGate) holds(pid peer.ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, busy := g.inflight[pid]
	return busy
}
