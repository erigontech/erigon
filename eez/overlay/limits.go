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

package overlay

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// gater refuses a secured connection past MaxInbound inbound connections or
// MaxConnsPerPeer connections to its peer.
type gater struct {
	mu sync.RWMutex
	// count returns the established inbound connections and those to p; nil
	// until the host is up, refusing every connection.
	count func(p peer.ID) (inbound, toPeer int)
}

func (g *gater) setCounter(count func(peer.ID) (int, int)) {
	g.mu.Lock()
	g.count = count
	g.mu.Unlock()
}

func counter(n network.Network) func(peer.ID) (int, int) {
	return func(p peer.ID) (inbound, toPeer int) {
		for _, c := range n.Conns() {
			if c.Stat().Direction == network.DirInbound {
				inbound++
			}
		}
		return inbound, len(n.ConnsToPeer(p))
	}
}

func (g *gater) InterceptPeerDial(peer.ID) bool               { return true }
func (g *gater) InterceptAddrDial(peer.ID, ma.Multiaddr) bool { return true }
func (g *gater) InterceptAccept(network.ConnMultiaddrs) bool  { return true }

func (g *gater) InterceptSecured(dir network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	g.mu.RLock()
	count := g.count
	g.mu.RUnlock()
	if count == nil {
		return false
	}
	inbound, toPeer := count(p)
	return toPeer < MaxConnsPerPeer && (dir != network.DirInbound || inbound < MaxInbound)
}

func (g *gater) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }
