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
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// gater refuses every secured connection until the host is ready. The
// resource manager holds the connection caps: it counts a connection from
// its opening, which a check here of the established ones cannot.
type gater struct{ ready atomic.Bool }

func (g *gater) InterceptPeerDial(peer.ID) bool               { return true }
func (g *gater) InterceptAddrDial(peer.ID, ma.Multiaddr) bool { return true }
func (g *gater) InterceptAccept(network.ConnMultiaddrs) bool  { return true }

func (g *gater) InterceptSecured(network.Direction, peer.ID, network.ConnMultiaddrs) bool {
	return g.ready.Load()
}

func (g *gater) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }
