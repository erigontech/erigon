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
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestConnectionLimits(t *testing.T) {
	g := &gater{}
	require.False(t, g.InterceptSecured(network.DirOutbound, "p", nil), "before the host is up")

	for _, c := range []struct {
		name            string
		dir             network.Direction
		inbound, toPeer int
		allow           bool
	}{
		{"inbound below the cap", network.DirInbound, MaxInbound - 1, 0, true},
		{"inbound at the cap", network.DirInbound, MaxInbound, 0, false},
		{"outbound at the inbound cap", network.DirOutbound, MaxInbound, 0, true},
		{"second connection to a peer", network.DirInbound, 1, MaxConnsPerPeer - 1, true},
		{"third connection to a peer", network.DirOutbound, 0, MaxConnsPerPeer, false},
		{"third inbound from a peer", network.DirInbound, 2, MaxConnsPerPeer, false},
	} {
		g.setCounter(func(peer.ID) (int, int) { return c.inbound, c.toPeer })
		require.Equal(t, c.allow, g.InterceptSecured(c.dir, "p", nil), c.name)
	}
	require.Equal(t, 64, MaxInbound)
	require.Equal(t, 2, MaxConnsPerPeer)
}

func TestResourceManagerPerAddress(t *testing.T) {
	h, err := newHost(key(1), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer h.Close()
	rm := h.Network().ResourceManager()
	// One public address, as followers behind one NAT share, at once.
	for i := range MaxInbound {
		scope, err := rm.OpenConnection(network.DirInbound, true, ma.StringCast(fmt.Sprintf("/ip4/203.0.113.7/tcp/%d", 40000+i)))
		require.NoError(t, err, "connection %d", i)
		t.Cleanup(scope.Done)
		id, err := PeerID(&key(byte(i)).PublicKey)
		require.NoError(t, err)
		require.NoError(t, scope.SetPeer(id), "connection %d", i)
	}
}

func TestConnectionCounter(t *testing.T) {
	a, b := newTestNode(t, 1), newTestNode(t, 2)
	oa := a.start()
	ob := b.start(a)
	require.Eventually(t, func() bool { return ob.Peers() == 1 && oa.Peers() == 1 }, deadline, 50*time.Millisecond)
	inbound, toPeer := counter(oa.host.Network())(ob.ID())
	require.Equal(t, 1, inbound)
	require.Equal(t, 1, toPeer)
	inbound, toPeer = counter(ob.host.Network())(oa.ID())
	require.Equal(t, 0, inbound)
	require.Equal(t, 1, toPeer)
}
