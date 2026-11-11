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

	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

// TestGaterReady: the gater refuses every connection until the host is
// ready.
func TestGaterReady(t *testing.T) {
	g := &gater{}
	for _, dir := range []network.Direction{network.DirInbound, network.DirOutbound} {
		require.False(t, g.InterceptSecured(dir, "p", nil), "before the host is up")
	}
	g.ready.Store(true)
	for _, dir := range []network.Direction{network.DirInbound, network.DirOutbound} {
		require.True(t, g.InterceptSecured(dir, "p", nil), "once the host is up")
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

// testAddr is the address of the ith of distinct remote hosts.
func testAddr(i int) ma.Multiaddr {
	return ma.StringCast(fmt.Sprintf("/ip4/198.51.100.%d/tcp/40000", i+1))
}

// TestResourceManagerInboundCap: the resource manager holds MaxInbound,
// counting an inbound connection from its opening, before its handshake
// completes, until it is done, so concurrent handshakes cannot pass the cap
// together.
func TestResourceManagerInboundCap(t *testing.T) {
	h, err := newHost(key(1), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer h.Close()
	rm := h.Network().ResourceManager()

	// Every inbound connection pending.
	inbound := make([]network.ConnManagementScope, MaxInbound)
	for i := range inbound {
		inbound[i], err = rm.OpenConnection(network.DirInbound, true, testAddr(i))
		require.NoError(t, err, "inbound connection %d", i)
	}
	_, err = rm.OpenConnection(network.DirInbound, true, testAddr(MaxInbound))
	require.ErrorIs(t, err, network.ErrResourceLimitExceeded, "pending inbound connections past MaxInbound")

	// Half of them handshaken.
	for i := 0; i < len(inbound); i += 2 {
		id, err := PeerID(&key(byte(i + 2)).PublicKey)
		require.NoError(t, err)
		require.NoError(t, inbound[i].SetPeer(id), "inbound connection %d", i)
	}
	_, err = rm.OpenConnection(network.DirInbound, true, testAddr(MaxInbound))
	require.ErrorIs(t, err, network.ErrResourceLimitExceeded, "inbound connections past MaxInbound")
	require.ErrorContains(t, err, "system")
	out, err := rm.OpenConnection(network.DirOutbound, true, testAddr(MaxInbound))
	require.NoError(t, err, "outbound connection at the inbound cap")
	out.Done()
	inbound[1].Done()
	again, err := rm.OpenConnection(network.DirInbound, true, testAddr(MaxInbound))
	require.NoError(t, err, "inbound connection after a pending one is done")
	again.Done()
	for i, s := range inbound {
		if i != 1 {
			s.Done()
		}
	}
}

// TestConnCaps: the caps hold whatever the memory and file descriptors the
// default limits scale with.
func TestConnCaps(t *testing.T) {
	for _, base := range []rcmgr.ConcreteLimitConfig{rcmgr.DefaultLimits.Scale(0, 0), rcmgr.DefaultLimits.Scale(64<<30, 1<<16)} {
		limits := connCaps(base).ToPartialLimitConfig()
		require.Equal(t, rcmgr.LimitVal(MaxInbound), limits.System.ConnsInbound)
		require.Equal(t, rcmgr.LimitVal(MaxInbound), limits.Transient.ConnsInbound)
		require.Equal(t, rcmgr.LimitVal(MaxConnsPerPeer), limits.PeerDefault.Conns)
	}
}

// TestResourceManagerPeerCap: the resource manager holds MaxConnsPerPeer
// over the connections to one peer, whatever their direction, counting each
// from its handshake until it is done.
func TestResourceManagerPeerCap(t *testing.T) {
	h, err := newHost(key(1), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer h.Close()
	rm := h.Network().ResourceManager()

	p, err := PeerID(&key(200).PublicKey)
	require.NoError(t, err)
	toPeer := make([]network.ConnManagementScope, MaxConnsPerPeer)
	for i := range toPeer {
		dir := network.DirInbound
		if i%2 == 1 {
			dir = network.DirOutbound
		}
		toPeer[i], err = rm.OpenConnection(dir, true, testAddr(i))
		require.NoError(t, err)
		require.NoError(t, toPeer[i].SetPeer(p), "connection %d to the peer", i)
	}
	extra, err := rm.OpenConnection(network.DirOutbound, true, testAddr(MaxConnsPerPeer))
	require.NoError(t, err)
	err = extra.SetPeer(p)
	require.ErrorIs(t, err, network.ErrResourceLimitExceeded, "connection to the peer past MaxConnsPerPeer")
	require.ErrorContains(t, err, "peer:")
	extra.Done()
	toPeer[0].Done()
	extra, err = rm.OpenConnection(network.DirOutbound, true, testAddr(MaxConnsPerPeer))
	require.NoError(t, err)
	require.NoError(t, extra.SetPeer(p), "connection to the peer after one is done")
	extra.Done()
	toPeer[1].Done()
}
