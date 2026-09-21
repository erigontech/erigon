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
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/p2p/discover"
)

// stubP2P satisfies p2p.P2PManager with only the host the connection handler needs.
type stubP2P struct{ host host.Host }

func (s stubP2P) Pubsub() *pubsub.PubSub                       { return nil }
func (s stubP2P) Host() host.Host                              { return s.host }
func (s stubP2P) BandwidthCounter() *metrics.BandwidthCounter  { return nil }
func (s stubP2P) UDPv5Listener() *discover.UDPv5               { return nil }
func (s stubP2P) UpdateENRAttSubnets(subnetIndex int, on bool) {}
func (s stubP2P) UpdateENRSyncNets(subnetIndex int, on bool)   {}

func testSentinel(t *testing.T, h host.Host) *Sentinel {
	t.Helper()
	return &Sentinel{
		peers: peers.NewPool(h),
		p2p:   stubP2P{host: h},
		cfg:   &SentinelConfig{P2PConfig: p2p.P2PConfig{MaxPeerCount: 100}},
	}
}

// connectedPair returns a host acting as the local node and a peer connected to it.
func connectedPair(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	local, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	remote, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })

	require.NoError(t, local.Connect(t.Context(), peer.AddrInfo{ID: remote.ID(), Addrs: remote.Addrs()}))
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))
	return local, remote
}

func waitDisconnected(t *testing.T, local host.Host, pid peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for local.Network().Connectedness(pid) == network.Connected {
		if time.Now().After(deadline) {
			t.Fatal("peer was still connected: the ban did not close it")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ConnectWithPeer consults the ban list, but it only covers dials we initiate. A peer banned
// for repeated handshake failures reconnects and its connection event must close it rather
// than run another status handshake — otherwise the ban never takes effect on inbound
// connections and the peer is handshaked again on every reconnect.
func TestBannedPeerIsClosedInsteadOfHandshaked(t *testing.T) {
	local, remote := connectedPair(t)
	s := testSentinel(t, local)
	s.peers.SetBanStatus(remote.ID(), true)

	kept := s.handleNewConnection(remote.ID(), func() (bool, error) {
		t.Error("a banned peer must not be handshaked")
		return false, nil
	})
	require.False(t, kept)
	waitDisconnected(t, local, remote.ID())
}

// The connection callback must route through that check: onConnection is what libp2p calls,
// and it is the only path an inbound connection takes.
func TestOnConnectionClosesABannedPeer(t *testing.T) {
	local, remote := connectedPair(t)
	s := testSentinel(t, local)
	s.peers.SetBanStatus(remote.ID(), true)

	conns := local.Network().ConnsToPeer(remote.ID())
	require.NotEmpty(t, conns)
	s.onConnection(local.Network(), conns[0])

	waitDisconnected(t, local, remote.ID())
}

// Three handshake failures ban the peer, and the ban must then be honoured: the storm this
// fixes was one peer handshaked repeatedly because its failures were recorded and never read.
func TestRepeatedHandshakeFailuresStopBeingHandshaked(t *testing.T) {
	local, remote := connectedPair(t)
	s := testSentinel(t, local)

	validations := 0
	failing := func() (bool, error) {
		validations++
		return false, errors.New("stream reset")
	}

	for range 3 {
		require.True(t, s.handleNewConnection(remote.ID(), failing),
			"a transport error keeps the peer: it may still serve gossip")
	}
	require.Equal(t, 3, validations)
	require.True(t, s.peers.BanStatus(remote.ID()), "three handshake failures must ban the peer")

	require.False(t, s.handleNewConnection(remote.ID(), failing))
	require.Equal(t, 3, validations, "a banned peer must not be handshaked again")
	waitDisconnected(t, local, remote.ID())
}

// A peer that is not banned still reaches its handshake and is kept.
func TestUnbannedPeerIsHandshakedAndKept(t *testing.T) {
	local, remote := connectedPair(t)
	s := testSentinel(t, local)

	validations := 0
	require.True(t, s.handleNewConnection(remote.ID(), func() (bool, error) {
		validations++
		return true, nil
	}))
	require.Equal(t, 1, validations)
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))
}
