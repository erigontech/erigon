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

package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/stretchr/testify/require"
)

func TestGaterDialPolicy(t *testing.T) {
	gater, err := NewGater(&P2PConfig{LocalDiscovery: true})
	require.NoError(t, err)

	local, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.ConnectionGater(gater),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })

	remote, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })

	const protocolID = protocol.ID("/erigon/test/gater/1")
	remote.SetStreamHandler(protocolID, func(stream network.Stream) {
		_ = stream.Close()
	})
	gater.SetDialPolicy(func(pid peer.ID) bool { return pid != remote.ID() })

	remoteInfo := peer.AddrInfo{ID: remote.ID(), Addrs: remote.Addrs()}
	err = local.Connect(t.Context(), remoteInfo)
	require.ErrorIs(t, err, swarm.ErrGaterDisallowedConnection)
	require.Equal(t, network.NotConnected, local.Network().Connectedness(remote.ID()))

	localInfo := peer.AddrInfo{ID: local.ID(), Addrs: local.Addrs()}
	require.NoError(t, remote.Connect(t.Context(), localInfo))
	require.Eventually(t, func() bool {
		return local.Network().Connectedness(remote.ID()) == network.Connected
	}, 5*time.Second, 10*time.Millisecond)

	stream, err := local.NewStream(t.Context(), remote.ID(), protocolID)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
}
