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
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestParseListen(t *testing.T) {
	a, err := ParseListen(" /ip4/0.0.0.0/tcp/9100 ")
	require.NoError(t, err)
	require.Equal(t, "/ip4/0.0.0.0/tcp/9100", a.String())
	port, err := ListenPort(a)
	require.NoError(t, err)
	require.Equal(t, uint16(9100), port)

	for _, s := range []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/9100", "127.0.0.1:9100", ""} {
		_, err := ParseListen(s)
		require.Error(t, err, s)
	}
}

func TestParsePeers(t *testing.T) {
	id, err := PeerID(&key(1).PublicKey)
	require.NoError(t, err)
	peers, err := ParsePeers("/ip4/10.0.0.1/tcp/9100/p2p/" + id.String() + ", /ip6/::1/tcp/9101/p2p/" + id.String() + ",")
	require.NoError(t, err)
	require.Equal(t, []peer.AddrInfo{
		{ID: id, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/10.0.0.1/tcp/9100")}},
		{ID: id, Addrs: []ma.Multiaddr{ma.StringCast("/ip6/::1/tcp/9101")}},
	}, peers)

	peers, err = ParsePeers("")
	require.NoError(t, err)
	require.Empty(t, peers)

	for _, s := range []string{
		"/ip4/10.0.0.1/tcp/9100",
		"/dns4/seq.example/tcp/9100/p2p/" + id.String(),
		"/ip4/10.0.0.1/tcp/9100/p2p/notapeer",
	} {
		_, err := ParsePeers(s)
		require.Error(t, err, s)
	}
	_, err = ParsePeers("/dns4/seq.example/tcp/9100/p2p/" + id.String())
	require.ErrorIs(t, err, errNoIP)
}
