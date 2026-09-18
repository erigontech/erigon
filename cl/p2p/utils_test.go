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
	"net"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

const (
	publicNodeChiadoENR       = "enr:-MK4QIw4k7aR8hxCa_3aWTmpaKuPMu8xG_R94Ue-xxwHtsWDU657Sc8ubff_vu51DfIf8NcESC4wnnEuTmzdqqPnGu-CBOWHYXR0bmV0c4gAAAADAAAAAINjZ2MEhGV0aDKQJfkUjgYAAG___________4JpZIJ2NIJpcISkmKNpg25mZIQAAAAAiXNlY3AyNTZrMaEC8I-uGch5hJkoAVCxlOvnwtRQjbN2XWttxP1ZXiFg7sqDdGNwgko4g3VkcIJF1w"
	publicNodeChiadoMultiaddr = "/ip4/164.152.163.105/tcp/19000/p2p/16Uiu2HAmBciu61DBo623TByPbuBaGh9So6hRQfvHpCegCE71JNp9"
)

func TestParseStaticPeerAcceptsENRAndLibp2pMultiaddr(t *testing.T) {
	t.Parallel()

	for _, input := range []string{publicNodeChiadoENR, publicNodeChiadoMultiaddr} {
		parsed, err := ParseStaticPeer(input)
		require.NoError(t, err)
		require.Equal(t, publicNodeChiadoMultiaddr, parsed.String())
	}
}

func TestParseBootstrapNodesClassifiesLibp2pMultiaddrAsDirectPeer(t *testing.T) {
	t.Parallel()

	discoveryNodes, directPeers, unsupportedPeers, err := ParseBootstrapNodes([]string{publicNodeChiadoMultiaddr})
	require.NoError(t, err)
	require.Empty(t, discoveryNodes)
	require.Equal(t, []string{publicNodeChiadoMultiaddr}, directPeers)
	require.Empty(t, unsupportedPeers)
}

func TestParseBootstrapNodesClassifiesMixedInputs(t *testing.T) {
	t.Parallel()

	secondDirectPeer := "/dns4/chiado.example/tcp/9000/p2p/16Uiu2HAmBciu61DBo623TByPbuBaGh9So6hRQfvHpCegCE71JNp9"
	discoveryNodes, directPeers, unsupportedPeers, err := ParseBootstrapNodes([]string{
		publicNodeChiadoMultiaddr,
		publicNodeChiadoENR,
		secondDirectPeer,
	})
	require.NoError(t, err)
	require.Equal(t, []string{publicNodeChiadoENR}, discoveryNodes)
	require.Equal(t, []string{publicNodeChiadoMultiaddr, secondDirectPeer}, directPeers)
	require.Empty(t, unsupportedPeers)
}

func TestParseBootstrapNodesAcceptsEmptyInput(t *testing.T) {
	t.Parallel()

	discoveryNodes, directPeers, unsupportedPeers, err := ParseBootstrapNodes(nil)
	require.NoError(t, err)
	require.Empty(t, discoveryNodes)
	require.Empty(t, directPeers)
	require.Empty(t, unsupportedPeers)
}

func TestParseBootstrapNodesClassifiesQUICMultiaddrAsDirectPeer(t *testing.T) {
	t.Parallel()

	quicPeer := "/ip4/51.68.224.153/udp/9001/quic-v1/p2p/16Uiu2HAkxcBE3LK7zhnyZERguonkKmXLgYPRcuDPaF6C2vaigYuT"
	discoveryNodes, directPeers, unsupportedPeers, err := ParseBootstrapNodes([]string{
		publicNodeChiadoMultiaddr,
		quicPeer,
	})
	require.NoError(t, err)
	require.Empty(t, discoveryNodes)
	require.Equal(t, []string{publicNodeChiadoMultiaddr, quicPeer}, directPeers)
	require.Empty(t, unsupportedPeers)
}

func TestParseBootstrapNodesAcceptsOnlyQUICTransport(t *testing.T) {
	t.Parallel()

	quicPeer := "/ip4/51.68.224.153/udp/9001/quic-v1/p2p/16Uiu2HAkxcBE3LK7zhnyZERguonkKmXLgYPRcuDPaF6C2vaigYuT"
	discoveryNodes, directPeers, unsupportedPeers, err := ParseBootstrapNodes([]string{quicPeer})
	require.NoError(t, err)
	require.Empty(t, discoveryNodes)
	require.Equal(t, []string{quicPeer}, directPeers)
	require.Empty(t, unsupportedPeers)
}

func TestParseBootstrapNodesRejectsMalformedMixedInput(t *testing.T) {
	t.Parallel()

	discoveryNodes, directPeers, unsupportedPeers, err := ParseBootstrapNodes([]string{
		publicNodeChiadoENR,
		"not-a-bootstrap-node",
		publicNodeChiadoMultiaddr,
	})
	require.Error(t, err)
	require.Nil(t, discoveryNodes)
	require.Nil(t, directPeers)
	require.Nil(t, unsupportedPeers)
}

func TestParseStaticPeerRejectsMultiaddrWithoutPeerID(t *testing.T) {
	t.Parallel()

	_, err := ParseStaticPeer("/ip4/192.0.2.1/tcp/9000")
	require.Error(t, err)
}

func TestParseStaticPeerRejectsPeerIDWithoutDialAddress(t *testing.T) {
	t.Parallel()

	_, err := ParseStaticPeer("/p2p/16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA")
	require.Error(t, err)
}

func TestParseStaticPeerRejectsUnsupportedTransports(t *testing.T) {
	t.Parallel()

	peerID := "16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA"
	for _, input := range []string{
		"/ip4/192.0.2.1/p2p/" + peerID,
		"/ip4/192.0.2.1/tcp/9000/ws/p2p/" + peerID,
		"/ip4/192.0.2.1/tcp/9000/p2p/" + peerID + "/p2p-circuit/p2p/" + peerID,
	} {
		_, err := ParseStaticPeer(input)
		require.Error(t, err)
	}
}

func TestParseStaticPeerRejectsNonDialableTCPAddresses(t *testing.T) {
	t.Parallel()

	peerID := "16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA"
	for _, input := range []string{
		"/ip4/192.0.2.1/tcp/0/p2p/" + peerID,
		"/ip4/192.0.2.1/udp/0/quic-v1/p2p/" + peerID,
		"/ip4/0.0.0.0/tcp/9000/p2p/" + peerID,
		"/ip4/0.0.0.0/udp/9001/quic-v1/p2p/" + peerID,
		"/ip6/::/tcp/9000/p2p/" + peerID,
		"/ip6/::/udp/9001/quic-v1/p2p/" + peerID,
	} {
		_, err := ParseStaticPeer(input)
		require.Error(t, err)
	}
}

func TestParseStaticPeerAcceptsSupportedTCPAddresses(t *testing.T) {
	t.Parallel()

	peerID := "16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA"
	for _, input := range []string{
		"/ip6/2001:db8::1/tcp/9000/p2p/" + peerID,
		"/dns4/chiado.example/tcp/9000/p2p/" + peerID,
		"/dns6/chiado.example/tcp/9000/p2p/" + peerID,
	} {
		parsed, err := ParseStaticPeer(input)
		require.NoError(t, err)
		require.Equal(t, input, parsed.String())
	}
}

func TestParseStaticPeerAcceptsSupportedQUICAddresses(t *testing.T) {
	t.Parallel()

	peerID := "16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA"
	for _, input := range []string{
		"/ip4/192.0.2.1/udp/9001/quic-v1/p2p/" + peerID,
		"/ip6/2001:db8::1/udp/9001/quic-v1/p2p/" + peerID,
		"/dns4/chiado.example/udp/9001/quic-v1/p2p/" + peerID,
		"/dns6/chiado.example/udp/9001/quic-v1/p2p/" + peerID,
	} {
		parsed, err := ParseStaticPeer(input)
		require.NoError(t, err)
		require.Equal(t, input, parsed.String())
	}
}

func TestMultiAddressBuilderWithIDPreservesGenericProtocolSupport(t *testing.T) {
	peerID, err := peer.Decode("16Uiu2HAmEG2vHsiGdask9Weg5qVCsxtrezWCde1WArakqSNCY1EA")
	require.NoError(t, err)

	addr, err := MultiAddressBuilderWithID("192.0.2.1", "udp", 9000, peerID)
	require.NoError(t, err)
	require.Equal(t, "/ip4/192.0.2.1/udp/9000/p2p/"+peerID.String(), addr.String())
}

func TestConvertToAddrInfoPrefersQUICAndRetainsTCPFallback(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	var record enr.Record
	record.Set(enr.IP(net.ParseIP("192.0.2.1")))
	record.Set(enr.TCP(9000))
	record.Set(enr.QUIC(9001))
	require.NoError(t, enode.SignV4(&record, key))
	node, err := enode.New(enode.ValidSchemes, &record)
	require.NoError(t, err)

	info, preferred, err := ConvertToAddrInfo(node)
	require.NoError(t, err)
	require.Contains(t, preferred.String(), "/udp/9001/quic-v1/")
	require.Len(t, info.Addrs, 2)
	require.Equal(t, "/ip4/192.0.2.1/udp/9001/quic-v1", info.Addrs[0].String())
	require.Equal(t, "/ip4/192.0.2.1/tcp/9000", info.Addrs[1].String())
}

func TestParseStaticPeerAddrsRetainsENRFallback(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	var record enr.Record
	record.Set(enr.IP(net.ParseIP("192.0.2.1")))
	record.Set(enr.TCP(9000))
	record.Set(enr.QUIC(9001))
	require.NoError(t, enode.SignV4(&record, key))
	node, err := enode.New(enode.ValidSchemes, &record)
	require.NoError(t, err)

	addrs, err := ParseStaticPeerAddrs(node.String())
	require.NoError(t, err)
	require.Len(t, addrs, 2)
	require.Contains(t, addrs[0].String(), "/udp/9001/quic-v1/")
	require.Contains(t, addrs[1].String(), "/tcp/9000/")
}

func TestConvertToSingleMultiAddrAcceptsQUICOnlyNode(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	var record enr.Record
	record.Set(enr.IP(net.ParseIP("192.0.2.1")))
	record.Set(enr.QUIC(9001))
	require.NoError(t, enode.SignV4(&record, key))
	node, err := enode.New(enode.ValidSchemes, &record)
	require.NoError(t, err)

	addr, err := ConvertToSingleMultiAddr(node)
	require.NoError(t, err)
	require.Contains(t, addr.String(), "/udp/9001/quic-v1/")
}

func TestConvertToSingleMultiAddrRejectsNodeWithoutTransportPort(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	noTransport := enode.NewV4(&key.PublicKey, net.ParseIP("192.0.2.2"), 0, 30301)

	_, err = ConvertToSingleMultiAddr(noTransport)
	require.Error(t, err)
}

func TestConvertToMultiAddrSkipsNodesWithoutTransportPort(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	withTcp := enode.NewV4(&key.PublicKey, net.ParseIP("192.0.2.1"), 30303, 30301)
	noTcp := enode.NewV4(&key.PublicKey, net.ParseIP("192.0.2.2"), 0, 30301)

	multiAddrs := ConvertToMultiAddr([]*enode.Node{withTcp, noTcp})

	require.Len(t, multiAddrs, 1)
	require.Contains(t, multiAddrs[0].String(), "/tcp/30303")
}
