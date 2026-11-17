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
	"crypto/rand"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	ecrypto "github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func newListeningHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	return h
}

func connectInbound(t *testing.T, local host.Host) host.Host {
	t.Helper()
	remote := newListeningHost(t)
	require.NoError(t, remote.Connect(t.Context(), peer.AddrInfo{ID: local.ID(), Addrs: local.Addrs()}))
	require.Eventually(t, func() bool {
		return local.Network().Connectedness(remote.ID()) == network.Connected
	}, 5*time.Second, 5*time.Millisecond)
	return remote
}

func connectOutbound(t *testing.T, local host.Host) host.Host {
	t.Helper()
	remote := newListeningHost(t)
	require.NoError(t, local.Connect(t.Context(), peer.AddrInfo{ID: remote.ID(), Addrs: remote.Addrs()}))
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))
	return remote
}

func TestInboundPeerRejectedAtInboundCap(t *testing.T) {
	local := newListeningHost(t)
	s := testSentinel(t, local)
	s.cfg.MaxPeerCount = 10
	validate := func() (bool, error) { return true, nil }

	for range 7 {
		connectInbound(t, local)
	}
	eighth := connectInbound(t, local)
	require.True(t, s.handleNewConnection(eighth.ID(), network.DirInbound, nil, validate))

	ninth := connectInbound(t, local)
	require.False(t, s.handleNewConnection(ninth.ID(), network.DirInbound, nil, validate))
	waitDisconnected(t, local, ninth.ID())
}

func TestOutboundPeerKeptWithinReserveAtPeerCap(t *testing.T) {
	local := newListeningHost(t)
	s := testSentinel(t, local)
	s.cfg.MaxPeerCount = 10
	validate := func() (bool, error) { return true, nil }

	for range 10 {
		connectInbound(t, local)
	}
	first := connectOutbound(t, local)
	require.True(t, s.handleNewConnection(first.ID(), network.DirOutbound, nil, validate))
	second := connectOutbound(t, local)
	require.True(t, s.handleNewConnection(second.ID(), network.DirOutbound, nil, validate))
	third := connectOutbound(t, local)
	require.False(t, s.handleNewConnection(third.ID(), network.DirOutbound, nil, validate))
	waitDisconnected(t, local, third.ID())
}

func TestInboundConnectionFromOutboundPeerKeepsPeerAtInboundCap(t *testing.T) {
	local := newListeningHost(t)
	s := testSentinel(t, local)
	s.cfg.MaxPeerCount = 10
	validate := func() (bool, error) { return true, nil }

	for range 8 {
		connectInbound(t, local)
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	listening, err := libp2p.New(libp2p.Identity(priv), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listening.Close()) })
	require.NoError(t, local.Connect(t.Context(), peer.AddrInfo{ID: listening.ID(), Addrs: listening.Addrs()}))
	require.True(t, s.handleNewConnection(listening.ID(), network.DirOutbound, nil, validate))

	dialing, err := libp2p.New(libp2p.Identity(priv), libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dialing.Close()) })
	require.NoError(t, dialing.Connect(t.Context(), peer.AddrInfo{ID: local.ID(), Addrs: local.Addrs()}))
	require.Eventually(t, func() bool {
		return len(local.Network().ConnsToPeer(listening.ID())) == 2
	}, 5*time.Second, 5*time.Millisecond)

	require.True(t, s.handleNewConnection(listening.ID(), network.DirInbound, nil, validate))
	require.Equal(t, network.Connected, local.Network().Connectedness(listening.ID()))
}

func TestInboundPeerGoneBeforeAdmissionRejectedAtInboundCap(t *testing.T) {
	local := newListeningHost(t)
	s := testSentinel(t, local)
	s.cfg.MaxPeerCount = 10

	for range 8 {
		connectInbound(t, local)
	}
	gone := connectInbound(t, local)
	require.NoError(t, local.Network().ClosePeer(gone.ID()))
	waitDisconnected(t, local, gone.ID())

	require.False(t, s.handleNewConnection(gone.ID(), network.DirInbound, nil, func() (bool, error) {
		t.Error("a peer rejected at the inbound cap must not be handshaked")
		return false, nil
	}))
}

func storeAttnetsENR(t *testing.T, s *Sentinel, pid peer.ID, attnets bitfield.Bitvector64) {
	t.Helper()
	key, err := ecrypto.GenerateKey()
	require.NoError(t, err)
	var record enr.Record
	record.Set(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, attnets))
	require.NoError(t, enode.SignV4(&record, key))
	node, err := enode.New(enode.ValidSchemes, &record)
	require.NoError(t, err)
	s.pidToEnr.Store(pid, node)
}

func TestUnderservedSubnetPeerAdmittedAtInboundCap(t *testing.T) {
	local := newListeningHost(t)
	s := testSentinel(t, local)
	s.cfg.MaxPeerCount = 10
	s.cfg.NetworkConfig = &clparams.NetworkConfig{AttSubnetKey: "attnets"}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	// Without a fork digest every subnet has zero subscribers, so every advertised subnet is underserved.
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{}, errors.New("no fork digest")).AnyTimes()
	s.ethClock = clock
	validate := func() (bool, error) { return true, nil }

	for range 8 {
		connectInbound(t, local)
	}
	helper := connectInbound(t, local)
	storeAttnetsENR(t, s, helper.ID(), bitfield.Bitvector64{0x01, 0, 0, 0, 0, 0, 0, 0})
	require.True(t, s.handleNewConnection(helper.ID(), network.DirInbound, nil, validate))
	require.Equal(t, network.Connected, local.Network().Connectedness(helper.ID()))

	nonHelper := connectInbound(t, local)
	storeAttnetsENR(t, s, nonHelper.ID(), bitfield.NewBitvector64())
	require.False(t, s.handleNewConnection(nonHelper.ID(), network.DirInbound, nil, validate))
	waitDisconnected(t, local, nonHelper.ID())
}

func TestOutboundReserve(t *testing.T) {
	tests := []struct {
		max  int
		want int
	}{
		{max: 0, want: 0},
		{max: 1, want: 1},
		{max: 10, want: 2},
		{max: 11, want: 3},
		{max: 128, want: 26},
		{max: math.MaxInt, want: math.MaxInt/5 + 1},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, outboundReserve(tt.max), "max=%d", tt.max)
	}
}

func TestWantsOutbound(t *testing.T) {
	tests := []struct {
		name   string
		counts peerCounts
		want   bool
	}{
		{name: "empty", want: true},
		{name: "below cap", counts: peerCounts{inbound: 7, outbound: 1}, want: true},
		{name: "at cap without outbound", counts: peerCounts{inbound: 10}, want: true},
		{name: "at cap below reserve", counts: peerCounts{inbound: 9, outbound: 1}, want: true},
		{name: "at cap at reserve", counts: peerCounts{inbound: 8, outbound: 2}, want: false},
		{name: "outbound only at cap", counts: peerCounts{outbound: 10}, want: false},
		{name: "above cap below reserve", counts: peerCounts{inbound: 10, outbound: 1}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, wantsOutbound(tt.counts, 10))
		})
	}
}

func TestCanAcceptInbound(t *testing.T) {
	tests := []struct {
		name   string
		counts peerCounts
		want   bool
	}{
		{name: "empty", want: true},
		{name: "below inbound cap", counts: peerCounts{inbound: 7, outbound: 2}, want: true},
		{name: "at inbound cap", counts: peerCounts{inbound: 8}, want: false},
		{name: "at total cap", counts: peerCounts{inbound: 7, outbound: 3}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, canAcceptInbound(tt.counts, 10))
		})
	}
}

func TestPeerCountToPrune(t *testing.T) {
	tests := []struct {
		name   string
		counts peerCounts
		max    int
		want   int
	}{
		{name: "nothing fires", counts: peerCounts{inbound: 8, outbound: 3}, max: 10},
		{name: "total trigger", counts: peerCounts{inbound: 8, outbound: 4}, max: 10, want: 2},
		{name: "inbound within buffer", counts: peerCounts{inbound: 9, outbound: 1}, max: 10},
		{name: "inbound helper above cap at default max", counts: peerCounts{inbound: 103, outbound: 25}, max: 128},
		{name: "inbound trigger", counts: peerCounts{inbound: 10}, max: 10, want: 2},
		{name: "larger trigger wins", counts: peerCounts{inbound: 11, outbound: 1}, max: 10, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, peerCountToPrune(tt.counts, tt.max))
		})
	}
}

func TestSelectPeersToPrunePicksInboundFirst(t *testing.T) {
	candidates := []pruneCandidate{
		{pid: "outbound", opened: time.Unix(2, 0)},
		{pid: "inbound", inbound: true, opened: time.Unix(1, 0)},
	}

	require.Equal(t, []peer.ID{"inbound"}, selectPeersToPrune(candidates, [attestationSubnetCount]int{}, 1, 0))
}

func TestSelectPeersToPrunePreservesOutboundReserve(t *testing.T) {
	twoOutbound := []pruneCandidate{{pid: "first"}, {pid: "second"}}
	require.Empty(t, selectPeersToPrune(twoOutbound, [attestationSubnetCount]int{}, 1, 2))

	threeOutbound := []pruneCandidate{{pid: "first"}, {pid: "second"}, {pid: "third"}}
	require.Len(t, selectPeersToPrune(threeOutbound, [attestationSubnetCount]int{}, 1, 2), 1)
}

func TestSelectPeersToPruneKeepsCriticalSubnetPeer(t *testing.T) {
	coverage := [attestationSubnetCount]int{1}
	candidates := []pruneCandidate{
		{pid: "critical", inbound: true, subnets: []int{0}},
		{pid: "unsubscribed", inbound: true},
	}

	require.Equal(t, []peer.ID{"unsubscribed"}, selectPeersToPrune(candidates, coverage, 2, 0))
}

func TestSelectPeersToPruneDoesNotEmptySubnet(t *testing.T) {
	coverage := [attestationSubnetCount]int{2}
	candidates := []pruneCandidate{
		{pid: "first", inbound: true, subnets: []int{0}, opened: time.Unix(2, 0)},
		{pid: "second", inbound: true, subnets: []int{0}, opened: time.Unix(1, 0)},
	}

	require.Equal(t, []peer.ID{"first"}, selectPeersToPrune(candidates, coverage, 2, 0))
}

func TestSelectPeersToPrunePrefersFewerSubnetsOverDirection(t *testing.T) {
	coverage := [attestationSubnetCount]int{2}
	candidates := []pruneCandidate{
		{pid: "inboundWithSubnet", inbound: true, subnets: []int{0}, opened: time.Unix(3, 0)},
		{pid: "olderOutbound", opened: time.Unix(1, 0)},
		{pid: "youngerOutbound", opened: time.Unix(2, 0)},
	}

	require.Equal(t, []peer.ID{"youngerOutbound"}, selectPeersToPrune(candidates, coverage, 1, 1))
}

func TestSelectPeersToPruneHandlesHugeCount(t *testing.T) {
	candidates := []pruneCandidate{{pid: "only", inbound: true}}

	require.Equal(t, []peer.ID{"only"}, selectPeersToPrune(candidates, [attestationSubnetCount]int{}, math.MaxInt, 0))
}

func TestSelectPeersToPrunePicksYoungestFirst(t *testing.T) {
	candidates := []pruneCandidate{
		{pid: "older", inbound: true, opened: time.Unix(1, 0)},
		{pid: "younger", inbound: true, opened: time.Unix(2, 0)},
	}

	require.Equal(t, []peer.ID{"younger"}, selectPeersToPrune(candidates, [attestationSubnetCount]int{}, 1, 0))
}
