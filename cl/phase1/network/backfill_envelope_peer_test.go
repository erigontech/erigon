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

package network

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/service"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type envelopeTestP2P struct {
	p2p.P2PManager
	host host.Host
}

func (p *envelopeTestP2P) Host() host.Host                  { return p.host }
func (p *envelopeTestP2P) SetDialPolicy(func(peer.ID) bool) {}

type connectedEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	response []byte
	pid      peer.ID
}

func (s *connectedEnvelopeSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: s.pid.String()}}, nil
}

func (s *connectedEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func TestBackwardBeaconDownloaderBansInvalidEnvelopeWithOnePeer(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	requestEnvelopeBatchExpiration = 200 * time.Millisecond
	t.Cleanup(func() { requestEnvelopeBatchExpiration = previousExpiration })

	local, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	remote, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	require.NoError(t, local.Connect(t.Context(), peer.AddrInfo{ID: remote.ID(), Addrs: remote.Addrs()}))

	cfg := gloasFromGenesisConfig()
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
	node, err := sentinel.New(t.Context(), &sentinel.SentinelConfig{P2PConfig: p2p.P2PConfig{
		NetworkConfig: &clparams.NetworkConfig{},
		BeaconConfig:  cfg,
	}}, clock, nil, nil, nil, log.New(), nil, nil, nil, &envelopeTestP2P{host: local})
	require.NoError(t, err)
	t.Cleanup(node.Stop)
	active, _, _ := node.GetPeersCount()
	require.Equal(t, 1, active)

	_, block, envelope := backfilledGloasEnvelopeFixture(t, cfg, 10, clparams.BuilderIndexSelfBuild, 0, backfillProposerKey)
	envelope.Message.BuilderIndex++
	digest, err := clock.ComputeForkDigest(0)
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, digest[:]...))
	client := rpc.NewBeaconRpcP2P(t.Context(), &connectedEnvelopeSentinel{
		SentinelClient: direct.NewSentinelClientDirect(service.NewSentinelServer(t.Context(), node, log.New())),
		response:       wire.Bytes(),
		pid:            remote.ID(),
	}, cfg, clock)
	client.BanPeer(remote.ID().String())
	require.False(t, node.Peers().BanStatus(remote.ID()))
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))

	downloader := NewBackwardBeaconDownloader(t.Context(), client, nil, nil, nil, cfg)
	t.Cleanup(downloader.reqInterval.Stop)
	envelopes, err := downloader.requestEnvelopes(t.Context(), [][32]byte{envelope.Message.BeaconBlockRoot}, block)
	require.NoError(t, err)
	require.Empty(t, envelopes)
	require.True(t, node.Peers().BanStatus(remote.ID()))
	require.Equal(t, network.NotConnected, local.Network().Connectedness(remote.ID()))
}
