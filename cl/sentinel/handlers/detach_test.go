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

package handlers

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
)

// startStoppedHandlers serves a handler set, then stops it as a runtime
// teardown does, and returns a peer host to make requests from.
func startStoppedHandlers(t *testing.T) (srv, cli host.Host) {
	t.Helper()
	ctx := context.Background()

	srv, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close() })

	cli, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })

	require.NoError(t, srv.Connect(ctx, peer.AddrInfo{ID: cli.ID(), Addrs: cli.Addrs()}))

	beaconDB, indiciesDB := setupStore(t)
	f := forkchoicemock.NewForkChoiceStorageMock(t)
	f.NewestLCUpdate = &cltypes.LightClientUpdate{
		AttestedHeader:          cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee:       solid.NewSyncCommittee(),
		SignatureSlot:           1234,
		SyncAggregate:           cltypes.NewSyncAggregate(),
		FinalizedHeader:         cltypes.NewLightClientHeader(clparams.AltairVersion),
		FinalityBranch:          solid.NewHashVector(8),
		NextSyncCommitteeBranch: solid.NewHashVector(8),
	}
	f.NewestLCUpdate.AttestedHeader.Beacon.Slot = altairSlot

	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx, beaconDB, indiciesDB, srv,
		&clparams.NetworkConfig{}, testLocalNode(t), beaconCfg, getEthClock(t),
		nil, f, nil, nil, nil, true, nil,
	)
	c.Start()
	c.Stop()
	return srv, cli
}

func requestCode(t *testing.T, srv, cli host.Host, id string, request func(network.Stream)) byte {
	t.Helper()
	stream, err := cli.NewStream(context.Background(), srv.ID(), protocol.ID(id))
	require.NoError(t, err, "the protocol stays advertised, so the peer is not told it is unsupported")
	request(stream)

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	return code[0]
}

func noRequestBody(t *testing.T) func(network.Stream) {
	return func(s network.Stream) {
		_, err := s.Write(nil)
		require.NoError(t, err)
	}
}

func pingRequest(t *testing.T) func(network.Stream) {
	return func(s network.Stream) {
		require.NoError(t, ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{Id: 1}))
		require.NoError(t, s.CloseWrite())
	}
}

// A stopped handler set must neither read the stores its runtime closed nor
// drop the protocol: peers that see a protocol vanish penalise the node.
func TestConsensusHandlers_StopReportsResourceUnavailableForStoreBackedRequests(t *testing.T) {
	srv, cli := startStoppedHandlers(t)
	require.Equal(t, byte(ResourceUnavailablePrefix),
		requestCode(t, srv, cli, communication.LightClientOptimisticUpdateProtocolV1, noRequestBody(t)),
		"a request that would read the runtime is refused, not served from closed stores")
}

// Status, ping and metadata are answered from the sentinel's own state, so a
// peer polling a node between runtimes still gets its last known status.
func TestConsensusHandlers_StopKeepsServingSelfAnsweredRequests(t *testing.T) {
	srv, cli := startStoppedHandlers(t)
	require.Equal(t, byte(SuccessfulResponsePrefix),
		requestCode(t, srv, cli, communication.PingProtocolV1, pingRequest(t)),
		"ping needs no runtime and must keep answering")
}
