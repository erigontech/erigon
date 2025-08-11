// Copyright 2024 The Erigon Authors
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
	"bytes"
	"context"
	"crypto/ecdsa"
	"net/http"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/handshake"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

var (
	attnetsTestVal  = [8]byte{1, 5, 6}
	syncnetsTestVal = [1]byte{56}
)

func newkey() *ecdsa.PrivateKey {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("couldn't generate key: " + err.Error())
	}
	return key
}

func testLocalNode() *enode.LocalNode {
	db, err := enode.OpenDB(context.TODO(), "", "", log.Root())
	if err != nil {
		panic(err)
	}
	ln := enode.NewLocalNode(db, newkey(), log.Root())
	ln.Set(enr.WithEntry("attnets", attnetsTestVal))
	ln.Set(enr.WithEntry("syncnets", syncnetsTestVal))
	return ln
}

func TestPing(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/4501"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4503"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoicemock.NewForkChoiceStorageMock(t)
	ethClock := getEthClock(t)

	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		testLocalNode(),
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.PingProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Ping{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)
}

func TestGoodbye(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/4509"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4512"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoicemock.NewForkChoiceStorageMock(t)
	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		testLocalNode(),
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.GoodbyeProtocolV1))
	require.NoError(t, err)

	req := &cltypes.Ping{}
	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Ping{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)
}

func TestMetadataV2(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/2509"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/7510"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoicemock.NewForkChoiceStorageMock(t)
	ethClock := getEthClock(t)
	nc := clparams.NetworkConfigs[chainspec.MainnetChainID]
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&nc,
		testLocalNode(),
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.MetadataProtocolV2))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Metadata{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, attnetsTestVal, p.Attnets)
	require.Equal(t, &syncnetsTestVal, p.Syncnets)
}

func TestMetadataV1(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/4519"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4578"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoicemock.NewForkChoiceStorageMock(t)

	nc := clparams.NetworkConfigs[chainspec.MainnetChainID]
	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&nc,
		testLocalNode(),
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.MetadataProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Metadata{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, attnetsTestVal, p.Attnets)
}

func TestStatus(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/1519"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4518"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoicemock.NewForkChoiceStorageMock(t)

	// Create mock for PeerDasStateReader
	ctrl := gomock.NewController(t)
	mockPeerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	mockPeerDasStateReader.EXPECT().
		GetEarliestAvailableSlot().
		Return(uint64(0)).
		AnyTimes()
	mockPeerDasStateReader.EXPECT().
		GetRealCgc().
		Return(uint64(0)).
		AnyTimes()
	mockPeerDasStateReader.EXPECT().
		GetAdvertisedCgc().
		Return(uint64(0)).
		AnyTimes()

	// Create a simple HTTP handler for the handshake
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simple handler that returns success
		w.WriteHeader(http.StatusOK)
	})

	hs := handshake.New(ctx, getEthClock(t), &clparams.MainnetBeaconConfig, handler, mockPeerDasStateReader)
	s := &cltypes.Status{
		FinalizedRoot:  common.Hash{1, 2, 4},
		HeadRoot:       common.Hash{1, 2, 4},
		FinalizedEpoch: 1,
		HeadSlot:       1,
	}
	hs.SetStatus(s)
	nc := clparams.NetworkConfigs[chainspec.MainnetChainID]
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&nc,
		testLocalNode(),
		beaconCfg,
		getEthClock(t),
		hs, f, nil, nil, mockPeerDasStateReader, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Status{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, s, p)
}
