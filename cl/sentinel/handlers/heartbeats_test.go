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
	"fmt"
	"net/http"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/handshake"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

var (
	attnetsTestVal  = [8]byte{1, 5, 6}
	syncnetsTestVal = [1]byte{56}
)

type rawSSZ []byte

func (r rawSSZ) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, r...), nil
}

func (r rawSSZ) EncodingSizeSSZ() int {
	return len(r)
}

func newkey() *ecdsa.PrivateKey {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("couldn't generate key: " + err.Error())
	}
	return key
}

func testLocalNode(t *testing.T) *enode.LocalNode {
	db, err := enode.OpenDBEx(context.TODO(), "", "", log.Root())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ln := enode.NewLocalNode(db, newkey())
	ln.Set(enr.WithEntry("attnets", attnetsTestVal))
	ln.Set(enr.WithEntry("syncnets", syncnetsTestVal))
	return ln
}

func newPingTestStream(t *testing.T) network.Stream {
	t.Helper()
	ctx := t.Context()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	beaconDB, indiciesDB := setupStore(t)
	f := forkchoicemock.NewForkChoiceStorageMock(t)
	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peers.NewPool(host),
		&clparams.NetworkConfig{},
		testLocalNode(t),
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.PingProtocolV1))
	require.NoError(t, err)
	return stream
}

func requireResponseCode(t *testing.T, stream network.Stream, expected byte) {
	t.Helper()
	responseCode := make([]byte, 1)
	_, err := stream.Read(responseCode)
	require.NoError(t, err)
	require.Equal(t, expected, responseCode[0])
}

func TestPing(t *testing.T) {
	stream := newPingTestStream(t)

	err := ssz_snappy.EncodeAndWrite(stream, &cltypes.Ping{Id: 1})
	require.NoError(t, err)
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(SuccessfulResponsePrefix))

	p := &cltypes.Ping{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)
}

func TestPingRejectsEmptyRequest(t *testing.T) {
	stream := newPingTestStream(t)
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestPingRejectsTruncatedRequest(t *testing.T) {
	stream := newPingTestStream(t)
	require.NoError(t, ssz_snappy.EncodeAndWrite(stream, rawSSZ(make([]byte, 7))))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestPingRejectsOversizedRequest(t *testing.T) {
	stream := newPingTestStream(t)
	require.NoError(t, ssz_snappy.EncodeAndWrite(stream, rawSSZ(make([]byte, 9))))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestPingRejectsTrailingBytes(t *testing.T) {
	stream := newPingTestStream(t)
	var request bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&request, &cltypes.Ping{Id: 1}))
	require.NoError(t, request.WriteByte(0))
	_, err := stream.Write(request.Bytes())
	require.NoError(t, err)
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestGoodbye(t *testing.T) {
	ctx := context.Background()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool(host)
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
		testLocalNode(t),
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

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool(host)
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
		testLocalNode(t),
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

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool(host)
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
		testLocalNode(t),
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

func newStatusTestStream(t *testing.T, protocolID protocol.ID) (network.Stream, *cltypes.Status) {
	t.Helper()
	ctx := t.Context()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	beaconDB, indiciesDB := setupStore(t)
	f := forkchoicemock.NewForkChoiceStorageMock(t)
	ctrl := gomock.NewController(t)
	peerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	peerDasStateReader.EXPECT().GetEarliestAvailableSlot().Return(uint64(0)).AnyTimes()
	peerDasStateReader.EXPECT().GetRealCgc().Return(uint64(0)).AnyTimes()
	peerDasStateReader.EXPECT().GetAdvertisedCgc().Return(uint64(0)).AnyTimes()

	ethClock := getEthClock(t)
	hs := handshake.New(ctx, ethClock, &clparams.MainnetBeaconConfig, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), peerDasStateReader)
	forkDigest, err := ethClock.CurrentForkDigest()
	require.NoError(t, err)
	status := &cltypes.Status{
		ForkDigest:     forkDigest,
		FinalizedRoot:  common.Hash{1, 2, 4},
		HeadRoot:       common.Hash{1, 2, 4},
		FinalizedEpoch: 1,
		HeadSlot:       1,
	}
	hs.SetStatus(status)
	nc := clparams.NetworkConfigs[chainspec.MainnetChainID]
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peers.NewPool(host),
		&nc,
		testLocalNode(t),
		beaconCfg,
		ethClock,
		hs, f, nil, nil, peerDasStateReader, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocolID)
	require.NoError(t, err)
	return stream, status
}

func TestStatus(t *testing.T) {
	stream, expectedStatus := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV1))

	// Send a Status request body (per eth2 spec the requester sends its own Status).
	reqStatus := &cltypes.Status{
		FinalizedRoot:  common.Hash{9, 8, 7},
		HeadRoot:       common.Hash{9, 8, 7},
		FinalizedEpoch: 2,
		HeadSlot:       2,
	}
	err := ssz_snappy.EncodeAndWrite(stream, reqStatus)
	require.NoError(t, err)
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(SuccessfulResponsePrefix))

	p := &cltypes.Status{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, expectedStatus, p)
}

func TestStatusRejectsEmptyRequest(t *testing.T) {
	stream, _ := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestStatusRejectsTruncatedRequest(t *testing.T) {
	stream, _ := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, ssz_snappy.EncodeAndWrite(stream, rawSSZ(make([]byte, 83))))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestStatusRejectsOversizedRequest(t *testing.T) {
	stream, _ := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, ssz_snappy.EncodeAndWrite(stream, rawSSZ(make([]byte, 85))))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestStatusV2(t *testing.T) {
	stream, expectedStatus := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV2))
	earliestAvailableSlot := uint64(9)
	requestStatus := &cltypes.Status{
		FinalizedRoot:         common.Hash{9, 8, 7},
		HeadRoot:              common.Hash{9, 8, 7},
		FinalizedEpoch:        2,
		HeadSlot:              2,
		EarliestAvailableSlot: &earliestAvailableSlot,
	}
	require.NoError(t, ssz_snappy.EncodeAndWrite(stream, requestStatus))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(SuccessfulResponsePrefix))

	responseStatus := &cltypes.Status{}
	require.NoError(t, ssz_snappy.DecodeAndReadNoForkDigest(stream, responseStatus, clparams.FuluVersion))
	earliestAvailableSlot = 0
	expectedStatus.EarliestAvailableSlot = &earliestAvailableSlot
	require.Equal(t, expectedStatus, responseStatus)
}

func TestStatusV2RejectsEmptyRequest(t *testing.T) {
	stream, _ := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV2))
	require.NoError(t, stream.CloseWrite())

	requireResponseCode(t, stream, byte(InvalidRequestPrefix))
}

func TestStatusV2RejectsInvalidRequestSize(t *testing.T) {
	for _, size := range []int{91, 93} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			stream, _ := newStatusTestStream(t, protocol.ID(communication.StatusProtocolV2))
			require.NoError(t, ssz_snappy.EncodeAndWrite(stream, rawSSZ(make([]byte, size))))
			require.NoError(t, stream.CloseWrite())

			requireResponseCode(t, stream, byte(InvalidRequestPrefix))
		})
	}
}
