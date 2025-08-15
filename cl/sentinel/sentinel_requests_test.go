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

package sentinel

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/antiquary"
	antiquarytests "github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func loadChain(t *testing.T) (db kv.RwDB, blocks []*cltypes.SignedBeaconBlock, f afero.Fs, preState, postState *state.CachingBeaconState, reader *antiquarytests.MockBlockReader) {
	blocks, preState, postState = antiquarytests.GetPhase0Random()
	db = memdb.NewTestDB(t, kv.ChainDB)
	reader = antiquarytests.LoadChain(blocks, postState, db, t)

	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	sn.OnHeadState(postState)

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New("/tmp"), nil, db, nil, nil, reader, sn, log.New(), true, true, false, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	return
}

func TestSentinelBlocksByRange(t *testing.T) {
	listenAddrHost := "127.0.0.1"

	ethClock := getEthClock(t)
	ctx := context.Background()
	db, blocks, _, _, _, reader := loadChain(t)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)

	// Create mock PeerDasStateReader
	ctrl := gomock.NewController(t)
	mockPeerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	mockPeerDasStateReader.EXPECT().GetEarliestAvailableSlot().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetRealCgc().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetAdvertisedCgc().Return(uint64(0)).AnyTimes()

	sentinel, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
		MaxPeerCount:  8883,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, mockPeerDasStateReader)
	require.NoError(t, err)
	defer sentinel.Stop()

	_, err = sentinel.Start()
	require.NoError(t, err)
	h := sentinel.host

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/3202"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
	require.NoError(t, err)

	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: blocks[0].Block.Slot,
		Count:     6,
	}

	if err := ssz_snappy.EncodeAndWrite(stream, req); err != nil {
		return
	}

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	require.Equal(t, uint8(0), code[0])

	var w bytes.Buffer
	_, err = io.Copy(&w, stream)
	require.NoError(t, err)

	responsePacket := make([]*cltypes.SignedBeaconBlock, 0)

	r := bytes.NewReader(w.Bytes())
	for i := 0; i < len(blocks); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		require.NoError(t, err)

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		require.NoError(t, err)

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		require.NoError(t, err)

		responseChunk := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)

		require.NoError(t, responseChunk.DecodeSSZ(raw, int(version)))

		responsePacket = append(responsePacket, responseChunk)
		// TODO(issues/5884): figure out why there is this extra byte.
		r.ReadByte()
	}
	require.Len(t, blocks, len(responsePacket))
	for i := 0; i < len(blocks); i++ {
		root1, err := responsePacket[i].HashSSZ()
		require.NoError(t, err)

		root2, err := blocks[i].HashSSZ()
		require.NoError(t, err)

		require.Equal(t, root1, root2)
	}
}

func TestSentinelBlocksByRoots(t *testing.T) {
	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, blocks, _, _, _, reader := loadChain(t)
	ethClock := getEthClock(t)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)

	// Create mock PeerDasStateReader
	ctrl := gomock.NewController(t)
	mockPeerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	mockPeerDasStateReader.EXPECT().GetEarliestAvailableSlot().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetRealCgc().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetAdvertisedCgc().Return(uint64(0)).AnyTimes()

	sentinel, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
		MaxPeerCount:  8883,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, mockPeerDasStateReader)
	require.NoError(t, err)
	defer sentinel.Stop()

	_, err = sentinel.Start()
	require.NoError(t, err)
	h := sentinel.host

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/5021"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.BeaconBlocksByRootProtocolV2))
	require.NoError(t, err)

	req := solid.NewHashList(1232)
	rt, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)

	req.Append(rt)
	rt, err = blocks[1].Block.HashSSZ()
	require.NoError(t, err)
	req.Append(rt)

	if err := ssz_snappy.EncodeAndWrite(stream, req); err != nil {
		return
	}

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	require.Equal(t, uint8(0), code[0])

	var w bytes.Buffer
	_, err = io.Copy(&w, stream)
	require.NoError(t, err)

	responsePacket := make([]*cltypes.SignedBeaconBlock, 0)

	r := bytes.NewReader(w.Bytes())
	for i := 0; i < len(blocks); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		require.NoError(t, err)

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		require.NoError(t, err)

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		require.NoError(t, err)

		responseChunk := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)

		require.NoError(t, responseChunk.DecodeSSZ(raw, int(version)))

		responsePacket = append(responsePacket, responseChunk)
		// TODO(issues/5884): figure out why there is this extra byte.
		r.ReadByte()
	}

	require.Len(t, blocks, len(responsePacket))
	for i := 0; i < len(responsePacket); i++ {
		root1, err := responsePacket[i].HashSSZ()
		require.NoError(t, err)

		root2, err := blocks[i].HashSSZ()
		require.NoError(t, err)

		require.Equal(t, root1, root2)
	}
}

func TestSentinelStatusRequest(t *testing.T) {
	t.Skip("TODO: fix me")
	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, blocks, _, _, _, reader := loadChain(t)
	ethClock := getEthClock(t)
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)

	// Create mock PeerDasStateReader
	ctrl := gomock.NewController(t)
	mockPeerDasStateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	mockPeerDasStateReader.EXPECT().GetEarliestAvailableSlot().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetRealCgc().Return(uint64(0)).AnyTimes()
	mockPeerDasStateReader.EXPECT().GetAdvertisedCgc().Return(uint64(0)).AnyTimes()

	sentinel, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
		MaxPeerCount:  8883,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, mockPeerDasStateReader)
	require.NoError(t, err)
	defer sentinel.Stop()

	_, err = sentinel.Start()
	require.NoError(t, err)
	h := sentinel.host

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/5001"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)
	req := &cltypes.Status{
		HeadRoot: blocks[0].Block.ParentRoot,
		HeadSlot: 1234,
	}
	sentinel.SetStatus(req)
	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, err)

	if err := ssz_snappy.EncodeAndWrite(stream, req); err != nil {
		return
	}

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	require.Equal(t, uint8(0), code[0])

	resp := &cltypes.Status{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(stream, resp, 0); err != nil {
		return
	}
	require.NoError(t, err)

	require.Equal(t, resp, req)
}
