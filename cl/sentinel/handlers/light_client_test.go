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
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
)

var (
	altairSlot = clparams.MainnetBeaconConfig.AltairForkEpoch*clparams.MainnetBeaconConfig.SlotsPerEpoch + 1
)

func TestLightClientOptimistic(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6011"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6013"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := mock_services.NewForkChoiceStorageMock(t)

	f.NewestLCUpdate = &cltypes.LightClientUpdate{
		AttestedHeader:    cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee: &solid.SyncCommittee{},
		SignatureSlot:     1234,
		SyncAggregate:     &cltypes.SyncAggregate{},
		FinalizedHeader:   cltypes.NewLightClientHeader(clparams.AltairVersion),
		// 8 is fine as this is a test
		FinalityBranch:          solid.NewHashVector(8),
		NextSyncCommitteeBranch: solid.NewHashVector(8),
	}
	f.NewestLCUpdate.AttestedHeader.Beacon.Slot = altairSlot

	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.LightClientOptimisticUpdateProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	optimistic := &cltypes.LightClientOptimisticUpdate{}

	err = ssz_snappy.DecodeAndRead(stream, optimistic, &clparams.MainnetBeaconConfig, ethClock)
	require.NoError(t, err)

	require.Equal(t, f.NewestLCUpdate.AttestedHeader, optimistic.AttestedHeader)
	require.Equal(t, f.NewestLCUpdate.SignatureSlot, optimistic.SignatureSlot)
	require.Equal(t, f.NewestLCUpdate.SyncAggregate, optimistic.SyncAggregate)
}

func TestLightClientFinality(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6005"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6006"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := mock_services.NewForkChoiceStorageMock(t)

	f.NewestLCUpdate = &cltypes.LightClientUpdate{
		AttestedHeader:          cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee:       &solid.SyncCommittee{},
		SignatureSlot:           altairSlot,
		SyncAggregate:           &cltypes.SyncAggregate{},
		FinalizedHeader:         cltypes.NewLightClientHeader(clparams.AltairVersion),
		FinalityBranch:          solid.NewHashVector(cltypes.FinalizedBranchSize),
		NextSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	f.NewestLCUpdate.AttestedHeader.Beacon.Slot = altairSlot
	ethClock := getEthClock(t)

	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.LightClientFinalityUpdateProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	got := &cltypes.LightClientFinalityUpdate{}

	err = ssz_snappy.DecodeAndRead(stream, got, &clparams.MainnetBeaconConfig, ethClock)
	require.NoError(t, err)

	require.Equal(t, got.AttestedHeader, f.NewestLCUpdate.AttestedHeader)
	require.Equal(t, got.SyncAggregate, f.NewestLCUpdate.SyncAggregate)
	require.Equal(t, got.FinalizedHeader, f.NewestLCUpdate.FinalizedHeader)
	require.Equal(t, got.FinalityBranch, f.NewestLCUpdate.FinalityBranch)
	require.Equal(t, got.SignatureSlot, f.NewestLCUpdate.SignatureSlot)
}

func TestLightClientBootstrap(t *testing.T) {
	ctx := context.Background()
	ethClock := getEthClock(t)

	listenAddrHost := "/ip4/127.0.0.1/tcp/6007"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6008"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := mock_services.NewForkChoiceStorageMock(t)

	f.NewestLCUpdate = &cltypes.LightClientUpdate{
		AttestedHeader:          cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee:       &solid.SyncCommittee{},
		SignatureSlot:           altairSlot,
		SyncAggregate:           &cltypes.SyncAggregate{},
		FinalizedHeader:         cltypes.NewLightClientHeader(clparams.AltairVersion),
		FinalityBranch:          solid.NewHashVector(cltypes.FinalizedBranchSize),
		NextSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	f.NewestLCUpdate.AttestedHeader.Beacon.Slot = altairSlot
	reqRoot := common.Hash{1, 2, 3}
	f.LightClientBootstraps[reqRoot] = &cltypes.LightClientBootstrap{
		Header:                     cltypes.NewLightClientHeader(clparams.AltairVersion),
		CurrentSyncCommittee:       &solid.SyncCommittee{1, 2, 3, 5, 6},
		CurrentSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	f.LightClientBootstraps[reqRoot].Header.Beacon.Slot = altairSlot
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.LightClientBootstrapProtocolV1))
	require.NoError(t, err)

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, &cltypes.Root{Root: reqRoot}); err != nil {
		return
	}
	require.NoError(t, err)

	reqData := common.CopyBytes(reqBuf.Bytes())
	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	got := &cltypes.LightClientBootstrap{}

	err = ssz_snappy.DecodeAndRead(stream, got, &clparams.MainnetBeaconConfig, ethClock)
	require.NoError(t, err)

	expected := f.LightClientBootstraps[reqRoot]
	require.Equal(t, expected.Header, got.Header)
	require.Equal(t, expected.CurrentSyncCommittee, got.CurrentSyncCommittee)
	require.Equal(t, expected.CurrentSyncCommitteeBranch, got.CurrentSyncCommitteeBranch)
}

func TestLightClientUpdates(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6009"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6041"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := mock_services.NewForkChoiceStorageMock(t)
	ethClock := getEthClock(t)

	up := &cltypes.LightClientUpdate{
		AttestedHeader:          cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee:       &solid.SyncCommittee{},
		SignatureSlot:           altairSlot,
		SyncAggregate:           &cltypes.SyncAggregate{},
		FinalizedHeader:         cltypes.NewLightClientHeader(clparams.AltairVersion),
		FinalityBranch:          solid.NewHashVector(cltypes.FinalizedBranchSize),
		NextSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	up.AttestedHeader.Beacon.Slot = altairSlot
	// just some stupid randomization
	for i := 1; i < 5; i++ {
		upC := *up
		upC.SignatureSlot = uint64(i)
		f.LCUpdates[uint64(i)] = &upC
	}
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, f, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.LightClientUpdatesByRangeProtocolV1))
	require.NoError(t, err)

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, &cltypes.LightClientUpdatesByRangeRequest{StartPeriod: 0, Count: 2}); err != nil {
		return
	}
	require.NoError(t, err)

	reqData := common.CopyBytes(reqBuf.Bytes())
	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	got := []*cltypes.LightClientUpdate{}
	_ = got
	expectedCount := 1
	currentPeriod := 1
	for i := 0; i < expectedCount; i++ {
		forkDigest := make([]byte, 4)

		_, err := stream.Read(forkDigest)
		if err != nil {
			if err == io.EOF {
				t.Fatal("Stream is empty")
			} else {
				require.NoError(t, err)
			}
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}

		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			require.NoError(t, fmt.Errorf("null fork digest"))
		}

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			require.NoError(t, err)
		}

		update := cltypes.NewLightClientUpdate(version)
		if err = update.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, f.LCUpdates[uint64(currentPeriod)], update)
		currentPeriod++

		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

}
