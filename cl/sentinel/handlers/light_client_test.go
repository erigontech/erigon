package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
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

	f := forkchoice.NewForkChoiceStorageMock()

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

	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		genesisCfg,
		nil, f, true,
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

	err = ssz_snappy.DecodeAndRead(stream, optimistic, &clparams.MainnetBeaconConfig, c.genesisConfig.GenesisValidatorRoot)
	require.NoError(t, err)

	require.Equal(t, optimistic.AttestedHeader, f.NewestLCUpdate.AttestedHeader)
	require.Equal(t, optimistic.SignatureSlot, f.NewestLCUpdate.SignatureSlot)
	require.Equal(t, optimistic.SyncAggregate, f.NewestLCUpdate.SyncAggregate)
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

	f := forkchoice.NewForkChoiceStorageMock()

	f.NewestLCUpdate = &cltypes.LightClientUpdate{
		AttestedHeader:    cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee: &solid.SyncCommittee{},
		SignatureSlot:     1234,
		SyncAggregate:     &cltypes.SyncAggregate{},
		FinalizedHeader:   cltypes.NewLightClientHeader(clparams.AltairVersion),
		// 8 is fine as this is a test
		FinalityBranch:          solid.NewHashVector(cltypes.FinalizedBranchSize),
		NextSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}

	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		genesisCfg,
		nil, f, true,
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

	err = ssz_snappy.DecodeAndRead(stream, got, &clparams.MainnetBeaconConfig, c.genesisConfig.GenesisValidatorRoot)
	require.NoError(t, err)

	require.Equal(t, got.AttestedHeader, f.NewestLCUpdate.AttestedHeader)
	require.Equal(t, got.SyncAggregate, f.NewestLCUpdate.SyncAggregate)
	require.Equal(t, got.FinalizedHeader, f.NewestLCUpdate.FinalizedHeader)
	require.Equal(t, got.FinalityBranch, f.NewestLCUpdate.FinalityBranch)
	require.Equal(t, got.SignatureSlot, f.NewestLCUpdate.SignatureSlot)
}

func TestLightClientBootstrap(t *testing.T) {
	ctx := context.Background()

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

	f := forkchoice.NewForkChoiceStorageMock()

	f.NewestLCUpdate = &cltypes.LightClientUpdate{
		AttestedHeader:    cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee: &solid.SyncCommittee{},
		SignatureSlot:     1234,
		SyncAggregate:     &cltypes.SyncAggregate{},
		FinalizedHeader:   cltypes.NewLightClientHeader(clparams.AltairVersion),
		// 8 is fine as this is a test
		FinalityBranch:          solid.NewHashVector(cltypes.FinalizedBranchSize),
		NextSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	reqRoot := common.Hash{1, 2, 3}
	f.LightClientBootstraps[reqRoot] = &cltypes.LightClientBootstrap{
		Header:                     cltypes.NewLightClientHeader(clparams.AltairVersion),
		CurrentSyncCommittee:       &solid.SyncCommittee{1, 2, 3, 5, 6},
		CurrentSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		genesisCfg,
		nil, f, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.LightClientBootstrapProtocolV1))
	require.NoError(t, err)

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, &cltypes.Root{Root: reqRoot}); err != nil {
		return
	}
	require.NoError(t, err)

	reqData := libcommon.CopyBytes(reqBuf.Bytes())
	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	got := &cltypes.LightClientBootstrap{}

	err = ssz_snappy.DecodeAndRead(stream, got, &clparams.MainnetBeaconConfig, c.genesisConfig.GenesisValidatorRoot)
	require.NoError(t, err)

	expected := f.LightClientBootstraps[reqRoot]
	require.Equal(t, got.Header, expected.Header)
	require.Equal(t, got.CurrentSyncCommittee, expected.CurrentSyncCommittee)
	require.Equal(t, got.CurrentSyncCommitteeBranch, expected.CurrentSyncCommitteeBranch)
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

	f := forkchoice.NewForkChoiceStorageMock()

	up := &cltypes.LightClientUpdate{
		AttestedHeader:    cltypes.NewLightClientHeader(clparams.AltairVersion),
		NextSyncCommittee: &solid.SyncCommittee{},
		SignatureSlot:     1234,
		SyncAggregate:     &cltypes.SyncAggregate{},
		FinalizedHeader:   cltypes.NewLightClientHeader(clparams.AltairVersion),
		// 8 is fine as this is a test
		FinalityBranch:          solid.NewHashVector(cltypes.FinalizedBranchSize),
		NextSyncCommitteeBranch: solid.NewHashVector(cltypes.SyncCommitteeBranchSize),
	}
	// just some stupid randomization
	for i := 1; i < 5; i++ {
		upC := *up
		upC.SignatureSlot = uint64(i)
		f.LCUpdates[uint64(i)] = &upC
	}
	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		genesisCfg,
		nil, f, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.LightClientUpdatesByRangeProtocolV1))
	require.NoError(t, err)

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, &cltypes.LightClientUpdatesByRangeRequest{StartPeriod: 0, Count: 2}); err != nil {
		return
	}
	require.NoError(t, err)

	reqData := libcommon.CopyBytes(reqBuf.Bytes())
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

		version, err := fork.ForkDigestVersion(utils.Uint32ToBytes4(respForkDigest), beaconCfg, genesisCfg.GenesisValidatorRoot)
		if err != nil {
			require.NoError(t, err)
		}

		update := cltypes.NewLightClientUpdate(version)
		if err = update.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, update, f.LCUpdates[uint64(currentPeriod)])
		currentPeriod++

		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

}
