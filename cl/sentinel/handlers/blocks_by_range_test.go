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

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func TestBlocksByRootHandler(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/5000"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/5001"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, _ := indiciesDB.BeginRw(ctx)

	startSlot := uint64(100)
	count := uint64(10)
	step := uint64(1)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	tx.Commit()

	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, nil, true,
	)
	c.Start()
	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: startSlot,
		Count:     count,
		Step:      step,
	}
	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := libcommon.CopyBytes(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	for i := 0; i < int(count); i++ {
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
		require.NoError(t, err)

		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
		if err = block.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, expBlocks[i].Block.Slot, block.Block.Slot)
		require.Equal(t, expBlocks[i].Block.StateRoot, block.Block.StateRoot)
		require.Equal(t, expBlocks[i].Block.ParentRoot, block.Block.ParentRoot)
		require.Equal(t, expBlocks[i].Block.ProposerIndex, block.Block.ProposerIndex)
		require.Equal(t, expBlocks[i].Block.Body.ExecutionPayload.BlockNumber, block.Block.Body.ExecutionPayload.BlockNumber)
		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

	defer indiciesDB.Close()
	defer tx.Rollback()
}
