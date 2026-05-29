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
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func TestBlocksByHeadCountCap(t *testing.T) {
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	count := beaconCfg.MaxRequestBlocksDeneb + 5
	blocks, roots := makeBlocksByHeadChain(t, 100, count)

	store, stream := setupBlocksByHeadTest(t, blocks, nil)
	_ = store

	writeBlocksByHeadRequest(t, stream, roots[len(roots)-1], count)

	got := readBlocksByHeadResponses(t, stream, int(beaconCfg.MaxRequestBlocksDeneb))
	require.Len(t, got, int(beaconCfg.MaxRequestBlocksDeneb))
	require.Equal(t, blocks[len(blocks)-1].Block.Slot, got[0].Block.Slot)
	require.Equal(t, blocks[len(blocks)-int(beaconCfg.MaxRequestBlocksDeneb)].Block.Slot, got[len(got)-1].Block.Slot)
	expectNoBlocksByHeadResponse(t, stream)
}

func TestBlocksByHeadParentChainTraversal(t *testing.T) {
	blocks, roots := makeBlocksByHeadChain(t, 200, 4)
	_, stream := setupBlocksByHeadTest(t, blocks, nil)

	writeBlocksByHeadRequest(t, stream, roots[len(roots)-1], 3)

	got := readBlocksByHeadResponses(t, stream, 3)
	require.Equal(t, []uint64{203, 202, 201}, []uint64{
		got[0].Block.Slot,
		got[1].Block.Slot,
		got[2].Block.Slot,
	})
	require.Equal(t, roots[2], got[0].Block.ParentRoot)
	require.Equal(t, roots[1], got[1].Block.ParentRoot)
	require.Equal(t, roots[0], got[2].Block.ParentRoot)
}

func TestBlocksByHeadForkChoiceFallback(t *testing.T) {
	blocks, roots := makeBlocksByHeadChain(t, 300, 1)
	forkChoice := mock_services.NewForkChoiceStorageMock(t)
	forkChoice.Blocks[roots[0]] = blocks[0]
	_, stream := setupBlocksByHeadTest(t, nil, forkChoice)

	writeBlocksByHeadRequest(t, stream, roots[0], 1)

	got := readBlocksByHeadResponses(t, stream, 1)
	require.Equal(t, blocks[0].Block.Slot, got[0].Block.Slot)
	require.Equal(t, blocks[0].Block.StateRoot, got[0].Block.StateRoot)
}

func TestBlocksByHeadMissingRoot(t *testing.T) {
	_, stream := setupBlocksByHeadTest(t, nil, nil)

	writeBlocksByHeadRequest(t, stream, common.Hash{0x42}, 3)

	expectNoBlocksByHeadResponse(t, stream)
}

func TestBlocksByHeadZeroCount(t *testing.T) {
	blocks, roots := makeBlocksByHeadChain(t, 400, 1)
	_, stream := setupBlocksByHeadTest(t, blocks, nil)

	writeBlocksByHeadRequest(t, stream, roots[0], 0)

	expectNoBlocksByHeadResponse(t, stream)
}

func setupBlocksByHeadTest(
	t *testing.T,
	blocks []*cltypes.SignedBeaconBlock,
	forkChoice *mock_services.ForkChoiceStorageMock,
) (*tests.MockBlockReader, network.Stream) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, host.Close()) })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, host1.Close()) })

	require.NoError(t, host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	}))

	_, indiciesDB := setupStore(t)
	t.Cleanup(func() { indiciesDB.Close() })

	store := tests.NewMockBlockReader()
	for _, block := range blocks {
		store.U[block.Block.Slot] = block
	}
	if forkChoice == nil {
		forkChoice = mock_services.NewForkChoiceStorageMock(t)
	}

	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		peers.NewPool(host),
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, forkChoice, nil, nil, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BeaconBlocksByHeadProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	return store, stream
}

func makeBlocksByHeadChain(t *testing.T, startSlot, count uint64) ([]*cltypes.SignedBeaconBlock, []common.Hash) {
	t.Helper()

	parentRoot := common.Hash{0x99}
	blocks := make([]*cltypes.SignedBeaconBlock, 0, count)
	roots := make([]common.Hash, 0, count)
	for i := uint64(0); i < count; i++ {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = startSlot + i
		block.Block.StateRoot = common.Hash{byte(i + 1)}
		block.Block.ParentRoot = parentRoot
		block.Block.ProposerIndex = i
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		parentRoot = root
		blocks = append(blocks, block)
		roots = append(roots, root)
	}
	return blocks, roots
}

func writeBlocksByHeadRequest(t *testing.T, stream network.Stream, root common.Hash, count uint64) {
	t.Helper()

	req := &cltypes.BeaconBlocksByHeadRequest{
		BeaconRoot: root,
		Count:      count,
	}
	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))
	_, err := stream.Write(common.Copy(reqBuf.Bytes()))
	require.NoError(t, err)
}

func readBlocksByHeadResponses(t *testing.T, stream network.Stream, count int) []*cltypes.SignedBeaconBlock {
	t.Helper()

	ethClock := getEthClock(t)
	blocks := make([]*cltypes.SignedBeaconBlock, 0, count)
	for i := 0; i < count; i++ {
		prefix := make([]byte, 1)
		require.NoError(t, stream.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, err := io.ReadFull(stream, prefix)
		require.NoError(t, err)
		require.Equal(t, byte(SuccessfulResponsePrefix), prefix[0])

		forkDigest := make([]byte, 4)
		_, err = io.ReadFull(stream, forkDigest)
		require.NoError(t, err)

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(stream)
		_, err = io.ReadFull(sr, raw)
		require.NoError(t, err)

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(binary.BigEndian.Uint32(forkDigest)))
		require.NoError(t, err)
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
		require.NoError(t, block.DecodeSSZ(raw, int(version)))
		blocks = append(blocks, block)
	}
	return blocks
}

func expectNoBlocksByHeadResponse(t *testing.T, stream network.Stream) {
	t.Helper()

	require.NoError(t, stream.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	var buf [1]byte
	n, err := stream.Read(buf[:])
	require.Zero(t, n)
	require.Error(t, err)
}
