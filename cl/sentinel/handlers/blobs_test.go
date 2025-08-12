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
	"errors"
	"io"
	"math"
	"testing"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func getTestBlobSidecars(blockHeader *cltypes.SignedBeaconBlockHeader) []*cltypes.BlobSidecar {
	out := []*cltypes.BlobSidecar{}
	for i := 0; i < 4; i++ {
		out = append(out, cltypes.NewBlobSidecar(
			uint64(i),
			&cltypes.Blob{byte(i)},
			common.Bytes48{byte(i)},
			common.Bytes48{byte(i)},
			blockHeader,
			solid.NewHashVector(cltypes.CommitmentBranchSize),
		))
	}
	return out

}

func TestBlobsByRangeHandler(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6121"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6358"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	blobDb := memdb.NewTestDB(t, kv.ChainDB)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, _ := indiciesDB.BeginRw(ctx)

	startSlot := uint64(100)
	count := uint64(10)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	h := expBlocks[0].SignedBeaconBlockHeader()
	sidecars := getTestBlobSidecars(h)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs(), math.MaxUint64, beaconCfg, nil)
	r, _ := h.Header.HashSSZ()
	require.NoError(t, blobStorage.WriteBlobSidecars(ctx, r, sidecars))

	tx.Commit()

	ethClock := getEthClock(t)
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
		nil, &mock_services.ForkChoiceStorageMock{}, blobStorage, nil, nil, true,
	)
	c.Start()
	req := &cltypes.BlobsByRangeRequest{
		StartSlot: h.Header.Slot,
		Count:     1,
	}

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := common.CopyBytes(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BlobSidecarByRangeProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	for i := 0; i < len(sidecars); i++ {
		forkDigest := make([]byte, 4)
		_, err := stream.Read(forkDigest)
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				require.NoError(t, err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			require.NoError(t, errors.New("null fork digest"))
		}
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			require.NoError(t, err)
		}

		sidecar := &cltypes.BlobSidecar{}
		if err = sidecar.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, sidecars[i], sidecar)
		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

	defer indiciesDB.Close()
	defer tx.Rollback()
}

func TestBlobsByIdentifiersHandler(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6125"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6350"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	blobDb := memdb.NewTestDB(t, kv.ChainDB)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, _ := indiciesDB.BeginRw(ctx)

	startSlot := uint64(100)
	count := uint64(10)

	ethClock := getEthClock(t)
	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	h := expBlocks[0].SignedBeaconBlockHeader()
	sidecars := getTestBlobSidecars(h)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs(), math.MaxUint64, beaconCfg, ethClock)
	r, _ := h.Header.HashSSZ()
	require.NoError(t, blobStorage.WriteBlobSidecars(ctx, r, sidecars))

	tx.Commit()

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
		nil, &mock_services.ForkChoiceStorageMock{}, blobStorage, nil, nil, true,
	)
	c.Start()
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](40269, 40)
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 0})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 1})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 2})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 3})

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := common.CopyBytes(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BlobSidecarByRootProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	for i := 0; i < len(sidecars); i++ {
		forkDigest := make([]byte, 4)
		_, err := stream.Read(forkDigest)
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				require.NoError(t, err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			require.NoError(t, errors.New("null fork digest"))
		}
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			require.NoError(t, err)
		}

		sidecar := &cltypes.BlobSidecar{}
		if err = sidecar.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, sidecars[i], sidecar)
		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

	defer indiciesDB.Close()
	defer tx.Rollback()
}
