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
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/snappypool"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestBlobsByRangeRequestCost(t *testing.T) {
	for _, tc := range []struct {
		count uint64
		cost  int
	}{
		{0, 0},
		{1, 8},
		{2, 17},
		{8, 71},
		{9, 71},
		{math.MaxUint64 - 100, 71},
		{1 << 60, 71},
		{1 << 63, 71},
	} {
		t.Run(fmt.Sprint(tc.count), func(t *testing.T) {
			_, db := setupStore(t)
			cfg := clparams.MainnetBeaconConfig
			cfg.BlobSchedule = []clparams.BlobParameters{{Epoch: 0, MaxBlobsPerBlock: 9}}
			c := &ConsensusHandlers{ctx: t.Context(), indiciesDB: db, beaconConfig: &cfg, rateLimiter: newPeerRateLimiter()}
			proto := communication.BlobSidecarByRangeProtocolV1
			stream := newHandlerTestStream(t, proto, &cltypes.BlobsByRangeRequest{StartSlot: 100, Count: tc.count})
			bucket := admitHandlerTestRequest(t, c, stream)

			require.NoError(t, c.blobsSidecarsByRangeHandlerDeneb(stream))
			require.Empty(t, stream.response.Bytes())
			require.Equal(t, bucket.maxTokens-1-float64(tc.cost), bucket.tokens)
		})
	}
}

func TestBlobsByIdentifiersRequestCost(t *testing.T) {
	for _, tc := range []struct{ count, cost int }{{0, 0}, {1, 0}, {3, 2}, {72, 71}, {73, 71}} {
		t.Run(fmt.Sprint(tc.count), func(t *testing.T) {
			_, db := setupStore(t)
			cfg := clparams.MainnetBeaconConfig
			c := &ConsensusHandlers{
				ctx: t.Context(), indiciesDB: db, beaconConfig: &cfg,
				ethClock: getEthClock(t), rateLimiter: newPeerRateLimiter(),
			}
			req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](tc.count, 40)
			for i := range tc.count {
				req.Append(&cltypes.BlobIdentifier{BlockRoot: common.Hash{0xff}, Index: uint64(i)})
			}
			stream := newHandlerTestStream(t, communication.BlobSidecarByRootProtocolV1, req)
			bucket := admitHandlerTestRequest(t, c, stream)
			require.NoError(t, c.blobsSidecarsByIdsHandlerDeneb(stream))
			require.Empty(t, stream.response.Bytes())
			require.Equal(t, bucket.maxTokens-1-float64(tc.cost), bucket.tokens)
		})
	}
}

func TestBlobsByRangeRejectsOverflow(t *testing.T) {
	_, db := setupStore(t)
	cfg := clparams.MainnetBeaconConfig
	c := &ConsensusHandlers{ctx: t.Context(), indiciesDB: db, beaconConfig: &cfg, rateLimiter: newPeerRateLimiter()}
	stream := newHandlerTestStream(t, communication.BlobSidecarByRangeProtocolV1,
		&cltypes.BlobsByRangeRequest{StartSlot: math.MaxUint64 - 1, Count: 2})
	require.NoError(t, c.blobsSidecarsByRangeHandlerDeneb(stream))
	code, err := stream.response.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(InvalidRequestPrefix), code)
}

func TestBlobsByIdentifiersSkipsDuplicatesAndUnavailable(t *testing.T) {
	for _, first := range []string{"duplicate", "unknown", "post Fulu"} {
		t.Run(first, func(t *testing.T) {
			_, db := setupStore(t)
			store := tests.NewMockBlockReader()
			cfg := clparams.MainnetBeaconConfig
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			block := populateDatabaseWithBlocks(t, store, tx, 100, 0)[0]
			fuluBlock := populateDatabaseWithBlocks(t, store, tx, cfg.FuluForkEpoch*cfg.SlotsPerEpoch, 0)[0]
			require.NoError(t, tx.Commit())
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			fuluRoot, err := fuluBlock.Block.HashSSZ()
			require.NoError(t, err)
			blobDB := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			storage := blob_storage.NewBlobStore(blobDB, afero.NewMemMapFs())
			sidecars := getTestBlobSidecars(block.SignedBeaconBlockHeader())
			require.NoError(t, storage.WriteBlobSidecars(t.Context(), root, sidecars))
			require.NoError(t, storage.WriteBlobSidecars(t.Context(), fuluRoot, getTestBlobSidecars(fuluBlock.SignedBeaconBlockHeader())))
			c := &ConsensusHandlers{
				ctx: t.Context(), indiciesDB: db, beaconConfig: &cfg,
				blobsStorage: storage, ethClock: getEthClock(t), rateLimiter: newPeerRateLimiter(),
			}
			req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](3, 40)
			switch first {
			case "duplicate":
				req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 0})
			case "unknown":
				req.Append(&cltypes.BlobIdentifier{BlockRoot: common.Hash{0xff}, Index: 0})
			case "post Fulu":
				req.Append(&cltypes.BlobIdentifier{BlockRoot: fuluRoot, Index: 0})
			}
			req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 0})
			req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 1})
			stream := newHandlerTestStream(t, communication.BlobSidecarByRootProtocolV1, req)
			require.NoError(t, c.blobsSidecarsByIdsHandlerDeneb(stream))
			var indices []uint64
			for stream.response.Len() > 0 {
				code, err := stream.response.ReadByte()
				require.NoError(t, err)
				require.Equal(t, byte(SuccessfulResponsePrefix), code)
				sidecar := new(cltypes.BlobSidecar)
				require.NoError(t, decodeAndRead(&stream.response, sidecar, c.ethClock))
				indices = append(indices, sidecar.Index)
			}
			require.Equal(t, []uint64{0, 1}, indices)
		})
	}
}

func TestBlobsByIdentifiersRequestLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	for _, tc := range []struct {
		name  string
		epoch uint64
		limit int
	}{
		{"before Deneb", cfg.DenebForkEpoch - 1, int(cfg.MaxRequestBlobSidecars)},
		{"Deneb", cfg.ElectraForkEpoch - 1, int(cfg.MaxRequestBlobSidecars)},
		{"Electra", cfg.ElectraForkEpoch, int(cfg.MaxRequestBlobSidecarsElectra)},
		{"Fulu", cfg.FuluForkEpoch, int(cfg.MaxRequestBlobSidecarsElectra)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, count := range []int{tc.limit, tc.limit + 1} {
				t.Run(fmt.Sprint(count), func(t *testing.T) {
					_, db := setupStore(t)
					clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
					clock.EXPECT().GetCurrentEpoch().Return(tc.epoch).AnyTimes()
					c := &ConsensusHandlers{
						ctx: t.Context(), indiciesDB: db, beaconConfig: &cfg,
						ethClock: clock, rateLimiter: newPeerRateLimiter(),
					}
					req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](count, 40)
					for range count {
						req.Append(&cltypes.BlobIdentifier{BlockRoot: common.Hash{0xff}})
					}
					stream := newHandlerTestStream(t, communication.BlobSidecarByRootProtocolV1, req)
					require.NoError(t, c.blobsSidecarsByIdsHandlerDeneb(stream))
					if count > tc.limit {
						code, err := stream.response.ReadByte()
						require.NoError(t, err)
						require.Equal(t, byte(InvalidRequestPrefix), code)
						return
					}
					require.Zero(t, stream.response.Len())
				})
			}
		})
	}
}

func TestBlobsByIdentifiersRejectsOversizedPrefix(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	identifierSize := uint64(new(cltypes.BlobIdentifier).EncodingSizeSSZ())
	for _, tc := range []struct {
		name  string
		epoch uint64
		size  uint64
	}{
		{"Deneb", cfg.DenebForkEpoch, cfg.MaxRequestBlobSidecars*identifierSize + 1},
		{"Electra", cfg.ElectraForkEpoch, cfg.MaxRequestBlobSidecarsElectra*identifierSize + 1},
		{"above global limit", cfg.ElectraForkEpoch, clparams.MaxPayloadSize + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentEpoch().Return(tc.epoch)
			c := &ConsensusHandlers{beaconConfig: &cfg, ethClock: clock}
			stream := &envelopeHandlerStream{request: bytes.NewReader(binary.AppendUvarint(nil, tc.size))}
			require.NoError(t, c.blobsSidecarsByIdsHandlerDeneb(stream))
			code, err := stream.response.ReadByte()
			require.NoError(t, err)
			require.Equal(t, byte(InvalidRequestPrefix), code)
		})
	}
}

func getTestBlobSidecars(blockHeader *cltypes.SignedBeaconBlockHeader) []*cltypes.BlobSidecar {
	out := []*cltypes.BlobSidecar{}
	for i := range 4 {
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

	blobDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	startSlot := uint64(100)
	count := uint64(10)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	h := expBlocks[0].SignedBeaconBlockHeader()
	sidecars := getTestBlobSidecars(h)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs())
	r, _ := h.Header.HashSSZ()
	require.NoError(t, blobStorage.WriteBlobSidecars(ctx, r, sidecars))

	require.NoError(t, tx.Commit())

	ethClock := getEthClock(t)
	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, blobStorage, nil, nil, true, nil,
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

	reqData := bytes.Clone(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BlobSidecarByRangeProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	for i := range sidecars {
		forkDigest := make([]byte, 4)
		_, err := stream.Read(forkDigest)
		if err != nil && err != io.EOF { //nolint:errorlint // intentional bare sentinel check
			require.NoError(t, err)
		}

		encodedLn, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr.Reset(stream)
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
		if _, err := stream.Read(make([]byte, 1)); err != nil && err != io.EOF { //nolint:errorlint // intentional bare sentinel check
			require.NoError(t, err)
		}
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF { //nolint:errorlint // intentional bare sentinel check
		t.Fatal("Stream is not empty")
	}

	indiciesDB.Close()
	tx.Rollback()
}

func TestBlobsByIdentifiersHandler(t *testing.T) {
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

	blobDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	startSlot := uint64(100)
	count := uint64(10)

	ethClock := getEthClock(t)
	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	h := expBlocks[0].SignedBeaconBlockHeader()
	sidecars := getTestBlobSidecars(h)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs())
	r, _ := h.Header.HashSSZ()
	require.NoError(t, blobStorage.WriteBlobSidecars(ctx, r, sidecars))

	require.NoError(t, tx.Commit())

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, blobStorage, nil, nil, true, nil,
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

	reqData := bytes.Clone(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BlobSidecarByRootProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	for i := range sidecars {
		forkDigest := make([]byte, 4)
		_, err := stream.Read(forkDigest)
		if err != nil && err != io.EOF { //nolint:errorlint // intentional bare sentinel check
			require.NoError(t, err)
		}

		encodedLn, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr.Reset(stream)
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
		if _, err := stream.Read(make([]byte, 1)); err != nil && err != io.EOF { //nolint:errorlint // intentional bare sentinel check
			require.NoError(t, err)
		}
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF { //nolint:errorlint // intentional bare sentinel check
		t.Fatal("Stream is not empty")
	}

	indiciesDB.Close()
	tx.Rollback()
}
