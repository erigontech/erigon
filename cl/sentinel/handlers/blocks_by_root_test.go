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
	"os"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/snappypool"
	"github.com/erigontech/erigon/db/kv"
)

type countingBlockReader struct {
	*tests.MockBlockReader
	readBlockByRootCalls int
}

func (r *countingBlockReader) ReadBlockByRoot(ctx context.Context, tx kv.Tx, blockRoot common.Hash) (*cltypes.SignedBeaconBlock, error) {
	r.readBlockByRootCalls++
	return r.MockBlockReader.ReadBlockByRoot(ctx, tx, blockRoot)
}

func TestBeaconBlocksByRootRequestLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	for _, tc := range []struct {
		name  string
		count int
		epoch uint64
		limit int
	}{
		{"Deneb at limit", 128, cfg.DenebForkEpoch, 128},
		{"Deneb above limit", 129, cfg.DenebForkEpoch, 128},
		{"before Deneb above 128", 129, cfg.DenebForkEpoch - 1, 1024},
		{"before Deneb at limit", 1024, cfg.DenebForkEpoch - 1, 1024},
		{"before Deneb above limit", 1025, cfg.DenebForkEpoch - 1, 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db := setupStore(t)
			store := &countingBlockReader{MockBlockReader: tests.NewMockBlockReader()}
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			blocks := populateDatabaseWithBlocks(t, store.MockBlockReader, tx, 100, uint64(tc.count-1))
			require.NoError(t, tx.Commit())
			forkClock := getEthClock(t)
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentEpoch().Return(tc.epoch)
			clock.EXPECT().ComputeForkDigest(gomock.Any()).DoAndReturn(forkClock.ComputeForkDigest).AnyTimes()
			c := &ConsensusHandlers{
				ctx: t.Context(), beaconDB: store, indiciesDB: db, beaconConfig: &cfg,
				ethClock: clock, rateLimiter: newPeerRateLimiter(),
			}
			req := solid.NewHashList(tc.count)
			for _, block := range blocks {
				root, err := block.Block.HashSSZ()
				require.NoError(t, err)
				req.Append(root)
			}
			stream := newHandlerTestStream(t, communication.BeaconBlocksByRootProtocolV2, req)
			require.NoError(t, c.beaconBlocksByRootHandler(stream))
			if tc.count > tc.limit {
				code, err := stream.response.ReadByte()
				require.NoError(t, err)
				require.Equal(t, byte(InvalidRequestPrefix), code)
				require.Zero(t, store.readBlockByRootCalls)
				return
			}
			require.Equal(t, MaxRequestsBlocks, store.readBlockByRootCalls)
			stream.request = bytes.NewReader(stream.response.Bytes())
			require.Len(t, readBlocksByRangeSlots(t, stream, forkClock), MaxRequestsBlocks)
		})
	}
}

func TestBeaconBlocksByRootLeavesCreditForRemainingRoots(t *testing.T) {
	_, db := setupStore(t)
	store := &countingBlockReader{MockBlockReader: tests.NewMockBlockReader()}
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	blocks := populateDatabaseWithBlocks(t, store.MockBlockReader, tx, 100, 127)
	require.NoError(t, tx.Commit())
	cfg := clparams.MainnetBeaconConfig
	c := &ConsensusHandlers{
		ctx: t.Context(), beaconDB: store, indiciesDB: db, beaconConfig: &cfg,
		ethClock: getEthClock(t), rateLimiter: newPeerRateLimiter(),
	}
	for _, tc := range []struct {
		start, responses int
		tokensLeft       float64
	}{{0, 96, 32}, {96, 32, 0}} {
		req := solid.NewHashList(len(blocks) - tc.start)
		for _, block := range blocks[tc.start:] {
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			req.Append(root)
		}
		stream := newHandlerTestStream(t, communication.BeaconBlocksByRootProtocolV2, req)
		bucket := admitHandlerTestRequest(t, c, stream)
		require.NoError(t, c.beaconBlocksByRootHandler(stream))
		stream.request = bytes.NewReader(stream.response.Bytes())
		require.Len(t, readBlocksByRangeSlots(t, stream, c.ethClock), tc.responses)
		require.Equal(t, tc.tokensLeft, bucket.tokens)
	}
	require.Equal(t, 128, store.readBlockByRootCalls)
}

func TestBeaconBlocksByRootReturnsWriteError(t *testing.T) {
	_, db := setupStore(t)
	store := tests.NewMockBlockReader()
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	block := populateDatabaseWithBlocks(t, store, tx, 100, 0)[0]
	require.NoError(t, tx.Commit())
	cfg := clparams.MainnetBeaconConfig
	c := &ConsensusHandlers{
		ctx: t.Context(), beaconDB: store, indiciesDB: db, beaconConfig: &cfg,
		ethClock: getEthClock(t), rateLimiter: newPeerRateLimiter(),
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	req := solid.NewHashList(1)
	req.Append(root)
	stream := newHandlerTestStream(t, communication.BeaconBlocksByRootProtocolV2, req)
	stream.expired = true
	require.ErrorIs(t, c.beaconBlocksByRootHandler(stream), os.ErrDeadlineExceeded)
}

func TestBeaconBlocksByRootDeduplicates(t *testing.T) {
	_, db := setupStore(t)
	store := &countingBlockReader{MockBlockReader: tests.NewMockBlockReader()}
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	blocks := populateDatabaseWithBlocks(t, store.MockBlockReader, tx, 100, 1)
	require.NoError(t, tx.Commit())
	cfg := clparams.MainnetBeaconConfig
	c := &ConsensusHandlers{
		ctx: t.Context(), beaconDB: store, indiciesDB: db, beaconConfig: &cfg,
		ethClock: getEthClock(t), rateLimiter: newPeerRateLimiter(),
	}
	req := solid.NewHashList(6)
	req.Append(common.Hash{0xff})
	req.Append(common.Hash{0xff})
	for _, block := range blocks {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		req.Append(root)
		req.Append(root)
	}
	stream := newHandlerTestStream(t, communication.BeaconBlocksByRootProtocolV2, req)
	require.NoError(t, c.beaconBlocksByRootHandler(stream))
	require.Equal(t, 3, store.readBlockByRootCalls)
	stream.request = bytes.NewReader(stream.response.Bytes())
	require.Equal(t, []uint64{100, 101}, readBlocksByRangeSlots(t, stream, c.ethClock))
}

func TestBlocksByRangeHandler(t *testing.T) {
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

	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	startSlot := uint64(100)
	count := uint64(10)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	require.NoError(t, tx.Commit())

	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, nil, nil, nil, true,
	)
	c.Start()
	var req solid.HashListSSZ = solid.NewHashList(len(expBlocks))

	for _, block := range expBlocks {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		req.Append(root)
	}
	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := bytes.Clone(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BeaconBlocksByRootProtocolV2))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	for i := range expBlocks {
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
			require.NoError(t, fmt.Errorf("null fork digest"))
		}
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			require.NoError(t, err)
		}

		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
		if err = block.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, expBlocks[i].Block.Slot, block.Block.Slot)
		require.Equal(t, expBlocks[i].Block.StateRoot, block.Block.StateRoot)
		require.Equal(t, expBlocks[i].Block.ParentRoot, block.Block.ParentRoot)
		require.Equal(t, expBlocks[i].Block.ProposerIndex, block.Block.ProposerIndex)
		require.Equal(t, expBlocks[i].Block.Body.ExecutionPayload.BlockNumber, block.Block.Body.ExecutionPayload.BlockNumber)
		if _, err := stream.Read(make([]byte, 1)); err != nil && err != io.EOF { //nolint:errorlint // intentional bare sentinel check
			require.NoError(t, err)
		}
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF { //nolint:errorlint // intentional bare sentinel check
		t.Fatal("Stream is not empty")
	}

	indiciesDB.Close()
}

type blocksByRootTester struct {
	requestRaw func(payload []byte) []byte
	// clearBuckets empties the rate limiter; requestRaw does not, so requests share a peer bucket.
	clearBuckets func()
	root         common.Hash
	chunkSize    int
}

// request sends roots from a fresh rate-limit bucket.
func (b *blocksByRootTester) request(t *testing.T, roots []common.Hash) []byte {
	b.clearBuckets()
	return b.requestSharedBucket(t, roots)
}

// requestSharedBucket sends roots without refilling the peer's rate-limit bucket.
func (b *blocksByRootTester) requestSharedBucket(t *testing.T, roots []common.Hash) []byte {
	req := solid.NewHashList(len(roots))
	for _, r := range roots {
		req.Append(r)
	}
	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))
	return b.requestRaw(reqBuf.Bytes())
}

func setupBlocksByRootHandler(t *testing.T, denebForkEpoch uint64) *blocksByRootTester {
	ctx := t.Context()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	require.NoError(t, host.Connect(ctx, peer.AddrInfo{ID: host1.ID(), Addrs: host1.Addrs()}))

	_, indiciesDB := setupStore(t)
	t.Cleanup(indiciesDB.Close)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	blocks := populateDatabaseWithBlocks(t, store, tx, 100, 1)
	require.NoError(t, tx.Commit())
	root, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)

	_, mainnetCfg := clparams.GetConfigsByNetwork(1)
	beaconCfg := *mainnetCfg
	beaconCfg.DenebForkEpoch = denebForkEpoch
	c := NewConsensusHandlers(ctx, store, indiciesDB, host, &clparams.NetworkConfig{}, nil, &beaconCfg, getEthClock(t),
		nil, &mock_services.ForkChoiceStorageMock{}, nil, nil, nil, true)
	c.Start()

	requestRaw := func(payload []byte) []byte {
		stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BeaconBlocksByRootProtocolV2))
		require.NoError(t, err)
		_, err = stream.Write(payload)
		require.NoError(t, err)
		require.NoError(t, stream.CloseWrite())
		resp, err := io.ReadAll(stream)
		require.NoError(t, err)
		return resp
	}
	b := &blocksByRootTester{requestRaw: requestRaw, clearBuckets: c.rateLimiter.buckets.Clear, root: root}
	single := b.request(t, []common.Hash{root})
	require.NotEmpty(t, single)
	require.Equal(t, byte(SuccessfulResponsePrefix), single[0])
	b.chunkSize = len(single)
	return b
}

func (b *blocksByRootTester) requireBlocks(t *testing.T, resp []byte, want int) {
	t.Helper()
	require.Zero(t, len(resp)%b.chunkSize)
	require.Equal(t, want, len(resp)/b.chunkSize)
}

func requireInvalidRequest(t *testing.T, resp []byte) {
	t.Helper()
	require.NotEmpty(t, resp)
	require.Equal(t, byte(InvalidRequestPrefix), resp[0])
}

func TestBlocksByRootHandlerServesBlockAfterUnknownRoots(t *testing.T) {
	b := setupBlocksByRootHandler(t, 0)

	roots := make([]common.Hash, MaxRequestsBlocks, MaxRequestsBlocks+1)
	for i := range roots {
		roots[i][0] = 0xff
		roots[i][1] = byte(i)
	}
	roots = append(roots, b.root)

	b.requireBlocks(t, b.request(t, roots), 1)
}

// The handler must reject an oversized length prefix before it reads the compressed payload.
func TestBlocksByRootHandlerRejectsOversizedLengthPrefix(t *testing.T) {
	b := setupBlocksByRootHandler(t, 0)

	prefix := binary.AppendUvarint(nil, (clparams.MainnetBeaconConfig.MaxRequestBlocksDeneb+1)*length.Hash)
	requireInvalidRequest(t, b.requestRaw(prefix))
}

func TestBlocksByRootHandlerRejectsLengthPrefixAboveMaxPayloadSize(t *testing.T) {
	b := setupBlocksByRootHandler(t, 0)

	prefix := binary.AppendUvarint(nil, clparams.MaxPayloadSize+1)
	requireInvalidRequest(t, b.requestRaw(prefix))
}
