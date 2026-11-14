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
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/snappypool"
)

func TestBeaconBlocksByRangeRequestCost(t *testing.T) {
	for _, tc := range []struct {
		count uint64
		cost  int
	}{{0, 0}, {1, 0}, {3, 2}, {96, 95}, {128, 95}} {
		t.Run(fmt.Sprint(tc.count), func(t *testing.T) {
			_, db := setupStore(t)
			cfg := clparams.MainnetBeaconConfig
			c := &ConsensusHandlers{
				ctx: t.Context(), indiciesDB: db, beaconConfig: &cfg,
				beaconDB: tests.NewMockBlockReader(), rateLimiter: newPeerRateLimiter(),
			}
			stream := newHandlerTestStream(t, communication.BeaconBlocksByRangeProtocolV2,
				&cltypes.BeaconBlocksByRangeRequest{StartSlot: 100, Count: tc.count, Step: 1})
			bucket := admitHandlerTestRequest(t, c, stream)
			require.NoError(t, c.beaconBlocksByRangeHandler(stream))
			require.Empty(t, stream.response.Bytes())
			require.Equal(t, bucket.maxTokens-1-float64(tc.cost), bucket.tokens)
		})
	}
}

func TestBlocksByRootHandler(t *testing.T) {
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
	step := uint64(1)

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
	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: startSlot,
		Count:     count,
		Step:      step,
	}
	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := bytes.Clone(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	for i := 0; i < int(count); i++ {
		forkDigest := make([]byte, 4)

		_, err := stream.Read(forkDigest)
		if err != nil {
			if err == io.EOF { //nolint:errorlint // intentional bare sentinel check
				t.Fatal("Stream is empty")
			} else {
				require.NoError(t, err)
			}
		}

		encodedLn, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr.Reset(stream)
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
	tx.Rollback()
}

// TestBeaconBlocksByRangeHandlerStaysInRequestedRange checks that a response only holds blocks with
// start_slot <= slot < start_slot+count. Empty slots inside the range are left out, not replaced by
// blocks after it.
func TestBeaconBlocksByRangeHandlerStaysInRequestedRange(t *testing.T) {
	slotRange := func(start, n uint64) []uint64 {
		slots := make([]uint64, n)
		for i := range slots {
			slots[i] = start + uint64(i)
		}
		return slots
	}
	for _, tc := range []struct {
		name        string
		blockSlots  []uint64
		start       uint64
		count       uint64
		want        []uint64
		wantInvalid bool
	}{
		{name: "empty slot in range", blockSlots: []uint64{100, 102, 110, 111, 112}, start: 100, count: 5, want: []uint64{100, 102}},
		{name: "zero count", blockSlots: []uint64{100, 102, 110, 111, 112}, start: 100, count: 0, want: nil},
		// MAX_REQUEST_BLOCKS_DENEB is 128: the whole range is searched, not only its first 96 slots.
		{name: "first block late in a full-size range", blockSlots: []uint64{200}, start: 100, count: 128, want: []uint64{200}},
		{name: "response limited to 96 blocks", blockSlots: slotRange(100, 128), start: 100, count: 128, want: slotRange(100, MaxRequestsBlocks)},
		// A larger count is capped, not rejected: Caplin's chain tip sync can request more than 128 slots.
		{name: "count above the request limit is capped", blockSlots: []uint64{100, 300}, start: 100, count: 1000, want: []uint64{100}},
		{name: "end slot overflow", blockSlots: []uint64{100}, start: math.MaxUint64 - 2, count: 5, wantInvalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			require.NoError(t, err)
			t.Cleanup(func() { host.Close() })
			host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			require.NoError(t, err)
			t.Cleanup(func() { host1.Close() })
			require.NoError(t, host.Connect(ctx, peer.AddrInfo{ID: host1.ID(), Addrs: host1.Addrs()}))

			_, indiciesDB := setupStore(t)
			store := tests.NewMockBlockReader()
			tx, err := indiciesDB.BeginRw(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			for _, slot := range tc.blockSlots {
				populateDatabaseWithBlocks(t, store, tx, slot, 0)
			}
			require.NoError(t, tx.Commit())

			ethClock := getEthClock(t)
			_, beaconCfg := clparams.GetConfigsByNetwork(1)
			c := NewConsensusHandlers(ctx, store, indiciesDB, host, &clparams.NetworkConfig{}, nil,
				beaconCfg, ethClock, nil, &mock_services.ForkChoiceStorageMock{}, nil, nil, nil, true)
			c.Start()

			var reqBuf bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, &cltypes.BeaconBlocksByRangeRequest{StartSlot: tc.start, Count: tc.count, Step: 1}))
			stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
			require.NoError(t, err)
			_, err = stream.Write(reqBuf.Bytes())
			require.NoError(t, err)

			if tc.wantInvalid {
				code := make([]byte, 1)
				_, err := io.ReadFull(stream, code)
				require.NoError(t, err)
				require.Equal(t, byte(InvalidRequestPrefix), code[0])
				return
			}
			require.Equal(t, tc.want, readBlocksByRangeSlots(t, stream, ethClock))
		})
	}
}

// readBlocksByRangeSlots reads every response chunk until the stream ends and returns the block slots.
func readBlocksByRangeSlots(t *testing.T, stream network.Stream, ethClock eth_clock.EthereumClock) []uint64 {
	t.Helper()
	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	var slots []uint64
	for {
		code := make([]byte, 1)
		if _, err := io.ReadFull(stream, code); errors.Is(err, io.EOF) {
			return slots
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, byte(0), code[0])
		forkDigest := make([]byte, 4)
		_, err := io.ReadFull(stream, forkDigest)
		require.NoError(t, err)
		encodedLn, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)
		raw := make([]byte, encodedLn)
		sr.Reset(stream)
		_, err = io.ReadFull(sr, raw)
		require.NoError(t, err)
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(binary.BigEndian.Uint32(forkDigest)))
		require.NoError(t, err)
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
		require.NoError(t, block.DecodeSSZ(raw, int(version)))
		slots = append(slots, block.Block.Slot)
	}
}

type countingStream struct {
	network.Stream
	r    io.Reader
	read int
}

func (s *countingStream) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.read += n
	return n, err
}

// A fixed-size request whose prefix claims a large payload must be refused before that payload is read.
func TestBlocksByRangeRefusesOversizedRequest(t *testing.T) {
	payload := make([]byte, clparams.MaxPayloadSize)
	var req bytes.Buffer
	req.Write(binary.AppendUvarint(nil, uint64(len(payload))))
	sw := snappy.NewBufferedWriter(&req)
	_, err := sw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	s := &countingStream{r: &req}
	require.ErrorIs(t, (&ConsensusHandlers{}).beaconBlocksByRangeHandler(s), ssz_snappy.ErrPayloadExceedsLimit)
	require.Less(t, s.read, 64, "the handler read the claimed payload")
}

func TestOversizedFixedSizeRequestGetsInvalidRequest(t *testing.T) {
	for _, p := range []string{
		communication.BeaconBlocksByRangeProtocolV2,
		communication.BeaconBlocksByHeadProtocolV1,
		communication.BlobSidecarByRangeProtocolV1,
		communication.LightClientBootstrapProtocolV1,
		communication.LightClientUpdatesByRangeProtocolV1,
	} {
		t.Run(p, func(t *testing.T) {
			stream, _ := newHeartbeatTestStream(t, protocol.ID(p))
			require.NoError(t, stream.SetDeadline(time.Now().Add(5*time.Second)))
			_, err := stream.Write(binary.AppendUvarint(nil, uint64(clparams.MaxPayloadSize)))
			require.NoError(t, err)
			requireResponseCode(t, stream, InvalidRequestPrefix)
		})
	}
}
