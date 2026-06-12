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

package p2p

import (
	"context"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/stateless"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

type testWitnessEntry struct {
	blockNumber uint64
	blockHash   common.Hash
	data        []byte
}

type testWitnessBuffer struct {
	mu        sync.Mutex
	witnesses []testWitnessEntry
}

func (b *testWitnessBuffer) AddWitness(blockNumber uint64, blockHash common.Hash, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.witnesses = append(b.witnesses, testWitnessEntry{blockNumber: blockNumber, blockHash: blockHash, data: data})
}

func (b *testWitnessBuffer) drain() []testWitnessEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	witnesses := b.witnesses
	b.witnesses = nil
	return witnesses
}

type mockHeaderReader struct {
	headers map[common.Hash]*types.Header
}

func newMockHeaderReader() *mockHeaderReader {
	return &mockHeaderReader{
		headers: make(map[common.Hash]*types.Header),
	}
}

func (m *mockHeaderReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return m.headers[hash]
}

func (m *mockHeaderReader) addHeader(header *types.Header) {
	m.headers[header.Hash()] = header
}

func createTestWitness(t *testing.T, header *types.Header) *stateless.Witness {
	t.Helper()
	parentHeader := &types.Header{
		Number: *uint256.NewInt(header.Number.Uint64() - 1),
		Root:   common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
	}
	header.ParentHash = parentHeader.Hash()
	headerReader := newMockHeaderReader()
	headerReader.addHeader(parentHeader)
	witness, err := stateless.NewWitness(header, headerReader)
	require.NoError(t, err)
	testState := map[string]struct{}{
		"test_state_node_1": {},
		"test_state_node_2": {},
	}
	witness.AddState(testState)
	witness.AddCode([]byte("test_code_data"))
	return witness
}

type witnessFetcherTest struct {
	fetcher      *WitnessFetcher
	buffer       *testWitnessBuffer
	db           kv.TemporalRwDB
	sentryClient *direct.MockSentryClient
}

func newWitnessFetcherTest(t *testing.T) *witnessFetcherTest {
	dirs := datadir.New(t.TempDir())
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	stepSize := uint64(16)
	tdb := temporaltest.NewTestDBWithStepSize(t, dirs, stepSize)
	buffer := &testWitnessBuffer{}
	messageListener := NewMessageListener(logger, sentryClient, nil, nil)
	fetcher := NewWitnessFetcher(
		logger,
		messageListener,
		NewMessageSender(sentryClient),
		tdb,
		freezeblocks.NewBlockReader(nil, nil),
		buffer,
	)
	return &witnessFetcherTest{
		fetcher:      fetcher,
		buffer:       buffer,
		db:           tdb,
		sentryClient: sentryClient,
	}
}

func witnessMessage(packet wit.WitnessPacketRLPPacket, peerId *PeerId) *DecodedInboundMessage[*wit.WitnessPacketRLPPacket] {
	return &DecodedInboundMessage[*wit.WitnessPacketRLPPacket]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_BLOCK_WITNESS_W0,
			PeerId: peerId.H512(),
		},
		Decoded: &packet,
		PeerId:  peerId,
	}
}

func TestWitnessFetcherNewWitnessStoresInBuffer(t *testing.T) {
	ctx := context.Background()
	test := newWitnessFetcherTest(t)
	testHeader := &types.Header{
		Number:     *uint256.NewInt(200),
		ParentHash: common.HexToHash("0xparent"),
		Root:       common.HexToHash("0xroot"),
	}
	witness := createTestWitness(t, testHeader)
	expectedBlockHash := testHeader.Hash()
	err := test.fetcher.handleNewWitness(ctx, &DecodedInboundMessage[*wit.NewWitnessPacket]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_NEW_WITNESS_W0,
			PeerId: PeerIdFromUint64(1).H512(),
		},
		Decoded: &wit.NewWitnessPacket{Witness: witness},
		PeerId:  PeerIdFromUint64(1),
	})
	require.NoError(t, err)
	witnesses := test.buffer.drain()
	require.Len(t, witnesses, 1, "Should have exactly one witness in buffer")
	storedWitness := witnesses[0]
	require.Equal(t, uint64(200), storedWitness.blockNumber, "Block number should match")
	require.Equal(t, expectedBlockHash, storedWitness.blockHash, "Block hash should match")
	require.Greater(t, len(storedWitness.data), 0, "Witness data should not be empty")
	var encodedWitness wit.NewWitnessPacket
	require.NoError(t, rlp.DecodeBytes(storedWitness.data, &encodedWitness.Witness))
}

// TestWitnessFetcherRejectsInflatedTotalPages is a regression for a
// single-packet DoS: an inflated TotalPages claim used to drive the
// missing-pages loop into allocating gigabytes.
func TestWitnessFetcherRejectsInflatedTotalPages(t *testing.T) {
	ctx := context.Background()
	test := newWitnessFetcherTest(t)
	t.Run("TotalPages above cap is rejected", func(t *testing.T) {
		err := test.fetcher.handleWitness(ctx, witnessMessage(wit.WitnessPacketRLPPacket{
			RequestId: 1,
			WitnessPacketResponse: wit.WitnessPacketResponse{{
				Data:       []byte{0xDE, 0xAD},
				Hash:       common.Hash{0x42},
				Page:       0,
				TotalPages: 100_000_000,
			}},
		}, PeerIdFromUint64(1)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "TotalPages")
	})
	t.Run("inconsistent TotalPages across pages for same hash is rejected", func(t *testing.T) {
		hash := common.Hash{0x99}
		err := test.fetcher.handleWitness(ctx, witnessMessage(wit.WitnessPacketRLPPacket{
			RequestId: 2,
			WitnessPacketResponse: wit.WitnessPacketResponse{
				{Data: []byte{0x01}, Hash: hash, Page: 0, TotalPages: 2},
				{Data: []byte{0x02}, Hash: hash, Page: 1, TotalPages: 3},
			},
		}, PeerIdFromUint64(2)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "inconsistent TotalPages")
	})
	t.Run("Page >= TotalPages is skipped (peer-has-no-witness sentinel)", func(t *testing.T) {
		err := test.fetcher.handleWitness(ctx, witnessMessage(wit.WitnessPacketRLPPacket{
			RequestId: 3,
			WitnessPacketResponse: wit.WitnessPacketResponse{{
				Data:       nil,
				Hash:       common.Hash{0x33},
				Page:       0,
				TotalPages: 0,
			}},
		}, PeerIdFromUint64(3)))
		require.NoError(t, err)
		require.Empty(t, test.buffer.drain())
	})
	t.Run("TotalPages at cap is accepted", func(t *testing.T) {
		// the single received page is far from completing the witness, so the
		// fetcher re-requests the missing pages from the same peer
		test.sentryClient.EXPECT().
			SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
				require.Equal(t, sentryproto.MessageId_GET_BLOCK_WITNESS_W0, req.Data.Id)
				return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
			}).
			Times(1)
		err := test.fetcher.handleWitness(ctx, witnessMessage(wit.WitnessPacketRLPPacket{
			RequestId: 4,
			WitnessPacketResponse: wit.WitnessPacketResponse{{
				Data:       []byte{0xAB},
				Hash:       common.Hash{0x44},
				Page:       0,
				TotalPages: wit.MaxWitnessPages,
			}},
		}, PeerIdFromUint64(4)))
		require.NoError(t, err)
	})
}
