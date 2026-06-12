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
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

func addTestWitnessData(db kv.TemporalRwDB, hash common.Hash, witnessData []byte, blockNumber uint64) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	header := &types.Header{
		Number: *uint256.NewInt(blockNumber),
	}
	headerBytes, err := rlp.EncodeToBytes(header)
	if err != nil {
		return err
	}
	err = rawdb.WriteHeaderNumber(tx, hash, blockNumber)
	if err != nil {
		return err
	}
	headerKey := dbutils.HeaderKey(blockNumber, hash)
	err = tx.Put(kv.Headers, headerKey, headerBytes)
	if err != nil {
		return err
	}
	err = tx.Put(kv.BorWitnesses, headerKey, witnessData)
	if err != nil {
		return err
	}
	err = tx.Put(kv.BorWitnessSizes, headerKey, dbutils.EncodeBlockNumber(uint64(len(witnessData))))
	if err != nil {
		return err
	}
	return tx.Commit()
}

type witnessResponderTest struct {
	responder    *WitnessResponder
	db           kv.TemporalRwDB
	sentryClient *direct.MockSentryClient
}

func newWitnessResponderTest(t *testing.T) *witnessResponderTest {
	dirs := datadir.New(t.TempDir())
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	stepSize := uint64(16)
	tdb := temporaltest.NewTestDBWithStepSize(t, dirs, stepSize)
	messageListener := NewMessageListener(logger, sentryClient, nil, nil)
	responder := NewWitnessResponder(
		logger,
		messageListener,
		NewWitnessPublisher(NewMessageSender(sentryClient)),
		tdb,
		freezeblocks.NewBlockReader(nil, nil),
	)
	return &witnessResponderTest{
		responder:    responder,
		db:           tdb,
		sentryClient: sentryClient,
	}
}

func getWitnessMessage(req wit.GetWitnessPacket, peerId *PeerId) *DecodedInboundMessage[*wit.GetWitnessPacket] {
	return &DecodedInboundMessage[*wit.GetWitnessPacket]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_GET_BLOCK_WITNESS_W0,
			PeerId: peerId.H512(),
		},
		Decoded: &req,
		PeerId:  peerId,
	}
}

func TestWitnessResponderServesWitness(t *testing.T) {
	ctx := context.Background()
	test := newWitnessResponderTest(t)
	testBlockHash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	testWitnessData := []byte("test_witness")
	require.NoError(t, addTestWitnessData(test.db, testBlockHash, testWitnessData, 100))
	req := wit.GetWitnessPacket{
		RequestId: 123,
		GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
			{
				Hash: testBlockHash,
				Page: 0,
			},
		}},
	}
	test.sentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			require.Equal(t, sentryproto.MessageId_BLOCK_WITNESS_W0, request.Data.Id)
			var response wit.WitnessPacketRLPPacket
			require.NoError(t, rlp.DecodeBytes(request.Data.Data, &response))
			require.Equal(t, uint64(123), response.RequestId)
			require.Len(t, response.WitnessPacketResponse, 1)
			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, testBlockHash, pageResp.Hash)
			require.Equal(t, uint64(0), pageResp.Page)
			require.Equal(t, uint64(1), pageResp.TotalPages)
			require.Equal(t, testWitnessData, pageResp.Data)
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{request.PeerId}}, nil
		},
	).Times(1)
	err := test.responder.handleGetWitness(ctx, getWitnessMessage(req, PeerIdFromUint64(1)))
	require.NoError(t, err)
}

// TestWitnessResponderPagination serves witness data that spans multiple pages:
// page 0 (full), page 1 (full), page 2 (partial).
func TestWitnessResponderPagination(t *testing.T) {
	ctx := context.Background()
	test := newWitnessResponderTest(t)
	peerId := PeerIdFromUint64(1)
	testBlockHash := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	pageSize := wit.PageSize                          // 15 MB
	largeWitnessData := make([]byte, pageSize*2+1000) // ~30MB + 1KB
	for i := range largeWitnessData {
		largeWitnessData[i] = byte(i % 256)
	}
	require.NoError(t, addTestWitnessData(test.db, testBlockHash, largeWitnessData, 100))
	expectResponse := func(check func(response wit.WitnessPacketRLPPacket)) {
		test.sentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
				var response wit.WitnessPacketRLPPacket
				require.NoError(t, rlp.DecodeBytes(request.Data.Data, &response))
				check(response)
				return &sentryproto.SentPeers{Peers: []*typesproto.H512{request.PeerId}}, nil
			},
		).Times(1)
	}
	t.Run("Request Page 0 - First Page", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 456,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{Hash: testBlockHash, Page: 0},
			}},
		}
		expectResponse(func(response wit.WitnessPacketRLPPacket) {
			require.Equal(t, uint64(456), response.RequestId)
			require.Len(t, response.WitnessPacketResponse, 1)
			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, testBlockHash, pageResp.Hash)
			require.Equal(t, uint64(0), pageResp.Page)
			require.Equal(t, uint64(3), pageResp.TotalPages)
			require.Equal(t, pageSize, len(pageResp.Data))
			require.Equal(t, largeWitnessData[:pageSize], pageResp.Data)
		})
		require.NoError(t, test.responder.handleGetWitness(ctx, getWitnessMessage(req, peerId)))
	})
	t.Run("Request Page 1 - Middle Page", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 457,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{Hash: testBlockHash, Page: 1},
			}},
		}
		expectResponse(func(response wit.WitnessPacketRLPPacket) {
			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, uint64(1), pageResp.Page)
			require.Equal(t, uint64(3), pageResp.TotalPages)
			require.Equal(t, largeWitnessData[pageSize:pageSize*2], pageResp.Data)
		})
		require.NoError(t, test.responder.handleGetWitness(ctx, getWitnessMessage(req, peerId)))
	})
	t.Run("Request Page 2 - Last Partial Page", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 458,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{Hash: testBlockHash, Page: 2},
			}},
		}
		expectResponse(func(response wit.WitnessPacketRLPPacket) {
			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, uint64(2), pageResp.Page)
			require.Equal(t, uint64(3), pageResp.TotalPages)
			require.Equal(t, 1000, len(pageResp.Data))
			require.Equal(t, largeWitnessData[pageSize*2:], pageResp.Data)
		})
		require.NoError(t, test.responder.handleGetWitness(ctx, getWitnessMessage(req, peerId)))
	})
	t.Run("Request Multiple Pages in Single Request", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 459,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{Hash: testBlockHash, Page: 0},
				{Hash: testBlockHash, Page: 2},
			}},
		}
		expectResponse(func(response wit.WitnessPacketRLPPacket) {
			require.Equal(t, uint64(459), response.RequestId)
			require.Len(t, response.WitnessPacketResponse, 2)
			page0 := response.WitnessPacketResponse[0]
			require.Equal(t, uint64(0), page0.Page)
			require.Equal(t, uint64(3), page0.TotalPages)
			require.Equal(t, pageSize, len(page0.Data))
			page2 := response.WitnessPacketResponse[1]
			require.Equal(t, uint64(2), page2.Page)
			require.Equal(t, uint64(3), page2.TotalPages)
			require.Equal(t, 1000, len(page2.Data))
		})
		require.NoError(t, test.responder.handleGetWitness(ctx, getWitnessMessage(req, peerId)))
	})
	t.Run("Request Invalid Page Number", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 460,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{Hash: testBlockHash, Page: 10}, // Invalid page (only 3 pages exist)
			}},
		}
		expectResponse(func(response wit.WitnessPacketRLPPacket) {
			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, uint64(10), pageResp.Page)
			require.Equal(t, uint64(3), pageResp.TotalPages)
			require.Empty(t, pageResp.Data)
		})
		require.NoError(t, test.responder.handleGetWitness(ctx, getWitnessMessage(req, peerId)))
	})
}

// TestWitnessResponderExactPageSize serves a witness that is exactly one page.
func TestWitnessResponderExactPageSize(t *testing.T) {
	ctx := context.Background()
	test := newWitnessResponderTest(t)
	testBlockHash := common.HexToHash("0xedgecase1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	pageSize := wit.PageSize
	exactPageSizeData := make([]byte, pageSize)
	for i := range exactPageSizeData {
		exactPageSizeData[i] = byte(i % 256)
	}
	require.NoError(t, addTestWitnessData(test.db, testBlockHash, exactPageSizeData, 100))
	req := wit.GetWitnessPacket{
		RequestId: 999,
		GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
			{Hash: testBlockHash, Page: 0},
		}},
	}
	test.sentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			var response wit.WitnessPacketRLPPacket
			require.NoError(t, rlp.DecodeBytes(request.Data.Data, &response))
			require.Equal(t, uint64(999), response.RequestId)
			require.Len(t, response.WitnessPacketResponse, 1)
			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, uint64(0), pageResp.Page)
			require.Equal(t, uint64(1), pageResp.TotalPages)
			require.Equal(t, exactPageSizeData, pageResp.Data)
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{request.PeerId}}, nil
		},
	).Times(1)
	err := test.responder.handleGetWitness(ctx, getWitnessMessage(req, gointerfacesPeerId()))
	require.NoError(t, err)
}

func gointerfacesPeerId() *PeerId {
	return PeerIdFromH512(gointerfaces.ConvertHashToH512([64]byte{0x99, 0x99, 0x99}))
}
