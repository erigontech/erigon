package sentry_multi_client

import (
	"context"
	"testing"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-p2p/protocols/eth"
	"github.com/erigontech/erigon/turbo/services"
)

type receiptRLP69 struct {
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	Logs              []*types.Log
}

func TestMultiClient_GetReceipts69(t *testing.T) {
	ctx := context.Background()

	testHash := common.HexToHash("0x123")
	testReceipts := types.Receipts{
		{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 21000,
			Logs:              []*types.Log{},
			TxHash:            testHash,
			GasUsed:           21000,
		},
	}

	var sentMessage *proto_sentry.SendMessageByIdRequest
	mockSentry := &mockSentryClient{
		sendMessageByIdFunc: func(ctx context.Context, req *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sentMessage = req
			return &proto_sentry.SentPeers{}, nil
		},
	}
	mockBlockReader := &mockBlockReader{}
	mockReceiptsGenerator := &mockReceiptsGenerator{
		getCachedReceiptsFunc: func(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
			if hash == testHash {
				return testReceipts, true
			}
			return nil, false
		},
	}

	cs := &MultiClient{
		blockReader:                      mockBlockReader,
		ethApiWrapper:                    mockReceiptsGenerator,
		getReceiptsActiveGoroutineNumber: semaphore.NewWeighted(1),
		logger:                           log.New(),
	}

	request := eth.GetReceiptsPacket66{
		RequestId: 1,
		GetReceiptsPacket: eth.GetReceiptsPacket{
			testHash,
		},
	}
	encodedRequest, err := rlp.EncodeToBytes(&request)
	if err != nil {
		t.Fatalf("Failed to encode request: %v", err)
	}

	inreq := &proto_sentry.InboundMessage{
		Id:   proto_sentry.MessageId_GET_RECEIPTS_69,
		Data: encodedRequest,
		PeerId: &proto_types.H512{
			Hi: &proto_types.H256{},
			Lo: &proto_types.H256{},
		},
	}
	err = cs.getReceipts69(ctx, inreq, mockSentry)
	if err != nil {
		t.Fatalf("getReceipts69 failed: %v", err)
	}

	if sentMessage == nil {
		t.Fatal("No message was sent")
	}
	if sentMessage.Data.Id != proto_sentry.MessageId_RECEIPTS_66 {
		t.Errorf("Expected message ID %v, got %v", proto_sentry.MessageId_RECEIPTS_66, sentMessage.Data.Id)
	}

	var response eth.ReceiptsRLPPacket66
	if err := rlp.DecodeBytes(sentMessage.Data.Data, &response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if response.RequestId != request.RequestId {
		t.Errorf("Expected request ID %d, got %d", request.RequestId, response.RequestId)
	}
	if len(response.ReceiptsRLPPacket) != 1 {
		t.Errorf("Expected 1 receipt, got %d", len(response.ReceiptsRLPPacket))
	}

	// Decode the receipt to verify Bloom field is not populated
	var receipt receiptRLP69
	if err := rlp.DecodeBytes(response.ReceiptsRLPPacket[0], &receipt); err != nil {
		t.Fatalf("Failed to decode receipt: %v", err)
	}
}

func TestMultiClient_AnnounceBlockRangeLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testEarliestBlockHeight := uint64(100)
	testLatestBlockHeight := uint64(200)
	testBestHash := common.HexToHash("0xabc")

	var sentMessage *proto_sentry.OutboundMessageData
	mockSentry := &mockSentryClient{
		sendMessageToAllFunc: func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sentMessage = req
			return &proto_sentry.SentPeers{}, nil
		},
	}

	mockStatus := &mockStatusDataProvider{
		getStatusDataFunc: func(ctx context.Context) (*proto_sentry.StatusData, error) {
			return &proto_sentry.StatusData{
				EarliestBlockHeight: testEarliestBlockHeight,
				MaxBlockHeight:      testLatestBlockHeight,
				BestHash:            gointerfaces.ConvertHashToH256(testBestHash),
			}, nil
		},
	}

	mockBlockReader := &mockFullBlockReader{
		readyFunc: func(ctx context.Context) <-chan error {
			ch := make(chan error, 1)
			ch <- nil // Signal that the block reader is ready
			return ch
		},
	}

	cs := &MultiClient{
		sentries:           []proto_sentry.SentryClient{mockSentry},
		statusDataProvider: mockStatus,
		blockReader:        mockBlockReader,
		logger:             log.New(),
	}

	cs.doAnnounceBlockRange(ctx)

	if sentMessage == nil {
		t.Fatal("No message was sent")
	}
	if sentMessage.Id != proto_sentry.MessageId_BLOCK_RANGE_UPDATE_69 {
		t.Errorf("Expected message ID %v, got %v", proto_sentry.MessageId_BLOCK_RANGE_UPDATE_69, sentMessage.Id)
	}

	var response eth.BlockRangeUpdatePacket
	if err := rlp.DecodeBytes(sentMessage.Data, &response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Earliest != testEarliestBlockHeight {
		t.Errorf("Expected earliest block height %d, got %d", testEarliestBlockHeight, response.Earliest)
	}
	if response.Latest != testLatestBlockHeight {
		t.Errorf("Expected latest block height %d, got %d", testLatestBlockHeight, response.Latest)
	}
	if response.LatestHash != testBestHash {
		t.Errorf("Expected latest hash %s, got %s", testBestHash.Hex(), response.LatestHash.Hex())
	}
}

// Mock implementations
type mockSentryClient struct {
	proto_sentry.SentryClient
	sendMessageByIdFunc  func(ctx context.Context, req *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error)
	sendMessageToAllFunc func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error)
}

func (m *mockSentryClient) SendMessageById(ctx context.Context, req *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return m.sendMessageByIdFunc(ctx, req, opts...)
}

func (m *mockSentryClient) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return m.sendMessageToAllFunc(ctx, req, opts...)
}

type mockBlockReader struct {
	services.FullBlockReader
}

type mockReceiptsGenerator struct {
	eth.ReceiptsGetter
	getCachedReceiptsFunc func(ctx context.Context, hash common.Hash) (types.Receipts, bool)
}

func (m *mockReceiptsGenerator) GetCachedReceipts(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
	return m.getCachedReceiptsFunc(ctx, hash)
}

type mockStatusDataProvider struct {
	getStatusDataFunc func(ctx context.Context) (*proto_sentry.StatusData, error)
}

func (m *mockStatusDataProvider) GetStatusData(ctx context.Context) (*proto_sentry.StatusData, error) {
	return m.getStatusDataFunc(ctx)
}

type mockFullBlockReader struct {
	services.FullBlockReader
	readyFunc func(ctx context.Context) <-chan error
}

func (m *mockFullBlockReader) Ready(ctx context.Context) <-chan error {
	return m.readyFunc(ctx)
}
