package sentry_multi_client

import (
	"context"
	"testing"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	proto_sentry "github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
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
		{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 42000,
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

	// Decode the receipt to verify Bloom field is not populated
	// The ReceiptsRLPPacket contains an RLP-encoded list of receipts
	var receiptsList []*receiptRLP69
	if err := rlp.DecodeBytes(response.ReceiptsRLPPacket[0], &receiptsList); err != nil {
		t.Fatalf("Failed to decode receipts list: %v", err)
	}

	if len(receiptsList) != 2 {
		t.Fatalf("Expected 2 receipt in list, got %d", len(receiptsList))
	}

	receipt := receiptsList[0]

	// Verify the receipt was decoded correctly
	if receipt.CumulativeGasUsed != 21000 {
		t.Errorf("Expected CumulativeGasUsed 21000, got %d", receipt.CumulativeGasUsed)
	}
}

func TestMultiClient_AnnounceBlockRangeLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testMinimumBlockHeight := uint64(100)
	testLatestBlockHeight := uint64(200)
	testBestHash := common.HexToHash("0xabc")

	var sentMessage *proto_sentry.OutboundMessageData
	mockSentry := &mockSentryClient{
		sendMessageToAllFunc: func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sentMessage = req
			return &proto_sentry.SentPeers{}, nil
		},
		handShakeFunc: func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error) {
			return &proto_sentry.HandShakeReply{
				Protocol: proto_sentry.Protocol_ETH69,
			}, nil
		},
	}

	mockStatus := &mockStatusDataProvider{
		getStatusDataFunc: func(ctx context.Context) (*proto_sentry.StatusData, error) {
			return &proto_sentry.StatusData{
				MinimumBlockHeight: testMinimumBlockHeight,
				MaxBlockHeight:     testLatestBlockHeight,
				BestHash:           gointerfaces.ConvertHashToH256(testBestHash),
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

	if response.Earliest != testMinimumBlockHeight {
		t.Errorf("Expected earliest block height %d, got %d", testMinimumBlockHeight, response.Earliest)
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
	handShakeFunc        func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error)
}

func (m *mockSentryClient) SendMessageById(ctx context.Context, req *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return m.sendMessageByIdFunc(ctx, req, opts...)
}

func (m *mockSentryClient) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return m.sendMessageToAllFunc(ctx, req, opts...)
}

func (m *mockSentryClient) HandShake(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error) {
	return m.handShakeFunc(ctx, req, opts...)
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
