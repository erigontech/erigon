package sentry_multi_client

import (
	"context"
	"testing"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	proto_sentry "github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

type receiptRLP69 struct {
	Type              uint8
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	Logs              []*types.Log
}

func TestMultiClient_GetReceipts69(t *testing.T) {
	ctx := context.Background()

	testHash := common.HexToHash("0x123")
	testReceipts := types.Receipts{
		{
			Type:              types.LegacyTxType,
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 21000,
			Logs:              []*types.Log{},
			TxHash:            testHash,
			GasUsed:           21000,
		},
		{
			Type:              types.DynamicFeeTxType,
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
		t.Fatalf("Expected 2 receipts in list, got %d", len(receiptsList))
	}

	// Verify legacy receipt (type 0)
	receipt0 := receiptsList[0]
	if receipt0.Type != types.LegacyTxType {
		t.Errorf("Expected receipt[0] Type %d (legacy), got %d", types.LegacyTxType, receipt0.Type)
	}
	if receipt0.CumulativeGasUsed != 21000 {
		t.Errorf("Expected receipt[0] CumulativeGasUsed 21000, got %d", receipt0.CumulativeGasUsed)
	}

	// Verify typed receipt (DynamicFee, type 2) — this is the critical case:
	// ETH69 requires typed receipts to be list-encoded [type, status, gas, logs],
	// NOT the old byte-string envelope (type_byte || rlp(data)) used in ETH68.
	receipt1 := receiptsList[1]
	if receipt1.Type != types.DynamicFeeTxType {
		t.Errorf("Expected receipt[1] Type %d (DynamicFee), got %d", types.DynamicFeeTxType, receipt1.Type)
	}
	if receipt1.CumulativeGasUsed != 42000 {
		t.Errorf("Expected receipt[1] CumulativeGasUsed 42000, got %d", receipt1.CumulativeGasUsed)
	}
}

type mockSentryClient struct {
	proto_sentry.SentryClient
	sendMessageByIdFunc  func(ctx context.Context, req *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error)
	sendMessageToAllFunc func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error)
	handShakeFunc        func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error)
	penalizePeerFunc     func(ctx context.Context, req *proto_sentry.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
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

func (m *mockSentryClient) PenalizePeer(ctx context.Context, req *proto_sentry.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if m.penalizePeerFunc == nil {
		return &emptypb.Empty{}, nil
	}
	return m.penalizePeerFunc(ctx, req, opts...)
}

type mockBlockReader struct {
	services.FullBlockReader
}

// TestBlockRange69_InvalidPacketKicksPeer verifies that an invalid
// BlockRangeUpdate (e.g. Earliest > Latest) causes the peer to be penalized.
// This mirrors the Hive `TestBlockRangeUpdateInvalid` simulator check.
func TestBlockRange69_InvalidPacketKicksPeer(t *testing.T) {
	ctx := context.Background()

	peerId := &proto_types.H512{
		Hi: &proto_types.H256{Hi: &proto_types.H128{}, Lo: &proto_types.H128{}},
		Lo: &proto_types.H256{Hi: &proto_types.H128{}, Lo: &proto_types.H128{}},
	}

	// Packet with Earliest > Latest is invalid per BlockRangeUpdatePacket.Validate.
	invalid := eth.BlockRangeUpdatePacket{
		Earliest:   10,
		Latest:     8,
		LatestHash: common.HexToHash("0xdeadbeef"),
	}
	payload, err := rlp.EncodeToBytes(&invalid)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	var penalized *proto_sentry.PenalizePeerRequest
	mockSentry := &mockSentryClient{
		penalizePeerFunc: func(_ context.Context, req *proto_sentry.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			penalized = req
			return &emptypb.Empty{}, nil
		},
	}

	cs := &MultiClient{logger: log.New()}

	if err := cs.blockRange69(ctx, &proto_sentry.InboundMessage{
		PeerId: peerId,
		Data:   payload,
	}, mockSentry); err == nil {
		t.Fatal("expected validation error, got nil")
	}

	if penalized == nil {
		t.Fatal("expected PenalizePeer to be called, got nil")
	}
	if penalized.Penalty != proto_sentry.PenaltyKind_Kick {
		t.Fatalf("expected Kick penalty, got %v", penalized.Penalty)
	}
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
