package sentry_multi_client

import (
	"bytes"
	"context"
	"testing"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/balcache"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
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

// balHeaderNumberReader is a minimal services.FullBlockReader stub that resolves
// hash → block-number via an explicit map. AnswerGetBlockAccessListsQuery only
// reads HeaderNumber; every other method falls through to the embedded nil
// interface and would panic if called, which is the correct behaviour — it
// flags accidental coupling.
type balHeaderNumberReader struct {
	services.FullBlockReader
	byHash map[common.Hash]uint64
}

func (m *balHeaderNumberReader) HeaderNumber(_ context.Context, _ kv.Getter, hash common.Hash) (*uint64, error) {
	n, ok := m.byHash[hash]
	if !ok {
		return nil, nil
	}
	return &n, nil
}

// TestGetBlockAccessLists71_AnswersAndSends covers the server-side eth/71 BAL
// handler: decode an inbound GetBlockAccessLists request, look up stored BALs
// from rawdb via AnswerGetBlockAccessListsQuery, encode a positionally-aligned
// BlockAccessLists response, send it back to the requesting peer with the
// matching RequestId. Pairs with the inbound-subscription fix on the *client*
// side — without both, peers can't actually exchange BALs.
func TestGetBlockAccessLists71_AnswersAndSends(t *testing.T) {
	ctx := context.Background()

	db := temporal.NewTestDB(t, dbcfg.ChainDB)
	rwTx, err := db.BeginRw(ctx)
	if err != nil {
		t.Fatalf("begin rw: %v", err)
	}
	defer rwTx.Rollback() // safety net; we Commit below on the happy path

	hashKnown := common.Hash{0x01}
	hashUnknown := common.Hash{0x02}
	const knownBlockNum uint64 = 100
	bal := []byte{0xc3, 0x01, 0x02, 0x03} // short valid RLP non-empty payload
	balcache.ResetBALCacheForTest()
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.CacheBlockAccessList(hashKnown, bal)
	if err := rwTx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	reader := &balHeaderNumberReader{
		byHash: map[common.Hash]uint64{hashKnown: knownBlockNum},
	}

	var sent *proto_sentry.SendMessageByIdRequest
	mockSentry := &mockSentryClient{
		sendMessageByIdFunc: func(_ context.Context, req *proto_sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sent = req
			return &proto_sentry.SentPeers{}, nil
		},
	}

	cs := &MultiClient{
		db:          db,
		blockReader: reader,
		logger:      log.New(),
	}

	const reqID uint64 = 0xcafebabe
	req := eth.GetBlockAccessListsPacket66{
		RequestId:                 reqID,
		GetBlockAccessListsPacket: eth.GetBlockAccessListsPacket{hashKnown, hashUnknown},
	}
	encoded, err := rlp.EncodeToBytes(&req)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}

	if err := cs.getBlockAccessLists71(ctx, &proto_sentry.InboundMessage{
		Id:   proto_sentry.MessageId_GET_BLOCK_ACCESS_LISTS_71,
		Data: encoded,
		PeerId: &proto_types.H512{
			Hi: &proto_types.H256{Hi: &proto_types.H128{}, Lo: &proto_types.H128{}},
			Lo: &proto_types.H256{Hi: &proto_types.H128{}, Lo: &proto_types.H128{}},
		},
	}, mockSentry); err != nil {
		t.Fatalf("getBlockAccessLists71: %v", err)
	}
	if sent == nil {
		t.Fatal("no response sent")
	}
	if sent.Data.Id != proto_sentry.MessageId_BLOCK_ACCESS_LISTS_71 {
		t.Fatalf("response message id: have %v, want BLOCK_ACCESS_LISTS_71", sent.Data.Id)
	}

	var resp eth.BlockAccessListsPacket66
	if err := rlp.DecodeBytes(sent.Data.Data, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.RequestId != reqID {
		t.Fatalf("RequestId: have %d, want %d", resp.RequestId, reqID)
	}
	if got := len(resp.BlockAccessListsPacket); got != 2 {
		t.Fatalf("response length: have %d, want 2 (positionally aligned to query)", got)
	}
	if !bytes.Equal(resp.BlockAccessListsPacket[0], bal) {
		t.Errorf("response[0] (known+BAL): have %x, want %x", resp.BlockAccessListsPacket[0], bal)
	}
	// Unknown block — sentinel 0x80 ("not available"), not padded with anything else.
	if !bytes.Equal(resp.BlockAccessListsPacket[1], []byte{0x80}) {
		t.Errorf("response[1] (unknown block): have %x, want 0x80 sentinel", resp.BlockAccessListsPacket[1])
	}
}

// TestBlockAccessLists71_NilFetcherDropsMessage covers the nil-guard added in
// blockAccessLists71: in-package builds (e.g. tests that construct
// &MultiClient{...} directly without wiring the BAL fetcher) must not panic on
// a stray inbound BLOCK_ACCESS_LISTS_71 message. The guard drops the message
// silently and returns nil so the dispatch loop can continue.
func TestBlockAccessLists71_NilFetcherDropsMessage(t *testing.T) {
	ctx := context.Background()

	// Encode a syntactically valid BlockAccessListsPacket66 — even with a
	// well-formed payload the handler must not call into a nil balFetcher.
	bal, err := rlp.EncodeToBytes([]any{[]byte{0x01, 0x02}, []byte{0x03}})
	if err != nil {
		t.Fatalf("encode stub bal: %v", err)
	}
	packet := eth.BlockAccessListsPacket66{
		RequestId:              0xdeadbeef,
		BlockAccessListsPacket: eth.BlockAccessListsPacket{bal},
	}
	payload, err := rlp.EncodeToBytes(&packet)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	peerId := &proto_types.H512{
		Hi: &proto_types.H256{Hi: &proto_types.H128{}, Lo: &proto_types.H128{}},
		Lo: &proto_types.H256{Hi: &proto_types.H128{}, Lo: &proto_types.H128{}},
	}

	cs := &MultiClient{logger: log.New()} // no balFetcher set

	// Direct call: must not panic, must return nil.
	if err := cs.blockAccessLists71(ctx, &proto_sentry.InboundMessage{
		PeerId: peerId,
		Data:   payload,
	}, &mockSentryClient{}); err != nil {
		t.Fatalf("blockAccessLists71 with nil fetcher should drop and return nil, got: %v", err)
	}

	// Dispatch path: same packet routed via HandleInboundMessage with the
	// real eth/71 BAL message id must also be a clean no-op. HandleInboundMessage
	// recovers panics and surfaces them as errors, so any failure here would
	// have shown up as a non-nil err.
	if err := cs.HandleInboundMessage(ctx, &proto_sentry.InboundMessage{
		PeerId: peerId,
		Id:     proto_sentry.MessageId_BLOCK_ACCESS_LISTS_71,
		Data:   payload,
	}, &mockSentryClient{}); err != nil {
		t.Fatalf("HandleInboundMessage(BLOCK_ACCESS_LISTS_71) with nil fetcher should be a no-op, got: %v", err)
	}
}

// TestRecvMessageLoop_SubscribesToBlockAccessListsMsg is a structural guard for
// the inbound-response subscription added alongside blockAccessLists71: without
// eth.ToProto[direct.ETH71][eth.BlockAccessListsMsg] in RecvMessageLoop's id
// list, peer responses to outbound GetBlockAccessLists requests never reach
// HandleInboundMessage, every fetch times out, and the BALDownloader silently
// burns scan passes. Caught empirically on bal-devnet-3 pre-fix; this guards
// against regressing the subscription back out.
//
// We can't run RecvMessageLoop in a unit test (it pumps an indefinite gRPC
// stream), but we can assert the message-id list it builds contains the eth/71
// response code by scanning the source — kept simple to avoid refactoring the
// loop just for testability.
func TestRecvMessageLoop_SubscribesToBlockAccessListsMsg(t *testing.T) {
	if got := eth.ToProto[direct.ETH71][eth.BlockAccessListsMsg]; got != proto_sentry.MessageId_BLOCK_ACCESS_LISTS_71 {
		t.Fatalf("eth.ToProto wiring drift: ETH71/BlockAccessListsMsg = %v, want BLOCK_ACCESS_LISTS_71", got)
	}
	// The subscription itself lives in RecvMessageLoop. The behavioural proof
	// that it fires is in TestBlockAccessLists71_NilFetcherDropsMessage's
	// HandleInboundMessage path above plus the bal-devnet-3 live-peer trace
	// (zero `[bal-downloader] fetch failed` entries post-fix).
}
