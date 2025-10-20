package sentry_multi_client

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	proto_sentry "github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
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

	packet, err := cs.buildBlockRangePacket(ctx)
	if err != nil {
		t.Fatalf("failed to build block range packet: %v", err)
	}
	send := cs.blockRange.decide(packet)
	if !send {
		t.Fatal("expected block range to be announced")
	}
	cs.broadcastBlockRange(ctx, packet)

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

func TestBlockRangeTrackerAccept(t *testing.T) {
	var tracker blockRangeTracker

	first := eth.BlockRangeUpdatePacket{
		Earliest:   0,
		Latest:     100,
		LatestHash: common.HexToHash("0x1"),
	}
	if send := tracker.decide(first); !send {
		t.Fatal("expected initial announcement without forward gating")
	}
	tracker.record(first)

	if send := tracker.decide(first); send {
		t.Fatal("expected identical announcement to be skipped")
	}

	advanceBelowThreshold := first
	advanceBelowThreshold.Latest += blockRangeEpochBlocks - 1
	if send := tracker.decide(advanceBelowThreshold); send {
		t.Fatal("expected insufficient latest advance to be skipped")
	}

	advanceThreshold := first
	advanceThreshold.Latest += blockRangeEpochBlocks
	if send := tracker.decide(advanceThreshold); !send {
		t.Fatal("expected latest advance >= 32 to be sent")
	}
	tracker.record(advanceThreshold)

	regression := advanceThreshold
	regression.Latest -= 10
	if send := tracker.decide(regression); !send {
		t.Fatal("expected regression to trigger immediate announcement")
	}
	tracker.record(regression)

	sameHeightReorg := regression
	sameHeightReorg.LatestHash = common.HexToHash("0x3")
	if send := tracker.decide(sameHeightReorg); !send {
		t.Fatal("expected equal-height hash change to trigger announcement")
	}
	tracker.record(sameHeightReorg)

	if send := tracker.decide(sameHeightReorg); send {
		t.Fatal("expected identical announcement after reorg to be skipped")
	}
}

func TestMultiClient_DoAnnounceBlockRangeSkipsUntilThreshold(t *testing.T) {
	ctx := context.Background()

	status := &proto_sentry.StatusData{
		MinimumBlockHeight: 100,
		MaxBlockHeight:     200,
		BestHash:           gointerfaces.ConvertHashToH256(common.HexToHash("0xaa")),
	}

	var (
		sentCount   int
		lastMessage *proto_sentry.OutboundMessageData
	)

	mockSentry := &mockSentryClient{
		sendMessageToAllFunc: func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sentCount++
			lastMessage = req
			return &proto_sentry.SentPeers{}, nil
		},
		handShakeFunc: func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error) {
			return &proto_sentry.HandShakeReply{Protocol: proto_sentry.Protocol_ETH69}, nil
		},
	}

	statusProvider := &mockStatusDataProvider{
		getStatusDataFunc: func(ctx context.Context) (*proto_sentry.StatusData, error) {
			return status, nil
		},
	}

	readyReader := &mockFullBlockReader{
		readyFunc: func(ctx context.Context) <-chan error {
			ch := make(chan error, 1)
			ch <- nil
			return ch
		},
	}

	cs := &MultiClient{
		sentries:           []proto_sentry.SentryClient{mockSentry},
		statusDataProvider: statusProvider,
		blockReader:        readyReader,
		logger:             log.New(),
	}

	attempt := func() bool {
		packet, err := cs.buildBlockRangePacket(ctx)
		if err != nil {
			t.Fatalf("failed to build block range packet: %v", err)
		}
		send := cs.blockRange.decide(packet)
		if !send {
			return false
		}
		cs.broadcastBlockRange(ctx, packet)
		return true
	}

	if !attempt() {
		t.Fatal("expected first announcement to be sent")
	}
	if sentCount != 1 {
		t.Fatalf("expected first announcement to be sent, got %d", sentCount)
	}

	if attempt() {
		t.Fatalf("expected unchanged range to be skipped, got %d", sentCount)
	}

	status.MaxBlockHeight = 200 + blockRangeEpochBlocks - 1
	if attempt() {
		t.Fatalf("expected latest advance below threshold to be skipped, got %d", sentCount)
	}

	status.MaxBlockHeight = 200 + blockRangeEpochBlocks
	status.BestHash = gointerfaces.ConvertHashToH256(common.HexToHash("0xbb"))
	if !attempt() {
		t.Fatalf("expected announcement after reaching threshold, got %d", sentCount)
	}
	if sentCount != 2 {
		t.Fatalf("expected announcement after reaching threshold, got %d", sentCount)
	}

	if lastMessage == nil {
		t.Fatal("expected block range message to be captured")
	}

	var payload eth.BlockRangeUpdatePacket
	if err := rlp.DecodeBytes(lastMessage.Data, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	if payload.Latest != status.MaxBlockHeight {
		t.Fatalf("expected latest %d, got %d", status.MaxBlockHeight, payload.Latest)
	}

	status.MaxBlockHeight = status.MaxBlockHeight - blockRangeEpochBlocks/2
	status.BestHash = gointerfaces.ConvertHashToH256(common.HexToHash("0xcc"))
	if !attempt() {
		t.Fatalf("expected regression to trigger announcement, got %d", sentCount)
	}
	if sentCount != 3 {
		t.Fatalf("expected regression to trigger announcement, got %d", sentCount)
	}
}

func TestMultiClient_BlockRangeChannelLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testMinimumBlockHeight := uint64(10)
	testLatestBlockHeight := uint64(20)
	testBestHash := common.HexToHash("0xabc")

	var (
		sentMu    sync.Mutex
		sentCount int
	)

	mockSentry := &mockSentryClient{
		sendMessageToAllFunc: func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sentMu.Lock()
			sentCount++
			sentMu.Unlock()
			return &proto_sentry.SentPeers{}, nil
		},
		handShakeFunc: func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error) {
			return &proto_sentry.HandShakeReply{Protocol: proto_sentry.Protocol_ETH69}, nil
		},
	}

	statusMu := sync.Mutex{}
	currentLatest := testLatestBlockHeight
	currentHash := testBestHash

	mockStatus := &mockStatusDataProvider{
		getStatusDataFunc: func(ctx context.Context) (*proto_sentry.StatusData, error) {
			statusMu.Lock()
			defer statusMu.Unlock()
			return &proto_sentry.StatusData{
				MinimumBlockHeight: testMinimumBlockHeight,
				MaxBlockHeight:     currentLatest,
				BestHash:           gointerfaces.ConvertHashToH256(currentHash),
			}, nil
		},
	}

	advanceStatus := func() {
		statusMu.Lock()
		defer statusMu.Unlock()
		currentLatest++
		currentHash = common.BigToHash(new(big.Int).SetUint64(currentLatest))
	}

	readyReader := &mockFullBlockReader{
		readyFunc: func(ctx context.Context) <-chan error {
			ch := make(chan error, 1)
			ch <- nil
			return ch
		},
	}

	progressCh := make(chan [][]byte, 1)
	unsubCalled := make(chan struct{}, 1)
	unsub := func() { unsubCalled <- struct{}{} }

	cs := &MultiClient{
		sentries:           []proto_sentry.SentryClient{mockSentry},
		statusDataProvider: mockStatus,
		blockReader:        readyReader,
		ChainConfig:        &chain.Config{},
		logger:             log.New(),
	}
	cs.db = newTestDBWithHeader(t)

	cs.SetBlockProgressChannel(progressCh, unsub)

	done := make(chan struct{})
	go func() {
		cs.AnnounceBlockRangeLoop(ctx)
		close(done)
	}()

	if !waitFor(func() bool { return currentSentCount(&sentMu, &sentCount) >= 1 }, time.Second) {
		t.Fatal("expected initial block range announcement")
	}

	baseline := currentSentCount(&sentMu, &sentCount)

	for i := 0; i < int(blockRangeEpochBlocks-1); i++ {
		advanceStatus()
		progressCh <- [][]byte{{0x01}}
	}

	if waitFor(func() bool { return currentSentCount(&sentMu, &sentCount) > baseline }, 200*time.Millisecond) {
		t.Fatalf("unexpected announcement before reaching threshold")
	}

	advanceStatus()
	progressCh <- [][]byte{{0x01}}

	if !waitFor(func() bool { return currentSentCount(&sentMu, &sentCount) == baseline+1 }, time.Second) {
		t.Fatalf("expected announcement after %d blocks", blockRangeEpochBlocks)
	}

	cancel()
	<-done

	select {
	case <-unsubCalled:
	default:
		t.Fatal("expected unsubscribe to be invoked on shutdown")
	}
}

func TestBlockProgressChannelCleanupDoesNotDropNewChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCalled := make(chan struct{})
	var statusOnce sync.Once

	mockStatus := &mockStatusDataProvider{
		getStatusDataFunc: func(ctx context.Context) (*proto_sentry.StatusData, error) {
			statusOnce.Do(func() { close(statusCalled) })
			return &proto_sentry.StatusData{
				MinimumBlockHeight: 1,
				MaxBlockHeight:     1,
				BestHash:           gointerfaces.ConvertHashToH256(common.HexToHash("0x1")),
			}, nil
		},
	}

	readyReader := &mockFullBlockReader{
		readyFunc: func(ctx context.Context) <-chan error {
			ch := make(chan error, 1)
			ch <- nil
			return ch
		},
	}

	progressCh1 := make(chan [][]byte)
	unsub1Called := make(chan struct{}, 2)
	var unsub1Once sync.Once
	unsub1 := func() {
		unsub1Once.Do(func() { close(progressCh1) })
		unsub1Called <- struct{}{}
	}

	cs := &MultiClient{
		statusDataProvider: mockStatus,
		blockReader:        readyReader,
		ChainConfig:        &chain.Config{},
		logger:             log.New(),
	}
	cs.db = newTestDBWithHeader(t)

	cs.SetBlockProgressChannel(progressCh1, unsub1)

	done := make(chan struct{})
	go func() {
		cs.announceBlockRangeFromChannel(ctx, func() bool { return true })
		close(done)
	}()

	select {
	case <-statusCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial status fetch")
	}

	progressCh2 := make(chan [][]byte)
	unsub2 := func() { close(progressCh2) }

	cs.SetBlockProgressChannel(progressCh2, unsub2)

	select {
	case <-unsub1Called:
	case <-time.After(time.Second):
		t.Fatal("expected previous unsubscribe to run")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("announceBlockRangeFromChannel did not exit")
	}

	if got := cs.getBlockProgressChannel(); got != progressCh2 {
		t.Fatal("expected new channel to remain registered after cleanup")
	}

	select {
	case <-unsub1Called:
		t.Fatal("unsubscribe called more than once")
	default:
	}
}

func TestAnnounceBlockRangeLoopWaitsForChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testMinimumBlockHeight := uint64(5)
	testLatestBlockHeight := uint64(10)
	testBestHash := common.HexToHash("0xabc")

	var (
		sentMu    sync.Mutex
		sentCount int
	)

	mockSentry := &mockSentryClient{
		sendMessageToAllFunc: func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			sentMu.Lock()
			sentCount++
			sentMu.Unlock()
			return &proto_sentry.SentPeers{}, nil
		},
		handShakeFunc: func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error) {
			return &proto_sentry.HandShakeReply{Protocol: proto_sentry.Protocol_ETH69}, nil
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

	readyReader := &mockFullBlockReader{
		readyFunc: func(ctx context.Context) <-chan error {
			ch := make(chan error, 1)
			ch <- nil
			return ch
		},
	}

	cs := &MultiClient{
		sentries:           []proto_sentry.SentryClient{mockSentry},
		statusDataProvider: mockStatus,
		blockReader:        readyReader,
		ChainConfig:        &chain.Config{},
		logger:             log.New(),
	}
	cs.db = newTestDBWithHeader(t)

	done := make(chan struct{})
	go func() {
		cs.AnnounceBlockRangeLoop(ctx)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("announce loop exited before channel was configured")
	case <-time.After(200 * time.Millisecond):
	}

	progressCh := make(chan [][]byte, 1)
	cs.SetBlockProgressChannel(progressCh, func() {})

	if !waitFor(func() bool { return currentSentCount(&sentMu, &sentCount) >= 1 }, time.Second) {
		t.Fatal("expected announcement after channel configuration")
	}

	cancel()
	<-done
}

func currentSentCount(mu *sync.Mutex, count *int) int {
	mu.Lock()
	defer mu.Unlock()
	return *count
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

func newTestDBWithHeader(t *testing.T) kv.TemporalRwDB {
	t.Helper()

	tdb := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	header := &types.Header{
		ParentHash:  common.Hash{},
		UncleHash:   common.Hash{},
		Root:        common.Hash{},
		TxHash:      common.Hash{},
		ReceiptHash: common.Hash{},
		Difficulty:  big.NewInt(1),
		Number:      big.NewInt(1),
		GasLimit:    30_000_000,
		GasUsed:     0,
		Time:        uint64(time.Now().Unix()),
		Extra:       []byte{},
	}

	err := tdb.Update(context.Background(), func(tx kv.RwTx) error {
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}
		if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
			return err
		}
		if err := rawdb.WriteBody(tx, header.Hash(), header.Number.Uint64(), &types.Body{}); err != nil {
			return err
		}
		rawdb.WriteHeadHeaderHash(tx, header.Hash())
		rawdb.WriteHeadBlockHash(tx, header.Hash())
		return nil
	})
	if err != nil {
		t.Fatalf("initialise temp db: %v", err)
	}

	return tdb
}

// Mock implementations
func TestMultiClient_AnnounceBlockRangeLoop_SkipInvalidRanges(t *testing.T) {
	ctx := context.Background()
	nonZeroHash := common.HexToHash("0x1")

	testcases := []struct {
		name   string
		status *proto_sentry.StatusData
	}{
		{
			name: "earliestGreaterThanLatest",
			status: &proto_sentry.StatusData{
				MinimumBlockHeight: 10,
				MaxBlockHeight:     5,
				BestHash:           gointerfaces.ConvertHashToH256(nonZeroHash),
			},
		},
		{
			name: "zeroBestHash",
			status: &proto_sentry.StatusData{
				MinimumBlockHeight: 5,
				MaxBlockHeight:     10,
				BestHash:           gointerfaces.ConvertHashToH256(common.Hash{}),
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockSentry := &mockSentryClient{
				handShakeFunc: func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*proto_sentry.HandShakeReply, error) {
					t.Fatalf("handshake should not be called for invalid status %q", tc.name)
					return nil, nil
				},
				sendMessageToAllFunc: func(ctx context.Context, req *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
					t.Fatalf("sendMessageToAll should not be called for invalid status %q", tc.name)
					return nil, nil
				},
			}

			cs := &MultiClient{
				sentries: []proto_sentry.SentryClient{mockSentry},
				statusDataProvider: &mockStatusDataProvider{
					getStatusDataFunc: func(context.Context) (*proto_sentry.StatusData, error) {
						return tc.status, nil
					},
				},
				logger: log.New(),
			}

			packet, err := cs.buildBlockRangePacket(ctx)
			if err != nil {
				t.Fatalf("buildBlockRangePacket: %v", err)
			}
			if cs.blockRange.decide(packet) {
				cs.broadcastBlockRange(ctx, packet)
			}
		})
	}
}

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
