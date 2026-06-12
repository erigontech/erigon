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

package p2p

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/generics"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/protocols/wit"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

func TestMessageListenerRegisterBlockHeadersObserver(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *DecodedInboundMessage[*eth.BlockHeadersPacket66]) {
			require.Equal(t, peerId, message.PeerId)
			require.Equal(t, uint64(1), message.Decoded.RequestId)
			require.Len(t, message.Decoded.BlockHeadersPacket, 2)
			require.Equal(t, uint64(1), message.Decoded.BlockHeadersPacket[0].Number.Uint64())
			require.Equal(t, uint64(2), message.Decoded.BlockHeadersPacket[1].Number.Uint64())
			done.Store(true)
		}

		unregister := test.messageListener.RegisterBlockHeadersObserver(observer)
		t.Cleanup(unregister)

		test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
			message: &sentryproto.InboundMessage{
				Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
				PeerId: peerId.H512(),
				Data:   newMockBlockHeadersPacket66Bytes(t, 1, 2),
			},
		}

		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func TestMessageListenerRegisterPeerEventObserver(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *sentryproto.PeerEvent) {
			require.Equal(t, peerId.H512(), message.PeerId)
			require.Equal(t, sentryproto.PeerEvent_Connect, message.EventId)
			done.Store(true)
		}

		unregister := test.messageListener.RegisterPeerEventObserver(observer)
		t.Cleanup(unregister)

		test.peerEventsStream <- &delayedMessage[*sentryproto.PeerEvent]{
			message: &sentryproto.PeerEvent{
				PeerId:  peerId.H512(),
				EventId: sentryproto.PeerEvent_Connect,
			},
		}

		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func TestMessageListenerRegisterNewBlockObserver(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *DecodedInboundMessage[*eth.NewBlockPacket]) {
			require.Equal(t, peerId, message.PeerId)
			require.Equal(t, uint64(1), message.Decoded.Block.NumberU64())
			done.Store(true)
		}

		unregister := test.messageListener.RegisterNewBlockObserver(observer)
		t.Cleanup(unregister)

		test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
			message: &sentryproto.InboundMessage{
				Id:     sentryproto.MessageId_NEW_BLOCK_66,
				PeerId: peerId.H512(),
				Data:   newMockNewBlockPacketBytes(t),
			},
		}

		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func TestMessageListenerRegisterNewBlockHashesObserver(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
			require.Equal(t, peerId, message.PeerId)
			require.Len(t, *message.Decoded, 1)
			require.Equal(t, uint64(1), (*message.Decoded)[0].Number)
			done.Store(true)
		}

		unregister := test.messageListener.RegisterNewBlockHashesObserver(observer)
		t.Cleanup(unregister)

		test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
			message: &sentryproto.InboundMessage{
				Id:     sentryproto.MessageId_NEW_BLOCK_HASHES_66,
				PeerId: peerId.H512(),
				Data:   newMockNewBlockHashesPacketBytes(t),
			},
		}

		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func TestMessageListenerRegisterBlockBodiesObserver(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *DecodedInboundMessage[*eth.BlockBodiesPacket66]) {
			require.Equal(t, peerId, message.PeerId)
			require.Equal(t, uint64(23), message.Decoded.RequestId)
			require.Len(t, message.Decoded.BlockBodiesPacket, 1)
			done.Store(true)
		}

		unregister := test.messageListener.RegisterBlockBodiesObserver(observer)
		t.Cleanup(unregister)

		test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
			message: &sentryproto.InboundMessage{
				Id:     sentryproto.MessageId_BLOCK_BODIES_66,
				PeerId: peerId.H512(),
				Data:   newMockBlockBodiesPacketBytes(t, 23, &types.Body{}),
			},
		}

		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func TestMessageListenerShouldPenalizePeerWhenErrInvalidRlp(t *testing.T) {
	t.Parallel()

	peerId1 := PeerIdFromUint64(1)
	peerId2 := PeerIdFromUint64(2)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	mockExpectPenalizePeer(t, test.sentryClient, peerId1)
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *DecodedInboundMessage[*eth.BlockHeadersPacket66]) {
			require.Equal(t, peerId2, message.PeerId)
			done.Store(true)
		}

		unregister := test.messageListener.RegisterBlockHeadersObserver(observer)
		t.Cleanup(unregister)

		test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
			message: &sentryproto.InboundMessage{
				Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
				PeerId: peerId1.H512(),
				Data:   []byte{'i', 'n', 'v', 'a', 'l', 'i', 'd', '.', 'r', 'l', 'p'},
			},
		}

		test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
			message: &sentryproto.InboundMessage{
				Id:     sentryproto.MessageId_BLOCK_HEADERS_66,
				PeerId: peerId2.H512(),
				Data:   newMockBlockHeadersPacket66Bytes(t, 1, 1),
			},
		}

		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

// TestMessageListenerRoutesResponderMessages covers subscription + dispatch for
// every message id added for the responder components: each id must arrive on
// the right stream, decode, and reach its observer registry.
func TestMessageListenerRoutesResponderMessages(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	blockHash := common.HexToHash("0xdeadbeef")
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		mustEncode := func(packet any) []byte {
			encoded, err := rlp.EncodeToBytes(packet)
			require.NoError(t, err)
			return encoded
		}
		expected := map[sentryproto.MessageId]*atomic.Bool{
			sentryproto.MessageId_GET_BLOCK_HEADERS_66:      {},
			sentryproto.MessageId_GET_BLOCK_BODIES_66:       {},
			sentryproto.MessageId_GET_RECEIPTS_66:           {},
			sentryproto.MessageId_GET_RECEIPTS_69:           {},
			sentryproto.MessageId_GET_RECEIPTS_70:           {},
			sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71: {},
			sentryproto.MessageId_BLOCK_RANGE_UPDATE_69:     {},
			sentryproto.MessageId_GET_BLOCK_WITNESS_W0:      {},
			sentryproto.MessageId_BLOCK_WITNESS_W0:          {},
			sentryproto.MessageId_NEW_WITNESS_W0:            {},
		}
		seen := func(id sentryproto.MessageId) {
			expected[id].Store(true)
		}
		t.Cleanup(test.messageListener.RegisterGetBlockHeadersObserver(func(message *DecodedInboundMessage[*eth.GetBlockHeadersPacket66]) {
			require.Equal(t, uint64(11), message.Decoded.RequestId)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterGetBlockBodiesObserver(func(message *DecodedInboundMessage[*eth.GetBlockBodiesPacket66]) {
			require.Equal(t, uint64(12), message.Decoded.RequestId)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterGetReceiptsObserver(func(message *DecodedInboundMessage[*eth.GetReceiptsPacket66]) {
			require.Equal(t, uint64(13), message.Decoded.RequestId)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterGetReceipts70Observer(func(message *DecodedInboundMessage[*eth.GetReceiptsPacket70]) {
			require.Equal(t, uint64(14), message.Decoded.RequestId)
			require.Equal(t, uint64(5), message.Decoded.FirstBlockReceiptIndex)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterGetBlockAccessListsObserver(func(message *DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]) {
			require.Equal(t, uint64(15), message.Decoded.RequestId)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterBlockRangeUpdateObserver(func(message *DecodedInboundMessage[*eth.BlockRangeUpdatePacket]) {
			require.Equal(t, uint64(16), message.Decoded.Latest)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterGetWitnessObserver(func(message *DecodedInboundMessage[*wit.GetWitnessPacket]) {
			require.Equal(t, uint64(17), message.Decoded.RequestId)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterWitnessObserver(func(message *DecodedInboundMessage[*wit.WitnessPacketRLPPacket]) {
			require.Equal(t, uint64(18), message.Decoded.RequestId)
			seen(message.Id)
		}))
		t.Cleanup(test.messageListener.RegisterNewWitnessObserver(func(message *DecodedInboundMessage[*wit.NewWitnessPacket]) {
			require.NotNil(t, message.Decoded.Witness)
			seen(message.Id)
		}))
		witness := createTestWitness(t, &types.Header{Number: *uint256.NewInt(100)})
		payloads := map[sentryproto.MessageId][]byte{
			sentryproto.MessageId_GET_BLOCK_HEADERS_66: mustEncode(eth.GetBlockHeadersPacket66{
				RequestId:             11,
				GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{Origin: eth.HashOrNumber{Number: 1}, Amount: 2},
			}),
			sentryproto.MessageId_GET_BLOCK_BODIES_66: mustEncode(eth.GetBlockBodiesPacket66{
				RequestId:            12,
				GetBlockBodiesPacket: eth.GetBlockBodiesPacket{blockHash},
			}),
			sentryproto.MessageId_GET_RECEIPTS_66: mustEncode(eth.GetReceiptsPacket66{
				RequestId:         13,
				GetReceiptsPacket: eth.GetReceiptsPacket{blockHash},
			}),
			sentryproto.MessageId_GET_RECEIPTS_69: mustEncode(eth.GetReceiptsPacket66{
				RequestId:         13,
				GetReceiptsPacket: eth.GetReceiptsPacket{blockHash},
			}),
			sentryproto.MessageId_GET_RECEIPTS_70: mustEncode(eth.GetReceiptsPacket70{
				RequestId:              14,
				FirstBlockReceiptIndex: 5,
				GetReceiptsPacket:      eth.GetReceiptsPacket{blockHash},
			}),
			sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71: mustEncode(eth.GetBlockAccessListsPacket66{
				RequestId:                 15,
				GetBlockAccessListsPacket: eth.GetBlockAccessListsPacket{blockHash},
			}),
			sentryproto.MessageId_BLOCK_RANGE_UPDATE_69: mustEncode(eth.BlockRangeUpdatePacket{
				Earliest:   1,
				Latest:     16,
				LatestHash: blockHash,
			}),
			sentryproto.MessageId_GET_BLOCK_WITNESS_W0: mustEncode(wit.GetWitnessPacket{
				RequestId:         17,
				GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{{Hash: blockHash, Page: 0}}},
			}),
			sentryproto.MessageId_BLOCK_WITNESS_W0: mustEncode(wit.WitnessPacketRLPPacket{
				RequestId:             18,
				WitnessPacketResponse: wit.WitnessPacketResponse{{Hash: blockHash, Page: 0, TotalPages: 1, Data: []byte{0x01}}},
			}),
			sentryproto.MessageId_NEW_WITNESS_W0: mustEncode(wit.NewWitnessPacket{Witness: witness}),
		}
		for id, payload := range payloads {
			test.inboundMessagesStream <- &delayedMessage[*sentryproto.InboundMessage]{
				message: &sentryproto.InboundMessage{
					Id:     id,
					PeerId: peerId.H512(),
					Data:   payload,
				},
			}
		}
		require.Eventually(t, func() bool {
			for _, done := range expected {
				if !done.Load() {
					return false
				}
			}
			return true
		}, 2*time.Second, 5*time.Millisecond)
	})
}

func newMessageListenerTest(t *testing.T) *messageListenerTest {
	ctx, cancel := context.WithCancel(context.Background())
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	router := newInboundMessagesRouter(ctx, 0)
	peerEventsStream := make(chan *delayedMessage[*sentryproto.PeerEvent])
	sentryClient := direct.NewMockSentryClient(ctrl)
	statusDataFactory := libsentry.StatusDataFactory(func(ctx context.Context) (*sentryproto.StatusData, error) {
		return &sentryproto.StatusData{}, nil
	})
	peerPenalizer := NewPeerPenalizer(sentryClient)
	return &messageListenerTest{
		ctx:                   ctx,
		ctxCancel:             cancel,
		t:                     t,
		logger:                logger,
		sentryClient:          sentryClient,
		messageListener:       NewMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer),
		router:                router,
		inboundMessagesStream: router.input,
		peerEventsStream:      peerEventsStream,
	}
}

type messageListenerTest struct {
	ctx                   context.Context
	ctxCancel             context.CancelFunc
	t                     *testing.T
	logger                log.Logger
	sentryClient          *direct.MockSentryClient
	messageListener       *MessageListener
	router                *inboundMessagesRouter
	inboundMessagesStream chan *delayedMessage[*sentryproto.InboundMessage]
	peerEventsStream      chan *delayedMessage[*sentryproto.PeerEvent]
}

// run is needed so that we can properly shut down tests due to how the sentry multi client
// SentryReconnectAndPumpStreamLoop works.
//
// Using t.Cleanup to call fetcher.Stop instead does not work since the mocks generated by gomock cause
// an error when their methods are called after a test has finished - t.Cleanup is run after a
// test has finished, and so we need to make sure that the SentryReconnectAndPumpStreamLoop loop has been stopped
// before the test finishes otherwise we will have flaky tests.
//
// If changing the behaviour here please run "go test -v -count=1000" and "go test -v -count=1 -race" to confirm there
// are no regressions.
func (mlt *messageListenerTest) run(f func(ctx context.Context, t *testing.T)) {
	var done atomic.Bool
	mlt.t.Run("start", func(t *testing.T) {
		go func() {
			defer done.Store(true)
			err := mlt.messageListener.Run(mlt.ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()
	})

	mlt.t.Run("test", func(t *testing.T) {
		f(mlt.ctx, t)
	})

	mlt.t.Run("stop", func(t *testing.T) {
		mlt.ctxCancel()
		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func (mlt *messageListenerTest) mockSentryStreams() {
	mlt.sentryClient.
		EXPECT().
		HandShake(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	mlt.sentryClient.
		EXPECT().
		SetStatus(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	mlt.sentryClient.
		EXPECT().
		MarkDisconnected().
		AnyTimes()
	mlt.sentryClient.
		EXPECT().
		Messages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(mlt.router.messagesFactory()).
		AnyTimes()
	mlt.sentryClient.
		EXPECT().
		PeerEvents(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockSentryMessagesStream[*sentryproto.PeerEvent]{
			ctx:    mlt.ctx,
			stream: mlt.peerEventsStream,
		}, nil).
		AnyTimes()
}

type delayedMessage[M any] struct {
	message       M
	responseDelay time.Duration
}

// inboundMessagesRouter mimics the sentry server's subscription semantics for
// tests: each Messages call gets its own stream receiving only the message ids
// it subscribed to. Needed because the MessageListener opens several streams
// with disjoint id sets over one mocked sentry client.
type inboundMessagesRouter struct {
	ctx   context.Context
	mu    sync.Mutex
	subs  []*inboundMessagesSub
	input chan *delayedMessage[*sentryproto.InboundMessage]
}

type inboundMessagesSub struct {
	ids map[sentryproto.MessageId]struct{}
	ch  chan *delayedMessage[*sentryproto.InboundMessage]
}

func newInboundMessagesRouter(ctx context.Context, bufSize int) *inboundMessagesRouter {
	r := &inboundMessagesRouter{
		ctx:   ctx,
		input: make(chan *delayedMessage[*sentryproto.InboundMessage], bufSize),
	}
	go r.dispatch()
	return r
}

func (r *inboundMessagesRouter) messagesFactory() func(ctx context.Context, req *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
	return func(_ context.Context, req *sentryproto.MessagesRequest, _ ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
		ids := make(map[sentryproto.MessageId]struct{}, len(req.Ids))
		for _, id := range req.Ids {
			ids[id] = struct{}{}
		}
		sub := &inboundMessagesSub{
			ids: ids,
			ch:  make(chan *delayedMessage[*sentryproto.InboundMessage], 128),
		}
		r.mu.Lock()
		r.subs = append(r.subs, sub)
		r.mu.Unlock()
		return &mockSentryMessagesStream[*sentryproto.InboundMessage]{ctx: r.ctx, stream: sub.ch}, nil
	}
}

func (r *inboundMessagesRouter) dispatch() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-r.input:
			// retry until a subscriber for this message id appears (streams
			// subscribe asynchronously after the listener starts)
			for !r.trySend(msg) {
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(time.Millisecond):
				}
			}
		}
	}
}

func (r *inboundMessagesRouter) trySend(msg *delayedMessage[*sentryproto.InboundMessage]) bool {
	r.mu.Lock()
	var targets []chan *delayedMessage[*sentryproto.InboundMessage]
	for _, sub := range r.subs {
		if _, ok := sub.ids[msg.message.Id]; ok {
			targets = append(targets, sub.ch)
		}
	}
	r.mu.Unlock()
	if len(targets) == 0 {
		return false
	}
	for _, target := range targets {
		select {
		case <-r.ctx.Done():
		case target <- msg:
		}
	}
	return true
}

type mockSentryMessagesStream[M any] struct {
	ctx    context.Context
	stream <-chan *delayedMessage[M]
}

func (s *mockSentryMessagesStream[M]) Recv() (M, error) {
	return generics.Zero[M](), nil
}

func (s *mockSentryMessagesStream[M]) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *mockSentryMessagesStream[M]) Trailer() metadata.MD {
	return nil
}

func (s *mockSentryMessagesStream[M]) CloseSend() error {
	return nil
}

func (s *mockSentryMessagesStream[M]) Context() context.Context {
	return s.ctx
}

func (s *mockSentryMessagesStream[M]) SendMsg(_ any) error {
	return nil
}

func (s *mockSentryMessagesStream[M]) RecvMsg(msg any) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case mockMsg := <-s.stream:
		if mockMsg.responseDelay > time.Duration(0) {
			time.Sleep(mockMsg.responseDelay)
		}

		switch any(mockMsg.message).(type) {
		case *sentryproto.InboundMessage:
			msg, ok := msg.(*sentryproto.InboundMessage)
			if !ok {
				return errors.New("unexpected msg type")
			}

			mockMsg := any(mockMsg.message).(*sentryproto.InboundMessage)
			msg.Id = mockMsg.Id
			msg.Data = mockMsg.Data
			msg.PeerId = mockMsg.PeerId
		case *sentryproto.PeerEvent:
			msg, ok := msg.(*sentryproto.PeerEvent)
			if !ok {
				return errors.New("unexpected msg type")
			}

			mockMsg := any(mockMsg.message).(*sentryproto.PeerEvent)
			msg.PeerId = mockMsg.PeerId
			msg.EventId = mockMsg.EventId
		default:
			return errors.New("unsupported type")
		}

		return nil
	}
}

func newMockBlockHeadersPacket66Bytes(t *testing.T, requestId uint64, numHeaders int) []byte {
	headers := newMockBlockHeaders(numHeaders)
	return blockHeadersPacket66Bytes(t, requestId, headers)
}

func newMockBlockHeaders(numHeaders int) []*types.Header {
	headers := make([]*types.Header, numHeaders)
	var parentHeader *types.Header
	for i := range headers {
		var parentHash common.Hash
		if parentHeader != nil {
			parentHash = parentHeader.Hash()
		}

		headers[i] = &types.Header{
			Number:     *uint256.NewInt(uint64(i) + 1),
			ParentHash: parentHash,
		}

		parentHeader = headers[i]
	}

	return headers
}

func blockHeadersPacket66Bytes(t *testing.T, requestId uint64, headers []*types.Header) []byte {
	blockHeadersPacket66 := eth.BlockHeadersPacket66{
		RequestId:          requestId,
		BlockHeadersPacket: headers,
	}
	blockHeadersPacket66Bytes, err := rlp.EncodeToBytes(&blockHeadersPacket66)
	require.NoError(t, err)
	return blockHeadersPacket66Bytes
}

func newMockNewBlockPacketBytes(t *testing.T) []byte {
	newBlockPacket := eth.NewBlockPacket{
		Block: types.NewBlock(newMockBlockHeaders(1)[0], nil, nil, nil, nil),
	}
	newBlockPacketBytes, err := rlp.EncodeToBytes(&newBlockPacket)
	require.NoError(t, err)
	return newBlockPacketBytes
}

func newMockNewBlockHashesPacketBytes(t *testing.T) []byte {
	newBlockHashesPacket := eth.NewBlockHashesPacket{
		{
			Number: 1,
		},
	}
	newBlockHashesPacketBytes, err := rlp.EncodeToBytes(&newBlockHashesPacket)
	require.NoError(t, err)
	return newBlockHashesPacketBytes
}

func newMockBlockBodiesPacketBytes(t *testing.T, requestId uint64, bodies ...*types.Body) []byte {
	newBlockHashesPacket := eth.BlockBodiesPacket66{
		RequestId:         requestId,
		BlockBodiesPacket: bodies,
	}
	newBlockHashesPacketBytes, err := rlp.EncodeToBytes(&newBlockHashesPacket)
	require.NoError(t, err)
	return newBlockHashesPacketBytes
}

func mockExpectPenalizePeer(t *testing.T, sentryClient *direct.MockSentryClient, peerId *PeerId) {
	sentryClient.EXPECT().
		PenalizePeer(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			require.Equal(t, peerId, PeerIdFromH512(req.PeerId))
			return &emptypb.Empty{}, nil
		}).
		Times(1)
}
