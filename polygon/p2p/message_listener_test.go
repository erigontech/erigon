package p2p

import (
	"context"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentryproto"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	sentrymulticlient "github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/testlog"
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

		test.inboundMessagesStream <- &delayedMessage[*sentry.InboundMessage]{
			message: &sentry.InboundMessage{
				Id:     sentry.MessageId_BLOCK_HEADERS_66,
				PeerId: peerId.H512(),
				Data:   newMockBlockHeadersPacket66Bytes(t, 1, 2),
			},
		}

		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
	})
}

func TestMessageListenerRegisterPeerEventObserver(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	test := newMessageListenerTest(t)
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		var done atomic.Bool
		observer := func(message *sentry.PeerEvent) {
			require.Equal(t, peerId.H512(), message.PeerId)
			require.Equal(t, sentry.PeerEvent_Connect, message.EventId)
			done.Store(true)
		}

		unregister := test.messageListener.RegisterPeerEventObserver(observer)
		t.Cleanup(unregister)

		test.peerEventsStream <- &delayedMessage[*sentry.PeerEvent]{
			message: &sentry.PeerEvent{
				PeerId:  peerId.H512(),
				EventId: sentry.PeerEvent_Connect,
			},
		}

		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
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
			require.Equal(t, uint64(1), message.Decoded.Block.Number().Uint64())
			done.Store(true)
		}

		unregister := test.messageListener.RegisterNewBlockObserver(observer)
		t.Cleanup(unregister)

		test.inboundMessagesStream <- &delayedMessage[*sentry.InboundMessage]{
			message: &sentry.InboundMessage{
				Id:     sentry.MessageId_NEW_BLOCK_66,
				PeerId: peerId.H512(),
				Data:   newMockNewBlockPacketBytes(t),
			},
		}

		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
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

		test.inboundMessagesStream <- &delayedMessage[*sentry.InboundMessage]{
			message: &sentry.InboundMessage{
				Id:     sentry.MessageId_NEW_BLOCK_HASHES_66,
				PeerId: peerId.H512(),
				Data:   newMockNewBlockHashesPacketBytes(t),
			},
		}

		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
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

		test.inboundMessagesStream <- &delayedMessage[*sentry.InboundMessage]{
			message: &sentry.InboundMessage{
				Id:     sentry.MessageId_BLOCK_BODIES_66,
				PeerId: peerId.H512(),
				Data:   newMockBlockBodiesPacketBytes(t, 23, &types.Body{}),
			},
		}

		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
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

		test.inboundMessagesStream <- &delayedMessage[*sentry.InboundMessage]{
			message: &sentry.InboundMessage{
				Id:     sentry.MessageId_BLOCK_HEADERS_66,
				PeerId: peerId1.H512(),
				Data:   []byte{'i', 'n', 'v', 'a', 'l', 'i', 'd', '.', 'r', 'l', 'p'},
			},
		}

		test.inboundMessagesStream <- &delayedMessage[*sentry.InboundMessage]{
			message: &sentry.InboundMessage{
				Id:     sentry.MessageId_BLOCK_HEADERS_66,
				PeerId: peerId2.H512(),
				Data:   newMockBlockHeadersPacket66Bytes(t, 1, 1),
			},
		}

		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
	})
}

func newMessageListenerTest(t *testing.T) *messageListenerTest {
	ctx, cancel := context.WithCancel(context.Background())
	logger := testlog.Logger(t, log.LvlTrace)
	ctrl := gomock.NewController(t)
	inboundMessagesStream := make(chan *delayedMessage[*sentry.InboundMessage])
	peerEventsStream := make(chan *delayedMessage[*sentry.PeerEvent])
	sentryClient := direct.NewMockSentryClient(ctrl)
	statusDataFactory := sentrymulticlient.StatusDataFactory(func(ctx context.Context) (*sentry.StatusData, error) {
		return &sentry.StatusData{}, nil
	})
	return &messageListenerTest{
		ctx:                   ctx,
		ctxCancel:             cancel,
		t:                     t,
		logger:                logger,
		sentryClient:          sentryClient,
		messageListener:       newMessageListener(logger, sentryClient, statusDataFactory, NewPeerPenalizer(sentryClient)),
		inboundMessagesStream: inboundMessagesStream,
		peerEventsStream:      peerEventsStream,
	}
}

type messageListenerTest struct {
	ctx                   context.Context
	ctxCancel             context.CancelFunc
	t                     *testing.T
	logger                log.Logger
	sentryClient          *direct.MockSentryClient
	messageListener       *messageListener
	inboundMessagesStream chan *delayedMessage[*sentry.InboundMessage]
	peerEventsStream      chan *delayedMessage[*sentry.PeerEvent]
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
	mlt.t.Run("start", func(_ *testing.T) {
		go func() {
			mlt.messageListener.Run(mlt.ctx)
			done.Store(true)
		}()
	})

	mlt.t.Run("test", func(t *testing.T) {
		f(mlt.ctx, t)
	})

	mlt.t.Run("stop", func(t *testing.T) {
		mlt.ctxCancel()
		require.Eventually(t, func() bool { return done.Load() }, time.Second, 5*time.Millisecond)
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
		Return(&mockSentryMessagesStream[*sentry.InboundMessage]{
			ctx:    mlt.ctx,
			stream: mlt.inboundMessagesStream,
		}, nil).
		AnyTimes()
	mlt.sentryClient.
		EXPECT().
		PeerEvents(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockSentryMessagesStream[*sentry.PeerEvent]{
			ctx:    mlt.ctx,
			stream: mlt.peerEventsStream,
		}, nil).
		AnyTimes()
}

type delayedMessage[M any] struct {
	message       M
	responseDelay time.Duration
}

type mockSentryMessagesStream[M any] struct {
	ctx    context.Context
	stream <-chan *delayedMessage[M]
}

func (s *mockSentryMessagesStream[M]) Recv() (M, error) {
	var nilValue M
	return nilValue, nil
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
		case *sentry.InboundMessage:
			msg, ok := msg.(*sentry.InboundMessage)
			if !ok {
				return errors.New("unexpected msg type")
			}

			mockMsg := any(mockMsg.message).(*sentry.InboundMessage)
			msg.Id = mockMsg.Id
			msg.Data = mockMsg.Data
			msg.PeerId = mockMsg.PeerId
		case *sentry.PeerEvent:
			msg, ok := msg.(*sentry.PeerEvent)
			if !ok {
				return errors.New("unexpected msg type")
			}

			mockMsg := any(mockMsg.message).(*sentry.PeerEvent)
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
			Number:     big.NewInt(int64(i) + 1),
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
		Block: types.NewBlock(newMockBlockHeaders(1)[0], nil, nil, nil, nil, nil),
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
		DoAndReturn(func(_ context.Context, req *sentry.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			require.Equal(t, peerId, PeerIdFromH512(req.PeerId))
			return &emptypb.Empty{}, nil
		}).
		Times(1)
}
