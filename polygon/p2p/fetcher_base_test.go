package p2p

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	erigonlibtypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

func TestFetcherFetchHeaders(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockInboundMessages := []*sentry.InboundMessage{
		{
			// should get filtered because it is from a different peer id
			PeerId: PeerIdFromUint64(2).H512(),
		},
		{
			// should get filtered because it is from a different request id
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId*2, 2),
		},
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId, 2),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           2,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 3, peerId)
		require.NoError(t, err)
		require.Len(t, headers, 2)
		require.Equal(t, uint64(1), headers[0].Number.Uint64())
		require.Equal(t, uint64(2), headers[1].Number.Uint64())
	})
}

func TestFetcherFetchHeadersWithChunking(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeaders := newMockBlockHeaders(1999)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// 1024 headers in first response
			Data: blockHeadersPacket66Bytes(t, requestId1, mockHeaders[:1025]),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           1024,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// remaining 975 headers in second response
			Data: blockHeadersPacket66Bytes(t, requestId2, mockHeaders[1025:]),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1025,
		wantRequestAmount:           975,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 2000, peerId)
		require.NoError(t, err)
		require.Len(t, headers, 1999)
		require.Equal(t, uint64(1), headers[0].Number.Uint64())
		require.Equal(t, uint64(1999), headers[len(headers)-1].Number.Uint64())
	})
}

func TestFetcherFetchHeadersResponseTimeout(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// requestId2 takes too long and causes response timeout
			Data: nil,
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           10,
		// cause response timeout
		responseDelay: 600 * time.Millisecond,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// requestId2 takes too long and causes response timeout
			Data: nil,
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           10,
		// cause response timeout
		responseDelay: 600 * time.Millisecond,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 11, peerId)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, headers)
	})
}

func TestFetcherFetchHeadersResponseTimeoutRetrySuccess(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	mockHeaders := newMockBlockHeaders(1999)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// 1024 headers in first response
			Data: blockHeadersPacket66Bytes(t, requestId1, mockHeaders[:1025]),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           1024,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// requestId2 takes too long and causes response timeout
			Data: nil,
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1025,
		wantRequestAmount:           975,
		// cause response timeout
		responseDelay: 600 * time.Millisecond,
	}
	requestId3 := uint64(1236)
	mockInboundMessages3 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			// remaining 975 headers in third response
			Data: blockHeadersPacket66Bytes(t, requestId3, mockHeaders[1025:]),
		},
	}
	mockRequestResponse3 := requestResponseMock{
		requestId:                   requestId3,
		mockResponseInboundMessages: mockInboundMessages3,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1025,
		wantRequestAmount:           975,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId1, requestId2, requestId3))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2, mockRequestResponse3)
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 2000, peerId)
		require.NoError(t, err)
		require.Len(t, headers, 1999)
		require.Equal(t, uint64(1), headers[0].Number.Uint64())
		require.Equal(t, uint64(1999), headers[len(headers)-1].Number.Uint64())
	})
}

func TestFetcherErrInvalidFetchHeadersRange(t *testing.T) {
	t.Parallel()

	test := newFetcherTest(t, newMockRequestGenerator(1))
	test.mockSentryStreams()
	test.run(func(ctx context.Context, t *testing.T) {
		headers, err := test.fetcher.FetchHeaders(ctx, 3, 1, PeerIdFromUint64(1))
		var errInvalidFetchHeadersRange *ErrInvalidFetchHeadersRange
		require.ErrorAs(t, err, &errInvalidFetchHeadersRange)
		require.Equal(t, uint64(3), errInvalidFetchHeadersRange.start)
		require.Equal(t, uint64(1), errInvalidFetchHeadersRange.end)
		require.Nil(t, headers)
	})
}

func TestFetcherErrIncompleteHeaders(t *testing.T) {
	t.Parallel()

	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockInboundMessages := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId, 2),
		},
	}
	mockRequestResponse := requestResponseMock{
		requestId:                   requestId,
		mockResponseInboundMessages: mockInboundMessages,
		wantRequestPeerId:           peerId,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           3,
	}

	test := newFetcherTest(t, newMockRequestGenerator(requestId))
	test.mockSentryStreams(mockRequestResponse)
	test.run(func(ctx context.Context, t *testing.T) {
		var errIncompleteHeaders *ErrIncompleteHeaders
		headers, err := test.fetcher.FetchHeaders(ctx, 1, 4, peerId)
		require.ErrorAs(t, err, &errIncompleteHeaders)
		require.Equal(t, uint64(3), errIncompleteHeaders.LowestMissingBlockNum())
		require.Nil(t, headers)
	})
}

func newFetcherTest(t *testing.T, requestIdGenerator RequestIdGenerator) *fetcherTest {
	fetcherConfig := FetcherConfig{
		responseTimeout: 200 * time.Millisecond,
		retryBackOff:    time.Second,
		maxRetries:      1,
	}
	messageListenerTest := newMessageListenerTest(t)
	messageListener := messageListenerTest.messageListener
	messageSender := NewMessageSender(messageListenerTest.sentryClient)
	fetcher := newFetcher(fetcherConfig, messageListenerTest.logger, messageListener, messageSender, requestIdGenerator)
	return &fetcherTest{
		messageListenerTest:         messageListenerTest,
		fetcher:                     fetcher,
		headersRequestResponseMocks: map[uint64]requestResponseMock{},
	}
}

type fetcherTest struct {
	*messageListenerTest
	fetcher                     *fetcher
	headersRequestResponseMocks map[uint64]requestResponseMock
	peerEvents                  chan *delayedMessage[*sentry.PeerEvent]
}

func (st *fetcherTest) mockSentryStreams(mocks ...requestResponseMock) {
	// default mocks
	st.sentryClient.
		EXPECT().
		HandShake(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	st.sentryClient.
		EXPECT().
		SetStatus(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	st.sentryClient.
		EXPECT().
		MarkDisconnected().
		AnyTimes()

	st.mockSentryInboundMessagesStream(mocks...)
	st.mockSentryPeerEventsStream()
}

func (st *fetcherTest) mockSentryInboundMessagesStream(mocks ...requestResponseMock) {
	var numInboundMessages int
	for _, mock := range mocks {
		numInboundMessages += len(mock.mockResponseInboundMessages)
		st.headersRequestResponseMocks[mock.requestId] = mock
	}

	inboundMessageStreamChan := make(chan *delayedMessage[*sentry.InboundMessage], numInboundMessages)
	mockSentryInboundMessagesStream := &mockSentryMessagesStream[*sentry.InboundMessage]{
		ctx:    st.ctx,
		stream: inboundMessageStreamChan,
	}

	st.sentryClient.
		EXPECT().
		Messages(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockSentryInboundMessagesStream, nil).
		AnyTimes()
	st.sentryClient.
		EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentry.SentPeers, error) {
			if sentry.MessageId_GET_BLOCK_HEADERS_66 != req.Data.Id {
				return nil, fmt.Errorf("MessageId_GET_BLOCK_HEADERS_66 != req.Data.Id - %v", req.Data.Id)
			}

			var pkt eth.GetBlockHeadersPacket66
			if err := rlp.DecodeBytes(req.Data.Data, &pkt); err != nil {
				return nil, err
			}

			mock, ok := st.headersRequestResponseMocks[pkt.RequestId]
			if !ok {
				return &sentry.SentPeers{}, nil
			}

			delete(st.headersRequestResponseMocks, pkt.RequestId)
			reqPeerId := PeerIdFromH512(req.PeerId)
			if !mock.wantRequestPeerId.Equal(reqPeerId) {
				return nil, fmt.Errorf("wantRequestPeerId != reqPeerId - %v vs %v", mock.wantRequestPeerId, reqPeerId)
			}

			if mock.wantRequestOriginNumber != pkt.Origin.Number {
				return nil, fmt.Errorf("wantRequestOriginNumber != pkt.Origin.Number - %v vs %v", mock.wantRequestOriginNumber, pkt.Origin.Number)
			}

			if mock.wantRequestAmount != pkt.Amount {
				return nil, fmt.Errorf("wantRequestAmount != pkt.Amount - %v vs %v", mock.wantRequestAmount, pkt.Amount)
			}

			for _, inboundMessage := range mock.mockResponseInboundMessages {
				inboundMessageStreamChan <- &delayedMessage[*sentry.InboundMessage]{
					message:       inboundMessage,
					responseDelay: mock.responseDelay,
				}
			}

			return &sentry.SentPeers{
				Peers: []*erigonlibtypes.H512{req.PeerId},
			}, nil
		}).
		AnyTimes()
}

func (st *fetcherTest) mockSentryPeerEventsStream() {
	peerConnectEvents := []*sentry.PeerEvent{
		{
			EventId: sentry.PeerEvent_Connect,
			PeerId:  PeerIdFromUint64(1).H512(),
		},
		{
			EventId: sentry.PeerEvent_Connect,
			PeerId:  PeerIdFromUint64(2).H512(),
		},
	}

	streamChan := make(chan *delayedMessage[*sentry.PeerEvent], len(peerConnectEvents))
	for _, event := range peerConnectEvents {
		streamChan <- &delayedMessage[*sentry.PeerEvent]{
			message: event,
		}
	}

	st.peerEvents = streamChan
	st.sentryClient.
		EXPECT().
		PeerEvents(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockSentryMessagesStream[*sentry.PeerEvent]{
			ctx:    st.ctx,
			stream: streamChan,
		}, nil).
		AnyTimes()
}

func (st *fetcherTest) mockDisconnectPeerEvent(peerId *PeerId) {
	st.peerEvents <- &delayedMessage[*sentry.PeerEvent]{
		message: &sentry.PeerEvent{
			EventId: sentry.PeerEvent_Disconnect,
			PeerId:  peerId.H512(),
		},
	}
}

type requestResponseMock struct {
	requestId                   uint64
	mockResponseInboundMessages []*sentry.InboundMessage
	wantRequestPeerId           *PeerId
	wantRequestOriginNumber     uint64
	wantRequestAmount           uint64
	responseDelay               time.Duration
}

func newMockRequestGenerator(requestIds ...uint64) RequestIdGenerator {
	var idx int
	idxPtr := &idx
	return func() uint64 {
		if *idxPtr >= len(requestIds) {
			panic("mock request generator does not have any request ids left")
		}

		res := requestIds[*idxPtr]
		*idxPtr++
		return res
	}
}
