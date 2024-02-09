package p2p

import (
	"context"
	"errors"
	"io"
	"math/big"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func newMockRequestGenerator(reqId uint64) requestIdGenerator {
	return func() uint64 {
		return reqId
	}
}

func newServiceTest(ctx context.Context, t *testing.T, reqIdGen requestIdGenerator) *serviceTest {
	ctrl := gomock.NewController(t)
	logger := testlog.Logger(t, log.LvlTrace)
	sentryClient := direct.NewMockSentryClient(ctrl)
	return &serviceTest{
		sentryClient: sentryClient,
		service:      newService(ctx, logger, sentryClient, reqIdGen),
	}
}

type serviceTest struct {
	sentryClient *direct.MockSentryClient
	service      Service
}

type sendMessageByIdMock func(context.Context, *sentry.SendMessageByIdRequest, ...grpc.CallOption) (*sentry.SentPeers, error)

func (st serviceTest) mockSendGetBlockHeaders66(
	t *testing.T,
	wg *sync.WaitGroup,
	wantPeerId PeerId,
	wantMessageId sentry.MessageId,
	wantOriginNumber uint64,
	wantAmount uint64,
) sendMessageByIdMock {
	return func(_ context.Context, req *sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentry.SentPeers, error) {
		defer wg.Done()
		reqPeerId := PeerIdFromH512(req.PeerId)
		require.Equal(t, wantPeerId, reqPeerId)
		require.Equal(t, wantMessageId, req.Data.Id)
		var pkt eth.GetBlockHeadersPacket66
		err := rlp.DecodeBytes(req.Data.Data, &pkt)
		require.NoError(t, err)
		require.Equal(t, wantOriginNumber, pkt.Origin.Number)
		require.Equal(t, wantAmount, pkt.Amount)
		return nil, nil
	}
}

func (st serviceTest) mockSentryStream(wg *sync.WaitGroup, msgs []*sentry.InboundMessage) sentry.Sentry_MessagesClient {
	return &mockSentryMessagesStream{
		wg:   wg,
		msgs: msgs,
	}
}

type mockSentryMessagesStream struct {
	wg   *sync.WaitGroup
	msgs []*sentry.InboundMessage
}

func (s *mockSentryMessagesStream) Recv() (*sentry.InboundMessage, error) {
	return nil, nil
}

func (s *mockSentryMessagesStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *mockSentryMessagesStream) Trailer() metadata.MD {
	return nil
}

func (s *mockSentryMessagesStream) CloseSend() error {
	return nil
}

func (s *mockSentryMessagesStream) Context() context.Context {
	return context.Background()
}

func (s *mockSentryMessagesStream) SendMsg(_ any) error {
	return nil
}

func (s *mockSentryMessagesStream) RecvMsg(msg any) error {
	s.wg.Wait()

	if len(s.msgs) == 0 {
		return io.EOF
	}

	inboundMsg, ok := msg.(*sentry.InboundMessage)
	if !ok {
		return errors.New("unexpected msg type")
	}

	mockMsg := s.msgs[0]
	s.msgs = s.msgs[1:]
	inboundMsg.Id = mockMsg.Id
	inboundMsg.Data = mockMsg.Data
	inboundMsg.PeerId = mockMsg.PeerId
	return nil
}

func newMockBlockHeadersPacket66Bytes(t *testing.T, requestId uint64) []byte {
	blockHeadersPacket66 := eth.BlockHeadersPacket66{
		RequestId: requestId,
		BlockHeadersPacket: []*types.Header{
			{
				Number: big.NewInt(1),
			},
			{
				Number: big.NewInt(2),
			},
			{
				Number: big.NewInt(3),
			},
		},
	}
	blockHeadersPacket66Bytes, err := rlp.EncodeToBytes(&blockHeadersPacket66)
	require.NoError(t, err)
	return blockHeadersPacket66Bytes
}

func TestServiceDownloadHeaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	peerId := PeerIdFromUint64(1)
	requestId := uint64(1234)
	mockInboundMessages := []*sentry.InboundMessage{
		{
			// should get filtered because it is from a different peer id
			PeerId: PeerIdFromUint64(2).H512(),
		},
		{
			// should get filtered because it is for a different msg id
			Id: sentry.MessageId_BLOCK_BODIES_66,
		},
		{
			// should get filtered because it is from a different request id
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId*2),
		},
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId),
		},
	}

	test := newServiceTest(ctx, t, newMockRequestGenerator(requestId))
	test.sentryClient.
		EXPECT().
		Messages(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(test.mockSentryStream(&wg, mockInboundMessages), nil).
		Times(1)
	test.sentryClient.
		EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.mockSendGetBlockHeaders66(t, &wg, peerId, sentry.MessageId_GET_BLOCK_HEADERS_66, 1, 3)).
		Times(1)
	test.sentryClient.
		EXPECT().
		HandShake(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)
	test.sentryClient.
		EXPECT().
		SetStatus(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)
	test.sentryClient.
		EXPECT().
		MarkDisconnected().
		Times(1)

	headers, err := test.service.DownloadHeaders(ctx, 1, 3, peerId)
	require.NoError(t, err)
	require.Len(t, headers, 3)
	require.Equal(t, uint64(1), headers[0].Number.Uint64())
	require.Equal(t, uint64(2), headers[1].Number.Uint64())
	require.Equal(t, uint64(3), headers[2].Number.Uint64())
}

func TestServiceInvalidDownloadHeadersRangeErr(t *testing.T) {
	ctx := context.Background()
	test := newServiceTest(ctx, t, newMockRequestGenerator(1))
	_, err := test.service.DownloadHeaders(ctx, 3, 1, PeerIdFromUint64(1))
	require.ErrorIs(t, err, invalidDownloadHeadersRangeErr)
}
