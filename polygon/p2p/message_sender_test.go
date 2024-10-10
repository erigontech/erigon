package p2p

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	erigonlibtypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/rlp"
)

func TestMessageSenderSendGetBlockHeaders(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, request *sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentry.SentPeers, error) {
			require.Equal(t, PeerIdFromUint64(123), PeerIdFromH512(request.PeerId))
			require.Equal(t, sentry.MessageId_GET_BLOCK_HEADERS_66, request.Data.Id)
			var payload eth.GetBlockHeadersPacket66
			err := rlp.DecodeBytes(request.Data.Data, &payload)
			require.NoError(t, err)
			require.Equal(t, uint64(10), payload.RequestId)
			require.Equal(t, uint64(3), payload.Origin.Number)
			require.Equal(t, uint64(5), payload.Amount)
			return &sentry.SentPeers{
				Peers: []*erigonlibtypes.H512{
					PeerIdFromUint64(123).H512(),
				},
			}, nil
		}).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendGetBlockHeaders(ctx, PeerIdFromUint64(123), eth.GetBlockHeadersPacket66{
		RequestId: 10,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Origin: eth.HashOrNumber{
				Number: 3,
			},
			Amount: 5,
		},
	})
	require.NoError(t, err)
}

func TestMessageSenderSendGetBlockHeadersErrPeerNotFound(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentry.SentPeers{}, nil).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendGetBlockHeaders(ctx, PeerIdFromUint64(123), eth.GetBlockHeadersPacket66{
		RequestId: 10,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Origin: eth.HashOrNumber{
				Number: 3,
			},
			Amount: 5,
		},
	})
	require.ErrorIs(t, err, ErrPeerNotFound)
}

func TestMessageSenderSendGetBlockBodies(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, request *sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentry.SentPeers, error) {
			require.Equal(t, PeerIdFromUint64(123), PeerIdFromH512(request.PeerId))
			require.Equal(t, sentry.MessageId_GET_BLOCK_BODIES_66, request.Data.Id)
			var payload eth.GetBlockBodiesPacket66
			err := rlp.DecodeBytes(request.Data.Data, &payload)
			require.NoError(t, err)
			require.Equal(t, uint64(10), payload.RequestId)
			require.Len(t, payload.GetBlockBodiesPacket, 1)
			return &sentry.SentPeers{
				Peers: []*erigonlibtypes.H512{
					PeerIdFromUint64(123).H512(),
				},
			}, nil
		}).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendGetBlockBodies(ctx, PeerIdFromUint64(123), eth.GetBlockBodiesPacket66{
		RequestId:            10,
		GetBlockBodiesPacket: []common.Hash{common.HexToHash("hi")},
	})
	require.NoError(t, err)
}

func TestMessageSenderSendGetBlockBodiesErrPeerNotFound(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentry.SentPeers{}, nil).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendGetBlockBodies(ctx, PeerIdFromUint64(123), eth.GetBlockBodiesPacket66{
		RequestId:            10,
		GetBlockBodiesPacket: []common.Hash{common.HexToHash("hi")},
	})
	require.ErrorIs(t, err, ErrPeerNotFound)
}
