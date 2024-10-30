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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	erigonlibtypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/eth/protocols/eth"
	"github.com/erigontech/erigon/v3/rlp"
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

func TestMessageSenderSendNewBlockHashes(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, request *sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentry.SentPeers, error) {
			require.Equal(t, PeerIdFromUint64(123), PeerIdFromH512(request.PeerId))
			require.Equal(t, sentry.MessageId_NEW_BLOCK_HASHES_66, request.Data.Id)
			var payload eth.NewBlockHashesPacket
			err := rlp.DecodeBytes(request.Data.Data, &payload)
			require.NoError(t, err)
			require.Len(t, payload, 1)
			require.Equal(t, uint64(1), payload[0].Number)
			require.Equal(t, common.HexToHash("0x0"), payload[0].Hash)
			return &sentry.SentPeers{
				Peers: []*erigonlibtypes.H512{
					PeerIdFromUint64(123).H512(),
				},
			}, nil
		}).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendNewBlockHashes(ctx, PeerIdFromUint64(123), eth.NewBlockHashesPacket{
		{
			Hash:   common.HexToHash("0x0"),
			Number: 1,
		},
	})
	require.NoError(t, err)
}

func TestMessageSenderSendNewBlockHashesErrPeerNotFound(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentry.SentPeers{}, nil).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendNewBlockHashes(ctx, PeerIdFromUint64(123), eth.NewBlockHashesPacket{
		{
			Hash:   common.HexToHash("0x0"),
			Number: 1,
		},
	})
	require.ErrorIs(t, err, ErrPeerNotFound)
}

func TestMessageSenderSendNewBlock(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	header := &types.Header{Number: big.NewInt(123)}
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, request *sentry.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentry.SentPeers, error) {
			require.Equal(t, PeerIdFromUint64(123), PeerIdFromH512(request.PeerId))
			require.Equal(t, sentry.MessageId_NEW_BLOCK_66, request.Data.Id)
			var payload eth.NewBlockPacket
			err := rlp.DecodeBytes(request.Data.Data, &payload)
			require.NoError(t, err)
			require.Equal(t, uint64(123), payload.Block.NumberU64())
			require.Equal(t, uint64(2), payload.TD.Uint64())
			return &sentry.SentPeers{
				Peers: []*erigonlibtypes.H512{
					PeerIdFromUint64(123).H512(),
				},
			}, nil
		}).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendNewBlock(ctx, PeerIdFromUint64(123), eth.NewBlockPacket{
		Block: types.NewBlockWithHeader(header),
		TD:    big.NewInt(2),
	})
	require.NoError(t, err)
}

func TestMessageSenderSendNewBlockErrPeerNotFound(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	header := &types.Header{Number: big.NewInt(123)}
	sentryClient := direct.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentry.SentPeers{}, nil).
		Times(1)

	messageSender := NewMessageSender(sentryClient)
	err := messageSender.SendNewBlock(ctx, PeerIdFromUint64(123), eth.NewBlockPacket{
		Block: types.NewBlockWithHeader(header),
		TD:    big.NewInt(2),
	})
	require.ErrorIs(t, err, ErrPeerNotFound)
}
