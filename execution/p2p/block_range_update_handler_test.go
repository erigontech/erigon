// Copyright 2026 The Erigon Authors
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// TestBlockRangeUpdateHandlerInvalidPacketKicksPeer verifies that an invalid
// BlockRangeUpdate (e.g. Earliest > Latest) causes the peer to be penalized.
// This mirrors the Hive `TestBlockRangeUpdateInvalid` simulator check.
func TestBlockRangeUpdateHandlerInvalidPacketKicksPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	var penalized *sentryproto.PenalizePeerRequest
	sentryClient.EXPECT().
		PenalizePeer(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			penalized = req
			return &emptypb.Empty{}, nil
		}).
		Times(1)
	messageListener := NewMessageListener(logger, sentryClient, nil, nil)
	handler := NewBlockRangeUpdateHandler(logger, sentryClient, NewPeerPenalizer(sentryClient), messageListener)
	peerId := PeerIdFromUint64(1)
	// Packet with Earliest > Latest is invalid per BlockRangeUpdatePacket.Validate.
	err := handler.handleBlockRangeUpdate(ctx, &DecodedInboundMessage[*eth.BlockRangeUpdatePacket]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_BLOCK_RANGE_UPDATE_69,
			PeerId: peerId.H512(),
		},
		Decoded: &eth.BlockRangeUpdatePacket{
			Earliest:   10,
			Latest:     8,
			LatestHash: common.HexToHash("0xdeadbeef"),
		},
		PeerId: peerId,
	})
	require.Error(t, err)
	require.NotNil(t, penalized)
	require.Equal(t, sentryproto.PenaltyKind_Kick, penalized.Penalty)
}

// TestBlockRangeUpdateHandlerValidPacketUpdatesSentries verifies that a valid
// BlockRangeUpdate flows from the listener registration through the task queue
// and worker to the sentries via SetPeerBlockRange.
func TestBlockRangeUpdateHandlerValidPacketUpdatesSentries(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	updates := make(chan *sentryproto.SetPeerBlockRangeRequest, 1)
	sentryClient.EXPECT().
		SetPeerBlockRange(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SetPeerBlockRangeRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			updates <- req
			return &emptypb.Empty{}, nil
		}).
		Times(1)
	messageListener := NewMessageListener(logger, sentryClient, nil, nil)
	handler := NewBlockRangeUpdateHandler(logger, sentryClient, NewPeerPenalizer(sentryClient), messageListener)
	go func() {
		_ = handler.Run(ctx)
	}()
	peerId := PeerIdFromUint64(1)
	messageListener.blockRangeUpdateObservers.Notify(&DecodedInboundMessage[*eth.BlockRangeUpdatePacket]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_BLOCK_RANGE_UPDATE_69,
			PeerId: peerId.H512(),
		},
		Decoded: &eth.BlockRangeUpdatePacket{
			Earliest:   8,
			Latest:     10,
			LatestHash: common.HexToHash("0xdeadbeef"),
		},
		PeerId: peerId,
	})
	var update *sentryproto.SetPeerBlockRangeRequest
	select {
	case update = <-updates:
	case <-time.After(time.Second):
		t.Fatal("no SetPeerBlockRange call")
	}
	require.Equal(t, uint64(10), update.LatestBlockHeight)
	require.Equal(t, uint64(8), update.MinBlockHeight)
	require.Equal(t, peerId.H512(), update.PeerId)
}
