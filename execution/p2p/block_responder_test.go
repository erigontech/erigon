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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

type receiptRLP69 struct {
	Type              uint8
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	Logs              []*types.Log
}

type mockReceiptsGetter struct {
	eth.ReceiptsGetter
	getCachedReceiptsFunc func(ctx context.Context, hash common.Hash) (types.Receipts, bool)
}

func (m *mockReceiptsGetter) GetCachedReceipts(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
	return m.getCachedReceiptsFunc(ctx, hash)
}

// TestBlockResponderGetReceipts69 sends an eth/69 GetReceipts request through the
// responder and verifies the eth/69 receipt encoding: typed receipts must be
// list-encoded [type, status, gas, logs] without the bloom field, NOT the old
// byte-string envelope (type_byte || rlp(data)) used in eth/68.
func TestBlockResponderGetReceipts69(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
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
	sent := make(chan *sentryproto.SendMessageByIdRequest, 1)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			sent <- req
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).
		Times(1)
	receiptsGetter := &mockReceiptsGetter{
		getCachedReceiptsFunc: func(_ context.Context, hash common.Hash) (types.Receipts, bool) {
			if hash == testHash {
				return testReceipts, true
			}
			return nil, false
		},
	}
	messageListener := NewMessageListener(logger, sentryClient, nil, nil)
	messageSender := NewMessageSender(sentryClient)
	publisher := NewPublisher(logger, messageSender, nil)
	responder := NewBlockResponder(logger, messageListener, publisher, nil, nil, nil, receiptsGetter)
	go func() {
		_ = responder.Run(ctx)
	}()
	peerId := PeerIdFromUint64(1)
	messageListener.getReceiptsObservers.Notify(&DecodedInboundMessage[*eth.GetReceiptsPacket66]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_GET_RECEIPTS_69,
			PeerId: peerId.H512(),
		},
		Decoded: &eth.GetReceiptsPacket66{
			RequestId:         1,
			GetReceiptsPacket: eth.GetReceiptsPacket{testHash},
		},
		PeerId: peerId,
	})
	var sentMessage *sentryproto.SendMessageByIdRequest
	select {
	case sentMessage = <-sent:
	case <-time.After(time.Second):
		t.Fatal("no message was sent")
	}
	require.Equal(t, sentryproto.MessageId_RECEIPTS_66, sentMessage.Data.Id)
	var response eth.ReceiptsRLPPacket66
	require.NoError(t, rlp.DecodeBytes(sentMessage.Data.Data, &response))
	require.Equal(t, uint64(1), response.RequestId)
	var receiptsList []*receiptRLP69
	require.NoError(t, rlp.DecodeBytes(response.ReceiptsRLPPacket[0], &receiptsList))
	require.Len(t, receiptsList, 2)
	require.Equal(t, uint8(types.LegacyTxType), receiptsList[0].Type)
	require.Equal(t, uint64(21000), receiptsList[0].CumulativeGasUsed)
	require.Equal(t, uint8(types.DynamicFeeTxType), receiptsList[1].Type)
	require.Equal(t, uint64(42000), receiptsList[1].CumulativeGasUsed)
}
