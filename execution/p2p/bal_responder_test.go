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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

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

// TestBALResponderAnswersAndSends covers the server-side eth/71 BAL handler:
// look up stored BALs via AnswerGetBlockAccessListsQuery and send back a
// positionally-aligned BlockAccessLists response with the matching RequestId.
func TestBALResponderAnswersAndSends(t *testing.T) {
	ctx := context.Background()
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	sentryClient := direct.NewMockSentryClient(ctrl)
	db := temporal.NewTestDB(t, dbcfg.ChainDB)
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback() // safety net; we Commit below on the happy path
	hashKnown := common.Hash{0x01}
	hashUnknown := common.Hash{0x02}
	const knownBlockNum uint64 = 100
	bal := []byte{0xc3, 0x01, 0x02, 0x03} // short valid RLP non-empty payload
	require.NoError(t, rawdb.WriteBlockAccessListBytes(rwTx, hashKnown, knownBlockNum, bal))
	require.NoError(t, rwTx.Commit())
	reader := &balHeaderNumberReader{
		byHash: map[common.Hash]uint64{hashKnown: knownBlockNum},
	}
	sentCh := make(chan *sentryproto.SendMessageByIdRequest, 1)
	sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			sentCh <- req
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).
		Times(1)
	messageListener := NewMessageListener(logger, sentryClient, nil, nil)
	responder := NewBALResponder(logger, messageListener, NewBALPublisher(NewMessageSender(sentryClient)), db, reader)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = responder.Run(runCtx)
	}()
	const reqID uint64 = 0xcafebabe
	peerId := PeerIdFromUint64(1)
	messageListener.getBlockAccessListsObservers.Notify(&DecodedInboundMessage[*eth.GetBlockAccessListsPacket66]{
		InboundMessage: &sentryproto.InboundMessage{
			Id:     sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71,
			PeerId: peerId.H512(),
		},
		Decoded: &eth.GetBlockAccessListsPacket66{
			RequestId:                 reqID,
			GetBlockAccessListsPacket: eth.GetBlockAccessListsPacket{hashKnown, hashUnknown},
		},
		PeerId: peerId,
	})
	var sent *sentryproto.SendMessageByIdRequest
	select {
	case sent = <-sentCh:
	case <-time.After(time.Second):
		t.Fatal("no BlockAccessLists response sent")
	}
	require.Equal(t, sentryproto.MessageId_BLOCK_ACCESS_LISTS_71, sent.Data.Id)
	var resp eth.BlockAccessListsPacket66
	require.NoError(t, rlp.DecodeBytes(sent.Data.Data, &resp))
	require.Equal(t, reqID, resp.RequestId)
	require.Len(t, resp.BlockAccessListsPacket, 2)
	require.True(t, bytes.Equal(resp.BlockAccessListsPacket[0], bal))
	// Unknown block — sentinel 0x80 ("not available"), not padded with anything else.
	require.True(t, bytes.Equal(resp.BlockAccessListsPacket[1], []byte{0x80}))
}
