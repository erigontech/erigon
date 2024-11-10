// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	erigonlibtypes "github.com/erigontech/erigon-lib/types"
)

func TestFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	remoteKvClient := remote.NewMockKVClient(ctrl)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)
	pool.EXPECT().Started().Return(true)

	m := NewMockSentry(ctx, sentryServer)
	sentryClient := direct.NewSentryClientDirect(direct.ETH67, m)
	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, remoteKvClient, nil, nil, *u256.N1, log.New())
	var wg sync.WaitGroup
	fetch.SetWaitGroup(&wg)
	m.StreamWg.Add(2)
	fetch.ConnectSentries()
	m.StreamWg.Wait()
	// Send one transaction id
	wg.Add(1)
	errs := m.Send(&sentryproto.InboundMessage{
		Id:     sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		Data:   decodeHex("e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328"),
		PeerId: peerID,
	})
	for i, err := range errs {
		if err != nil {
			t.Errorf("sending new pool txn hashes 66 (%d): %v", i, err)
		}
	}
	wg.Wait()
}

func TestSendTxPropagate(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	t.Run("few remote byHash", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sentryServer := sentryproto.NewMockSentryServer(ctrl)

		times := 2
		requests := make([]*sentryproto.SendMessageToRandomPeersRequest, 0, times)
		sentryServer.EXPECT().
			SendMessageToRandomPeers(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
				requests = append(requests, r)
				return nil, nil
			}).
			Times(times)

		sentryServer.EXPECT().PeerById(gomock.Any(), gomock.Any()).
			DoAndReturn(
				func(_ context.Context, r *sentryproto.PeerByIdRequest) (*sentryproto.PeerByIdReply, error) {
					return &sentryproto.PeerByIdReply{
						Peer: &typesproto.PeerInfo{
							Id:   r.PeerId.String(),
							Caps: []string{"eth/68"},
						}}, nil
				}).AnyTimes()

		m := NewMockSentry(ctx, sentryServer)
		send := NewSend(ctx, []sentryproto.SentryClient{direct.NewSentryClientDirect(direct.ETH68, m)}, nil, log.New())
		send.BroadcastPooledTxs(testRlps(2), 100)
		send.AnnouncePooledTxs([]byte{0, 1}, []uint32{10, 15}, toHashes(1, 42), 100)

		require.Equal(t, 2, len(requests))

		txsMessage := requests[0].Data
		assert.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txsMessage.Id)
		assert.Equal(t, 3, len(txsMessage.Data))

		txnHashesMessage := requests[1].Data
		assert.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, txnHashesMessage.Id)
		assert.Equal(t, 76, len(txnHashesMessage.Data))
	})

	t.Run("much remote byHash", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sentryServer := sentryproto.NewMockSentryServer(ctrl)

		times := 2
		requests := make([]*sentryproto.SendMessageToRandomPeersRequest, 0, times)
		sentryServer.EXPECT().
			SendMessageToRandomPeers(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
				requests = append(requests, r)
				return nil, nil
			}).
			Times(times)

		m := NewMockSentry(ctx, sentryServer)
		send := NewSend(ctx, []sentryproto.SentryClient{direct.NewSentryClientDirect(direct.ETH68, m)}, nil, log.New())
		list := make(erigonlibtypes.Hashes, p2pTxPacketLimit*3)
		for i := 0; i < len(list); i += 32 {
			b := []byte(fmt.Sprintf("%x", i))
			copy(list[i:i+32], b)
		}
		send.BroadcastPooledTxs(testRlps(len(list)/32), 100)
		send.AnnouncePooledTxs([]byte{0, 1, 2}, []uint32{10, 12, 14}, list, 100)

		require.Equal(t, 2, len(requests))

		txsMessage := requests[0].Data
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txsMessage.Id)
		require.True(t, len(txsMessage.Data) > 0)

		txnHashesMessage := requests[1].Data
		require.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, txnHashesMessage.Id)
		require.True(t, len(txnHashesMessage.Data) > 0)
	})

	t.Run("few local byHash", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sentryServer := sentryproto.NewMockSentryServer(ctrl)

		times := 2
		requests := make([]*sentryproto.SendMessageToRandomPeersRequest, 0, times)
		sentryServer.EXPECT().
			SendMessageToRandomPeers(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
				requests = append(requests, r)
				return nil, nil
			}).
			Times(times)

		m := NewMockSentry(ctx, sentryServer)
		send := NewSend(ctx, []sentryproto.SentryClient{direct.NewSentryClientDirect(direct.ETH68, m)}, nil, log.New())
		send.BroadcastPooledTxs(testRlps(2), 100)
		send.AnnouncePooledTxs([]byte{0, 1}, []uint32{10, 15}, toHashes(1, 42), 100)

		require.Equal(t, 2, len(requests))

		txsMessage := requests[0].Data
		assert.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txsMessage.Id)
		assert.True(t, len(txsMessage.Data) > 0)

		txnHashesMessage := requests[1].Data
		assert.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, txnHashesMessage.Id)
		assert.Equal(t, 76, len(txnHashesMessage.Data))
	})

	t.Run("sync with new peer", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sentryServer := sentryproto.NewMockSentryServer(ctrl)

		times := 3
		requests := make([]*sentryproto.SendMessageByIdRequest, 0, times)
		sentryServer.EXPECT().
			SendMessageById(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
				requests = append(requests, r)
				return nil, nil
			}).
			Times(times)

		sentryServer.EXPECT().PeerById(gomock.Any(), gomock.Any()).
			DoAndReturn(
				func(_ context.Context, r *sentryproto.PeerByIdRequest) (*sentryproto.PeerByIdReply, error) {
					return &sentryproto.PeerByIdReply{
						Peer: &typesproto.PeerInfo{
							Id:   r.PeerId.String(),
							Caps: []string{"eth/68"},
						}}, nil
				}).AnyTimes()

		m := NewMockSentry(ctx, sentryServer)
		send := NewSend(ctx, []sentryproto.SentryClient{direct.NewSentryClientDirect(direct.ETH68, m)}, nil, log.New())
		expectPeers := toPeerIDs(1, 2, 42)
		send.PropagatePooledTxsToPeersList(expectPeers, []byte{0, 1}, []uint32{10, 15}, toHashes(1, 42))

		require.Equal(t, 3, len(requests))
		for i, req := range requests {
			assert.Equal(t, expectPeers[i], erigonlibtypes.PeerID(req.PeerId))
			assert.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, req.Data.Id)
			assert.True(t, len(req.Data.Data) > 0)
		}
	})
}

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}
func TestOnNewBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coreDB, db := memdb.NewTestDB(t, kv.ChainDB), memdb.NewTestDB(t, kv.TxPoolDB)
	ctrl := gomock.NewController(t)

	stream := remote.NewMockKV_StateChangesClient(ctrl)
	i := 0
	stream.EXPECT().
		Recv().
		DoAndReturn(func() (*remote.StateChangeBatch, error) {
			if i > 0 {
				return nil, io.EOF
			}
			i++
			return &remote.StateChangeBatch{
				StateVersionId: 1,
				ChangeBatch: []*remote.StateChange{
					{
						Txs: [][]byte{
							decodeHex(erigonlibtypes.TxParseMainnetTests[0].PayloadStr),
							decodeHex(erigonlibtypes.TxParseMainnetTests[1].PayloadStr),
							decodeHex(erigonlibtypes.TxParseMainnetTests[2].PayloadStr),
						},
						BlockHeight: 1,
						BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
					},
				},
			}, nil
		}).
		AnyTimes()

	stateChanges := remote.NewMockKVClient(ctrl)
	stateChanges.
		EXPECT().
		StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *remote.StateChangeRequest, _ ...grpc.CallOption) (remote.KV_StateChangesClient, error) {
			return stream, nil
		})

	pool := NewMockPool(ctrl)

	pool.EXPECT().
		ValidateSerializedTxn(gomock.Any()).
		DoAndReturn(func(_ []byte) error {
			return nil
		}).
		Times(3)

	var minedTxs erigonlibtypes.TxSlots
	pool.EXPECT().
		OnNewBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(
				_ context.Context,
				_ *remote.StateChangeBatch,
				_ erigonlibtypes.TxSlots,
				_ erigonlibtypes.TxSlots,
				minedTxsArg erigonlibtypes.TxSlots,
				_ kv.Tx,
			) error {
				minedTxs = minedTxsArg
				return nil
			},
		).
		Times(1)

	fetch := NewFetch(ctx, nil, pool, stateChanges, coreDB, db, *u256.N1, log.New())
	err := fetch.handleStateChanges(ctx, stateChanges)
	assert.ErrorIs(t, io.EOF, err)
	assert.Equal(t, 3, len(minedTxs.Txs))
}
