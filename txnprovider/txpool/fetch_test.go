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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx := t.Context()
	t.Parallel()

	ctrl := gomock.NewController(t)
	remoteKvClient := remoteproto.NewMockKVClient(ctrl)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)
	pool.EXPECT().Started().Return(true)

	m := NewMockSentry(ctx, sentryServer)
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
	require.NoError(t, err)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, remoteKvClient, nil, u256.N1, log.New(), WithP2PFetcherWg(&wg))
	m.StreamWg.Add(2)
	fetch.ConnectSentries()
	m.StreamWg.Wait()
	// Send one transaction id
	wg.Add(1)
	errs := m.Send(&sentryproto.InboundMessage{
		Id:     sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		Data:   decodeHex("e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328"),
		PeerId: peerID,
	})
	for i, err := range errs {
		if err != nil {
			t.Errorf("sending new pool txn hashes 68 (%d): %v", i, err)
		}
	}
	wg.Wait()
}

func TestSendTxnPropagate(t *testing.T) {
	ctx := t.Context()
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
						},
					}, nil
				},
			).AnyTimes()

		m := NewMockSentry(ctx, sentryServer)
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
		require.NoError(t, err)
		send := NewSend(ctx, []sentryproto.SentryClient{sentryClient}, log.New())
		send.BroadcastPooledTxns(testRlps(2), 100)
		send.AnnouncePooledTxns([]byte{0, 1}, []uint32{10, 15}, toHashes(1, 42), 100)

		require.Len(t, requests, 2)

		txnsMessage := requests[0].Data
		assert.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txnsMessage.Id)
		assert.Len(t, txnsMessage.Data, 3)

		txnHashesMessage := requests[1].Data
		assert.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, txnHashesMessage.Id)
		assert.Len(t, txnHashesMessage.Data, 76)
	})

	t.Run("much remote byHash", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sentryServer := sentryproto.NewMockSentryServer(ctrl)

		times := 3
		requests := make([]*sentryproto.SendMessageToRandomPeersRequest, 0, times)
		sentryServer.EXPECT().
			SendMessageToRandomPeers(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
				requests = append(requests, r)
				return nil, nil
			}).
			Times(times)

		m := NewMockSentry(ctx, sentryServer)
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
		require.NoError(t, err)
		send := NewSend(ctx, []sentryproto.SentryClient{sentryClient}, log.New())
		list := make(Hashes, p2pTxPacketLimit*3)
		for i := 0; i < len(list); i += 32 {
			b := fmt.Appendf(nil, "%x", i)
			copy(list[i:i+32], b)
		}
		rlps := testRlps(len(list) / 32)
		send.BroadcastPooledTxns(rlps, 100)
		send.AnnouncePooledTxns([]byte{0, 1, 2}, []uint32{10, 12, 14}, list, 100)

		require.Len(t, requests, 3)

		txnsMessage := requests[0].Data
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txnsMessage.Id)
		require.Equal(t, EncodeTransactions(rlps[:maxTransactionsPerPacket], nil), txnsMessage.Data)
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, requests[1].Data.Id)
		require.Equal(t, EncodeTransactions(rlps[maxTransactionsPerPacket:], nil), requests[1].Data.Data)

		txnHashesMessage := requests[2].Data
		require.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, txnHashesMessage.Id)
		require.Positive(t, len(txnHashesMessage.Data))
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
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
		require.NoError(t, err)
		send := NewSend(ctx, []sentryproto.SentryClient{sentryClient}, log.New())
		send.BroadcastPooledTxns(testRlps(2), 100)
		send.AnnouncePooledTxns([]byte{0, 1}, []uint32{10, 15}, toHashes(1, 42), 100)

		require.Len(t, requests, 2)

		txnsMessage := requests[0].Data
		assert.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txnsMessage.Id)
		assert.Positive(t, len(txnsMessage.Data))

		txnHashesMessage := requests[1].Data
		assert.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, txnHashesMessage.Id)
		assert.Len(t, txnHashesMessage.Data, 76)
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
						},
					}, nil
				},
			).AnyTimes()

		m := NewMockSentry(ctx, sentryServer)
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
		require.NoError(t, err)
		send := NewSend(ctx, []sentryproto.SentryClient{sentryClient}, log.New())
		expectPeers := toPeerIDs(1, 2, 42)
		send.PropagatePooledTxnsToPeersList(expectPeers, []byte{0, 1}, []uint32{10, 15}, toHashes(1, 42))

		require.Len(t, requests, 3)
		for i, req := range requests {
			assert.Equal(t, expectPeers[i], PeerID(req.PeerId))
			assert.Equal(t, sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, req.Data.Id)
			assert.Positive(t, len(req.Data.Data))
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
	t.Parallel()
	ctx := t.Context()
	_, db := mdbxtest.NewTestDB(t, dbcfg.ChainDB), mdbxtest.NewTestDB(t, dbcfg.TxPoolDB)
	ctrl := gomock.NewController(t)

	stream := remoteproto.NewMockKV_StateChangesClient[*remoteproto.StateChangeBatch](ctrl)
	i := 0
	stream.EXPECT().
		Recv().
		DoAndReturn(func() (*remoteproto.StateChangeBatch, error) {
			if i > 0 {
				return nil, io.EOF
			}
			i++
			return &remoteproto.StateChangeBatch{
				StateVersionId: 1,
				ChangeBatch: []*remoteproto.StateChange{
					{
						Txs: [][]byte{
							decodeHex(TxnParseMainnetTests[0].PayloadStr),
							decodeHex(TxnParseMainnetTests[1].PayloadStr),
							decodeHex(TxnParseMainnetTests[2].PayloadStr),
						},
						BlockHeight: 1,
						BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
					},
				},
			}, nil
		}).
		AnyTimes()

	stateChanges := remoteproto.NewMockKVClient(ctrl)
	stateChanges.
		EXPECT().
		StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, request *remoteproto.StateChangeRequest, _ ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
			assert.True(t, request.WithStorage, "frame nonce revalidation needs changed storage slots")
			return stream, nil
		})

	pool := NewMockPool(ctrl)

	var minedTxns TxnSlots
	pool.EXPECT().
		OnNewBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *remoteproto.StateChangeBatch, _ TxnSlots, _ TxnSlots, minedTxnsArg TxnSlots) error {
			minedTxns = minedTxnsArg
			return nil
		}).
		Times(1)

	fetch := NewFetch(ctx, nil, pool, stateChanges, db, u256.N1, log.New())
	err := fetch.handleStateChanges(ctx, stateChanges)
	assert.ErrorIs(t, io.EOF, err)
	assert.Len(t, minedTxns.Txns, 3)
}

type MockSentry struct {
	ctx context.Context
	*sentryproto.MockSentryServer
	streams      map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer
	peersStreams []sentryproto.Sentry_PeerEventsServer
	StreamWg     sync.WaitGroup
	lock         sync.RWMutex
}

func NewMockSentry(ctx context.Context, sentryServer *sentryproto.MockSentryServer) *MockSentry {
	return &MockSentry{
		ctx:              ctx,
		MockSentryServer: sentryServer,
	}
}

var peerID PeerID = gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}) // "12345"

func (ms *MockSentry) Send(req *sentryproto.InboundMessage) (errs []error) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	for _, stream := range ms.streams[req.Id] {
		if err := stream.Send(req); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (ms *MockSentry) SetStatus(context.Context, *sentryproto.StatusData) (*sentryproto.SetStatusReply, error) {
	return &sentryproto.SetStatusReply{}, nil
}

func (ms *MockSentry) HandShake(context.Context, *emptypb.Empty) (*sentryproto.HandShakeReply, error) {
	return &sentryproto.HandShakeReply{Protocol: sentryproto.Protocol_ETH69}, nil
}

func (ms *MockSentry) Messages(req *sentryproto.MessagesRequest, stream sentryproto.Sentry_MessagesServer) error {
	ms.lock.Lock()
	if ms.streams == nil {
		ms.streams = map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer{}
	}
	for _, id := range req.Ids {
		ms.streams[id] = append(ms.streams[id], stream)
	}
	ms.lock.Unlock()
	ms.StreamWg.Done()
	select {
	case <-ms.ctx.Done():
		return nil
	case <-stream.Context().Done():
		return nil
	}
}

func (ms *MockSentry) PeerEvents(_ *sentryproto.PeerEventsRequest, stream sentryproto.Sentry_PeerEventsServer) error {
	ms.lock.Lock()
	ms.peersStreams = append(ms.peersStreams, stream)
	ms.lock.Unlock()
	ms.StreamWg.Done()
	select {
	case <-ms.ctx.Done():
		return nil
	case <-stream.Context().Done():
		return nil
	}
}

func TestFramePolicyRejectionKeepsOrdinaryTransactions(t *testing.T) {
	frame := framePoolSlot(t, [20]byte{1}, 1, 2).Txn.(*types.FrameTransaction)
	frame.Frames[0].Limits.Execution = 150_000
	require.NoError(t, frame.Validate())
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	ordinary := tpEncodeTests[1].txns
	txns := [][]byte{ordinary[0], encoded.Bytes(), ordinary[1]}

	for _, id := range []sentryproto.MessageId{sentryproto.MessageId_TRANSACTIONS_66, sentryproto.MessageId_POOLED_TRANSACTIONS_66} {
		t.Run(id.String(), func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			sentryServer := sentryproto.NewMockSentryServer(ctrl)
			sentryServer.EXPECT().PenalizePeer(gomock.Any(), gomock.Any()).Times(0)
			pool := NewMockPool(ctrl)
			pool.EXPECT().Started().Return(true)
			pool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
			pool.EXPECT().RejectFramePrefix(crypto.Keccak256Hash(encoded.Bytes()))
			pool.EXPECT().AddRemoteTxns(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Do(func(_ context.Context, slots TxnSlots, _ PeerID, _ sentryproto.SentryClient) {
					require.Len(t, slots.Txns, len(ordinary))
					for i, txn := range slots.Txns {
						require.Equal(t, ordinary[i], txn.Rlp)
					}
				})

			m := NewMockSentry(ctx, sentryServer)
			sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
			require.NoError(t, err)
			fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())
			payload := EncodeTransactions(txns, nil)
			if id == sentryproto.MessageId_POOLED_TRANSACTIONS_66 {
				payload = EncodePooledTransactions66(txns, 1, nil)
			}
			err = fetch.handleInboundMessageWithTx(ctx, nil, &sentryproto.InboundMessage{
				Id: id, Data: payload, PeerId: peerID,
			}, sentryClient)
			require.NoError(t, err)
		})
	}
}

func TestFramePolicyRejectionDeduplicatesGossip(t *testing.T) {
	ctx, pool, poolDB, _ := newTestPool(t)
	pool.started.Store(true)
	frame := framePoolSlot(t, [20]byte{1}, 1, 2).Txn.(*types.FrameTransaction)
	frame.Frames[0].Limits.Execution = 150_000
	require.NoError(t, frame.Validate())
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	hash := crypto.Keccak256Hash(encoded.Bytes())
	ctrl := gomock.NewController(t)
	sentry := sentryproto.NewMockSentryServer(ctrl)
	sentry.EXPECT().PenalizePeer(gomock.Any(), gomock.Any()).Times(0)
	requests := 0
	sentry.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
			require.Equal(t, sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66, req.Data.Id)
			requests++
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{req.PeerId}}, nil
		}).AnyTimes()
	client, err := direct.NewSentryClientDirect(direct.ETH68, NewMockSentry(ctx, sentry))
	require.NoError(t, err)
	fetch := NewFetch(ctx, []sentryproto.SentryClient{client}, pool, nil, poolDB, u256.N1, log.New())
	tx, err := poolDB.BeginRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	announce := func() {
		t.Helper()
		require.NoError(t, fetch.handleInboundMessageWithTx(ctx, tx, &sentryproto.InboundMessage{
			Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, Data: EncodeHashes(hash[:], nil), PeerId: peerID,
		}, client))
	}
	announce()
	require.Equal(t, 1, requests)
	require.NoError(t, fetch.handleInboundMessageWithTx(ctx, tx, &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: EncodePooledTransactions66([][]byte{encoded.Bytes()}, 1, nil), PeerId: peerID,
	}, client))
	announce()
	buf := make([]byte, 128)
	n := encodeAnnouncements([]byte{FrameTxnType}, []uint32{uint32(encoded.Len())}, hash[:], buf)
	require.NoError(t, fetch.handleInboundMessageWithTx(ctx, tx, &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, Data: buf[:n], PeerId: peerID,
	}, client))
	require.Equal(t, 1, requests, "rejected prefixes must not be fetched again")
	require.Empty(t, pool.unprocessedRemoteByHash)
}

func TestPenalizePeerForMalformedMessages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	txn := decodeHex("c98080808080801b0101")
	oversizedTxns := make([][]byte, maxTransactionsPerPacket+1)
	for i := range oversizedTxns {
		oversizedTxns[i] = txn
	}
	frame := framePoolSlot(t, [20]byte{1}, 1, 2).Txn.(*types.FrameTransaction)
	frame.Signatures = []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1, Signature: make([]byte, 65)}}
	var invalidFrame bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&invalidFrame))

	// Each sub-test sends a malformed message of a given type and asserts that PenalizePeer is called.
	tests := []struct {
		name string
		id   sentryproto.MessageId
		data []byte // deliberately malformed payload
	}{
		{
			name: "malformed Transactions66",
			id:   sentryproto.MessageId_TRANSACTIONS_66,
			data: []byte{0xff, 0xfe}, // invalid RLP
		},
		{
			name: "malformed PooledTransactions66",
			id:   sentryproto.MessageId_POOLED_TRANSACTIONS_66,
			data: []byte{0xff, 0xfe},
		},
		{
			name: "invalid frame signature Transactions66",
			id:   sentryproto.MessageId_TRANSACTIONS_66,
			data: EncodeTransactions([][]byte{invalidFrame.Bytes()}, nil),
		},
		{
			name: "invalid frame signature PooledTransactions66",
			id:   sentryproto.MessageId_POOLED_TRANSACTIONS_66,
			data: EncodePooledTransactions66([][]byte{invalidFrame.Bytes()}, 1, nil),
		},
		{
			name: "oversized Transactions66",
			id:   sentryproto.MessageId_TRANSACTIONS_66,
			data: EncodeTransactions(oversizedTxns, nil),
		},
		{
			name: "oversized PooledTransactions66",
			id:   sentryproto.MessageId_POOLED_TRANSACTIONS_66,
			data: EncodePooledTransactions66(oversizedTxns, 1, nil),
		},
		{
			name: "malformed NewPooledTransactionHashes66",
			id:   sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
			data: []byte{0xff, 0xfe},
		},
		{
			name: "malformed NewPooledTransactionHashes68",
			id:   sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
			data: []byte{0xff, 0xfe},
		},
		{
			name: "malformed GetPooledTransactions66",
			id:   sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
			data: []byte{0xff, 0xfe},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			sentryServer := sentryproto.NewMockSentryServer(ctrl)
			pool := NewMockPool(ctrl)
			pool.EXPECT().Started().Return(true)
			pool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			// Expect PenalizePeer to be called exactly once with a Kick penalty.
			sentryServer.EXPECT().
				PenalizePeer(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *sentryproto.PenalizePeerRequest) (*emptypb.Empty, error) {
					assert.Equal(t, sentryproto.PenaltyKind_Kick, req.Penalty)
					assert.Equal(t, peerID, PeerID(req.PeerId))
					return &emptypb.Empty{}, nil
				}).
				Times(1)

			m := NewMockSentry(ctx, sentryServer)
			sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
			require.NoError(t, err)

			fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())

			err = fetch.handleInboundMessageWithTx(ctx, nil, &sentryproto.InboundMessage{
				Id:     tt.id,
				Data:   tt.data,
				PeerId: peerID,
			}, sentryClient)
			require.NoError(t, err, "malformed message should be handled gracefully (penalize + return nil)")
		})
	}
}

// TestOversizedHashAnnouncement verifies that both eth/66 and eth/68 handlers
// penalize peers that send more than 4096 hashes per NewPooledTransactionHashes
// message (the devp2p soft limit) and do not issue a GetPooledTransactions request.
func TestOversizedHashAnnouncement(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	const oversizedCount = 4097

	// Build oversized eth/66 payload: an RLP list of 4097 hashes.
	hashes := make([]byte, 32*oversizedCount)
	for i := range oversizedCount {
		hashes[i*32] = byte(i)
		hashes[i*32+1] = byte(i >> 8)
	}
	payload66 := EncodeHashes(hashes, nil)

	// Build oversized eth/68 payload: announcement with 4097 entries.
	types68 := make([]byte, oversizedCount)
	sizes68 := make([]uint32, oversizedCount)
	for i := range oversizedCount {
		types68[i] = 2   // EIP-1559
		sizes68[i] = 100 // arbitrary size
	}
	encodeBuf := make([]byte, announcementsLen(types68, sizes68, hashes))
	n := encodeAnnouncements(types68, sizes68, hashes, encodeBuf)
	payload68 := make([]byte, n)
	copy(payload68, encodeBuf[:n])

	tests := []struct {
		name string
		id   sentryproto.MessageId
		data []byte
	}{
		{"eth/66", sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, payload66},
		{"eth/68", sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, payload68},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			sentryServer := sentryproto.NewMockSentryServer(ctrl)
			pool := NewMockPool(ctrl)
			pool.EXPECT().Started().Return(true)

			// Expect peer to be penalized.
			sentryServer.EXPECT().
				PenalizePeer(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *sentryproto.PenalizePeerRequest) (*emptypb.Empty, error) {
					assert.Equal(t, sentryproto.PenaltyKind_Kick, req.Penalty)
					return &emptypb.Empty{}, nil
				}).
				Times(1)

			m := NewMockSentry(ctx, sentryServer)
			sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
			require.NoError(t, err)

			fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())

			err = fetch.handleInboundMessageWithTx(ctx, nil, &sentryproto.InboundMessage{
				Id:     tt.id,
				Data:   tt.data,
				PeerId: peerID,
			}, sentryClient)
			require.NoError(t, err, "oversized announcement should be handled gracefully")
		})
	}
}

// TestCheckPooledTxnAnnouncement verifies that a peer delivering a tx whose
// type or size does not match what it previously announced is flagged, while
// mismatches from an unrelated peer or without a prior announcement are not.
func TestCheckPooledTxnAnnouncement(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)

	m := NewMockSentry(ctx, sentryServer)
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
	require.NoError(t, err)

	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())

	otherPeer := gointerfaces.ConvertHashToH512([64]byte{0xfe, 0xed})

	hash := [32]byte{0xaa, 0xbb}
	slot := &TxnSlot{IDHash: hash, Size: 200, Txn: &types.DynamicFeeTransaction{}}
	// No announcement yet — must be a no-op.
	require.NoError(t, fetch.checkPooledTxnAnnouncement(peerID, slot))

	// Record an announcement: peer promised (type=DynamicFee, size=200).
	// Tests pass nil filter to record every announced hash.
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{200}, hash[:], nil)

	// Same peer, matching (type, size) — ok.
	require.NoError(t, fetch.checkPooledTxnAnnouncement(peerID, slot))

	// After a successful match the entry is consumed; re-announce for further checks.
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{200}, hash[:], nil)

	// Same peer, mismatching type (announcement says DynamicFee but we deliver Legacy).
	badTypeSlot := &TxnSlot{IDHash: hash, Size: 200, Txn: &types.LegacyTx{}}
	require.Error(t, fetch.checkPooledTxnAnnouncement(peerID, badTypeSlot),
		"peer delivering wrong type should be flagged")

	// After the mismatch, re-announce and check the size path.
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{200}, hash[:], nil)
	badSizeSlot := &TxnSlot{IDHash: hash, Size: 999, Txn: &types.DynamicFeeTransaction{}}
	require.Error(t, fetch.checkPooledTxnAnnouncement(peerID, badSizeSlot),
		"peer delivering wrong size should be flagged")

	// Size within the 8-byte slack is not a violation — RLP vs consensus-format
	// accounting can disagree by a handful of bytes for legacy/typed txs. The
	// check is symmetric: both announced-larger-than-delivered and
	// announced-smaller-than-delivered are bounded by the same slack.
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{208}, hash[:], nil)
	require.NoError(t, fetch.checkPooledTxnAnnouncement(peerID, slot),
		"size diff of +8 bytes should be within slack")
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{209}, hash[:], nil)
	require.Error(t, fetch.checkPooledTxnAnnouncement(peerID, slot),
		"size diff of +9 bytes should exceed slack")
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{192}, hash[:], nil)
	require.NoError(t, fetch.checkPooledTxnAnnouncement(peerID, slot),
		"size diff of -8 bytes should be within slack")
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{191}, hash[:], nil)
	require.Error(t, fetch.checkPooledTxnAnnouncement(peerID, slot),
		"size diff of -9 bytes should exceed slack")

	// Different peer delivering the same hash must not be penalized based on
	// another peer's announcement.
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{200}, hash[:], nil)
	require.NoError(t, fetch.checkPooledTxnAnnouncement(otherPeer, badTypeSlot))

	// Filter subset: an announcement for a hash not in the filter is skipped.
	//
	//  - hashA is in the filter — its (type, size) should be recorded.
	//  - hashB is absent from the filter — its entry must NOT be recorded,
	//    so a subsequent check for hashB from the same peer is a no-op.
	hashA := [32]byte{0xcc, 0x01}
	hashB := [32]byte{0xcc, 0x02}
	both := append(append([]byte{}, hashA[:]...), hashB[:]...)
	filter := hashA[:] // only hashA survives FilterKnownIdHashes
	fetch.recordAnnouncement(peerID,
		[]byte{types.DynamicFeeTxType, types.DynamicFeeTxType},
		[]uint32{200, 200}, both, filter)
	require.NoError(t, fetch.checkPooledTxnAnnouncement(peerID,
		&TxnSlot{IDHash: hashB, Size: 999, Txn: &types.DynamicFeeTransaction{}}),
		"hashB was filtered out at record time, so no mismatch should fire")
	require.Error(t, fetch.checkPooledTxnAnnouncement(peerID,
		&TxnSlot{IDHash: hashA, Size: 999, Txn: &types.DynamicFeeTransaction{}}),
		"hashA was recorded, so the wrong-size delivery must fire")

	// Multi-peer clobber regression: peer A lies in its announcement, peer B
	// then announces the same hash with correct metadata. Without a composite
	// (hash, peer) LRU key, B's entry would overwrite A's and A's lie would
	// slip through the check. Both announcements must survive so A's delivery
	// is still matched against A's own record.
	//
	// Concretely: A announces (DynamicFee, wrong size), B announces
	// (DynamicFee, correct size), A then delivers the real 200-byte tx. The
	// mismatch against A's own announcement must still fire.
	hashC := [32]byte{0xdd, 0x01}
	fetch.recordAnnouncement(peerID, []byte{types.DynamicFeeTxType}, []uint32{999}, hashC[:], nil)
	fetch.recordAnnouncement(otherPeer, []byte{types.DynamicFeeTxType}, []uint32{200}, hashC[:], nil)
	require.Error(t, fetch.checkPooledTxnAnnouncement(peerID,
		&TxnSlot{IDHash: hashC, Size: 200, Txn: &types.DynamicFeeTransaction{}}),
		"peer A's own lying announcement must still match A's delivery after B also announces")
	// Peer B's delivery against its own (truthful) announcement is clean.
	require.NoError(t, fetch.checkPooledTxnAnnouncement(otherPeer,
		&TxnSlot{IDHash: hashC, Size: 200, Txn: &types.DynamicFeeTransaction{}}),
		"peer B's matching delivery must not fire")
}

// TestCheckBlobSidecar verifies that a blob tx whose wrapper commitments do not
// match its blob_versioned_hashes is flagged, while a correctly-formed wrapper
// and a non-blob tx pass through cleanly.
func TestCheckBlobSidecar(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)
	m := NewMockSentry(ctx, sentryServer)
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
	require.NoError(t, err)
	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())

	// Non-blob tx — unchecked.
	require.NoError(t, fetch.checkBlobSidecar(&TxnSlot{Txn: &types.DynamicFeeTransaction{}}))

	// Properly formed blob tx: a slot produced by makeBlobTxn() has both the
	// inner BlobTx (with BlobVersionedHashes patched to match commitments) and
	// the populated BlobBundles.
	good := makeBlobTxn()
	require.NoError(t, fetch.checkBlobSidecar(&good), "matching sidecar should pass")

	// Corrupt a commitment so it no longer hashes to the versioned-hash.
	bad := makeBlobTxn()
	bad.BlobBundles[0].Commitment[0] ^= 0xff
	require.Error(t, fetch.checkBlobSidecar(&bad), "mismatched commitment should be flagged")

	// Blob tx arriving with no sidecar at all: the inner blob_versioned_hashes
	// declares blobs but the wrapper delivers none. Treated as a protocol
	// violation via the count-mismatch check (peer gets kicked).
	missing := makeBlobTxn()
	missing.BlobBundles = nil
	require.Error(t, fetch.checkBlobSidecar(&missing), "blob tx with missing sidecar should be flagged")
}

// TestNoPenaltyOnInternalDBError verifies that when IdHashKnown returns a DB error
// during transaction parsing, the peer is NOT penalized (since it's our internal failure,
// not the peer's fault).
func TestNoPenaltyOnInternalDBError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Build a valid Transactions66 RLP payload from a known good transaction.
	txnRlp := decodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10")
	validPayload := EncodeTransactions([][]byte{txnRlp}, nil)

	ctrl := gomock.NewController(t)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)
	pool.EXPECT().Started().Return(true)

	dbErr := fmt.Errorf("mdbx read error")
	pool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(false, dbErr).AnyTimes()

	// PenalizePeer must NOT be called.
	sentryServer.EXPECT().PenalizePeer(gomock.Any(), gomock.Any()).Times(0)

	m := NewMockSentry(ctx, sentryServer)
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m)
	require.NoError(t, err)

	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())

	err = fetch.handleInboundMessageWithTx(ctx, nil, &sentryproto.InboundMessage{
		Id:     sentryproto.MessageId_TRANSACTIONS_66,
		Data:   validPayload,
		PeerId: peerID,
	}, sentryClient)
	require.Error(t, err, "internal DB error should be propagated, not swallowed")
}

// TestFetchConnectGoroutinesExitOnCancel is a regression test for a goroutine
// leak where ConnectCore/ConnectSentries spawned fire-and-forget goroutines
// that used bare time.Sleep in retry loops. After context cancellation, Run()
// would return via the errgroup while the fetch goroutines continued sleeping
// through a 3-second backoff, racing with downstream cleanup (DB.Close(), etc.).
//
// The fix replaced time.Sleep with select on ctx.Done() and added a WaitGroup
// so callers can block until all goroutines exit. This test verifies that all
// goroutines exit promptly (well under 3s) after cancellation.
func TestFetchConnectGoroutinesExitOnCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	// Mock sentry server: HandShake returns io.EOF, forcing receiveMessageLoop
	// and receivePeerLoop into retry-with-backoff loops.
	srv := &retrySentryServer{}
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, srv)
	require.NoError(t, err)

	// Mock state changes client: StateChanges returns io.EOF, forcing the
	// ConnectCore goroutine into its retry loop.
	ctrl := gomock.NewController(t)
	stateChanges := remoteproto.NewMockKVClient(ctrl)
	stateChanges.EXPECT().
		StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, io.EOF).AnyTimes()

	pool := NewMockPool(ctrl)

	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, stateChanges, nil, u256.N1, log.New())

	// Start all goroutines: 1 from ConnectCore + 2 from ConnectSentries.
	fetch.ConnectCore()
	fetch.ConnectSentries()

	// Let goroutines enter their retry-sleep selects.
	time.Sleep(50 * time.Millisecond)

	// Cancel the context — goroutines should notice via ctx.Done() and exit.
	cancel()

	// Wait must return well within 3 seconds. Before the fix, goroutines used
	// bare time.Sleep(3 * time.Second) and would not notice cancellation until
	// the sleep completed.
	done := make(chan struct{})
	go func() {
		fetch.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines exited promptly.
	case <-time.After(1 * time.Second):
		t.Fatal("goroutines did not exit within 1s of context cancellation — likely sleeping through backoff")
	}
}

// retrySentryServer is a minimal SentryServer where HandShake always returns
// io.EOF, forcing Fetch goroutines into their retry-with-backoff loops.
type retrySentryServer struct {
	sentryproto.UnimplementedSentryServer
}

func (s *retrySentryServer) HandShake(context.Context, *emptypb.Empty) (*sentryproto.HandShakeReply, error) {
	return nil, io.EOF
}

func testRlps(num int) [][]byte {
	rlps := make([][]byte, num)
	for i := range num {
		rlps[i] = []byte{1}
	}
	return rlps
}

func toPeerIDs(h ...byte) (out []PeerID) {
	for i := range h {
		hash := [64]byte{h[i]}
		out = append(out, gointerfaces.ConvertHashToH512(hash))
	}
	return out
}

func TestAnnouncedHashRequestedFromOnePeerUntilTimeout(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	require.Equal(t, [][64]byte{peerA}, a.requestedFrom)

	*a.now = a.now.Add(txFetchTimeout)
	a.announce(t, peerB, hash)
	require.Equal(t, [][64]byte{peerA}, a.requestedFrom, "the waiting alternate is asked by the sweep, in order")
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
}

type announceFetchTest struct {
	ctx           context.Context
	fetch         *Fetch
	sentryClient  sentryproto.SentryClient
	now           *time.Time
	requestedFrom [][64]byte
	filterErr     error
	known         map[[32]byte]bool // hashes the pool holds
	discarded     map[[32]byte]bool // hashes whose body the pool rejected
	queued        map[[32]byte]bool // hashes whose body waits for validation
	onFilter      func()
	onSend        func()
	lookedUp      int           // hashes passed to HeldOrQueuedIdHashes
	sentHashes    []Hashes      // hashes of each request, in send order
	blockSend     bool          // the next send blocks until its context is done
	blockStarted  chan struct{} // closed when the blocked send starts
	requestIDs    []uint64
	noDeadline    bool // a send reached sentry without a deadline
	sendErr       error
	failSendAt    int               // when > 0, only that send (1-based) fails with sendErr
	gone          map[[64]byte]bool // peers sentry no longer knows: a send reaches no one
	sends         int
}

func newAnnounceFetchTest(t *testing.T) *announceFetchTest {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)
	a := &announceFetchTest{ctx: ctx}
	pool.EXPECT().Started().Return(true).AnyTimes()
	pool.EXPECT().FilterKnownIdHashes(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ kv.Tx, hashes Hashes) (Hashes, error) {
			if a.onFilter != nil {
				a.onFilter()
			}
			var unknown Hashes
			for i := 0; i < len(hashes); i += 32 {
				if h := [32]byte(hashes[i : i+32]); !a.known[h] && !a.discarded[h] && !a.queued[h] {
					unknown = append(unknown, hashes[i:i+32]...)
				}
			}
			return unknown, a.filterErr
		}).AnyTimes()
	pool.EXPECT().AddNewGoodPeer(gomock.Any()).AnyTimes()
	pool.EXPECT().HeldOrQueuedIdHashes(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ kv.Tx, hashes Hashes) (Hashes, Hashes, error) {
			if a.onFilter != nil {
				a.onFilter()
			}
			a.lookedUp += len(hashes) / 32
			var held, queued Hashes
			for i := 0; i < len(hashes); i += 32 {
				switch h := [32]byte(hashes[i : i+32]); {
				case a.known[h]:
					held = append(held, h[:]...)
				case a.queued[h]:
					queued = append(queued, h[:]...)
				}
			}
			return held, queued, a.filterErr
		}).AnyTimes()
	sentryServer.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, r *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
			if a.blockSend {
				a.blockSend = false
				if a.blockStarted != nil {
					close(a.blockStarted)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if _, ok := ctx.Deadline(); !ok {
				a.noDeadline = true
			}
			a.requestedFrom = append(a.requestedFrom, gointerfaces.ConvertH512ToHash(r.PeerId))
			requestID, sentHashes, _, err := ParseGetPooledTransactions66(r.Data.Data, 0, nil)
			a.requestIDs = append(a.requestIDs, requestID)
			if err != nil {
				return nil, err
			}
			a.sentHashes = append(a.sentHashes, sentHashes)
			a.sends++
			if onSend := a.onSend; onSend != nil {
				a.onSend = nil
				onSend()
			}
			sent := &sentryproto.SentPeers{Peers: []*typesproto.H512{r.PeerId}}
			if a.gone[gointerfaces.ConvertH512ToHash(r.PeerId)] {
				sent = &sentryproto.SentPeers{}
			}
			if a.failSendAt > 0 && a.sends != a.failSendAt {
				return sent, nil
			}
			return sent, a.sendErr
		}).AnyTimes()
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, NewMockSentry(ctx, sentryServer))
	require.NoError(t, err)
	a.sentryClient = sentryClient
	a.fetch = NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())
	now := time.Unix(1_000_000, 0)
	a.now = &now
	a.fetch.now = func() time.Time { return *a.now }
	return a
}

func (a *announceFetchTest) announce(t *testing.T, peer [64]byte, hashes ...[32]byte) {
	a.announceSized(t, peer, 100, hashes...)
}

func (a *announceFetchTest) announceSized(t *testing.T, peer [64]byte, size uint32, hashes ...[32]byte) {
	var txTypes []byte
	var sizes []uint32
	var concat []byte
	for _, h := range hashes {
		txTypes, sizes, concat = append(txTypes, types.BlobTxType), append(sizes, size), append(concat, h[:]...)
	}
	buf := make([]byte, announcementsLen(txTypes, sizes, concat))
	announcement := buf[:encodeAnnouncements(txTypes, sizes, concat, buf)]
	require.NoError(t, a.fetch.handleInboundMessageWithTx(a.ctx, nil, &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, Data: announcement, PeerId: gointerfaces.ConvertHashToH512(peer),
	}, a.sentryClient))
}

// answer delivers n PooledTransactions replies from peer.
func (a *announceFetchTest) answer(peer [64]byte, n int) {
	p := fetchPeer{a.sentryClient, peer}
	for range n {
		a.fetch.requestedLock.Lock()
		var oldest uint64
		if slots := a.fetch.retriesOutstanding[p]; len(slots) > 0 {
			oldest = slots[0].id
		}
		a.fetch.requestedLock.Unlock()
		a.fetch.releaseRetry(p, oldest)
	}
}

func (a *announceFetchTest) timeoutSweep() {
	*a.now = a.now.Add(txFetchTimeout)
	a.fetch.requestFromAlternates(a.ctx, nil)
}

func TestAnnouncedHashRequestedFromAlternateAfterTimeout(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)

	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerA}, a.requestedFrom, "no retry before the timeout")
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "no announcers left")
}

func TestRepeatedAnnouncementsDoNotCrowdOutAlternates(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	for range 5 {
		a.announce(t, peerA, hash)
	}
	a.announce(t, peerB, hash)
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
}

func TestLateAnnouncementKeepsAlternates(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB, peerC := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	*a.now = a.now.Add(txFetchTimeout)
	a.announce(t, peerC, hash)
	require.Equal(t, [][64]byte{peerA}, a.requestedFrom, "a late announcer waits behind the saved alternate")
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB, peerC}, a.requestedFrom)
}

func TestAlternateAnnouncementIsRecorded(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash, [32]byte{0xbb})
	slot := &TxnSlot{IDHash: hash, Size: 100, Txn: &types.DynamicFeeTransaction{}}
	require.Error(t, a.fetch.checkPooledTxnAnnouncement(gointerfaces.ConvertHashToH512(peerB), slot))
}

func TestExpiredPeerReannouncementLetsAlternateBeAsked(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	*a.now = a.now.Add(txFetchTimeout)
	a.announce(t, peerA, hash)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
}

func TestPooledTxnRequestHoldsAtMost256Hashes(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, 300)
	for i := range hashes {
		hashes[i] = [32]byte{byte(i), byte(i >> 8), 0xcc}
	}
	a.announce(t, peerA, hashes...)
	require.Equal(t, [][64]byte{peerA, peerA}, a.requestedFrom, "one announcement of 300 hashes needs two requests")
	a.announce(t, peerB, hashes...)
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerA, peerB, peerB}, a.requestedFrom)
}

type boundedBatchStream struct {
	grpc.ClientStream
	recv func() (*sentryproto.InboundMessage, error)
}

func (s *boundedBatchStream) Recv() (*sentryproto.InboundMessage, error) { return s.recv() }

func TestFetchBoundsQueuedBatch(t *testing.T) {
	for _, tc := range []struct {
		name            string
		bytes, messages int
		wantFlushes     int32
	}{
		{"bytes", 1024 * 1024, 5, 1},
		{"messages", 1, 257, 1},
		{"bytes_before_append", 3 * 1024 * 1024, 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				pool := NewMockPool(ctrl)
				var flushes atomic.Int32
				pool.EXPECT().Started().DoAndReturn(func() bool { flushes.Add(1); return true }).AnyTimes()
				client := sentryproto.NewMockSentryClient(ctrl)
				var wg sync.WaitGroup
				fetch := NewFetch(t.Context(), nil, pool, nil, nil, u256.N1, log.New(), WithP2PFetcherWg(&wg))
				received := 0
				stream := &boundedBatchStream{recv: func() (*sentryproto.InboundMessage, error) {
					if received == tc.messages {
						require.Equal(t, tc.wantFlushes, flushes.Load(), "unexpected flush count before stream closure")
						return nil, nil
					}
					received++
					wg.Add(1)
					return &sentryproto.InboundMessage{Id: sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66, Data: make([]byte, tc.bytes)}, nil
				}}
				client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
				require.NoError(t, fetch.receiveMessage(t.Context(), client))
				wg.Wait()
				require.Equal(t, tc.messages, received)
			})
		})
	}
}

func retrySlotsLeftAfterReplies(t *testing.T, slots []retrySlot, replyIDs ...uint64) int {
	t.Helper()
	ctrl := gomock.NewController(t)
	pool := NewMockPool(ctrl)
	pool.EXPECT().Started().Return(false).AnyTimes()
	client := sentryproto.NewMockSentryClient(ctrl)
	fetch := NewFetch(t.Context(), nil, pool, nil, nil, u256.N1, log.New())
	peerB := [64]byte{0xb}
	peer := fetchPeer{client, peerB}
	fetch.retriesOutstanding[peer] = slots
	replies := make([]*sentryproto.InboundMessage, 0, len(replyIDs))
	for _, id := range replyIDs {
		replies = append(replies, &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: EncodePooledTransactions66(nil, id, nil), PeerId: gointerfaces.ConvertHashToH512(peerB)})
	}
	stream := &boundedBatchStream{recv: func() (*sentryproto.InboundMessage, error) {
		if len(replies) == 0 {
			return nil, nil
		}
		r := replies[0]
		replies = replies[1:]
		return r, nil
	}}
	client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
	require.NoError(t, fetch.receiveMessage(t.Context(), client))
	return len(fetch.retriesOutstanding[peer])
}

func TestRepliesFreeTheirOwnRetrySlots(t *testing.T) {
	require.Zero(t, retrySlotsLeftAfterReplies(t, []retrySlot{{id: 5}, {id: 6}}, 6, 5))
}

func TestDuplicateReplyFreesOneRetrySlot(t *testing.T) {
	require.Equal(t, 1, retrySlotsLeftAfterReplies(t, []retrySlot{{id: 5}, {id: 6}}, 5, 5))
}

func TestRepliesToInitialRequestsKeepTheRetrySlots(t *testing.T) {
	require.Equal(t, 2, retrySlotsLeftAfterReplies(t, []retrySlot{{id: 5}, {id: 6}}, initialRequestID))
}

func TestLateReplyToAnExpiredRetryKeepsANewerSlot(t *testing.T) {
	require.Equal(t, 1, retrySlotsLeftAfterReplies(t, []retrySlot{{id: 7}}, 5))
}

func TestRetriesCarryTheirSlotRequestID(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xe3}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.timeoutSweep()
	slots := a.fetch.retriesOutstanding[fetchPeer{a.sentryClient, peerB}]
	require.Len(t, slots, 1)
	require.Equal(t, []uint64{initialRequestID, slots[0].id}, a.requestIDs)
	require.NotEqual(t, initialRequestID, slots[0].id)
}

func TestFetchRevalidatesDuplicateReplyAfterNewBlock(t *testing.T) {
	t.Parallel()
	txnRlp := decodeHex("f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10")
	reply := &sentryproto.InboundMessage{
		Id:     sentryproto.MessageId_POOLED_TRANSACTIONS_66,
		Data:   EncodePooledTransactions66([][]byte{txnRlp}, 1, nil),
		PeerId: peerID,
	}
	ctrl := gomock.NewController(t)
	pool := NewMockPool(ctrl)
	pool.EXPECT().Started().Return(true).AnyTimes()
	pool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	pool.EXPECT().OnNewBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	pool.EXPECT().AddRemoteTxns(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(2)
	fetch := NewFetch(t.Context(), nil, pool, nil, mdbxtest.NewTestDB(t, dbcfg.TxPoolDB), u256.N1, log.New())
	newBlock := &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 1}}}

	// reply, duplicate reply in the same block, then the same reply after a new block
	received := 0
	stream := &boundedBatchStream{recv: func() (*sentryproto.InboundMessage, error) {
		received++
		if received == 3 {
			require.NoError(t, fetch.handleStateChangesRequest(t.Context(), newBlock))
		}
		if received > 3 {
			return nil, nil
		}
		return reply, nil
	}}
	client := sentryproto.NewMockSentryClient(ctrl)
	client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
	require.NoError(t, fetch.receiveMessage(t.Context(), client))
}

func TestFetchSerializesBatchFlushes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := NewMockPool(ctrl)
		started := make(chan struct{}, 3)
		release := make(chan struct{})
		releaseFirst := sync.OnceFunc(func() { close(release) })
		defer releaseFirst()
		pool.EXPECT().Started().DoAndReturn(func() bool {
			started <- struct{}{}
			<-release
			return true
		}).Times(3)
		messages := make(chan *sentryproto.InboundMessage)
		defer close(messages)
		stream := &boundedBatchStream{recv: func() (*sentryproto.InboundMessage, error) {
			return <-messages, nil
		}}
		client := sentryproto.NewMockSentryClient(ctrl)
		client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
		fetch := NewFetch(t.Context(), nil, pool, nil, nil, u256.N1, log.New())
		done := make(chan error, 1)
		go func() { done <- fetch.receiveMessage(t.Context(), client) }()

		message := &sentryproto.InboundMessage{Id: sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66}
		messages <- message
		<-started
		for range 257 {
			messages <- message
		}

		// Mutex waits do not quiesce synctest, so observe the blocked receiver directly.
		stacks := make([]runtime.StackRecord, 16)
	waitForThreshold:
		for {
			select {
			case <-started:
				t.Fatal("threshold flush started before the timer flush was released")
			default:
			}
			n, ok := runtime.GoroutineProfile(stacks)
			if !ok {
				stacks = make([]runtime.StackRecord, n)
				continue
			}
			for _, stack := range stacks[:n] {
				frames := runtime.CallersFrames(stack.Stack())
				var previous string
				var waiting, flushLocked, receiver bool
				for {
					frame, more := frames.Next()
					waiting = waiting || strings.HasSuffix(frame.Function, ".runtime_SemacquireMutex")
					if waiting && previous == "sync.(*Mutex).Lock" && strings.HasSuffix(frame.Function, ".(*Fetch).receiveMessage.func1") {
						flushLocked = true
					}
					receiver = receiver || strings.Contains(frame.Function, ".TestFetchSerializesBatchFlushes.")
					previous = frame.Function
					if !more {
						break
					}
				}
				if flushLocked && receiver {
					break waitForThreshold
				}
			}
			runtime.Gosched()
		}

		releaseFirst()
		<-started
		messages <- nil
		require.NoError(t, <-done)
	})
}

func makeWrappedFrameBlobTxnRlp(t *testing.T) []byte {
	frame := framePoolSlot(t, common.Address{1}, 1, 2).Txn.(*types.FrameTransaction)
	frame.BlobVersionedHashes = []common.Hash{{1}}
	wrapped := &types.FrameTxWrapper{FrameTransaction: frame, WrapperVersion: 1, Blobs: types.Blobs{{}}, Commitments: types.BlobKzgs{{}}, Proofs: make(types.KZGProofs, 128)}
	var encoded bytes.Buffer
	require.NoError(t, wrapped.MarshalBinaryWrapped(&encoded))
	return encoded.Bytes()
}

func makeBareFrameBlobTxnRlp(t *testing.T) []byte {
	frame := framePoolSlot(t, common.Address{1}, 1, 2).Txn.(*types.FrameTransaction)
	frame.BlobVersionedHashes = []common.Hash{{1}}
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	return encoded.Bytes()
}

func TestHasBlobTxnBareFrame(t *testing.T) {
	frame := framePoolSlot(t, common.Address{1}, 1, 2).Txn.(*types.FrameTransaction)
	frame.BlobVersionedHashes = nil
	var plain bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&plain))
	require.False(t, hasBlobTxn(EncodeTransactions([][]byte{plain.Bytes()}, nil)))
	require.True(t, hasBlobTxn(EncodeTransactions([][]byte{makeBareFrameBlobTxnRlp(t)}, nil)))
}

func TestHasBlobTxnBareFrameFieldLimit(t *testing.T) {
	// A type-6 body of many empty strings followed by a nonempty list is not a
	// valid frame body; the scan must give up instead of walking every field.
	body := bytes.Repeat([]byte{0x80}, frameTxnBodyFields)
	body = append(body, 0xc1, 0x01)
	rlpList := func(b []byte) []byte { return append([]byte{0xc0 + byte(len(b))}, b...) }
	require.False(t, isBlobEnvelope(append([]byte{FrameTxnType}, rlpList(body)...)))
	// Exactly frameTxnBodyFields fields with a nonempty last list is a blob frame.
	body = append(bytes.Repeat([]byte{0x80}, frameTxnBodyFields-1), 0xc1, 0x01)
	require.True(t, isBlobEnvelope(append([]byte{FrameTxnType}, rlpList(body)...)))
}

func TestPenalizePeerForBroadcastBlobTxn(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"blob", "frame-blob", "bare-frame-blob"} {
		for _, known := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/known=%t", kind, known), func(t *testing.T) {
				t.Parallel()
				blobTxnRlp := makeWrappedBlobTxnRlpWithCellProofs(t, uint256.NewInt(1), 1)
				switch kind {
				case "frame-blob":
					blobTxnRlp = makeWrappedFrameBlobTxnRlp(t)
				case "bare-frame-blob":
					blobTxnRlp = makeBareFrameBlobTxnRlp(t)
				}
				testPenalizePeerForBroadcastBlobTxn(t, known, blobTxnRlp)
			})
		}
	}
}

func TestHasBlobTxnStopsAtTransactionLimit(t *testing.T) {
	blobTxnRlp := []byte{BlobTxnType, 0xc0} // only the type byte is inspected
	ordinary := tpEncodeTests[1].txns[0]
	txns := make([][]byte, maxTransactionsPerPacket, maxTransactionsPerPacket+1)
	for i := range txns {
		txns[i] = ordinary
	}
	require.True(t, hasBlobTxn(EncodeTransactions(append(txns[:maxTransactionsPerPacket-1:maxTransactionsPerPacket-1], blobTxnRlp), nil)))
	require.False(t, hasBlobTxn(EncodeTransactions(append(txns, blobTxnRlp), nil)))
}

func TestPenalizeEveryPeerForDuplicateBroadcastBlobTxn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := NewMockPool(ctrl)
		pool.EXPECT().Started().Return(true).AnyTimes()
		client := sentryproto.NewMockSentryClient(ctrl)
		secondPeer := gointerfaces.ConvertHashToH512([64]byte{2})
		client.EXPECT().
			PenalizePeer(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, req *sentryproto.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
				assert.Equal(t, secondPeer, req.PeerId)
				assert.Equal(t, sentryproto.PenaltyKind_Kick, req.Penalty)
				return &emptypb.Empty{}, nil
			}).
			Times(1)
		// No db: the batched first message is dropped unhandled, so only the duplicate path can kick.
		fetch := NewFetch(t.Context(), nil, pool, nil, nil, u256.N1, log.New())
		payload := EncodeTransactions([][]byte{makeWrappedBlobTxnRlpWithCellProofs(t, uint256.NewInt(1), 1)}, nil)
		messages := []*sentryproto.InboundMessage{
			{Id: sentryproto.MessageId_TRANSACTIONS_66, Data: payload, PeerId: peerID},
			{Id: sentryproto.MessageId_TRANSACTIONS_66, Data: payload, PeerId: secondPeer},
		}
		stream := &boundedBatchStream{recv: func() (*sentryproto.InboundMessage, error) {
			if len(messages) == 0 {
				return nil, nil
			}
			req := messages[0]
			messages = messages[1:]
			return req, nil
		}}
		client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
		require.NoError(t, fetch.receiveMessage(t.Context(), client))
	})
}

func testPenalizePeerForBroadcastBlobTxn(t *testing.T, known bool, blobTxnRlp []byte) {
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	sentryServer := sentryproto.NewMockSentryServer(ctrl)
	pool := NewMockPool(ctrl)
	pool.EXPECT().Started().Return(true)
	pool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(known, nil).AnyTimes()
	pool.EXPECT().AddRemoteTxns(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	sentryServer.EXPECT().
		PenalizePeer(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.PenalizePeerRequest) (*emptypb.Empty, error) {
			assert.Equal(t, sentryproto.PenaltyKind_Kick, req.Penalty)
			return &emptypb.Empty{}, nil
		}).
		Times(1)

	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, NewMockSentry(ctx, sentryServer))
	require.NoError(t, err)
	fetch := NewFetch(ctx, []sentryproto.SentryClient{sentryClient}, pool, nil, nil, u256.N1, log.New())

	err = fetch.handleInboundMessageWithTx(ctx, nil, &sentryproto.InboundMessage{
		Id:     sentryproto.MessageId_TRANSACTIONS_66,
		Data:   EncodeTransactions([][]byte{tpEncodeTests[1].txns[0], blobTxnRlp}, nil),
		PeerId: peerID,
	}, sentryClient)
	require.NoError(t, err)
}

func TestAlternateRequestedWhenKnownHashLookupFails(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)

	a.filterErr = errors.New("db down")
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "a failed lookup must not drop the last alternate")
}

func TestPendingFetchesAreBounded(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hashes := make([][32]byte, 0, 1000)
	for i := range maxFetchRequests + 2000 {
		var h [32]byte
		binary.BigEndian.PutUint64(h[:], uint64(i)+1)
		if hashes = append(hashes, h); len(hashes) == cap(hashes) {
			a.announce(t, [64]byte{0xa}, hashes...)
			hashes = hashes[:0]
		}
	}
	require.LessOrEqual(t, len(a.fetch.pending), maxFetchRequests)
}

func TestHashDroppedFromPendingIsRequestedAgain(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hot, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hot)
	a.announce(t, peerB, hot)
	hashes := make([][32]byte, 0, 1000)
	for i := range maxFetchRequests {
		var h [32]byte
		binary.BigEndian.PutUint64(h[:], uint64(i)+1)
		if hashes = append(hashes, h); len(hashes) == cap(hashes) || i == maxFetchRequests-1 {
			a.announce(t, [64]byte{0xc}, hashes...)
			hashes = hashes[:0]
		}
		if i%10000 == 0 {
			a.announce(t, peerA, hot) // keeps the hot hash in the LRU
		}
	}
	*a.now = a.now.Add(txFetchTimeout)
	before := len(a.requestedFrom)
	a.announce(t, peerA, hot)
	require.Greater(t, len(a.requestedFrom), before, "a hash whose retry was dropped is requested on its next announcement")
}

func TestFullPendingDoesNotEvictAnotherPendingRequest(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	x, y, peerA, peerB := [32]byte{0xa1}, [32]byte{0xa2}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, x)
	a.announce(t, peerA, y)
	a.announce(t, peerB, y)
	a.announce(t, peerA, x) // x is now the most recently used, y the least
	hashes := make([][32]byte, 0, 1000)
	for i := range maxFetchRequests - 1 {
		var h [32]byte
		binary.BigEndian.PutUint64(h[:], uint64(i)+1)
		if hashes = append(hashes, h); len(hashes) == cap(hashes) || i == maxFetchRequests-2 {
			a.announce(t, [64]byte{0xc}, hashes...)
			hashes = hashes[:0]
		}
	}
	r, ok := a.fetch.requested.Get(y[:])
	require.True(t, ok, "y is still pending, so its request must stay")
	require.Equal(t, []fetchPeer{{a.sentryClient, peerB}}, r.alternates)
}

func TestRestampedRetryKeepsTheQueueWithinTheCache(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, peerB := [32]byte{0xb1}, [32]byte{0xb2}, [64]byte{0xa}, [64]byte{0xb}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, h1, h2)
	a.announceSized(t, peerB, blobTxnSize, h1, h2)
	*a.now = a.now.Add(2 * time.Second)
	hashes := make([][32]byte, 0, 1000)
	for i := range maxFetchRequests - 2 {
		var h [32]byte
		binary.BigEndian.PutUint64(h[:], uint64(i)+1)
		if hashes = append(hashes, h); len(hashes) == cap(hashes) || i == maxFetchRequests-3 {
			a.announce(t, [64]byte{0xc}, hashes...)
			hashes = hashes[:0]
		}
	}
	a.onSend = func() { *a.now = a.now.Add(time.Second) } // the second batch is claimed later and restamped
	*a.now = a.now.Add(txFetchTimeout - 2*time.Second)
	a.fetch.requestFromAlternates(a.ctx, nil)
	a.announce(t, [64]byte{0xc}, [32]byte{0xfe})
	require.LessOrEqual(t, len(a.fetch.pending), maxFetchRequests, "a trimmed stale entry frees no cache slot, so the next insert would evict a live request")
}

func TestRetryRequestsFitTheResponseLimit(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	blob1, blob2, peerA, peerB := [32]byte{0xb1}, [32]byte{0xb2}, [64]byte{0xa}, [64]byte{0xb}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, blob1)
	a.announceSized(t, peerA, blobTxnSize, blob2)
	a.announceSized(t, peerB, blobTxnSize, blob1, blob2)
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerB, peerB}, a.requestedFrom, "a responder stops at p2pTxPacketLimit, so each blob needs its own request")
}

func TestRetryRequestsPerPeerArePaced(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, (maxRetryRequestsPerPeer+2)*256)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	for c := range slices.Chunk(hashes, 1000) {
		a.announce(t, peerA, c...)
		a.announce(t, peerB, c...)
	}
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer, "a sweep must not overflow the peer's outbound queue")
	a.answer(peerB, maxRetryRequestsPerPeer)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer+2, "the rest keeps its alternate and goes once the peer answers")
}

func TestRetryWithUnknownSizeGetsItsOwnRequest(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, peerB := [32]byte{0xc1}, [32]byte{0xc2}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, h1, h2)
	a.announce(t, peerB, h1, h2)
	a.fetch.announcements, _ = maphash.NewLRU[announceInfo](16) // B's sizes were evicted
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerB, peerB}, a.requestedFrom, "an unknown size may be a blob, so it is asked alone")
}

func TestDeliveredTxnsDoNotUseRetryBudget(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, maxRetryRequestsPerPeer*256+1)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	for c := range slices.Chunk(hashes, 1000) {
		a.announce(t, peerA, c...)
		a.announce(t, peerB, c...)
	}
	a.known = map[[32]byte]bool{}
	for _, h := range hashes[:len(hashes)-1] {
		a.known[h] = true
	}
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerB}, a.requestedFrom, "only the missing txn is retried")
}

func TestSlowLookupDoesNotExpireTheRetry(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB, peerC := [32]byte{0xd1}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.announce(t, peerC, hash)
	a.onFilter = func() { *a.now = a.now.Add(txFetchTimeout) } // the known-hash lookup takes a whole timeout
	a.timeoutSweep()
	a.onFilter = nil
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "B was asked just now, not a timeout ago")
}

func TestQueuedBodyKeepsAlternatesUntilValidated(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xe1}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.queued = map[[32]byte]bool{hash: true} // A's body is queued, not yet validated
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA}, a.requestedFrom)
	delete(a.queued, hash) // the body failed validation
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "the alternate is still there")
}

func TestBudgetDeferredHashesWaitForAnAnswer(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, (maxRetryRequestsPerPeer+2)*256)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	for c := range slices.Chunk(hashes, 1000) {
		a.announce(t, peerA, c...)
		a.announce(t, peerB, c...)
	}
	a.timeoutSweep()
	looked := a.lookedUp
	for range 3 {
		*a.now = a.now.Add(time.Second)
		a.fetch.requestFromAlternates(a.ctx, nil)
	}
	require.Equal(t, looked, a.lookedUp, "a hash waiting for B's retry room is not looked up again before B answers")
	a.answer(peerB, 1)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Greater(t, a.lookedUp, looked)
}

func TestRetryBudgetHoldsAcrossSweeps(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, 2*maxRetryRequestsPerPeer*256)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	for c := range slices.Chunk(hashes, 1000) {
		a.announce(t, peerA, c...)
		a.announce(t, peerB, c...)
	}
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer)
	*a.now = a.now.Add(time.Second)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer, "the peer may still be draining the last sweep's requests")
	a.answer(peerB, maxRetryRequestsPerPeer)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Len(t, a.requestedFrom, 2*maxRetryRequestsPerPeer)
}

func TestSlowPeerGetsNoRetriesUntilItAnswers(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, 4*maxRetryRequestsPerPeer)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.requestedFrom = nil
	a.timeoutSweep()
	for range 32 { // a writer taking 19 s per message stays connected and drains sentry's full queue
		*a.now = a.now.Add(19 * time.Second)
		a.fetch.requestFromAlternates(a.ctx, nil)
	}
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer, "unanswered retries hold the peer's queue")
	a.answer(peerB, 1)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer+1, "each answer frees one retry")
}

// Sentry's queue can drop a retry, which is then never answered; its slot must not hold the peer forever.
func TestDroppedRetriesFreeTheirSlots(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, 4*maxRetryRequestsPerPeer)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Len(t, a.requestedFrom, maxRetryRequestsPerPeer)
	*a.now = a.now.Add(retryAnswerTimeout)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Greater(t, len(a.requestedFrom), maxRetryRequestsPerPeer, "dropped retries still hold the peer")
}

func TestStalledRetrySendKeepsLaterBatchesWithTheirPeer(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, peerB, peerC := [32]byte{0xd1}, [32]byte{0xd2}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	const blobTxnSize = 131 * 1024
	for _, peer := range [][64]byte{peerA, peerB, peerC} {
		a.announceSized(t, peer, blobTxnSize, h1, h2)
	}
	a.requestedFrom, a.sentHashes = nil, nil
	a.onSend = a.timeoutSweep // the first send to B stalls past the timeout, so another sweep runs
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerB, peerC, peerB}, a.requestedFrom, "only the sent hash moves on to C; the unsent one goes to B once")
	require.Equal(t, []Hashes{h1[:], h1[:], h2[:]}, a.sentHashes)
}

func TestBlockedFirstSendDoesNotStrandLaterChunks(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, 257)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	a.blockSend = true
	announcement := func() []byte {
		var txTypes []byte
		var sizes []uint32
		var concat []byte
		for _, h := range hashes {
			txTypes, sizes, concat = append(txTypes, types.BlobTxType), append(sizes, 100), append(concat, h[:]...)
		}
		buf := make([]byte, announcementsLen(txTypes, sizes, concat))
		return buf[:encodeAnnouncements(txTypes, sizes, concat, buf)]
	}()
	_ = a.fetch.handleInboundMessageWithTx(a.ctx, nil, &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, Data: announcement, PeerId: gointerfaces.ConvertHashToH512(peerA),
	}, a.sentryClient)
	a.requestedFrom, a.sentHashes = nil, nil
	a.announce(t, peerB, hashes[256])
	require.Equal(t, [][64]byte{peerB}, a.requestedFrom, "a hash left reserved for A would only gain B as an alternate")
	require.Equal(t, []Hashes{hashes[256][:]}, a.sentHashes)
}

func TestReplyDuringTheRetrySendFreesItsSlot(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xd8}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.onSend = func() { a.answer(peerB, 1) } // B answers before SendMessageById returns
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
	require.Empty(t, a.fetch.retriesOutstanding[fetchPeer{a.sentryClient, peerB}], "an answered retry must not stay outstanding")
}

func TestNewAnnouncerOfAWaitingHashIsAskedAtOnce(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB, peerC := [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	hashes := make([][32]byte, maxRetryRequestsPerPeer+1)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.timeoutSweep() // B's retry room is spent: the last hash waits
	last := hashes[len(hashes)-1]
	a.announceSized(t, peerC, blobTxnSize, last)
	a.requestedFrom, a.sentHashes = nil, nil
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerC}, a.requestedFrom, "C has room, so the waiting hash does not wait for B")
	require.Equal(t, []Hashes{last[:]}, a.sentHashes)
}

func TestStaleWaitingEntriesAreDropped(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, maxRetryRequestsPerPeer+1)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.timeoutSweep()
	require.Len(t, a.fetch.waiting, 1)
	a.fetch.requested.Delete(hashes[len(hashes)-1][:]) // evicted while waiting
	a.answer(peerB, 1)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Empty(t, a.fetch.waiting, "a sweep with nothing to retry still drops the stale entry")
}

func TestWaitingHashWithoutAlternatesIsDropped(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, maxRetryRequestsPerPeer+1)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.timeoutSweep()
	require.Len(t, a.fetch.waiting, 1)
	a.fetch.dropAlternate(fetchPeer{a.sentryClient, peerB}) // its only alternate disconnects
	a.fetch.requestFromAlternates(a.ctx, nil)
	_, ok := a.fetch.requested.Get(hashes[len(hashes)-1][:])
	require.False(t, ok, "a waiting hash with no alternate left must free its slot")
	require.Empty(t, a.fetch.waiting)
}

// A waiting hash whose body is queued for validation keeps its entry when its last alternate leaves: the body may
// still fail, and a later announcer must then be kept as a fallback.
func TestWaitingHashWithAQueuedBodyKeepsItsEntry(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, maxRetryRequestsPerPeer+1)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.timeoutSweep()
	require.Len(t, a.fetch.waiting, 1)
	waitingHash := hashes[len(hashes)-1]
	a.queued = map[[32]byte]bool{waitingHash: true} // A's late body waits for validation
	a.fetch.dropAlternate(fetchPeer{a.sentryClient, peerB})
	a.fetch.requestFromAlternates(a.ctx, nil)
	_, ok := a.fetch.requested.Get(waitingHash[:])
	require.True(t, ok, "a queued body may still be rejected")
}

// A waiting hash is sent only after a pool lookup in the same sweep, even if room opens during that lookup.
func TestRoomOpenedDuringTheLookupWaitsForTheNextLookup(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, maxRetryRequestsPerPeer+1)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, hashes...)
	a.announceSized(t, peerB, blobTxnSize, hashes...)
	a.timeoutSweep()
	require.Len(t, a.fetch.waiting, 1)
	waitingHash := hashes[len(hashes)-1]
	a.known = map[[32]byte]bool{waitingHash: true}                  // the pool got it meanwhile
	a.fetch.dropAlternate(fetchPeer{a.sentryClient, [64]byte{0xc}}) // an unrelated disconnect re-scans the waiting list
	a.onFilter = func() { a.onFilter = nil; a.answer(peerB, 1) }
	a.sentHashes = nil
	a.timeoutSweep() // looks up the expired retries; B answers during the lookup
	for _, sent := range a.sentHashes {
		require.NotContains(t, string(sent), string(waitingHash[:]), "a hash the pool holds was requested without a lookup")
	}
}

func TestDisconnectDuringAStalledSendReachesTheUnsentRequest(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, peerB, peerC := [32]byte{0xd5}, [32]byte{0xd6}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	const blobTxnSize = 131 * 1024
	for _, peer := range [][64]byte{peerA, peerB, peerC} {
		a.announceSized(t, peer, blobTxnSize, h1, h2)
	}
	gone := fetchPeer{a.sentryClient, peerC}
	a.onSend = func() { // the first send to B stalls: a sweep runs, then C disconnects
		a.timeoutSweep()
		a.fetch.dropAlternate(gone)
	}
	a.timeoutSweep()
	require.Zero(t, a.fetch.alternatesOf[gone], "the unsent request keeps its pending entry, so the cleanup finds C there")
}

func TestRetrySendHasADeadline(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xd7}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.noDeadline = false
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom)
	require.False(t, a.noDeadline, "a stalled sentry must not hold the one retry loop")
}

func TestBlockedSentryDoesNotDelayOtherSentries(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	asked := make(chan struct{}, 1)
	otherServer := sentryproto.NewMockSentryServer(gomock.NewController(t))
	otherServer.EXPECT().SendMessageById(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, r *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
			select { // waits for the blocked send, so serial sends fail in either order
			case <-a.blockStarted:
				asked <- struct{}{}
			case <-ctx.Done():
			}
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{r.PeerId}}, nil
		}).AnyTimes()
	other, err := direct.NewSentryClientDirect(direct.ETH68, NewMockSentry(a.ctx, otherServer))
	require.NoError(t, err)
	h1, h2, peerA, peerB, peerC := [32]byte{0xe1}, [32]byte{0xe2}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	a.announce(t, peerA, h1, h2)
	a.announce(t, peerB, h1)
	blockedSentry := a.sentryClient
	a.sentryClient = other
	a.announce(t, peerC, h2)
	a.sentryClient = blockedSentry
	a.blockSend, a.blockStarted = true, make(chan struct{})
	*a.now = a.now.Add(txFetchTimeout)
	ctx, cancel := context.WithCancel(a.ctx)
	defer cancel()

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		a.fetch.requestFromAlternates(ctx, nil)
	}()
	select {
	case <-asked:
	case <-time.After(3 * time.Second):
		t.Fatal("a blocked sentry delayed the request on another sentry")
	}
	cancel()
	<-swept
}

func TestStalledSendKeepsTheFinalFallback(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, peerB := [32]byte{0xd3}, [32]byte{0xd4}, [64]byte{0xa}, [64]byte{0xb}
	const blobTxnSize = 131 * 1024
	a.announceSized(t, peerA, blobTxnSize, h1, h2)
	a.announceSized(t, peerB, blobTxnSize, h1, h2)
	a.requestedFrom = nil
	a.onSend = a.timeoutSweep
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerB, peerB}, a.requestedFrom, "B is the last announcer of the unsent hash, so it is still asked")
}

func TestStalledFirstChunkKeepsTheLastChunkWithItsPeer(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, peerB := [64]byte{0xa}, [64]byte{0xb}
	hashes := make([][32]byte, 257)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	a.onSend = func() { // A's first request stalls past the timeout while B announces the same hashes
		*a.now = a.now.Add(txFetchTimeout)
		a.announce(t, peerB, hashes...)
	}
	a.announce(t, peerA, hashes...)
	require.Equal(t, [][64]byte{peerA, peerB, peerA}, a.requestedFrom, "B asks only for the timed-out chunk")
	r, ok := a.fetch.requested.Get(hashes[256][:])
	require.True(t, ok)
	require.Equal(t, fetchPeer{a.sentryClient, peerA}, r.peer)
	require.Equal(t, []fetchPeer{{a.sentryClient, peerB}}, r.alternates)
}

func TestRetryBudgetCountsTheSizeSlack(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, peerB := [32]byte{0xf1}, [32]byte{0xf2}, [64]byte{0xa}, [64]byte{0xb}
	const size = p2pTxPacketLimit - 4 // the delivered body may be up to announcedSizeSlack larger
	a.announceSized(t, peerA, size, h1, h2)
	a.announceSized(t, peerB, size, h1, h2)
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerB, peerB}, a.requestedFrom)
}

func TestFailedRetrySendKeepsThePeer(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xe7}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.sendErr = errors.New("sentry busy")
	a.timeoutSweep()
	require.Empty(t, a.fetch.retriesOutstanding[fetchPeer{a.sentryClient, peerB}], "a retry that was not sent holds no slot")
	a.sendErr = nil
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB, peerB}, a.requestedFrom, "the last alternate is asked again after a failed send")
}

func TestExhaustedRequestLeavesTheCache(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA := [32]byte{0xe8}, [64]byte{0xa}
	a.announce(t, peerA, hash)
	a.timeoutSweep()
	_, ok := a.fetch.requested.Get(hash[:])
	require.False(t, ok, "a timed-out request with no alternates must not hold a cache slot")
}

func TestFailedFirstSendReleasesTheHash(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xe9}, [64]byte{0xa}, [64]byte{0xb}
	a.sendErr = errors.New("sentry busy")
	buf := make([]byte, announcementsLen([]byte{types.BlobTxType}, []uint32{100}, hash[:]))
	ann := buf[:encodeAnnouncements([]byte{types.BlobTxType}, []uint32{100}, hash[:], buf)]
	_ = a.fetch.handleInboundMessageWithTx(a.ctx, nil, &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, Data: ann, PeerId: gointerfaces.ConvertHashToH512(peerA),
	}, a.sentryClient)
	a.sendErr = nil
	a.announce(t, peerB, hash)
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "an unsent request must not hold the hash")
}

func TestFailedSendDoesNotReleaseANewerReservation(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xea}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	*a.now = a.now.Add(txFetchTimeout)
	a.announce(t, peerB, hash) // A timed out; B holds the reservation now
	a.fetch.releaseRequested(fetchPeer{a.sentryClient, peerA}, hash[:])
	r, ok := a.fetch.requested.Get(hash[:])
	require.True(t, ok, "A's late failure must not drop B's reservation")
	require.Equal(t, peerB, r.peer.peerID)
}

func TestFailedChunkReleasesOnlyUnsentHashes(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hashes := make([][32]byte, 300)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:], uint64(i)+1)
	}
	a.sendErr, a.failSendAt = errors.New("sentry busy"), 2
	var concat []byte
	var txTypes []byte
	var sizes []uint32
	for _, h := range hashes {
		txTypes, sizes, concat = append(txTypes, types.BlobTxType), append(sizes, 100), append(concat, h[:]...)
	}
	buf := make([]byte, announcementsLen(txTypes, sizes, concat))
	ann := buf[:encodeAnnouncements(txTypes, sizes, concat, buf)]
	_ = a.fetch.handleInboundMessageWithTx(a.ctx, nil, &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, Data: ann, PeerId: gointerfaces.ConvertHashToH512([64]byte{0xa}),
	}, a.sentryClient)
	_, sentKept := a.fetch.requested.Get(hashes[0][:])
	_, unsentKept := a.fetch.requested.Get(hashes[299][:])
	require.True(t, sentKept, "the sent chunk stays reserved")
	require.False(t, unsentKept, "the failed chunk is released")
}

func TestSilentAnnouncersDoNotCrowdOutALaterAnnouncer(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, honest := [32]byte{0xaa}, [64]byte{0xee}
	for i := range 5 {
		a.announce(t, [64]byte{byte(i + 1)}, hash)
	}
	a.announce(t, honest, hash)
	for range 5 {
		a.timeoutSweep()
	}
	require.Contains(t, a.requestedFrom, honest, "the later announcer must be asked after the silent ones")
}

func TestReannouncementsAfterTheTimeoutDoNotPostponeAlternates(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, silentX, silentY, honest := [32]byte{0xaa}, [64]byte{0x1}, [64]byte{0x2}, [64]byte{0xee}
	a.announce(t, silentX, hash)
	a.announce(t, honest, hash)
	for i := range 3 {
		*a.now = a.now.Add(txFetchTimeout)
		a.announce(t, [][64]byte{silentY, silentX}[i%2], hash)
		a.fetch.requestFromAlternates(a.ctx, nil)
	}
	require.Contains(t, a.requestedFrom, honest, "a saved alternate must be asked before re-announcers")
}

func TestRejectedBodyKeepsAlternates(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.discarded = map[[32]byte]bool{hash: true} // peerA's body failed validation
	a.timeoutSweep()
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "a rejected body is no reason to drop the other announcers")
}

func TestDeferredHashUsesAnotherAlternate(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	peerA, busy, idle := [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	var hashes [][32]byte
	for i := range maxRetryRequestsPerPeer + 1 {
		hashes = append(hashes, [32]byte{byte(i), 0xcc})
	}
	a.announceSized(t, peerA, p2pTxPacketLimit, hashes...)
	a.announceSized(t, busy, p2pTxPacketLimit, hashes...) // one full-size hash per request: the last one does not fit
	a.announceSized(t, idle, p2pTxPacketLimit, hashes[len(hashes)-1])
	a.requestedFrom = nil
	a.timeoutSweep()
	require.Contains(t, a.requestedFrom, idle, "the hash beyond busy's budget goes to its other alternate")
}

func TestAlternateQuotaBoundsAPeerAndIsReleased(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB, full := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xf}
	fullPeer := fetchPeer{a.sentryClient, full}
	a.fetch.alternatesOf[fullPeer] = maxAlternatesPerPeer
	a.announce(t, peerA, hash)
	a.announce(t, full, hash)
	a.announce(t, peerB, hash)
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "a peer at its quota is not kept as an alternate")

	delete(a.fetch.alternatesOf, fullPeer)
	a.announce(t, [64]byte{0xc}, hash)
	a.announce(t, [64]byte{0xd}, hash)
	require.NotEmpty(t, a.fetch.alternatesOf)
	a.known = map[[32]byte]bool{hash: true}
	a.timeoutSweep()
	require.Empty(t, a.fetch.alternatesOf, "a held txn releases the alternates still waiting")
}

func TestPeerGoneAtSendLeavesEveryAlternate(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	h1, h2, peerA, gone, peerC := [32]byte{0xa1}, [32]byte{0xa2}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	a.announce(t, peerA, h1)
	a.announce(t, gone, h1)
	*a.now = a.now.Add(time.Second) // h2 is requested later, so h1 retries first
	a.announce(t, peerA, h2)
	a.announce(t, gone, h2)
	a.announce(t, peerC, h2)
	a.gone = map[[64]byte]bool{gone: true}           // its Disconnect event was lost
	*a.now = a.now.Add(txFetchTimeout - time.Second) // h1 expired, h2 not yet
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.NotContains(t, a.fetch.alternatesOf, fetchPeer{a.sentryClient, gone}, "a send that reaches no one drops the peer everywhere")
	a.requestedFrom = nil
	*a.now = a.now.Add(time.Second)
	a.fetch.requestFromAlternates(a.ctx, nil)
	require.Equal(t, [][64]byte{peerC}, a.requestedFrom, "h2 goes to its live alternate")
}

func TestFailedSendKeepsThePeerAtFullQuota(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.announce(t, peerB, hash)
	a.sendErr, a.failSendAt = errors.New("queue full"), 2
	a.fetch.alternatesOf[fetchPeer{a.sentryClient, peerB}] = maxAlternatesPerPeer + 1 // this hash plus a full quota of others, announced meanwhile
	a.timeoutSweep()
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB, peerB}, a.requestedFrom, "a peer whose send failed is asked again, even at its quota")
}

func TestDisconnectedPeerLeavesTheAlternates(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, gone, peerC := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}, [64]byte{0xc}
	a.announce(t, peerA, hash)
	a.announce(t, gone, hash)
	a.announce(t, peerC, hash)
	require.NoError(t, a.fetch.handleNewPeer(a.sentryClient, &sentryproto.PeerEvent{PeerId: gointerfaces.ConvertHashToH512(gone), EventId: sentryproto.PeerEvent_Disconnect}))
	require.NotContains(t, a.fetch.alternatesOf, fetchPeer{a.sentryClient, gone}, "a disconnected peer holds no alternate quota")
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerC}, a.requestedFrom, "the disconnected peer is not asked")
}

func TestDisconnectedPeerIsNotKeptByALateAnnouncementOrFailedSend(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hashLate, hashSent, peerA, gone := [32]byte{0xaa}, [32]byte{0xbb}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hashSent)
	a.announce(t, gone, hashSent)
	a.timeoutSweep() // gone is asked for hashSent
	require.NoError(t, a.fetch.handleNewPeer(a.sentryClient, &sentryproto.PeerEvent{PeerId: gointerfaces.ConvertHashToH512(gone), EventId: sentryproto.PeerEvent_Disconnect}))
	a.fetch.keepAlternate(fetchPeer{a.sentryClient, gone}, hashSent[:], *a.now) // its retry send failed after the disconnect

	a.announce(t, peerA, hashLate)
	a.announce(t, gone, hashLate) // buffered before the disconnect, handled after it
	require.NotContains(t, a.fetch.alternatesOf, fetchPeer{a.sentryClient, gone}, "a disconnected peer is not an alternate again")

	require.NoError(t, a.fetch.handleNewPeer(a.sentryClient, &sentryproto.PeerEvent{PeerId: gointerfaces.ConvertHashToH512(gone), EventId: sentryproto.PeerEvent_Connect}))
	a.announce(t, gone, hashLate)
	require.Contains(t, a.fetch.alternatesOf, fetchPeer{a.sentryClient, gone}, "a reconnected peer is an alternate again")
}

func TestAnnouncerOfAQueuedBodyIsKeptAsAlternate(t *testing.T) {
	t.Parallel()
	a := newAnnounceFetchTest(t)
	hash, peerA, peerB := [32]byte{0xaa}, [64]byte{0xa}, [64]byte{0xb}
	a.announce(t, peerA, hash)
	a.queued = map[[32]byte]bool{hash: true} // A's body waits for validation, so the pool knows the hash
	a.announce(t, peerB, hash)
	require.Equal(t, [][64]byte{peerA}, a.requestedFrom, "a known hash is not requested again")
	a.queued = nil // A's sidecar failed validation
	a.timeoutSweep()
	require.Equal(t, [][64]byte{peerA, peerB}, a.requestedFrom, "the peer that announced while the body was queued is asked")
}
