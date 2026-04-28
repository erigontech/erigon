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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
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
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
						}}, nil
				}).AnyTimes()

		m := NewMockSentry(ctx, sentryServer)
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
		require.NoError(t, err)
		send := NewSend(ctx, []sentryproto.SentryClient{sentryClient}, log.New())
		list := make(Hashes, p2pTxPacketLimit*3)
		for i := 0; i < len(list); i += 32 {
			b := fmt.Appendf(nil, "%x", i)
			copy(list[i:i+32], b)
		}
		send.BroadcastPooledTxns(testRlps(len(list)/32), 100)
		send.AnnouncePooledTxns([]byte{0, 1, 2}, []uint32{10, 12, 14}, list, 100)

		require.Len(t, requests, 2)

		txnsMessage := requests[0].Data
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, txnsMessage.Id)
		require.Positive(t, len(txnsMessage.Data))

		txnHashesMessage := requests[1].Data
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
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
						}}, nil
				}).AnyTimes()

		m := NewMockSentry(ctx, sentryServer)
		sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
	_, db := memdb.NewTestDB(t, dbcfg.ChainDB), memdb.NewTestDB(t, dbcfg.TxPoolDB)
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
		DoAndReturn(func(_ context.Context, _ *remoteproto.StateChangeRequest, _ ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
			return stream, nil
		})

	pool := NewMockPool(ctrl)

	pool.EXPECT().
		ValidateSerializedTxn(gomock.Any()).
		DoAndReturn(func(_ []byte) error {
			return nil
		}).
		Times(3)

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

func TestPenalizePeerForMalformedMessages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

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
			sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
			sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
	pool.EXPECT().ValidateSerializedTxn(gomock.Any()).Return(nil).AnyTimes()

	dbErr := fmt.Errorf("mdbx read error")
	pool.EXPECT().IdHashKnown(gomock.Any(), gomock.Any()).Return(false, dbErr).AnyTimes()

	// PenalizePeer must NOT be called.
	sentryServer.EXPECT().PenalizePeer(gomock.Any(), gomock.Any()).Times(0)

	m := NewMockSentry(ctx, sentryServer)
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, m, nil)
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
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH68, srv, nil)
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
	for i := 0; i < num; i++ {
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
