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

package txpool

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type fetchTestStream struct {
	grpc.ClientStream
	recv func() (*sentryproto.InboundMessage, error)
}

func (s *fetchTestStream) Recv() (*sentryproto.InboundMessage, error) { return s.recv() }

func newFetchTestClient(t *testing.T) (*sentryproto.MockSentryClient, <-chan *sentryproto.SendMessageByIdRequest, <-chan *sentryproto.PenalizePeerRequest) {
	t.Helper()
	// One extra slot preserves duplicate calls without blocking the fetcher.
	sent := make(chan *sentryproto.SendMessageByIdRequest, 2)
	penalties := make(chan *sentryproto.PenalizePeerRequest, 2)
	client := sentryproto.NewMockSentryClient(gomock.NewController(t))
	client.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			select {
			case sent <- request:
			default:
			}
			return &sentryproto.SentPeers{}, nil
		}).AnyTimes()
	client.EXPECT().PenalizePeer(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *sentryproto.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			select {
			case penalties <- request:
			default:
			}
			return &emptypb.Empty{}, nil
		}).AnyTimes()
	return client, sent, penalties
}

func receiveFetchTestMessages(t *testing.T, fetch *Fetch, client *sentryproto.MockSentryClient, steps ...func() *sentryproto.InboundMessage) {
	t.Helper()
	i := 0
	stream := &fetchTestStream{recv: func() (*sentryproto.InboundMessage, error) {
		fetch.wg.Wait()
		if i == len(steps) {
			return nil, nil
		}
		message := steps[i]()
		i++
		if message != nil {
			fetch.wg.Add(1)
		}
		return message, nil
	}}
	client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
	require.NoError(t, fetch.receiveMessage(t.Context(), client))
}

func newFetchTestTransaction(t *testing.T, nonce uint64) (*TxPool, kv.RwDB, types.Transaction, *remoteproto.StateChangeBatch) {
	t.Helper()
	pool, db, signed, change := newFetchTestTransactions(t, nonce)
	return pool, db, signed[0], change
}

func newFetchTestTransactions(t *testing.T, nonces ...uint64) (*TxPool, kv.RwDB, []types.Transaction, *remoteproto.StateChangeBatch) {
	t.Helper()
	ctx, pool, db, _, _ := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	require.NoError(t, pool.start(ctx))
	pool.cfg.MaxNonceGap = 64
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signed := make([]types.Transaction, 0, len(nonces))
	for _, nonce := range nonces {
		txn := newTestTxnSlot(nonce, 0, 300_000, 300_000, 100_000).Txn.(*types.DynamicFeeTransaction)
		txn.ChainID = pool.chainID
		signedTxn, err := types.SignTx(txn, *types.LatestSignerForChainID(&pool.chainID), key)
		require.NoError(t, err)
		signed = append(signed, signedTxn)
	}
	account := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 1, BlockGasLimit: 1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 1, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Changes: []*remoteproto.AccountChange{{
				Action: remoteproto.Action_UPSERT, Address: gointerfaces.ConvertAddressToH160(sender), Data: accounts.SerialiseV3(&account),
			}},
		}},
	}
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	return pool, db, signed, change
}

func TestFetchRetriesIdenticalTransactionAfterNonceGap(t *testing.T) {
	for _, tc := range []struct {
		name      string
		messageID sentryproto.MessageId
		otherPeer bool
	}{
		{"same peer", sentryproto.MessageId_POOLED_TRANSACTIONS_66, false},
		{"other peer", sentryproto.MessageId_POOLED_TRANSACTIONS_66, true},
		{"broadcast", sentryproto.MessageId_TRANSACTIONS_66, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			pool, db, signed, change := newFetchTestTransaction(t, 65)
			var encoded bytes.Buffer
			require.NoError(t, signed.MarshalBinary(&encoded))
			body := EncodePooledTransactions66([][]byte{encoded.Bytes()}, 1, nil)
			if tc.messageID == sentryproto.MessageId_TRANSACTIONS_66 {
				body = EncodeTransactions([][]byte{encoded.Bytes()}, nil)
			}
			hash := signed.Hash()
			client, sent, penalties := newFetchTestClient(t)
			var wg sync.WaitGroup
			fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
			reply := &sentryproto.InboundMessage{Id: tc.messageID, Data: body, PeerId: peerID}
			receiveFetchTestMessages(t, fetch, client,
				func() *sentryproto.InboundMessage { return reply },
				func() *sentryproto.InboundMessage {
					require.NoError(t, pool.processRemoteTxns(ctx))
					record, found := pool.discardReasonsLRU.Get(string(hash[:]))
					require.True(t, found)
					require.Equal(t, txpoolcfg.NonceTooDistant, record.reason)
					require.Empty(t, pool.byHash)
					change.ChangeBatch[0].BlockHeight = 2
					change.ChangeBatch[0].Changes[0].Data = accounts.SerialiseV3(&accounts.Account{
						Nonce: 65, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
					})
					require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
					if tc.otherPeer {
						reply = &sentryproto.InboundMessage{Id: tc.messageID, Data: body, PeerId: gointerfaces.ConvertHashToH512([64]byte{2})}
					}
					return &sentryproto.InboundMessage{Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, Data: EncodeHashes(hash[:], nil), PeerId: reply.PeerId}
				},
				func() *sentryproto.InboundMessage { return reply },
				func() *sentryproto.InboundMessage {
					require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
					return reply
				},
				func() *sentryproto.InboundMessage {
					require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
					require.NoError(t, pool.processRemoteTxns(ctx))
					require.Contains(t, pool.byHash, string(hash[:]))
					pending, baseFee, queued := pool.CountContent()
					require.Equal(t, 1, pending)
					require.Zero(t, baseFee+queued)
					return reply
				},
				func() *sentryproto.InboundMessage {
					require.Empty(t, pool.unprocessedRemoteTxns.Txns)
					return nil
				},
			)
			require.Len(t, sent, 1)
			require.Empty(t, penalties)
			request := <-sent
			require.NotNil(t, request)
			require.NotNil(t, request.Data)
			require.Equal(t, sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66, request.Data.Id)
			id, hashes, _, err := ParseGetPooledTransactions66(request.Data.Data, 0, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(1), id)
			require.Equal(t, hash[:], hashes)
		})
	}
}

func TestFetchIdenticalReplyFromHonestPeer(t *testing.T) {
	ctx := t.Context()
	pool, db, signed, _ := newFetchTestTransaction(t, 0)
	var encoded bytes.Buffer
	require.NoError(t, signed.MarshalBinary(&encoded))
	hash := signed.Hash()
	body := EncodePooledTransactions66([][]byte{encoded.Bytes()}, 1, nil)
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	fetch.recordAnnouncement(peerID, []byte{types.LegacyTxType}, []uint32{uint32(encoded.Len())}, hash[:], nil)
	reply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return reply },
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			return &sentryproto.InboundMessage{Id: reply.Id, Data: body, PeerId: gointerfaces.ConvertHashToH512([64]byte{2})}
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(hash[:]))
			return nil
		},
	)
	require.Empty(t, sent)
	require.Len(t, penalties, 1)
	request := <-penalties
	require.NotNil(t, request)
	require.Equal(t, peerID, PeerID(request.PeerId))
	require.Equal(t, sentryproto.PenaltyKind_Kick, request.Penalty)
}
