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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func countFetchTestParses(fetch *Fetch) func() int64 {
	var parses atomic.Int64
	validateRLP := fetch.pooledTxnsParseCtx.validateRlp
	fetch.pooledTxnsParseCtx.ValidateRLP(func(rlp []byte) error {
		parses.Add(1)
		return validateRLP(rlp)
	})
	return parses.Load
}

func encodeFetchTestTransactions(t *testing.T, signed ...types.Transaction) [][]byte {
	t.Helper()
	encoded := make([][]byte, 0, len(signed))
	for _, txn := range signed {
		var buffer bytes.Buffer
		require.NoError(t, txn.MarshalBinary(&buffer))
		encoded = append(encoded, buffer.Bytes())
	}
	return encoded
}

func addFetchTestLocalTransactions(t *testing.T, pool *TxPool, signed ...types.Transaction) {
	t.Helper()
	parse := NewTxnParseContext(pool.chainID).ChainIDRequired()
	parse.ValidateRLP(ValidateSerializedTxn)
	var txns TxnSlots
	for _, encoded := range encodeFetchTestTransactions(t, signed...) {
		var slot TxnSlot
		sender := make([]byte, 20)
		_, err := parse.ParseTransaction(encoded, 0, &slot, sender, false, false, nil)
		require.NoError(t, err)
		txns.Append(&slot, sender, true)
	}
	reasons, err := pool.AddLocalTxns(t.Context(), txns)
	require.NoError(t, err)
	require.Len(t, reasons, len(signed))
	for _, reason := range reasons {
		require.Equal(t, txpoolcfg.Success, reason)
	}
}

func receiveFetchTestBatch(t *testing.T, fetch *Fetch, client *sentryproto.MockSentryClient, next func() *sentryproto.InboundMessage) {
	t.Helper()
	stream := &fetchTestStream{recv: func() (*sentryproto.InboundMessage, error) {
		message := next()
		if message != nil {
			fetch.wg.Add(1)
		}
		return message, nil
	}}
	client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)
	require.NoError(t, fetch.receiveMessage(t.Context(), client))
	fetch.wg.Wait()
}

func TestFetchSuppressesIdenticalRejectedTransactions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		nonce  uint64
		reason txpoolcfg.DiscardReason
	}{
		{"underpriced", 0, txpoolcfg.UnderPriced},
		{"insufficient funds", 0, txpoolcfg.InsufficientFunds},
		{"nonce still distant", 65, txpoolcfg.NonceTooDistant},
	} {
		for _, messageID := range []sentryproto.MessageId{
			sentryproto.MessageId_POOLED_TRANSACTIONS_66,
			sentryproto.MessageId_TRANSACTIONS_66,
		} {
			t.Run(tc.name+"/"+messageID.String(), func(t *testing.T) {
				ctx := t.Context()
				pool, db, signed, change := newFetchTestTransaction(t, tc.nonce)
				switch tc.reason {
				case txpoolcfg.UnderPriced:
					pool.cfg.MinFeeCap = 300_001
				case txpoolcfg.InsufficientFunds:
					change.ChangeBatch[0].BlockHeight = 2
					change.ChangeBatch[0].Changes[0].Data = accounts.SerialiseV3(&accounts.Account{CodeHash: accounts.EmptyCodeHash})
					require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				}
				encoded := encodeFetchTestTransactions(t, signed)
				body := EncodePooledTransactions66(encoded, 1, nil)
				if messageID == sentryproto.MessageId_TRANSACTIONS_66 {
					body = EncodeTransactions(encoded, nil)
				}
				hash := signed.Hash()
				client, sent, penalties := newFetchTestClient(t)
				var wg sync.WaitGroup
				fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
				parses := countFetchTestParses(fetch)
				reply := &sentryproto.InboundMessage{Id: messageID, Data: body, PeerId: peerID}
				receiveFetchTestMessages(t, fetch, client,
					func() *sentryproto.InboundMessage { return reply },
					func() *sentryproto.InboundMessage {
						require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
						reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
						require.NoError(t, err)
						require.Equal(t, []txpoolcfg.DiscardReason{tc.reason}, reasons)
						require.Empty(t, pool.byHash)
						require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
							known, err := pool.IdHashKnown(tx, hash[:])
							require.NoError(t, err)
							require.False(t, known)
							return nil
						}))
						return reply
					},
					func() *sentryproto.InboundMessage {
						require.Empty(t, pool.unprocessedRemoteTxns.Txns)
						change.ChangeBatch[0].BlockHeight++
						require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
						return reply
					},
					func() *sentryproto.InboundMessage {
						require.Empty(t, pool.unprocessedRemoteTxns.Txns)
						return nil
					},
				)
				require.Equal(t, int64(1), parses())
				require.Empty(t, sent)
				require.Empty(t, penalties)
			})
		}
	}
}

func TestFetchRetriesAfterSameBlockNonceGapCloses(t *testing.T) {
	ctx := t.Context()
	pool, db, signed, _ := newFetchTestTransactions(t, 65, 0)
	hash := signed[0].Hash()
	body := EncodePooledTransactions66(encodeFetchTestTransactions(t, signed[0]), 1, nil)
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	block := pool.lastSeenBlock.Load()
	reply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return reply },
		func() *sentryproto.InboundMessage {
			reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooDistant}, reasons)
			addFetchTestLocalTransactions(t, pool, signed[1])
			require.Equal(t, block, pool.lastSeenBlock.Load())
			return reply
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(hash[:]))
			pending, baseFee, queued := pool.CountContent()
			require.Equal(t, 1, pending)
			require.Zero(t, baseFee)
			require.Equal(t, 1, queued)
			return reply
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			return nil
		},
	)
	require.Equal(t, int64(2), parses())
	require.Empty(t, sent)
	require.Empty(t, penalties)
}

func TestFetchHonestPeerAfterBadAnnouncementInSameBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		pool, db, signed, _ := newFetchTestTransaction(t, 0)
		encoded := encodeFetchTestTransactions(t, signed)
		hash := signed.Hash()
		body := EncodePooledTransactions66(encoded, 1, nil)
		client, sent, penalties := newFetchTestClient(t)
		var wg sync.WaitGroup
		fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
		parses := countFetchTestParses(fetch)
		fetch.recordAnnouncement(peerID, []byte{types.LegacyTxType}, []uint32{uint32(len(encoded[0]))}, hash[:], nil)
		messages := []*sentryproto.InboundMessage{
			{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID},
			{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: gointerfaces.ConvertHashToH512([64]byte{2})},
		}
		i := 0
		receiveFetchTestBatch(t, fetch, client, func() *sentryproto.InboundMessage {
			if i == len(messages) {
				return nil
			}
			message := messages[i]
			i++
			return message
		})
		require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
		require.NoError(t, pool.processRemoteTxns(ctx))
		require.Contains(t, pool.byHash, string(hash[:]))
		require.Equal(t, int64(2), parses())
		require.Empty(t, sent)
		require.Len(t, penalties, 1)
		require.Equal(t, peerID, PeerID((<-penalties).PeerId))
	})
}

func TestFetchAcceptsIdenticalReplyAfterCorrectedAnnouncement(t *testing.T) {
	ctx := t.Context()
	pool, db, signed, _ := newFetchTestTransaction(t, 0)
	encoded := encodeFetchTestTransactions(t, signed)
	hash := signed.Hash()
	body := EncodePooledTransactions66(encoded, 1, nil)
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	announcement := func(txType byte) *sentryproto.InboundMessage {
		types := []byte{txType}
		sizes := []uint32{uint32(len(encoded[0]))}
		data := make([]byte, announcementsLen(types, sizes, hash[:]))
		data = data[:encodeAnnouncements(types, sizes, hash[:], data)]
		return &sentryproto.InboundMessage{Id: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, Data: data, PeerId: peerID}
	}
	reply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return announcement(types.LegacyTxType) },
		func() *sentryproto.InboundMessage {
			require.Len(t, sent, 1)
			<-sent
			return reply
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			require.Len(t, penalties, 1)
			return announcement(types.DynamicFeeTxType)
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, sent, 1)
			<-sent
			return reply
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(hash[:]))
			return nil
		},
	)
	require.Equal(t, int64(2), parses())
	require.Empty(t, sent)
	require.Len(t, penalties, 1)
	require.Equal(t, peerID, PeerID((<-penalties).PeerId))
}

func TestFetchFlushesRepeatedBodiesBeforeStreamEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		pool, db, signed, _ := newFetchTestTransaction(t, 0)
		encoded := encodeFetchTestTransactions(t, signed)[0]
		const transactionsPerPacket = 1024
		transactions := make([][]byte, transactionsPerPacket)
		for i := range transactions {
			transactions[i] = encoded
		}
		body := EncodePooledTransactions66(transactions, 1, nil)
		messageCount := 8*1024*1024/len(body) + 1
		require.Less(t, messageCount, 256)
		client, sent, penalties := newFetchTestClient(t)
		var wg sync.WaitGroup
		fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
		parses := countFetchTestParses(fetch)
		received := 0
		receiveFetchTestBatch(t, fetch, client, func() *sentryproto.InboundMessage {
			if received == messageCount {
				// Processing progress is observable; retained buffer references are not.
				require.Positive(t, parses(), "the stream must flush before reading every packet body")
				return nil
			}
			received++
			return &sentryproto.InboundMessage{
				Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: bytes.Clone(body), PeerId: peerID,
			}
		})
		require.Equal(t, messageCount, received)
		require.Equal(t, int64(transactionsPerPacket), parses())
		require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
		require.NoError(t, pool.processRemoteTxns(ctx))
		hash := signed.Hash()
		require.Contains(t, pool.byHash, string(hash[:]))
		require.Empty(t, sent)
		require.Empty(t, penalties)
	})
}

func TestFetchRetriesPacketWithPreviouslyKnownTransaction(t *testing.T) {
	ctx := t.Context()
	pool, db, signed, change := newFetchTestTransactions(t, 66, 2, 0, 1)
	change.ChangeBatch[0].BlockHeight = 2
	change.ChangeBatch[0].Changes[0].Data = accounts.SerialiseV3(&accounts.Account{
		Nonce: 2, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
	})
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	addFetchTestLocalTransactions(t, pool, signed[0])
	hash := signed[0].Hash()
	otherHash := signed[1].Hash()
	body := EncodePooledTransactions66(encodeFetchTestTransactions(t, signed[0], signed[1]), 1, nil)
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	reply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return reply },
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.Equal(t, [32]byte(otherHash), pool.unprocessedRemoteTxns.Txns[0].IDHash)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(hash[:]))
			require.Contains(t, pool.byHash, string(otherHash[:]))
			change.ChangeBatch[0].BlockHeight = 3
			change.ChangeBatch[0].Changes[0].Data = accounts.SerialiseV3(&accounts.Account{
				Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
			})
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.NotContains(t, pool.byHash, string(hash[:]))
			require.Contains(t, pool.byHash, string(otherHash[:]))
			addFetchTestLocalTransactions(t, pool, signed[2], signed[3])
			require.Equal(t, uint64(3), pool.lastSeenBlock.Load())
			return reply
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.Equal(t, [32]byte(hash), pool.unprocessedRemoteTxns.Txns[0].IDHash)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(hash[:]))
			require.Contains(t, pool.byHash, string(otherHash[:]))
			return nil
		},
	)
	require.Equal(t, int64(4), parses())
	require.Empty(t, sent)
	require.Empty(t, penalties)
}
