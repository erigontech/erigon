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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
)

func TestFetchSuppressesIdenticalInvalidTransactionBodies(t *testing.T) {
	ctx := t.Context()
	pool, db, _, _ := newFetchTestTransaction(t, 0)
	chainID := pool.chainID
	chainID.AddUint64(&chainID, 1)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000).Txn.(*types.DynamicFeeTransaction)
	txn.ChainID = chainID
	signed, err := types.SignTx(txn, *types.LatestSignerForChainID(&chainID), key)
	require.NoError(t, err)
	body := EncodePooledTransactions66(encodeFetchTestTransactions(t, signed), 1, nil)
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	reply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return reply },
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			require.Equal(t, int64(1), parses())
			require.Len(t, penalties, 1)
			return reply
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			return nil
		},
	)
	require.Equal(t, int64(1), parses())
	require.Empty(t, pool.byHash)
	require.Empty(t, sent)
	require.Len(t, penalties, 1)
	penalty := <-penalties
	require.Equal(t, peerID, PeerID(penalty.PeerId))
	require.Equal(t, sentryproto.PenaltyKind_Kick, penalty.Penalty)
}

func TestFetchDoesNotShareBodyRejectionsAcrossMessageTypes(t *testing.T) {
	ctx := t.Context()
	pool, db, signed, _ := newFetchTestTransaction(t, 0)
	hash := signed.Hash()
	body := EncodePooledTransactions66(encodeFetchTestTransactions(t, signed), 1, nil)
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage {
			return &sentryproto.InboundMessage{Id: sentryproto.MessageId_TRANSACTIONS_66, Data: body, PeerId: peerID}
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			require.Len(t, penalties, 1)
			return &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID}
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(hash[:]))
			return nil
		},
	)
	require.Equal(t, int64(1), parses())
	require.Empty(t, sent)
	require.Len(t, penalties, 1)
	penalty := <-penalties
	require.Equal(t, peerID, PeerID(penalty.PeerId))
	require.Equal(t, sentryproto.PenaltyKind_Kick, penalty.Penalty)
}
