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

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestFetchSuppressesResidentBodiesAfterRejectionHistoryEviction(t *testing.T) {
	ctx := t.Context()
	pool, db, signed, _ := newFetchTestTransactions(t, 0, 1, 2)
	pool.cfg.MinFeeCap = 300_001
	history, err := simplelru.NewLRU[string, discardRecord](2, nil)
	require.NoError(t, err)
	pool.discardReasonsLRU = history
	messages := make([]*sentryproto.InboundMessage, 0, len(signed))
	for _, txn := range signed {
		body := EncodePooledTransactions66(encodeFetchTestTransactions(t, txn), 1, nil)
		messages = append(messages, &sentryproto.InboundMessage{
			Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: body, PeerId: peerID,
		})
	}
	client, sent, penalties := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, db, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	processRejected := func() {
		t.Helper()
		require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
		reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.UnderPriced}, reasons)
		require.Empty(t, pool.unprocessedRemoteTxns.Txns)
		require.Empty(t, pool.byHash)
	}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return messages[0] },
		func() *sentryproto.InboundMessage {
			processRejected()
			return messages[1]
		},
		func() *sentryproto.InboundMessage {
			processRejected()
			return messages[2]
		},
		func() *sentryproto.InboundMessage {
			processRejected()
			require.Equal(t, int64(3), parses())
			return messages[0]
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns, "a resident packet must retain its rejection after history eviction")
			return messages[1]
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			return messages[2]
		},
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			return nil
		},
	)
	require.Empty(t, pool.pinnedDiscards)
	require.Equal(t, int64(3), parses())
	require.Empty(t, sent)
	require.Empty(t, penalties)
}
