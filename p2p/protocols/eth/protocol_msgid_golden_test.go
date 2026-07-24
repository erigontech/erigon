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

package eth_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

var goldenToProto = map[uint]map[uint64]sentryproto.MessageId{
	68: {
		eth.GetBlockHeadersMsg:            sentryproto.MessageId_GET_BLOCK_HEADERS_66,
		eth.BlockHeadersMsg:               sentryproto.MessageId_BLOCK_HEADERS_66,
		eth.GetBlockBodiesMsg:             sentryproto.MessageId_GET_BLOCK_BODIES_66,
		eth.BlockBodiesMsg:                sentryproto.MessageId_BLOCK_BODIES_66,
		eth.GetReceiptsMsg:                sentryproto.MessageId_GET_RECEIPTS_66,
		eth.ReceiptsMsg:                   sentryproto.MessageId_RECEIPTS_66,
		eth.NewBlockHashesMsg:             sentryproto.MessageId_NEW_BLOCK_HASHES_66,
		eth.NewBlockMsg:                   sentryproto.MessageId_NEW_BLOCK_66,
		eth.TransactionsMsg:               sentryproto.MessageId_TRANSACTIONS_66,
		eth.NewPooledTransactionHashesMsg: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		eth.GetPooledTransactionsMsg:      sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
		eth.PooledTransactionsMsg:         sentryproto.MessageId_POOLED_TRANSACTIONS_66,
	},
	69: {
		eth.StatusMsg:                     sentryproto.MessageId_STATUS_69,
		eth.GetBlockHeadersMsg:            sentryproto.MessageId_GET_BLOCK_HEADERS_66,
		eth.BlockHeadersMsg:               sentryproto.MessageId_BLOCK_HEADERS_66,
		eth.GetBlockBodiesMsg:             sentryproto.MessageId_GET_BLOCK_BODIES_66,
		eth.BlockBodiesMsg:                sentryproto.MessageId_BLOCK_BODIES_66,
		eth.GetReceiptsMsg:                sentryproto.MessageId_GET_RECEIPTS_69,
		eth.ReceiptsMsg:                   sentryproto.MessageId_RECEIPTS_66,
		eth.NewBlockHashesMsg:             sentryproto.MessageId_NEW_BLOCK_HASHES_66,
		eth.NewBlockMsg:                   sentryproto.MessageId_NEW_BLOCK_66,
		eth.TransactionsMsg:               sentryproto.MessageId_TRANSACTIONS_66,
		eth.NewPooledTransactionHashesMsg: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		eth.GetPooledTransactionsMsg:      sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
		eth.PooledTransactionsMsg:         sentryproto.MessageId_POOLED_TRANSACTIONS_66,
		eth.BlockRangeUpdateMsg:           sentryproto.MessageId_BLOCK_RANGE_UPDATE_69,
	},
	70: {
		eth.StatusMsg:                     sentryproto.MessageId_STATUS_69,
		eth.GetBlockHeadersMsg:            sentryproto.MessageId_GET_BLOCK_HEADERS_66,
		eth.BlockHeadersMsg:               sentryproto.MessageId_BLOCK_HEADERS_66,
		eth.GetBlockBodiesMsg:             sentryproto.MessageId_GET_BLOCK_BODIES_66,
		eth.BlockBodiesMsg:                sentryproto.MessageId_BLOCK_BODIES_66,
		eth.GetReceiptsMsg:                sentryproto.MessageId_GET_RECEIPTS_70,
		eth.ReceiptsMsg:                   sentryproto.MessageId_RECEIPTS_70,
		eth.NewBlockHashesMsg:             sentryproto.MessageId_NEW_BLOCK_HASHES_66,
		eth.NewBlockMsg:                   sentryproto.MessageId_NEW_BLOCK_66,
		eth.TransactionsMsg:               sentryproto.MessageId_TRANSACTIONS_66,
		eth.NewPooledTransactionHashesMsg: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		eth.GetPooledTransactionsMsg:      sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
		eth.PooledTransactionsMsg:         sentryproto.MessageId_POOLED_TRANSACTIONS_66,
		eth.BlockRangeUpdateMsg:           sentryproto.MessageId_BLOCK_RANGE_UPDATE_69,
	},
	71: {
		eth.StatusMsg:                     sentryproto.MessageId_STATUS_69,
		eth.GetBlockHeadersMsg:            sentryproto.MessageId_GET_BLOCK_HEADERS_66,
		eth.BlockHeadersMsg:               sentryproto.MessageId_BLOCK_HEADERS_66,
		eth.GetBlockBodiesMsg:             sentryproto.MessageId_GET_BLOCK_BODIES_66,
		eth.BlockBodiesMsg:                sentryproto.MessageId_BLOCK_BODIES_66,
		eth.GetReceiptsMsg:                sentryproto.MessageId_GET_RECEIPTS_70,
		eth.ReceiptsMsg:                   sentryproto.MessageId_RECEIPTS_70,
		eth.NewBlockHashesMsg:             sentryproto.MessageId_NEW_BLOCK_HASHES_66,
		eth.NewBlockMsg:                   sentryproto.MessageId_NEW_BLOCK_66,
		eth.TransactionsMsg:               sentryproto.MessageId_TRANSACTIONS_66,
		eth.NewPooledTransactionHashesMsg: sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		eth.GetPooledTransactionsMsg:      sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
		eth.PooledTransactionsMsg:         sentryproto.MessageId_POOLED_TRANSACTIONS_66,
		eth.BlockRangeUpdateMsg:           sentryproto.MessageId_BLOCK_RANGE_UPDATE_69,
		eth.GetBlockAccessListsMsg:        sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71,
		eth.BlockAccessListsMsg:           sentryproto.MessageId_BLOCK_ACCESS_LISTS_71,
	},
}

var goldenFromProto = map[uint]map[sentryproto.MessageId]uint64{
	68: {
		sentryproto.MessageId_GET_BLOCK_HEADERS_66:             eth.GetBlockHeadersMsg,
		sentryproto.MessageId_BLOCK_HEADERS_66:                 eth.BlockHeadersMsg,
		sentryproto.MessageId_GET_BLOCK_BODIES_66:              eth.GetBlockBodiesMsg,
		sentryproto.MessageId_BLOCK_BODIES_66:                  eth.BlockBodiesMsg,
		sentryproto.MessageId_GET_RECEIPTS_66:                  eth.GetReceiptsMsg,
		sentryproto.MessageId_RECEIPTS_66:                      eth.ReceiptsMsg,
		sentryproto.MessageId_NEW_BLOCK_HASHES_66:              eth.NewBlockHashesMsg,
		sentryproto.MessageId_NEW_BLOCK_66:                     eth.NewBlockMsg,
		sentryproto.MessageId_TRANSACTIONS_66:                  eth.TransactionsMsg,
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: eth.NewPooledTransactionHashesMsg,
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:       eth.GetPooledTransactionsMsg,
		sentryproto.MessageId_POOLED_TRANSACTIONS_66:           eth.PooledTransactionsMsg,
	},
	69: {
		sentryproto.MessageId_GET_BLOCK_HEADERS_66:             eth.GetBlockHeadersMsg,
		sentryproto.MessageId_BLOCK_HEADERS_66:                 eth.BlockHeadersMsg,
		sentryproto.MessageId_GET_BLOCK_BODIES_66:              eth.GetBlockBodiesMsg,
		sentryproto.MessageId_BLOCK_BODIES_66:                  eth.BlockBodiesMsg,
		sentryproto.MessageId_GET_RECEIPTS_69:                  eth.GetReceiptsMsg,
		sentryproto.MessageId_RECEIPTS_66:                      eth.ReceiptsMsg,
		sentryproto.MessageId_NEW_BLOCK_HASHES_66:              eth.NewBlockHashesMsg,
		sentryproto.MessageId_NEW_BLOCK_66:                     eth.NewBlockMsg,
		sentryproto.MessageId_TRANSACTIONS_66:                  eth.TransactionsMsg,
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: eth.NewPooledTransactionHashesMsg,
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:       eth.GetPooledTransactionsMsg,
		sentryproto.MessageId_POOLED_TRANSACTIONS_66:           eth.PooledTransactionsMsg,
		sentryproto.MessageId_BLOCK_RANGE_UPDATE_69:            eth.BlockRangeUpdateMsg,
	},
	70: {
		sentryproto.MessageId_GET_BLOCK_HEADERS_66:             eth.GetBlockHeadersMsg,
		sentryproto.MessageId_BLOCK_HEADERS_66:                 eth.BlockHeadersMsg,
		sentryproto.MessageId_GET_BLOCK_BODIES_66:              eth.GetBlockBodiesMsg,
		sentryproto.MessageId_BLOCK_BODIES_66:                  eth.BlockBodiesMsg,
		sentryproto.MessageId_GET_RECEIPTS_70:                  eth.GetReceiptsMsg,
		sentryproto.MessageId_RECEIPTS_70:                      eth.ReceiptsMsg,
		sentryproto.MessageId_NEW_BLOCK_HASHES_66:              eth.NewBlockHashesMsg,
		sentryproto.MessageId_NEW_BLOCK_66:                     eth.NewBlockMsg,
		sentryproto.MessageId_TRANSACTIONS_66:                  eth.TransactionsMsg,
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: eth.NewPooledTransactionHashesMsg,
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:       eth.GetPooledTransactionsMsg,
		sentryproto.MessageId_POOLED_TRANSACTIONS_66:           eth.PooledTransactionsMsg,
		sentryproto.MessageId_BLOCK_RANGE_UPDATE_69:            eth.BlockRangeUpdateMsg,
	},
	71: {
		sentryproto.MessageId_GET_BLOCK_HEADERS_66:             eth.GetBlockHeadersMsg,
		sentryproto.MessageId_BLOCK_HEADERS_66:                 eth.BlockHeadersMsg,
		sentryproto.MessageId_GET_BLOCK_BODIES_66:              eth.GetBlockBodiesMsg,
		sentryproto.MessageId_BLOCK_BODIES_66:                  eth.BlockBodiesMsg,
		sentryproto.MessageId_GET_RECEIPTS_70:                  eth.GetReceiptsMsg,
		sentryproto.MessageId_RECEIPTS_70:                      eth.ReceiptsMsg,
		sentryproto.MessageId_NEW_BLOCK_HASHES_66:              eth.NewBlockHashesMsg,
		sentryproto.MessageId_NEW_BLOCK_66:                     eth.NewBlockMsg,
		sentryproto.MessageId_TRANSACTIONS_66:                  eth.TransactionsMsg,
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: eth.NewPooledTransactionHashesMsg,
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:       eth.GetPooledTransactionsMsg,
		sentryproto.MessageId_POOLED_TRANSACTIONS_66:           eth.PooledTransactionsMsg,
		sentryproto.MessageId_BLOCK_RANGE_UPDATE_69:            eth.BlockRangeUpdateMsg,
		sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71:        eth.GetBlockAccessListsMsg,
		sentryproto.MessageId_BLOCK_ACCESS_LISTS_71:            eth.BlockAccessListsMsg,
	},
}

func TestToProtoGolden(t *testing.T) {
	require.Equal(t, goldenToProto, eth.ToProto)
}

func TestFromProtoGolden(t *testing.T) {
	require.Equal(t, goldenFromProto, eth.FromProto)
}

// FromProto deliberately omits the status ids so that SendMessageById can
// never emit a status frame; status is exchanged only during the handshake.
func TestFromProtoOmitsStatus(t *testing.T) {
	omitted := make(map[uint]map[sentryproto.MessageId]struct{})
	for version, toProto := range goldenToProto {
		omitted[version] = make(map[sentryproto.MessageId]struct{})
		for _, id := range toProto {
			if _, ok := goldenFromProto[version][id]; !ok {
				omitted[version][id] = struct{}{}
			}
		}
	}

	statusOnly := map[sentryproto.MessageId]struct{}{sentryproto.MessageId_STATUS_69: {}}
	require.Equal(t, map[uint]map[sentryproto.MessageId]struct{}{
		68: {},
		69: statusOnly,
		70: statusOnly,
		71: statusOnly,
	}, omitted)

	for _, version := range []uint{69, 70, 71} {
		require.NotContains(t, eth.FromProto[version], sentryproto.MessageId_STATUS_69)
	}
}
