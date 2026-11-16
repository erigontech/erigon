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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func encodeRemoteTxn(t *testing.T, txn *TxnSlot) *TxnSlot {
	t.Helper()
	var encoded bytes.Buffer
	require.NoError(t, txn.Txn.MarshalBinary(&encoded))
	txn.Rlp, txn.Size = encoded.Bytes(), uint32(encoded.Len())
	txn.IDHash = txn.Txn.Hash()
	return txn
}

func TestRemoteTxnQueueLimit(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		t.Run(fmt.Sprintf("recovering=%t", recovering), func(t *testing.T) {
			ctx, pool, poolDB, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			pool.started.Store(true)
			if recovering {
				pool.MarkStateStale()
			}
			var initial TxnSlots
			for i := range remoteTxnQueueLimit {
				initial.Append(encodeRemoteTxn(t, newTestTxnSlot(0, 0, 1, uint64(i+2), 21_000)), sender[:], false)
			}
			pool.AddRemoteTxns(ctx, initial, nil, nil)
			var dropped *TxnSlot
			for batch := range 3 {
				var arrivals TxnSlots
				for i := range 64 {
					dropped = encodeRemoteTxn(t, newTestTxnSlot(1, 0, 1, uint64(2+batch*64+i), 21_000))
					arrivals.Append(dropped, sender[:], false)
				}
				pool.AddRemoteTxns(ctx, arrivals, nil, nil)
				if recovering {
					require.NoError(t, pool.processRemoteTxns(ctx))
				}
				require.Equal(t, remoteTxnQueueLimit, len(pool.unprocessedRemoteTxns.Txns))
				require.Equal(t, remoteTxnQueueLimit, len(pool.unprocessedRemoteByHash))
				require.Equal(t, remoteTxnQueueLimit, len(pool.unprocessedRemotePeers))
			}
			require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, dropped.IDHash[:])
				require.NoError(t, err)
				require.False(t, known, "queue pressure must allow later re-announcement")
				return nil
			}))

			if recovering {
				change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			}
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			require.Empty(t, pool.unprocessedRemoteByHash)
			require.Empty(t, pool.unprocessedRemotePeers)
			require.False(t, pool.hasUnprocessedRemoteTxns.Load())
			var retry TxnSlots
			retry.Append(dropped, sender[:], false)
			pool.AddRemoteTxns(ctx, retry, nil, nil)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Contains(t, pool.byHash, string(dropped.IDHash[:]))
		})
	}
}

func TestRemoteTxnQueueByteLimit(t *testing.T) {
	ctx, pool, _, _ := newTestPool(t)
	sender := common.Address{1}
	var expectedBytes uint64
	var dropped *TxnSlot
	for nonce := uint64(0); ; nonce++ {
		txn := newTestTxnSlot(nonce, 0, 1, 2, 1_000_000)
		txn.Txn.(*types.DynamicFeeTransaction).Data = make([]byte, 60_000)
		encodeRemoteTxn(t, txn)
		var arrivals TxnSlots
		arrivals.Append(txn, sender[:], false)
		pool.AddRemoteTxns(ctx, arrivals, nil, nil)
		if expectedBytes+uint64(txn.Size) > remoteTxnQueueByteLimit {
			dropped = txn
			break
		}
		expectedBytes += uint64(txn.Size)
	}
	_, queued := pool.unprocessedRemoteByHash[string(dropped.IDHash[:])]
	require.False(t, queued)
	require.Equal(t, expectedBytes, pool.unprocessedRemoteBytes)

	first := pool.unprocessedRemoteTxns.Txns[0]
	var duplicate TxnSlots
	duplicate.Append(first, sender[:], false)
	pool.AddRemoteTxns(ctx, duplicate, nil, nil)
	require.Equal(t, expectedBytes, pool.unprocessedRemoteBytes, "duplicates must not consume capacity")

	expectedBytes -= uint64(first.Size) + uint64(pool.unprocessedRemoteTxns.Txns[1].Size)
	pool.removeProcessedRemoteTxns(2)
	require.Equal(t, expectedBytes, pool.unprocessedRemoteBytes)
	var retry TxnSlots
	retry.Append(dropped, sender[:], false)
	pool.AddRemoteTxns(ctx, retry, nil, nil)
	require.Contains(t, pool.unprocessedRemoteByHash, string(dropped.IDHash[:]))
	require.Equal(t, expectedBytes+uint64(dropped.Size), pool.unprocessedRemoteBytes)
	pool.removeProcessedRemoteTxns(len(pool.unprocessedRemoteTxns.Txns))
	require.Zero(t, pool.unprocessedRemoteBytes)
}

func TestRemoteTxnQueueOwnsPayload(t *testing.T) {
	ctx, pool, _, _ := newTestPool(t)
	wire := encodeRemoteTxn(t, newTestTxnSlot(0, 0, 1, 2, 21_000)).Rlp
	packet := make([]byte, 4096+len(wire))
	copy(packet[4096:], wire)
	parser := NewTxnParseContext(pool.chainID)
	parser.WithSender(false)
	txn := &TxnSlot{}
	sender := common.Address{1}
	_, err := parser.ParseTransaction(packet[4096:], 0, txn, sender[:], false, false, nil)
	require.NoError(t, err)
	var arrivals TxnSlots
	arrivals.Append(txn, sender[:], false)
	pool.AddRemoteTxns(ctx, arrivals, nil, nil)
	clear(packet)
	require.Equal(t, wire, pool.unprocessedRemoteTxns.Txns[0].Rlp, "a queued transaction must not retain the whole network packet")
}
