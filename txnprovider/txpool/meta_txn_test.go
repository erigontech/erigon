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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// addTestTxns adds the sender's slots in one batch.
func addTestTxns(t testing.TB, pool *TxPool, sender common.Address, isLocal bool, slots ...*TxnSlot) []txpoolcfg.DiscardReason {
	t.Helper()
	var txns TxnSlots
	for _, slot := range slots {
		txns.Append(slot, sender[:], isLocal)
	}
	reasons, err := pool.addNewTxns(t.Context(), txns, nil)
	require.NoError(t, err)
	return reasons
}

func addRemoteTestTxn(t *testing.T, pool *TxPool, sender common.Address, nonce, tip, feeCap uint64, id byte) (*TxnSlot, txpoolcfg.DiscardReason) {
	t.Helper()
	txn := newTestTxnSlot(nonce, 0, tip, feeCap, 21_000)
	txn.IDHash[0] = id
	return txn, addTestTxns(t, pool, sender, false, txn)[0]
}

func requireEvicted(t *testing.T, pool *TxPool, evicted, kept *TxnSlot) {
	t.Helper()
	require.NotContains(t, pool.byHash, string(evicted.IDHash[:]))
	require.Contains(t, pool.byHash, string(kept.IDHash[:]))
}

func TestPendingOverflowEvictsLowestEffectiveTip(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		pooledTip, pooledFeeCap uint64
		addedTip, addedFeeCap   uint64
		evictAdded              bool
	}{
		{"zero tip despite higher fee cap", 1, 50, 0, 300, true},
		{"tip despite lower fee cap", 0, 300, 1, 50, false},
		{"equal tips, lower fee cap", 1, 50, 1, 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, _, _ := newTestPool(t)
			senders := []common.Address{{2}, {3}}
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, senders...), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			pool.pending.limit = 1

			pooled, reason := addRemoteTestTxn(t, pool, senders[0], 0, tc.pooledTip, tc.pooledFeeCap, 1)
			require.Equal(t, txpoolcfg.Success, reason)
			added, reason := addRemoteTestTxn(t, pool, senders[1], 0, tc.addedTip, tc.addedFeeCap, 2)
			if tc.evictAdded {
				require.Equal(t, txpoolcfg.PendingPoolOverflow, reason)
				requireEvicted(t, pool, added, pooled)
			} else {
				require.Equal(t, txpoolcfg.Success, reason)
				requireEvicted(t, pool, pooled, added)
			}
			require.Equal(t, 1, pool.pending.Len())
		})
	}
}

func TestBaseFeeOverflowEvictsLowestFeeCap(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		pooledFeeCap, addedFeeCap uint64
		evictAdded                bool
	}{
		{"higher fee cap", 2, 500, false},
		{"lower fee cap", 500, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, _, _ := newTestPool(t)
			senders := []common.Address{{2}, {3}}
			newBlock := func(height uint64) {
				change := newBlockChangeForAccounts(common.Hash{byte(height)}, 0, senders...)
				change.PendingBlockBaseFee = 1_000
				change.ChangeBatch[0].BlockHeight = height
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			}
			newBlock(0)
			pool.baseFee.limit = 1

			pooled, reason := addRemoteTestTxn(t, pool, senders[0], 0, 0, tc.pooledFeeCap, 1)
			require.Equal(t, txpoolcfg.Success, reason)
			// The arrival order must not decide which transaction stays.
			newBlock(1)
			added, reason := addRemoteTestTxn(t, pool, senders[1], 0, 0, tc.addedFeeCap, 2)
			if tc.evictAdded {
				require.Equal(t, txpoolcfg.BaseFeePoolOverflow, reason)
				requireEvicted(t, pool, added, pooled)
			} else {
				require.Equal(t, txpoolcfg.Success, reason)
				requireEvicted(t, pool, pooled, added)
			}
			require.Equal(t, 1, pool.baseFee.Len())
		})
	}
}

// Evicting a txn breaks its sender's nonce chain, so the sender's next txn cannot reach pending later.
func TestOverflowQueuesSuccessorOfEvictedTxn(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		firstFeeCap, otherFeeCap    uint64
		pendingLimit, baseFeeLimit  int
		localNext                   bool // a local txn ranks above remote ones, so it outlasts its predecessor
		evictedSubPool, nextSubPool SubPoolType
	}{
		{"pending overflow", 300, 50, 1, 2, false, PendingSubPool, BaseFeeSubPool},
		{"base fee overflow", 5, 8, 10, 2, true, BaseFeeSubPool, BaseFeeSubPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, _, _ := newTestPool(t)
			senders := []common.Address{{2}, {3}}
			newBlock := func(height, baseFee uint64, accounts ...common.Address) {
				change := newBlockChangeForAccounts(common.Hash{byte(height)}, 0, accounts...)
				change.PendingBlockBaseFee = baseFee
				change.ChangeBatch[0].BlockHeight = height
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			}
			newBlock(0, 10, senders...)
			pool.pending.limit, pool.baseFee.limit = tc.pendingLimit, tc.baseFeeLimit

			first, reason := addRemoteTestTxn(t, pool, senders[0], 0, 0, tc.firstFeeCap, 1)
			require.Equal(t, txpoolcfg.Success, reason)
			require.Equal(t, tc.evictedSubPool, pool.byHash[string(first.IDHash[:])].currentSubPool)
			next := newTestTxnSlot(1, 0, 0, 5, 21_000)
			next.IDHash[0] = 2
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, addTestTxns(t, pool, senders[0], tc.localNext, next))
			require.Equal(t, tc.nextSubPool, pool.byHash[string(next.IDHash[:])].currentSubPool)
			other, reason := addRemoteTestTxn(t, pool, senders[1], 0, 1, tc.otherFeeCap, 3)
			require.Equal(t, txpoolcfg.Success, reason)
			requireEvicted(t, pool, first, other)
			require.Equal(t, QueuedSubPool, pool.byHash[string(next.IDHash[:])].currentSubPool)

			// A base fee fall promotes base fee txns without refreshing their senders.
			pool.pending.limit = 10
			newBlock(1, 1, senders[1])
			require.Equal(t, QueuedSubPool, pool.byHash[string(next.IDHash[:])].currentSubPool)
			require.Equal(t, 1, pool.pending.Len())
		})
	}
}
