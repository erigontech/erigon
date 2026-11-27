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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// addTestTxns adds the sender's slots as one batch and returns their discard reasons.
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

// Evicting a txn leaves a nonce gap before its sender's later txns. They must move to the queued sub-pool
// and stay out of pending, also after a base fee fall.
func TestOverflowQueuesSuccessorOfEvictedTxn(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		firstFeeCap, otherFeeCap    uint64
		pendingLimit, baseFeeLimit  int
		localNext                   bool // a local txn ranks above remote ones, so it outlasts its predecessor
		evictedSubPool, nextSubPool SubPoolType
	}{
		{
			name: "pending overflow", firstFeeCap: 300, otherFeeCap: 50, pendingLimit: 1, baseFeeLimit: 2,
			evictedSubPool: PendingSubPool, nextSubPool: BaseFeeSubPool,
		},
		{
			name: "base fee overflow", firstFeeCap: 5, otherFeeCap: 8, pendingLimit: 10, baseFeeLimit: 3,
			localNext: true, evictedSubPool: BaseFeeSubPool, nextSubPool: BaseFeeSubPool,
		},
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
			var successors []*TxnSlot
			for nonce := uint64(1); nonce <= 2; nonce++ {
				txn := newTestTxnSlot(nonce, 0, 0, 5, 21_000)
				txn.IDHash[0] = byte(nonce + 1)
				successors = append(successors, txn)
			}
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, addTestTxns(t, pool, senders[0], tc.localNext, successors...))
			requireSuccessorsIn := func(subPool SubPoolType) {
				t.Helper()
				for _, txn := range successors {
					mt := pool.byHash[string(txn.IDHash[:])]
					require.Equal(t, subPool, mt.currentSubPool)
					// A queued successor has lost NoNonceGaps, so no promotion returns it before its sender is refreshed.
					require.Equal(t, subPool != QueuedSubPool, mt.subPool&NoNonceGaps != 0)
				}
			}
			requireSuccessorsIn(tc.nextSubPool)
			other, reason := addRemoteTestTxn(t, pool, senders[1], 0, 1, tc.otherFeeCap, 4)
			require.Equal(t, txpoolcfg.Success, reason)
			requireEvicted(t, pool, first, other)
			requireSuccessorsIn(QueuedSubPool)

			// A base fee fall promotes base fee txns without refreshing their senders.
			pool.pending.limit = 10
			newBlock(1, 1, senders[1])
			requireSuccessorsIn(QueuedSubPool)
			require.Equal(t, 1, pool.pending.Len())
		})
	}
}

// Two transactions with the same markers rank alike, whatever sub-pool each one is in, so a comparison
// across sub-pools does not depend on its direction.
func TestWorseRanksEqualMarkersAlike(t *testing.T) {
	baseFee := *uint256.NewInt(10)
	txn := func(subPool SubPoolType, feeCap, tip uint64) *metaTxn {
		return &metaTxn{subPool: BaseFeePoolBits, currentSubPool: subPool, minFeeCap: *uint256.NewInt(feeCap), minTip: tip}
	}
	for _, tc := range []struct {
		name    string
		subPool SubPoolType
	}{
		{"parked frame", 0},
		{"pending txn", PendingSubPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Both cover the base fee, so both have the markers of pending. A blob fee rise kept the first
			// out of pending; it has the higher tip, the other the higher fee cap.
			waitingForBlobFee := txn(BaseFeeSubPool, 50, 5)
			lowerTip := txn(tc.subPool, 100, 1)
			require.True(t, lowerTip.worse(waitingForBlobFee, baseFee))
			require.False(t, waitingForBlobFee.worse(lowerTip, baseFee))
		})
	}
}

// The successor that an eviction strands leaves pending before the next txn is evicted, so a batch
// evicts no more than adding its txns one at a time would.
func TestPendingOverflowFreesSlotOfStrandedSuccessor(t *testing.T) {
	ctx, pool, _, _ := newTestPool(t)
	senders := []common.Address{{2}, {3}, {4}}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, senders...), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	pool.pending.limit = 2

	first, reason := addRemoteTestTxn(t, pool, senders[0], 0, 0, 300, 1)
	require.Equal(t, txpoolcfg.Success, reason)
	// A local txn ranks above remote ones, so overflow evicts its remote predecessor first.
	next := newTestTxnSlot(1, 0, 0, 300, 21_000)
	next.IDHash[0] = 2
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, addTestTxns(t, pool, senders[0], true, next))
	require.Equal(t, 2, pool.pending.Len())

	var batch TxnSlots
	for i, sender := range senders[1:] {
		txn := newTestTxnSlot(0, 0, uint64(i+1), 50, 21_000)
		txn.IDHash[0] = byte(i + 3)
		batch.Append(txn, sender[:], false)
	}
	reasons, err := pool.addNewTxns(ctx, batch, nil)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	require.NotContains(t, pool.byHash, string(first.IDHash[:]))
	require.Equal(t, QueuedSubPool, pool.byHash[string(next.IDHash[:])].currentSubPool)
	require.Equal(t, 2, pool.pending.Len())
}
