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
	"github.com/erigontech/erigon/execution/types"
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
					// A queued successor follows a gap, so no promotion returns it to pending.
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

// Evicting a txn also frees what it cost its sender. Queued overflow then ranks the sender's later txns as
// funded: it keeps the stranded successor, which is nearer its sender's nonce, over another sender's later
// nonce.
func TestQueuedOverflowCountsFundsFreedByEviction(t *testing.T) {
	ctx, pool, _, _ := newTestPool(t)
	senders := []common.Address{{2}, {3}, {4}}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, senders...), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	pool.pending.limit, pool.queued.limit = 1, 1

	// Each costs over half the balance, so the successor is unfunded while its predecessor is pooled.
	const feeCap = 30_000 * common.GWei
	first, reason := addRemoteTestTxn(t, pool, senders[0], 0, 1, feeCap, 1)
	require.Equal(t, txpoolcfg.Success, reason)
	next, reason := addRemoteTestTxn(t, pool, senders[0], 1, 1, feeCap, 2)
	require.Equal(t, txpoolcfg.Success, reason)
	require.Zero(t, pool.byHash[string(next.IDHash[:])].subPool&EnoughBalance)

	other, reason := addRemoteTestTxn(t, pool, senders[1], 0, 5, 10, 3)
	require.Equal(t, txpoolcfg.Success, reason)
	requireEvicted(t, pool, first, other)
	require.NotZero(t, pool.byHash[string(next.IDHash[:])].subPool&EnoughBalance)

	distant, reason := addRemoteTestTxn(t, pool, senders[2], 2, 1, 10, 4)
	require.Equal(t, txpoolcfg.QueuedPoolOverflow, reason)
	requireEvicted(t, pool, distant, next)
}

// A blob txn after a nonce gap is discarded. When overflow strands one, it goes at once, and does not first
// take the queued slot of another txn.
func TestOverflowDiscardsStrandedBlobTxn(t *testing.T) {
	ctx, pool, _, _ := newPoolWithBlobTxn(t, "Osaka")
	senders := []common.Address{{1}, {3}, {4}}
	change := newBlockChangeForAccounts(common.Hash{1}, 0, senders...)
	change.ChangeBatch[0].BlockHeight = 1
	change.PendingBlobFeePerGas = 1
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	pool.pending.limit, pool.queued.limit = 2, 1

	first, reason := addRemoteTestTxn(t, pool, senders[0], 0, common.GWei, 2*common.GWei, 1)
	require.Equal(t, txpoolcfg.Success, reason)
	// A local txn ranks above remote ones, so overflow evicts its remote predecessor first.
	blob := &TxnSlot{}
	parser := NewTxnParseContext(*pool.chainConfig.ChainID)
	parser.WithSender(false)
	_, err := parser.ParseTransaction(makeWrappedBlobTxnRlpWithCellProofs(t, pool.chainConfig.ChainID, 1), 0, blob, nil, false, true, nil)
	require.NoError(t, err)
	blob.Nonce = 1
	blob.Txn.(*types.BlobTx).Nonce = 1
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, addTestTxns(t, pool, senders[0], true, blob))
	queued, reason := addRemoteTestTxn(t, pool, senders[1], 2, common.GWei, 2*common.GWei, 2)
	require.Equal(t, txpoolcfg.Success, reason)

	other, reason := addRemoteTestTxn(t, pool, senders[2], 0, 5*common.GWei, 10*common.GWei, 3)
	require.Equal(t, txpoolcfg.Success, reason)
	requireEvicted(t, pool, first, other)
	require.NotContains(t, pool.byHash, string(blob.IDHash[:]))
	require.Contains(t, pool.byHash, string(queued.IDHash[:]))
}

// The evicted txn's predecessors stay in pending; only the txns after the gap move to queued.
func TestOverflowKeepsPredecessorsOfEvictedTxn(t *testing.T) {
	ctx, pool, _, _ := newTestPool(t)
	senders := []common.Address{{2}, {3}}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, senders...), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	pool.pending.limit = 3

	var chain []*TxnSlot
	for nonce := range uint64(3) {
		txn := newTestTxnSlot(nonce, 0, 1, 2, 21_000)
		txn.IDHash[0] = byte(nonce + 1)
		chain = append(chain, txn)
	}
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, addTestTxns(t, pool, senders[0], false, chain[:2]...))
	// A local txn ranks above remote ones, so overflow evicts the remote txn before it.
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, addTestTxns(t, pool, senders[0], true, chain[2]))

	other, reason := addRemoteTestTxn(t, pool, senders[1], 0, 5, 10, 4)
	require.Equal(t, txpoolcfg.Success, reason)
	requireEvicted(t, pool, chain[1], other)
	require.Equal(t, PendingSubPool, pool.byHash[string(chain[0].IDHash[:])].currentSubPool)
	require.Equal(t, QueuedSubPool, pool.byHash[string(chain[2].IDHash[:])].currentSubPool)
}
