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
	"math"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func bestOf(t *testing.T, pool *TxPool) int {
	t.Helper()
	var selected TxnsRlp
	_, count, err := pool.best(t.Context(), 10, &selected, 0, nil, mdgas.NewFullMdGas(30_000_000, 30_000_000, math.MaxUint64), nil, math.MaxInt)
	require.NoError(t, err)
	return count
}

// A queued blob txn that becomes executable goes to pending only if it also covers the pending blob fee.
func TestPromoteKeepsABlobTxnBelowTheBlobFeeOutOfPending(t *testing.T) {
	_, pool, _, _ := newPoolWithBlobTxn(t, "Osaka")
	slot := newTestBlobTxnSlot(0, 1, 1, 10, 21_000)
	slot.Txn.(*types.BlobTx).MaxFeePerBlobGas = *uint256.NewInt(5)
	mt := newMetaTxn(slot, false, 0)
	mt.minFeeCap = *uint256.NewInt(10)
	mt.subPool = BaseFeePoolBits
	pool.queued.Add(mt, "test", log.New())

	var announcements Announcements
	pool.promoteSubPools(1, 6, &announcements)
	require.Zero(t, pool.pending.Len())
	require.Equal(t, 1, pool.baseFee.Len())
	require.Zero(t, announcements.Len())

	pool.promoteSubPools(1, 5, &announcements)
	require.Equal(t, 1, pool.pending.Len(), "promoted once the blob fee falls to its cap")
}

// Pending is ordered without the blob fee, so a blob txn the blob fee has risen past may not be the worst one, and the
// demotion pass does not reach it. Block building must not pick it, and moves it out of pending.
func TestBestDemotesABlobTxnBelowTheBlobFee(t *testing.T) {
	ctx, pool, slot, txns := newPoolWithBlobTxn(t, "Osaka")
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Equal(t, 1, pool.pending.Len())

	pool.pendingBlobFee.Store(slot.GetBlobFeeCap().Uint64() + 1)
	require.Zero(t, bestOf(t, pool))
	require.Zero(t, pool.pending.Len())
	require.Equal(t, 1, pool.baseFee.Len())
}

// best() reads RLPs through a snapshot taken before it retakes the pool lock, so a flush in between can move a txn's RLP
// to a row the snapshot does not see. The txn stays pending and is offered on the next call.
func TestBestKeepsATxnWhoseRlpTheSnapshotMisses(t *testing.T) {
	pool := newAmsterdamPoolWithPendingSelfTransfer(t, t.Context(), 100_000)
	mt := pool.pending.best.ms[0]
	rlp := mt.TxnSlot.Rlp
	mt.TxnSlot.Rlp = nil

	require.Zero(t, bestOf(t, pool))
	require.Equal(t, 1, pool.pending.Len())
	require.Contains(t, pool.byHash, string(mt.TxnSlot.IDHash[:]))

	mt.TxnSlot.Rlp = rlp
	require.Equal(t, 1, bestOf(t, pool))
}

// Removing a txn from the middle of pending swaps the last one into its place; best() must leave pending sorted.
func TestBestKeepsPendingSortedAfterADemotion(t *testing.T) {
	pool := newAmsterdamPoolWithPendingSelfTransfer(t, t.Context(), 21_000, 21_000, 21_000)
	pool.pending.best.ms[0].minFeeCap = *uint256.NewInt(1) // below the pending base fee, still first
	require.Equal(t, 2, bestOf(t, pool))
	require.Equal(t, 2, pool.pending.Len())
	require.True(t, sort.IsSorted(pool.pending.best))
}

// A blob txn waiting only for the blob fee must not hold back the base fee pool txns ordered after it.
func TestPromoteSkipsABlobTxnWaitingForTheBlobFee(t *testing.T) {
	_, pool, _, _ := newPoolWithBlobTxn(t, "Osaka")
	blob := newTestBlobTxnSlot(0, 1, 1, 100, 21_000)
	blob.Txn.(*types.BlobTx).MaxFeePerBlobGas = *uint256.NewInt(5)
	ordinary := newTestTxnSlot(0, 2, 1, 10, 21_000)
	for _, slot := range []*TxnSlot{blob, ordinary} {
		mt := newMetaTxn(slot, false, 0)
		mt.minFeeCap = *slot.GetFeeCap()
		mt.subPool = BaseFeePoolBits
		pool.baseFee.Add(mt, "test", log.New())
	}

	var announcements Announcements
	pool.promoteSubPools(1, 6, &announcements)
	require.Equal(t, 1, pool.pending.Len())
	require.Equal(t, ordinary, pool.pending.Best().TxnSlot)
	require.Equal(t, 1, pool.baseFee.Len())
	require.Equal(t, blob, pool.baseFee.Best().TxnSlot)
}

// Pending holds a sender's txns as a gap-free nonce chain, so a txn after a blob txn below the blob fee cannot be
// included either, even if it covers the fees itself.
func TestBestHoldsBackTheSuccessorOfABlobTxnBelowTheBlobFee(t *testing.T) {
	ctx, pool, blob, txns := newPoolWithBlobTxn(t, "Osaka")
	next := newTestTxnSlot(blob.Nonce+1, 0, 1, 1_000_000, 21_000)
	next.IDHash[0], next.Rlp = 0xee, []byte{0xee}
	next.Size = uint32(len(next.Rlp))
	sender := common.Address{1}
	txns.Append(next, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	require.Equal(t, 2, pool.pending.Len())

	pool.pendingBlobFee.Store(blob.GetBlobFeeCap().Uint64() + 1)
	require.Zero(t, bestOf(t, pool))
	require.Zero(t, pool.pending.Len())
}

func TestBlobFeeCapKeepsTheLowestCap(t *testing.T) {
	blob := func(feeCap *uint256.Int) *TxnSlot {
		slot := newTestBlobTxnSlot(0, 1, 1, 10, 21_000)
		slot.Txn.(*types.BlobTx).MaxFeePerBlobGas = *feeCap
		return slot
	}
	aboveUint64 := new(uint256.Int).Lsh(uint256.NewInt(1), 64)
	require.Equal(t, uint64(7), blobFeeCap(newTestTxnSlot(0, 1, 1, 10, 21_000), 7), "a txn without blobs keeps the chain's cap")
	require.Equal(t, uint64(5), blobFeeCap(blob(uint256.NewInt(5)), 7))
	require.Equal(t, uint64(7), blobFeeCap(blob(uint256.NewInt(9)), 7))
	require.Equal(t, uint64(math.MaxUint64), blobFeeCap(blob(aboveUint64), math.MaxUint64), "a cap above uint64 never lowers it")
}
