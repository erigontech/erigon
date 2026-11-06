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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

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
	pool.promoteSubPools(1, 6, &announcements, log.New())
	require.Zero(t, pool.pending.Len())
	require.Equal(t, 1, pool.baseFee.Len())
	require.Zero(t, announcements.Len())

	pool.promoteSubPools(1, 5, &announcements, log.New())
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

// A pending txn whose RLP is gone can never be provided; it leaves every index, not only pending.
func TestBestDiscardsATxnWithoutRlp(t *testing.T) {
	pool := newAmsterdamPoolWithPendingSelfTransfer(t, t.Context(), 100_000)
	mt := pool.pending.best.ms[0]
	mt.TxnSlot.Rlp = nil

	require.Zero(t, bestOf(t, pool))
	require.Zero(t, pool.pending.Len())
	require.NotContains(t, pool.byHash, string(mt.TxnSlot.IDHash[:]))
	require.Nil(t, pool.all.get(mt.TxnSlot.SenderID, mt.TxnSlot.Nonce))

	// Peers can still serve it, so the pool must not treat the hash as known.
	tx, err := pool.poolDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	known, err := pool.IdHashKnown(tx, mt.TxnSlot.IDHash[:])
	require.NoError(t, err)
	require.False(t, known)
}
