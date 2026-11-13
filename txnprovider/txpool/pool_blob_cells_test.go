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
	"context"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func newPoolWithBlobTxn(t *testing.T, fork string) (context.Context, *TxPool, *TxnSlot, TxnSlots) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	chainConfig := testforks.Forks[fork]
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	pool, err := New(ctx, make(chan Announcements, 5), mdbxtest.NewTestPoolDB(t), coreDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), chainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)
	pool.started.Store(true)

	sender := common.Address{1}
	acc := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash, Incarnation: 1}
	require.NoError(t, pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{
		PendingBlockBaseFee:  1,
		BlockGasLimit:        30_000_000,
		PendingBlobFeePerGas: 1,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(sender),
				Data:    accounts.SerialiseV3(&acc),
			}},
		}},
	}, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	parseCtx := NewTxnParseContext(*chainConfig.ChainID)
	parseCtx.WithSender(false)
	slot := &TxnSlot{}
	_, err = parseCtx.ParseTransaction(makeWrappedBlobTxnRlpWithCellProofs(t, chainConfig.ChainID, 2), 0, slot, nil, false, true, nil)
	require.NoError(t, err)
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	return ctx, pool, slot, txns
}

func TestValidationCellsServedByGetBlobs(t *testing.T) {
	ctx, pool, slot, txns := newPoolWithBlobTxn(t, "Amsterdam")
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	bundles := pool.GetBlobs(slot.GetBlobHashes())
	require.Len(t, bundles, 2)
	for i, bundle := range bundles {
		require.Nil(t, slot.BlobBundles[i].Cells, "pooled txn still holds the cells of blob %d", i)
		require.Nil(t, pool.blobs.bundles[slot.GetBlobHashes()[i]].bundle.Cells, "blob store entry holds the cells of blob %d", i)
		require.NotNil(t, bundle.Cells, "cells served for blob %d", i)
		want, err := kzg.Ctx().ComputeCells((*goethkzg.Blob)(bundle.Blob), 2)
		require.NoError(t, err)
		for c := range want {
			require.Equal(t, want[c][:], bundle.Cells[c][:], "blob %d cell %d", i, c)
		}
	}
}

func TestValidationCachesNoCellsOnBlobHashMismatch(t *testing.T) {
	ctx, pool, slot, txns := newPoolWithBlobTxn(t, "Amsterdam")
	slot.GetBlobHashes()[0] = common.Hash{0x01, 0xff}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.BlobHashCheckFail}, reasons)
	require.Zero(t, pool.blobs.cells.Len(), "cells cached for a txn whose blob hash does not match its commitment")
}

func TestValidationCachesNoCellsBeforeAmsterdam(t *testing.T) {
	ctx, pool, _, txns := newPoolWithBlobTxn(t, "Osaka")
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Zero(t, pool.blobs.cells.Len(), "cells cached before Amsterdam, where no getBlobs version serves them")
}

func TestValidationCachesNoCellsForRejectedTxn(t *testing.T) {
	ctx, pool, _, txns := newPoolWithBlobTxn(t, "Amsterdam")
	pool.pendingBlobFee.Store(1_000_000)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.FeeTooLow}, reasons)
	require.Zero(t, pool.blobs.cells.Len(), "cells cached for a txn the pool rejected")
}

func TestUnwoundBlobTxnDoesNotKeepValidationCells(t *testing.T) {
	ctx, pool, slot, txns := newPoolWithBlobTxn(t, "Amsterdam")
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	minedChange := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee:  1,
		BlockGasLimit:        30_000_000,
		PendingBlobFeePerGas: 1,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 1,
			BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{1}),
		}},
	}
	require.NoError(t, pool.OnNewBlock(ctx, minedChange, TxnSlots{}, TxnSlots{}, txns))
	require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
	for _, hash := range slot.GetBlobHashes() {
		require.NotContains(t, pool.blobs.bundles, hash)
	}

	var unwindBlobTxns TxnSlots
	sender := common.Address{1}
	unwindBlobTxns.Append(slot, sender[:], false)
	unwoundChange := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee:  1,
		BlockGasLimit:        30_000_000,
		PendingBlobFeePerGas: 1,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 2,
			BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{2}),
		}},
	}
	require.NoError(t, pool.OnNewBlock(ctx, unwoundChange, TxnSlots{}, unwindBlobTxns, TxnSlots{}))

	pooled := pool.byHash[string(slot.IDHash[:])]
	require.NotNil(t, pooled)
	for i, bundle := range pooled.TxnSlot.BlobBundles {
		require.Nil(t, bundle.Cells, "pooled txn keeps cells for blob %d", i)
	}
	bundles := pool.GetBlobs(slot.GetBlobHashes())
	require.Len(t, bundles, len(slot.BlobBundles))
	for i, bundle := range bundles {
		require.NotNil(t, bundle.Cells, "cells served for blob %d", i)
		want, err := kzg.Ctx().ComputeCells((*goethkzg.Blob)(bundle.Blob), 2)
		require.NoError(t, err)
		for c := range want {
			require.Equal(t, want[c][:], bundle.Cells[c][:], "blob %d cell %d", i, c)
		}
	}
}

func TestBlobCellsAddIsBoundedAndReplaces(t *testing.T) {
	cells := make(blobCells)
	for i := 0; i <= blobCellsCacheSize; i++ {
		hash := common.Hash{byte(i), byte(i >> 8)}
		cells.add(hash, &[goethkzg.CellsPerExtBlob]*goethkzg.Cell{})
	}

	require.Len(t, cells, blobCellsCacheSize)
	lastIndex := blobCellsCacheSize
	lastHash := common.Hash{byte(lastIndex), byte(lastIndex >> 8)}
	require.NotContains(t, cells, lastHash)

	hash := common.Hash{}
	original := cells[hash]
	replacement := &[goethkzg.CellsPerExtBlob]*goethkzg.Cell{}
	cells.add(hash, replacement)
	require.NotSame(t, original, cells[hash])
	require.Same(t, replacement, cells[hash])
}

func TestAmsterdamValidationDoesNotAttachCells(t *testing.T) {
	ctx, pool, _, txns := newPoolWithBlobTxn(t, "Amsterdam")
	coreDB, cache := pool.chainDB()
	coreTx, err := coreDB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer coreTx.Rollback()
	cacheView, err := cache.View(ctx, coreTx)
	require.NoError(t, err)

	pool.lock.Lock()
	defer pool.lock.Unlock()
	require.NoError(t, pool.senders.registerNewSenders(&txns, pool.logger))
	_, goodTxns, _, err := pool.validateTxns(&txns, cacheView, nil, nil)
	require.NoError(t, err)
	require.Len(t, goodTxns.Txns, 1)
	for i, bundle := range goodTxns.Txns[0].BlobBundles {
		require.Nil(t, bundle.Cells, "validation attached cells for blob %d", i)
	}
}

func TestFromDBAmsterdamRestoresCells(t *testing.T) {
	ctx, pool, template, _ := newPoolWithBlobTxn(t, "Amsterdam")
	raw := makeWrappedBlobTxnRlpWithCellProofs(t, &pool.chainID, 2)
	sender := common.Address{1}
	require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.PoolTransaction, make([]byte, 32), append(sender[:], raw...))
	}))
	require.NoError(t, pool.fromDB(ctx))

	bundles := pool.GetBlobs(template.GetBlobHashes())
	require.Len(t, bundles, len(template.BlobBundles))
	for i, bundle := range bundles {
		require.NotNil(t, bundle.Cells, "cells restored for blob %d", i)
		want, err := kzg.Ctx().ComputeCells((*goethkzg.Blob)(bundle.Blob), 2)
		require.NoError(t, err)
		for c := range want {
			require.Equal(t, want[c][:], bundle.Cells[c][:], "blob %d cell %d", i, c)
		}
	}
	senderID, ok := pool.senders.getID(sender)
	require.True(t, ok)
	restored := pool.all.get(senderID, template.Nonce)
	require.NotNil(t, restored)
	for i, bundle := range restored.TxnSlot.BlobBundles {
		require.Nil(t, bundle.Cells, "restored txn keeps cells for blob %d", i)
	}
}
