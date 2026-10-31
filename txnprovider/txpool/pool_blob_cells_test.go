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
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func newOsakaPoolWithBlobTxn(t *testing.T) (context.Context, *TxPool, *TxnSlot, TxnSlots) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	chainConfig := testforks.Forks["Osaka"]
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	pool, err := New(ctx, make(chan Announcements, 5), mdbxtest.NewTestPoolDB(t), coreDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), chainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)

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
	ctx, pool, slot, txns := newOsakaPoolWithBlobTxn(t)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	bundles := pool.GetBlobs(slot.GetBlobHashes())
	require.Len(t, bundles, 2)
	for i, bundle := range bundles {
		require.NotNil(t, bundle.Cells, "cells served for blob %d", i)
		want, err := kzg.Ctx().ComputeCells((*goethkzg.Blob)(bundle.Blob), 2)
		require.NoError(t, err)
		for c := range want {
			require.Equal(t, want[c][:], bundle.Cells[c][:], "blob %d cell %d", i, c)
		}
	}
}

func TestValidationCachesNoCellsOnBlobHashMismatch(t *testing.T) {
	ctx, pool, slot, txns := newOsakaPoolWithBlobTxn(t)
	slot.GetBlobHashes()[0] = common.Hash{0x01, 0xff}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.BlobHashCheckFail}, reasons)
	require.Zero(t, pool.blobs.cells.Len(), "cells cached for a txn whose blob hash does not match its commitment")
}
