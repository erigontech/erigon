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
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestBlobSlotsOverflowDoesNotPunishSender(t *testing.T) {
	for _, badSidecar := range []bool{false, true} {
		name := "valid sidecar"
		if badSidecar {
			name = "invalid sidecar"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			pool := seedBlobKZGTestPool(t, ctx)
			require.NoError(t, pool.start(ctx))
			sender := common.Address{1}
			var local TxnSlots
			for nonce := range uint64(2) {
				slot := makeBlobSlot(byte(nonce+1), false)
				slot.Nonce = nonce
				slot.Txn.(*types.BlobTx).Nonce = nonce
				slot.Txn.(*types.BlobTx).GasLimit = 100_000
				local.Append(&slot, sender[:], true)
			}
			reasons, err := pool.AddLocalTxns(ctx, local)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
			pool.cfg.BlobSlots = pool.totalBlobsInPool.Load()
			overflow := makeBlobSlot(3, badSidecar)
			overflow.Nonce = 2
			overflow.Txn.(*types.BlobTx).Nonce = 2
			overflow.Txn.(*types.BlobTx).GasLimit = 100_000
			var remote TxnSlots
			remote.Append(&overflow, sender[:], false)
			pool.AddRemoteTxns(ctx, remote, nil, nil)
			reasons, err = pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
			require.NoError(t, err)
			require.Len(t, pool.byHash, 2)
			require.Equal(t, txpoolcfg.BlobSlotsOverflow, reasons[0])
			for _, slot := range local.Txns {
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			}
		})
	}
}
