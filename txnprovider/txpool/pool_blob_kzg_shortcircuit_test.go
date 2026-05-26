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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// validateTxns must stop validating subsequent remote (IsLocal=false) txns
// once one fails KZG verify; local txns keep per-tx semantics.
func TestValidateTxnsBlobKZGShortCircuit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Cancun"],
		nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)

	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        30_000_000,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: gointerfaces.ConvertHashToH256([32]byte{})},
		},
	}
	acc := accounts.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts.SerialiseV3(&acc)
	var addr [20]byte
	for i := uint8(1); i <= 4; i++ {
		addr[0] = i
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes,
			&remoteproto.AccountChange{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    v,
			})
	}
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	mkSlot := func(idHashByte byte, breakKZG bool) TxnSlot {
		s := makeBlobTxn()
		s.IDHash[0] = idHashByte
		s.Nonce = 0
		s.Txn.(*types.BlobTx).Nonce = 0
		if breakKZG {
			// Mutate a low-order byte of an interior scalar: the
			// commit→versioned-hash check still passes, but the Fiat-Shamir
			// evaluation drifts and the proof's pairing check fails.
			s.BlobBundles[0].Blob[31] ^= 0x01
		}
		return s
	}

	runValidate := func(t *testing.T, slots TxnSlots) ([]txpoolcfg.DiscardReason, TxnSlots) {
		t.Helper()
		coreDb, cache := pool.chainDB()
		coreTx, err := coreDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer coreTx.Rollback()
		cacheView, err := cache.View(ctx, coreTx)
		require.NoError(t, err)

		pool.lock.Lock()
		defer pool.lock.Unlock()
		require.NoError(t, pool.senders.registerNewSenders(&slots, pool.logger))
		reasons, goodTxns, err := pool.validateTxns(&slots, cacheView)
		require.NoError(t, err)
		return reasons, goodTxns
	}

	t.Run("remote short-circuits after first KZG failure", func(t *testing.T) {
		var slots TxnSlots
		s0 := mkSlot(0x10, true)
		s1 := mkSlot(0x11, false)
		a0, a1 := [20]byte{1}, [20]byte{2}
		slots.Append(&s0, a0[:], false)
		slots.Append(&s1, a1[:], false)

		reasons, goodTxns := runValidate(t, slots)

		require.Len(t, reasons, 2)
		assert.Equal(t, txpoolcfg.UnmatchedBlobTxExt, reasons[0])
		assert.Equal(t, txpoolcfg.NotSet, reasons[1], "txn[1] should not have been validated")
		assert.Empty(t, goodTxns.Txns, "neither txn should be in goodTxns")
	})

	t.Run("local keeps per-tx semantics", func(t *testing.T) {
		var slots TxnSlots
		s0 := mkSlot(0x20, true)
		s1 := mkSlot(0x21, false)
		a0, a1 := [20]byte{3}, [20]byte{4}
		slots.Append(&s0, a0[:], true)
		slots.Append(&s1, a1[:], true)

		reasons, goodTxns := runValidate(t, slots)

		require.Len(t, reasons, 2)
		assert.Equal(t, txpoolcfg.UnmatchedBlobTxExt, reasons[0])
		assert.Equal(t, txpoolcfg.NotSet, reasons[1])
		require.Len(t, goodTxns.Txns, 1, "valid txn[1] should be in goodTxns")
		assert.Equal(t, byte(0x21), goodTxns.Txns[0].IDHash[0])
	})
}
