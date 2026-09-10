// Copyright 2021 The Erigon Authors
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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func BenchmarkProcessRemoteTxns(b *testing.B) {
	require := require.New(b)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	db := mdbxtest.NewTestPoolDB(b)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)

	// Start the transaction pool
	err = pool.start(ctx)
	require.NoError(err)

	// Set up initial blockchain state
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}

	// Create 100 test accounts with 1 ETH balance each
	for i := range 100 {
		var addr [20]byte
		addr[0] = uint8(i + 1)
		acc := accounts3.Account{
			Nonce:       0,
			Balance:     *uint256.NewInt(1 * common.Ether),
			CodeHash:    accounts.EmptyCodeHash,
			Incarnation: 1,
		}
		v := accounts3.SerialiseV3(&acc)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}

	// Apply the initial state to the pool
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	// Create test transactions for benchmarking
	var testTxns TxnSlots
	for i := 0; i < b.N; i++ {
		var addr [20]byte
		addr[0] = uint8(i%100 + 1)                                          // Use one of our test accounts
		txnSlot := newTestTxnSlot(uint64(i/100), 0, 300000, 300000, 100000) // Different nonce for each account
		txnSlot.IDHash[0] = uint8(i + 1)
		testTxns.Append(txnSlot, addr[:], true)
	}

	b.ResetTimer()

	// Run the benchmark: process transactions one by one
	// This measures the performance of adding and processing remote transactions
	for i := 0; i < b.N; i++ {
		pool.AddRemoteTxns(ctx, TxnSlots{testTxns.Txns[i : i+1], testTxns.Senders[i : i+1], testTxns.IsLocal[i : i+1]}, nil, nil)
		err := pool.processRemoteTxns(ctx)
		require.NoError(err)
	}

	b.StopTimer()

	// Log final pool statistics after processing all transactions
	pending, baseFee, queued := pool.CountContent()
	b.Logf("Final pool stats - pending: %d, baseFee: %d, queued: %d", pending, baseFee, queued)
}
