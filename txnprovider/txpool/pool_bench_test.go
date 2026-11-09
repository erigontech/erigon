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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func BenchmarkExecutionViewUpdate(b *testing.B) {
	for _, remote := range []bool{false, true} {
		for _, reset := range []bool{false, true} {
			b.Run(fmt.Sprintf("remote=%t/reset=%t", remote, reset), func(b *testing.B) {
				ctx, pool, _, coreDB := newTestPool(b)
				pool._stateCache = kvcache.NewLatestBatchCache()
				if remote {
					pool._chainDB = remoteTestDB(b, coreDB, pool.logger)
				}
				const senders = 1000
				encoded := accounts.SerialiseV3(&accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash})
				var slots TxnSlots
				require.NoError(b, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
					domains, err := execctx.NewSharedDomains(ctx, tx, pool.logger)
					if err != nil {
						return err
					}
					defer domains.Close()
					for i := range uint64(senders) {
						var address common.Address
						binary.BigEndian.PutUint64(address[:8], i)
						if err := domains.DomainPut(kv.AccountsDomain, tx, address[:], encoded, 1, nil); err != nil {
							return err
						}
						txn := newTestTxnSlot(0, 0, 1, 2, 100_000)
						binary.BigEndian.PutUint64(txn.IDHash[:8], i)
						slots.Append(txn, address[:], true)
					}
					return domains.Flush(ctx, tx)
				}))
				previous := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(common.Hash{})}
				require.NoError(b, coreDB.View(ctx, func(tx kv.Tx) (err error) {
					previous.StateVersionId, err = rawdb.GetStateVersion(tx)
					return err
				}))
				initial := &remoteproto.StateChangeBatch{
					StateVersionId: previous.StateVersionId, TargetView: previous, BlockGasLimit: 30_000_000, PendingBlockBaseFee: 1,
					ChangeBatch: []*remoteproto.StateChange{{BlockHash: previous.BlockHash}},
				}
				require.NoError(b, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				reasons, err := pool.AddLocalTxns(ctx, slots)
				require.NoError(b, err)
				for _, reason := range reasons {
					require.Equal(b, txpoolcfg.Success, reason)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					header := &types.Header{Number: *uint256.NewInt(uint64(i + 1)), ParentHash: common.Hash(gointerfaces.ConvertH256ToHash(previous.BlockHash)), GasLimit: 30_000_000}
					block := types.NewBlockWithHeader(header, nil)
					target := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(block.Hash()), BlockHeight: block.NumberU64(), StateVersionId: previous.StateVersionId + 1}
					require.NoError(b, coreDB.Update(ctx, func(tx kv.RwTx) error {
						if err := rawdb.WriteBlock(tx, block); err != nil {
							return err
						}
						rawdb.WriteHeadBlockHash(tx, block.Hash())
						return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, target.StateVersionId))
					}))
					batch := &remoteproto.StateChangeBatch{
						StateVersionId: target.StateVersionId, PreviousView: previous, TargetView: target,
						StateChangesIncomplete: reset, BlockGasLimit: 30_000_000, PendingBlockBaseFee: 1,
						ChangeBatch: []*remoteproto.StateChange{{
							BlockHash: target.BlockHash, BlockHeight: target.BlockHeight,
							Changes: []*remoteproto.AccountChange{{Address: gointerfaces.ConvertAddressToH160(common.Address{}), Action: remoteproto.Action_UPSERT, Data: encoded}},
						}},
					}
					b.StartTimer()
					require.NoError(b, pool.OnNewBlock(ctx, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
					previous = target
				}
			})
		}
	}
}

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
	require.NotNil(pool)

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
		acc := accounts.Account{
			Nonce:       0,
			Balance:     *uint256.NewInt(1 * common.Ether),
			CodeHash:    accounts.EmptyCodeHash,
			Incarnation: 1,
		}
		v := accounts.SerialiseV3(&acc)
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
	err = applyTestBlock(ctx, pool, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
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
