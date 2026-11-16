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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Each transaction has its own funded sender, maximizing recovery reads at the default pool limits.
func newStateRecoveryBenchmarkPool(b *testing.B, cache kvcache.Cache) (*TxPool, kv.TemporalRwDB, *remoteproto.StateChangeBatch) {
	b.Helper()
	ctx := b.Context()
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	cfg := txpoolcfg.DefaultConfig
	pool, poolErr := New(ctx, make(chan Announcements, 1), mdbxtest.NewTestPoolDB(b), db, cfg, cache,
		chain.TestChainOsakaConfig, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
	require.NoError(b, poolErr)
	pool.started.Store(true)
	count := cfg.PendingSubPoolLimit + cfg.BaseFeeSubPoolLimit + cfg.QueuedSubPoolLimit
	var txns TxnSlots
	account := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
	data := accounts.SerialiseV3(&account)
	require.NoError(b, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		sd, openErr := execctx.NewSharedDomains(ctx, tx, logger)
		if openErr != nil {
			return openErr
		}
		defer sd.Close()
		for i := range count {
			var sender common.Address
			binary.BigEndian.PutUint64(sender[12:], uint64(i+1))
			if err := sd.DomainPut(kv.AccountsDomain, tx, sender[:], data, 1, nil); err != nil {
				return err
			}
			nonce, fee := uint64(0), uint64(200)
			if i >= cfg.PendingSubPoolLimit+cfg.BaseFeeSubPoolLimit {
				nonce = 1
			} else if i >= cfg.PendingSubPoolLimit {
				fee = 50
			}
			slot := newTestTxnSlot(nonce, 0, 10, fee, 21_000)
			binary.BigEndian.PutUint64(slot.IDHash[24:], uint64(i+1))
			txns.Append(slot, sender[:], true)
		}
		return sd.Flush(ctx, tx)
	}))
	batch := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 100, BlockGasLimit: 60_000_000,
		ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 1, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{1})}},
	}
	require.NoError(b, db.View(ctx, func(tx kv.Tx) error {
		version, err := rawdb.GetStateVersion(tx)
		batch.StateVersionId = version
		return err
	}))
	require.NoError(b, pool.OnNewBlock(ctx, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(b, err)
	for _, reason := range reasons {
		require.Equal(b, txpoolcfg.Success, reason)
	}
	pending, baseFee, queued := pool.CountContent()
	require.Equal(b, cfg.PendingSubPoolLimit, pending)
	require.Equal(b, cfg.BaseFeeSubPoolLimit, baseFee)
	require.Equal(b, cfg.QueuedSubPoolLimit, queued)
	return pool, db, batch
}

func BenchmarkStateRecoveryLock(b *testing.B) {
	for _, coherent := range []bool{false, true} {
		b.Run(fmt.Sprintf("coherent=%t", coherent), func(b *testing.B) {
			var cache kvcache.Cache = kvcache.NewLatestBatchCache()
			if coherent {
				cache = kvcache.New(kvcache.DefaultCoherentConfig)
			}
			pool, db, batch := newStateRecoveryBenchmarkPool(b, cache)
			batch.StateChangesIncomplete = true
			var longest time.Duration
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				pool.MarkStateStale()
				cache.OnNewBlock(batch)
				require.Zero(b, cache.Len())
				require.NoError(b, db.ViewTemporal(b.Context(), func(tx kv.TemporalTx) error {
					view, err := cache.View(b.Context(), tx)
					if err != nil {
						return err
					}
					b.StartTimer()
					start := time.Now()
					_, err = pool.applyBlockUpdate(batch, TxnSlots{}, TxnSlots{}, TxnSlots{}, view, nil)
					elapsed := time.Since(start)
					b.StopTimer()
					longest = max(longest, elapsed)
					return err
				}))
				b.StartTimer()
			}
			b.ReportMetric(float64(longest)/float64(time.Millisecond), "max-lock-ms")
		})
	}
}

func BenchmarkStateChangeCatchup(b *testing.B) {
	const blocks = 128
	for _, interval := range []time.Duration{0, 10 * time.Millisecond} {
		b.Run(fmt.Sprintf("interval=%s", interval), func(b *testing.B) {
			pool, db, head := newStateRecoveryBenchmarkPool(b, kvcache.NewLatestBatchCache())
			server := remotedbserver.NewKvServer(b.Context(), db, nil, nil, pool.logger)
			server.SetStateChangesSnapshot(func(context.Context) (*remoteproto.StateChangeBatch, error) { return head, nil })
			client := direct.NewStateDiffClientDirect(server)
			var recovery time.Duration
			var received, resets, superseded int
			for b.Loop() {
				ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
				stream, err := client.StateChanges(ctx, &remoteproto.StateChangeRequest{})
				require.NoError(b, err)
				first := head.ChangeBatch[0].BlockHeight
				fee, gasLimit := head.PendingBlockBaseFee, head.BlockGasLimit
				published := make(chan error, 1)
				var lastPublication time.Time
				go func() {
					for i := uint64(1); i <= blocks; i++ {
						if err := common.Sleep(ctx, interval); err != nil {
							published <- err
							return
						}
						// Execution publishes the projected state version before committing it.
						err := db.Update(ctx, func(tx kv.RwTx) error {
							if _, err := rawdb.IncrementStateVersion(tx); err != nil {
								return err
							}
							version, err := rawdb.GetStateVersion(tx)
							if err != nil {
								return err
							}
							server.SendStateChanges(ctx, &remoteproto.StateChangeBatch{
								StateVersionId: version, PendingBlockBaseFee: fee, BlockGasLimit: gasLimit,
								ChangeBatch: []*remoteproto.StateChange{{BlockHeight: first + i, BlockTime: (first + i) * 12, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{1})}},
							})
							lastPublication = time.Now()
							return nil
						})
						if err != nil {
							published <- err
							cancel()
							return
						}
					}
					published <- nil
				}()
				for {
					batch, err := stream.Recv()
					require.NoError(b, err)
					received++
					if batch.StateChangesIncomplete {
						resets++
					}
					if err := pool.p2pFetcher.handleStateChangesRequest(ctx, batch); errors.Is(err, errStateViewSuperseded) {
						superseded++
						continue
					} else {
						require.NoError(b, err)
					}
					if batch.ChangeBatch[len(batch.ChangeBatch)-1].BlockHeight == first+blocks {
						head = batch
						break
					}
				}
				require.NoError(b, <-published)
				require.Equal(b, first+blocks, pool.lastSeenBlock.Load())
				recovery += max(time.Duration(0), time.Since(lastPublication))
				cancel()
			}
			b.ReportMetric(float64(recovery)/float64(time.Millisecond)/float64(b.N), "recovery-ms/burst")
			b.ReportMetric(float64(resets)/float64(b.N), "resets/burst")
			b.ReportMetric(float64(superseded)/float64(b.N), "superseded/burst")
			b.ReportMetric(float64(blocks+1)-float64(received)/float64(b.N), "dropped/burst")
		})
	}
}
