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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type stateReadHookDB struct {
	kv.TemporalRoDB
	beforeRead func() error
}

func (db stateReadHookDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	if err := db.beforeRead(); err != nil {
		return nil, err
	}
	return db.TemporalRoDB.BeginTemporalRo(ctx)
}

func TestFramePoolParksAroundFailedStateRead(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		t.Run(fmt.Sprintf("incomplete=%t", incomplete), func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			initial := newBlockChangeForAccounts(common.Hash{1}, 0)
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(10)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			frame := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			successor := newTestTxnSlot(1, 0, 1, 1, 21_000)
			successor.IDHash = successor.Txn.Hash()
			var encoded bytes.Buffer
			require.NoError(t, successor.Txn.MarshalBinary(&encoded))
			successor.Rlp = encoded.Bytes()
			successor.Size = uint32(encoded.Len())
			txns.Append(successor, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
			mt := pool.byHash[string(frame.IDHash[:])]
			require.Equal(t, PendingSubPool, mt.currentSubPool)
			require.Equal(t, PendingSubPool, pool.byHash[string(successor.IDHash[:])].currentSubPool)
			generation := pool.stateGeneration
			reservation := pool.frameReservations[sender]
			readErr := errors.New("state read failed")
			coreDB := pool._chainDB
			pool._chainDB = stateReadHookDB{coreDB, func() error {
				require.True(t, pool.lock.TryLock(), "waiting for state must not hold the pool lock")
				pool.lock.Unlock()
				if incomplete {
					require.Greater(t, pool.stateGeneration, generation, "in-flight validation must be invalidated before reading state")
					require.Zero(t, mt.currentSubPool, "frames must be parked before reading state")
				}
				return readErr
			}}
			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			change.ChangeBatch[0].SlotNumber = common.NewUint64(10)
			change.StateChangesIncomplete = incomplete
			require.Equal(t, initial.ChangeBatch[0].BlockHeight, change.ChangeBatch[0].BlockHeight)
			require.ErrorIs(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}), readErr)
			pool._chainDB = coreDB
			require.Greater(t, pool.stateGeneration, generation)
			require.Zero(t, mt.currentSubPool, "a failed block update must leave frames parked")

			var selected TxnsRlp
			_, count, err := pool.best(ctx, 10, &selected, change.ChangeBatch[0].BlockHeight, nil,
				mdgas.FullMdGas{MdGas: mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}}, nil, 1_000_000)
			require.NoError(t, err)
			require.Zero(t, count, "failed recovery must leave frames unselectable")
			require.Same(t, mt, pool.byHash[string(frame.IDHash[:])])
			require.Equal(t, reservation, pool.frameReservations[sender])

			pool.ethBackend.(*frameValidationBackend).valid = false
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
			require.Empty(t, pool.frameReservations)
		})
	}
}

func TestFramePoolFailedBlockUpdateBlocksSelection(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	initial := newBlockChangeForAccounts(common.Hash{1}, 0)
	initial.ChangeBatch[0].SlotNumber = common.NewUint64(10)
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	frame := recentRootPoolSlot(t, sender, 10, 1)
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	mt := pool.byHash[string(frame.IDHash[:])]
	require.Equal(t, PendingSubPool, mt.currentSubPool)
	reservation := pool.frameReservations[sender]

	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].SlotNumber = common.NewUint64(10)
	require.Equal(t, initial.ChangeBatch[0].BlockHeight, change.ChangeBatch[0].BlockHeight)
	readErr := errors.New("account read failed")
	// Observe the state at lock release, before OnNewBlock's outer error handler runs.
	_, err = pool.applyBlockUpdate(change, TxnSlots{}, TxnSlots{}, TxnSlots{}, failingFrameCacheView{err: readErr}, nil)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, common.Hash{2}, pool.stateBlockHash)

	var selected TxnsRlp
	_, count, err := pool.best(ctx, 10, &selected, change.ChangeBatch[0].BlockHeight, nil,
		mdgas.FullMdGas{MdGas: mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}}, nil, 1_000_000)
	require.NoError(t, err)
	require.Zero(t, count, "a failed update must block selection before releasing the pool lock")
	require.True(t, pool.stateViewStale)
	require.Zero(t, mt.currentSubPool)
	require.Same(t, mt, pool.byHash[string(frame.IDHash[:])])
	require.Equal(t, reservation, pool.frameReservations[sender])
}

func TestFramePoolAdmissionWaitsForStateRecovery(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		change := newBlockChangeForAccounts(common.Hash{1}, 0)
		change.StateChangesIncomplete = true
		reading, resume := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		defer unblock()
		pool._chainDB = stateReadHookDB{pool._chainDB, func() error {
			close(reading)
			<-resume
			return nil
		}}
		updated := make(chan error, 1)
		go func() { updated <- pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}) }()
		<-reading

		var txns TxnSlots
		frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
		txns.Append(frame, sender[:], true)
		admitted := make(chan error, 1)
		go func() {
			reasons, err := pool.AddLocalTxns(ctx, txns)
			if err == nil && (len(reasons) != 1 || reasons[0] != txpoolcfg.Success) {
				err = fmt.Errorf("frame admission failed: %v", reasons)
			}
			admitted <- err
		}()
		synctest.Wait()
		select {
		case err := <-admitted:
			t.Fatalf("admission finished before recovery: %v", err)
		default:
		}
		require.Empty(t, pool.ethBackend.(*frameValidationBackend).calls, "simulation must not use the stale head")
		time.Sleep(2 * frameValidationTimeout)
		unblock()
		require.NoError(t, <-updated)
		require.NoError(t, <-admitted)
		require.Equal(t, PendingSubPool, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	})
}

func TestLocalAdmissionBoundsRecoveryWait(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	pool.MarkStateStale()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stop := time.AfterFunc(time.Minute, cancel)
		defer stop.Stop()
		var txns TxnSlots
		txns.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], true)
		_, err := pool.AddLocalTxns(ctx, txns)
		require.ErrorIs(t, err, context.DeadlineExceeded, "admission must time out without caller cancellation")
		require.NoError(t, ctx.Err())
	})
}

func TestFramePoolRecoveryDuringAdmission(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			_, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.onValidate = sync.OnceFunc(pool.MarkStateStale)
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
				var txns TxnSlots
				txns.Append(frame, sender[:], local)
				var reasons []txpoolcfg.DiscardReason
				var admissionErr error
				done := make(chan struct{})
				if local {
					go func() {
						defer close(done)
						reasons, admissionErr = pool.AddLocalTxns(ctx, txns)
					}()
				} else {
					pool.started.Store(true)
					pool.AddRemoteTxns(ctx, txns, nil, nil)
					require.NoError(t, pool.processRemoteTxns(ctx))
					go func() {
						defer close(done)
						pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready)
					}()
				}
				synctest.Wait()
				require.True(t, pool.stateViewStale)
				time.Sleep(2 * frameValidationTimeout)
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("admission must wait for recovery despite the simulation deadline")
				default:
				}
				require.Len(t, backend.calls, 1, "recovery must not repeatedly simulate against stale state")
				if !local {
					require.Contains(t, pool.remoteFrames.pending, string(frame.IDHash[:]))
				}

				change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				<-done
				require.NoError(t, admissionErr)
				if local {
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				}
				require.Contains(t, pool.byHash, string(frame.IDHash[:]))
				require.Len(t, backend.calls, 2)
				require.Empty(t, pool.remoteFrames.pending)
			})
		})
	}
}

func TestRemoteAdmissionDefersUntilStateRecovery(t *testing.T) {
	for _, stage := range []string{"before admission", "opening state view", "precheck", "after precheck"} {
		t.Run(stage, func(t *testing.T) {
			_, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			pool.started.Store(true)
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				txn := newTestTxnSlot(0, 0, 1, 2, 100_000)
				switch stage {
				case "before admission":
					pool.MarkStateStale()
				case "opening state view":
					pool._stateCache = &stateViewProbe{Cache: pool._stateCache, onView: pool.MarkStateStale}
				case "precheck":
					txn = newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
					pool.MarkStateStale()
				case "after precheck":
					txn = newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
					txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, pool: pool, onUnlocked: pool.MarkStateStale}
				}
				txn.IDHash = txn.Txn.Hash()
				var txns TxnSlots
				txns.Append(txn, sender[:], false)
				pool.AddRemoteTxns(ctx, txns, nil, nil)
				done := make(chan error, 1)
				go func() { done <- pool.processRemoteTxns(ctx) }()
				synctest.Wait()
				select {
				case err := <-done:
					require.NoError(t, err)
				default:
					t.Fatal("remote admission blocks the maintenance loop while state is stale")
				}
				require.Empty(t, pool.byHash)
				require.Equal(t, txns, *pool.unprocessedRemoteTxns)
				require.Contains(t, pool.unprocessedRemoteByHash, string(txn.IDHash[:]))
				require.True(t, pool.hasUnprocessedRemoteTxns.Load())

				change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.NoError(t, pool.processRemoteTxns(ctx))
				require.Contains(t, pool.byHash, string(txn.IDHash[:]))
				require.Empty(t, pool.unprocessedRemoteTxns.Txns)
				require.Empty(t, pool.unprocessedRemoteByHash)
				require.False(t, pool.hasUnprocessedRemoteTxns.Load())
			})
		})
	}
}

func TestPoolRejectsSupersededIncompleteState(t *testing.T) {
	ctx, pool, _, db, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool._stateCache = kvcache.NewLatestBatchCache()
	account := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
	writeTestSenderState(t, ctx, db, pool.logger, sender, accounts.SerialiseV3(&account), 1)
	initial := newBlockChangeForAccounts(common.Hash{1}, 0)
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	txn := newTestTxnSlot(0, 0, 1, 2, 21_000)
	txn.IDHash = txn.Txn.Hash()
	var txns TxnSlots
	txns.Append(txn, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	stale := newBlockChangeForAccounts(common.Hash{2}, 0)
	stale.StateChangesIncomplete = true
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		var err error
		stale.StateVersionId, err = rawdb.GetStateVersion(tx)
		return err
	}))
	account.Nonce = 1
	stale.ChangeBatch[0].Changes = []*remoteproto.AccountChange{{
		Address: gointerfaces.ConvertAddressToH160(sender), Action: remoteproto.Action_UPSERT,
		Data: accounts.SerialiseV3(&account),
	}}
	account.Nonce = 0
	writeTestSenderState(t, ctx, db, pool.logger, sender, accounts.SerialiseV3(&account), 2)

	require.ErrorIs(t, pool.OnNewBlock(ctx, stale, TxnSlots{}, TxnSlots{}, TxnSlots{}), errStateViewSuperseded)
	require.Contains(t, pool.byHash, string(txn.IDHash[:]), "an obsolete account update must not discard the valid transaction")
	require.Equal(t, common.Hash{1}, pool.stateBlockHash)
	require.True(t, pool.stateViewStale)
}

func TestPoolRefreshesRemovedSender(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	txn := newTestTxnSlot(0, 0, 1, 2, 21_000)
	txn.IDHash = txn.Txn.Hash()
	var txns TxnSlots
	txns.Append(txn, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Equal(t, PendingSubPool, pool.byHash[string(txn.IDHash[:])].currentSubPool)

	change := newBlockChangeForAccounts(common.Hash{2}, 0)
	change.ChangeBatch = append([]*remoteproto.StateChange{{
		Direction: remoteproto.Direction_UNWIND,
		Changes: []*remoteproto.AccountChange{{
			Action: remoteproto.Action_REMOVE, Address: gointerfaces.ConvertAddressToH160(sender),
		}},
	}}, change.ChangeBatch...)
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, QueuedSubPool, pool.byHash[string(txn.IDHash[:])].currentSubPool, "the orphaned funding must no longer make the transaction executable")
}

func TestPoolBlockTimeIgnoresRecoveryRetries(t *testing.T) {
	_, pool, _, _ := newTestPool(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		change := newBlockChangeForAccounts(common.Hash{1}, 0)
		change.ChangeBatch[0].BlockHeight = 1
		pool.lastBlockTimestampMs.Store(time.Now().Add(-12 * time.Second).UnixMilli())
		pool.avgBlockTimeMs.Store(12_000)
		readErr := errors.New("account read failed")
		cache := pool._stateCache
		pool._stateCache = failingViewCache{Cache: cache, err: readErr}
		for range 3 {
			require.ErrorIs(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}), readErr)
			time.Sleep(time.Second)
		}
		require.Equal(t, int64(12_000), pool.avgBlockTimeMs.Load(), "failed attempts are not block arrivals")
		pool._stateCache = cache
	})
}

type failingViewCache struct {
	kvcache.Cache
	err error
}

func (c failingViewCache) View(context.Context, kv.TemporalTx) (kvcache.CacheView, error) {
	return nil, c.err
}

func TestPoolBlockTimeIgnoresSubscriptionReset(t *testing.T) {
	_, pool, _, _ := newTestPool(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		change := newBlockChangeForAccounts(common.Hash{1}, 0)
		change.ChangeBatch[0].BlockHeight = 1
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		previous := pool.lastBlockTimestampMs.Load()
		time.Sleep(time.Second)
		change.StateChangesIncomplete = true
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		require.Equal(t, previous, pool.lastBlockTimestampMs.Load(), "a reset is not a new block arrival")
		require.Equal(t, int64(12_000), pool.avgBlockTimeMs.Load())
	})
}

func TestPoolBlockTimeTracksAdvancingHeads(t *testing.T) {
	_, pool, _, _ := newTestPool(t)
	synctest.Test(t, func(t *testing.T) {
		change := newBlockChangeForAccounts(common.Hash{1}, 0)
		change.ChangeBatch[0].BlockHeight = 1
		require.NoError(t, pool.OnNewBlock(t.Context(), change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		time.Sleep(6 * time.Second)
		change.ChangeBatch[0].BlockHeight = 2
		require.NoError(t, pool.OnNewBlock(t.Context(), change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		require.Equal(t, int64(11_400), pool.avgBlockTimeMs.Load(), "new blocks must still update the moving average")
	})
}
