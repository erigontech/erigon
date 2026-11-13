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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
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

type startupReadDB struct {
	kv.RwDB
	reads      int
	readers    int
	beforeRead func() error
}

func (db *startupReadDB) View(ctx context.Context, f func(kv.Tx) error) error {
	db.reads++
	if db.beforeRead != nil {
		if err := db.beforeRead(); err != nil {
			return err
		}
	}
	return db.RwDB.View(ctx, func(tx kv.Tx) error {
		db.readers++
		defer func() { db.readers-- }()
		return f(tx)
	})
}

func TestPoolStartupWaitsWithoutDatabaseReader(t *testing.T) {
	for _, staleAtStart := range []bool{true, false} {
		t.Run(fmt.Sprintf("staleAtStart=%t", staleAtStart), func(t *testing.T) {
			_, pool, poolDB, _ := newTestPool(t)
			pool.started.Store(false)
			db := &startupReadDB{RwDB: poolDB}
			pool.poolDB = db
			markStale := sync.OnceFunc(pool.MarkStateStale)
			if staleAtStart {
				markStale()
			}
			db.beforeRead = func() error {
				if !pool.lock.TryLock() {
					return errors.New("opening a database reader must not hold the pool lock")
				}
				pool.lock.Unlock()
				markStale()
				return nil
			}
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- pool.start(ctx) }()
				synctest.Wait()
				require.Zero(t, db.readers, "waiting for recovery must not hold a pool database reader")
				if staleAtStart {
					require.Zero(t, db.reads, "wait for the initial reset before opening the database")
				} else {
					require.Equal(t, 1, db.reads)
				}
				require.False(t, pool.Started())
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0), TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.NoError(t, <-done)
				require.True(t, pool.Started())
				require.Zero(t, db.readers)
			})
		})
	}
}

func TestLocalAdmissionRejectsBeforeStartupRestore(t *testing.T) {
	ctx, pool, poolDB, coreDB := newTestPool(t)
	pool.started.Store(false)
	sender := common.Address{1}
	account := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
	writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&account), 1)
	original := encodeRemoteTxn(t, newTestTxnSlot(0, 0, 100, 200, 21_000))
	replacement := encodeRemoteTxn(t, newTestTxnSlot(0, 0, 101, 202, 21_000))
	require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.PoolTransaction, original.IDHash[:], append(sender[:], original.Rlp...))
	}))
	pool.MarkStateStale()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		reading, resume := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		defer unblock()
		pool.poolDB = &startupReadDB{RwDB: poolDB, beforeRead: func() error {
			close(reading)
			<-resume
			return nil
		}}
		started := make(chan error, 1)
		go func() { started <- pool.start(ctx) }()
		var txns TxnSlots
		txns.Append(replacement, sender[:], true)
		_, err := pool.AddLocalTxns(ctx, txns)
		require.ErrorIs(t, err, errPoolNotReady)
		reset := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
		require.NoError(t, coreDB.View(ctx, func(tx kv.Tx) error {
			var readErr error
			reset.StateVersionId, readErr = rawdb.GetStateVersion(tx)
			return readErr
		}))
		require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		<-reading
		_, err = pool.AddLocalTxns(ctx, txns)
		require.ErrorIs(t, err, errPoolNotReady, "a usable state view must not allow admission before persisted transactions are restored")
		unblock()
		require.NoError(t, <-started)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NotReplaced}, reasons)
		require.Contains(t, pool.byHash, string(original.IDHash[:]))
		require.NotContains(t, pool.byHash, string(replacement.IDHash[:]))
	})
}

func TestFramePoolReconcilesMinedKeyedNonces(t *testing.T) {
	for _, tc := range []struct {
		name     string
		key      uint256.Int
		sequence uint256.Int
		consumed bool
	}{
		{"unused", uint256.Int{5}, uint256.Int{}, false},
		{"first key consumed", uint256.Int{5}, uint256.Int{1}, true},
		{"second key consumed", uint256.Int{7}, uint256.Int{1}, true},
		{"full width sequence", uint256.Int{5}, uint256.Int{0, 1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{5}, uint256.Int{7})
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			coreDB := pool._chainDB.(kv.TemporalRwDB)
			require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
				domains, err := execctx.NewSharedDomains(ctx, tx, pool.logger)
				if err != nil {
					return err
				}
				defer domains.Close()
				slot := types.FrameNonceSlot(sender, tc.key)
				if err := domains.DomainPut(kv.StorageDomain, tx, append(types.FrameNonceManager[:], slot[:]...), tc.sequence.Bytes(), 1, nil); err != nil {
					return err
				}
				return domains.Flush(ctx, tx)
			}))
			reset := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
			reset.StateChangesIncomplete = true
			require.NoError(t, coreDB.View(ctx, func(tx kv.Tx) error {
				var err error
				reset.StateVersionId, err = rawdb.GetStateVersion(tx)
				return err
			}))
			require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, txns))
			_, present := pool.byHash[string(frame.IDHash[:])]
			require.Equal(t, !tc.consumed, present)
		})
	}
}

func TestFetchReconcilesRetainedFrameAfterGap(t *testing.T) {
	for _, withBlobs := range []bool{false, true} {
		for _, tc := range []struct {
			direction remoteproto.Direction
			consumed  bool
		}{
			{remoteproto.Direction_UNWIND, false},
			{remoteproto.Direction_UNWIND, true},
			{remoteproto.Direction_FORWARD, false},
			{remoteproto.Direction_FORWARD, true},
		} {
			t.Run(fmt.Sprintf("%s/blobs=%t/consumed=%t", tc.direction, withBlobs, tc.consumed), func(t *testing.T) {
				ctx, pool, sender := keyedFramePoolEnv(t)
				pool.chainID.SetUint64(1)
				backend := pool.ethBackend.(*frameValidationBackend)
				backend.balance = common.Ether
				fetch := NewFetch(ctx, nil, pool, nil, pool.poolDB, pool.chainID, pool.logger)
				frame := framePoolSlot(t, sender, 1, 2, uint256.Int{5})
				if withBlobs {
					wrapped, err := types.DecodeWrappedTransaction(makeWrappedBlobTxnRlpWithCellProofs(t, &pool.chainID, 1))
					require.NoError(t, err)
					blob := wrapped.(*types.BlobTxWrapper)
					txn := frame.Txn.(*types.FrameTransaction)
					txn.BlobVersionedHashes = blob.Tx.BlobVersionedHashes
					wrapper := &types.FrameTxWrapper{
						FrameTransaction: txn, WrapperVersion: 1,
						Blobs: blob.Blobs, Commitments: blob.Commitments, Proofs: blob.Proofs,
					}
					var encoded bytes.Buffer
					require.NoError(t, wrapper.MarshalBinaryWrapped(&encoded))
					frame = &TxnSlot{}
					parser := NewTxnParseContext(pool.chainID)
					parser.WithSender(false)
					_, err = parser.ParseTransaction(encoded.Bytes(), 0, frame, nil, false, true, nil)
					require.NoError(t, err)
				}
				var txns TxnSlots
				txns.Append(frame, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0, sender), TxnSlots{}, TxnSlots{}, txns))
				require.Empty(t, pool.byHash)

				coreDB := pool._chainDB.(kv.TemporalRwDB)
				require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
					domains, initErr := execctx.NewSharedDomains(ctx, tx, pool.logger)
					if initErr != nil {
						return initErr
					}
					defer domains.Close()
					if tc.consumed {
						slot := types.FrameNonceSlot(sender, uint256.Int{5})
						if writeErr := domains.DomainPut(kv.StorageDomain, tx, append(types.FrameNonceManager[:], slot[:]...), []byte{1}, 1, nil); writeErr != nil {
							return writeErr
						}
					}
					if flushErr := domains.Flush(ctx, tx); flushErr != nil {
						return flushErr
					}
					_, versionErr := rawdb.IncrementStateVersion(tx)
					return versionErr
				}))

				var encoded bytes.Buffer
				require.NoError(t, frame.Txn.MarshalBinary(&encoded))
				stale := newBlockChangeForAccounts(common.Hash{2}, 0)
				stale.StateChangesIncomplete = true
				stale.ChangeBatch[0].Direction = tc.direction
				stale.ChangeBatch[0].Txs = [][]byte{encoded.Bytes()}
				require.ErrorIs(t, fetch.handleStateChangesRequest(ctx, stale), errStateViewSuperseded)
				require.Len(t, fetch.pendingTxnChanges, 1)

				reset := newBlockChangeForAccounts(common.Hash{3}, 0, sender)
				reset.StateChangesIncomplete = true
				require.NoError(t, coreDB.View(ctx, func(tx kv.Tx) error {
					var readErr error
					reset.StateVersionId, readErr = rawdb.GetStateVersion(tx)
					return readErr
				}))
				backend.calls = nil
				backend.err = context.DeadlineExceeded
				require.NoError(t, fetch.handleStateChangesRequest(ctx, reset))
				require.Empty(t, fetch.pendingTxnChanges)
				if !tc.consumed && tc.direction == remoteproto.Direction_UNWIND {
					restored := pool.byHash[string(frame.IDHash[:])]
					require.NotNil(t, restored, "a usable nonce must survive a backend failure")
					require.Zero(t, restored.currentSubPool)
					require.Equal(t, frame.BlobBundles, restored.TxnSlot.BlobBundles)
					return
				}
				require.NotContains(t, pool.byHash, string(frame.IDHash[:]), "only an explicit unwind with an unused nonce may restore a transaction")
				require.Empty(t, backend.calls)
				_, knownSender := pool.senders.getID(sender)
				require.False(t, knownSender, "a rejected restoration must not leave an unused sender")

				backend.err = nil
				var next TxnSlots
				next.Append(framePoolSlotWithNonce(t, sender, 1, 1, 2, uint256.Int{5}), sender[:], true)
				reasons, err = pool.AddLocalTxns(ctx, next)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			})
		}
	}
}

func TestPoolRepeatedUnwindDoesNotPenalizeSender(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		t.Run(fmt.Sprintf("incomplete=%t", incomplete), func(t *testing.T) {
			ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			pool.cfg.AccountSlots = 16
			pool.cfg.MaxNonceGap = 64
			var txns TxnSlots
			for i := range 17 {
				txn := newTestTxnSlot(uint64(i), 0, 1, 2, 21_000)
				txn.IDHash = txn.Txn.Hash()
				txns.Append(txn, sender[:], false)
			}
			pool.AddRemoteTxns(ctx, txns, nil, nil)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.Len(t, pool.byHash, 17)

			change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
			change.StateChangesIncomplete = incomplete
			require.NoError(t, coreDB.View(ctx, func(tx kv.Tx) error {
				var err error
				change.StateVersionId, err = rawdb.GetStateVersion(tx)
				return err
			}))
			var unwind TxnSlots
			unwind.Append(txns.Txns[16], sender[:], false)
			require.NoError(t, pool.OnNewBlock(ctx, change, unwind, TxnSlots{}, TxnSlots{}))
			for _, txn := range txns.Txns {
				require.Contains(t, pool.byHash, string(txn.IDHash[:]), "replaying an unwind must preserve pooled transactions")
			}
		})
	}
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

func TestFramePoolAdmissionRejectsDuringStateRecovery(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		change := newBlockChangeForAccounts(common.Hash{1}, 0)
		change.StateChangesIncomplete = true
		reading, resume := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		defer unblock()
		coreDB := pool._chainDB
		pool._chainDB = stateReadHookDB{coreDB, func() error {
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
		_, err := pool.AddLocalTxns(ctx, txns)
		require.ErrorIs(t, err, errPoolNotReady)
		require.Empty(t, pool.ethBackend.(*frameValidationBackend).calls, "simulation must not use the stale head")
		unblock()
		require.NoError(t, <-updated)
		pool._chainDB = coreDB
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
		require.Equal(t, PendingSubPool, pool.byHash[string(frame.IDHash[:])].currentSubPool)
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
				if local {
					<-done
					require.ErrorIs(t, admissionErr, errPoolNotReady)
				} else {
					select {
					case <-done:
						t.Fatal("remote admission must wait for recovery despite the simulation deadline")
					default:
					}
				}
				require.Len(t, backend.calls, 1, "recovery must not repeatedly simulate against stale state")
				if !local {
					require.Contains(t, pool.remoteFrames.pending, string(frame.IDHash[:]))
				}

				change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				if local {
					reasons, admissionErr = pool.AddLocalTxns(ctx, txns)
					require.NoError(t, admissionErr)
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				} else {
					<-done
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

func TestPoolBlockTimeRestartsAfterCatchUp(t *testing.T) {
	_, pool, _, _ := newTestPool(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		change := newBlockChangeForAccounts(common.Hash{1}, 0)
		change.ChangeBatch[0].BlockHeight = 1
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

		time.Sleep(time.Second)
		change.ChangeBatch[0].BlockHeight = 10
		change.StateChangesIncomplete = true
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		require.Equal(t, int64(12_000), pool.avgBlockTimeMs.Load(), "catch-up is not a block-time sample")
		require.Equal(t, time.Now().UnixMilli(), pool.lastBlockTimestampMs.Load(), "catch-up restarts the clock")

		time.Sleep(6 * time.Second)
		change.ChangeBatch[0].BlockHeight = 11
		change.StateChangesIncomplete = false
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		require.Equal(t, int64(11_400), pool.avgBlockTimeMs.Load(), "the next block is measured from the reset")
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

func TestStartupKeepsAnnouncedGenesisHead(t *testing.T) {
	ctx, pool, poolDB, _ := newTestPool(t)
	pool.started.Store(false)
	require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
		return PutLastSeenBlock(tx, 100, nil)
	}))
	head := common.Hash{1}
	reset := newBlockChangeForAccounts(head, 0)
	reset.StateChangesIncomplete = true
	require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Zero(t, pool.lastSeenBlock.Load())

	require.NoError(t, pool.start(ctx))

	require.Equal(t, head, pool.stateBlockHash)
	require.Zero(t, pool.lastSeenBlock.Load(), "genesis is an announced head, not an unset height")
}

func TestStartupKeepsAnnouncedHeadAfterNewCommit(t *testing.T) {
	ctx, pool, _, coreDB := newTestPool(t)
	pool.started.Store(false)
	pool._stateCache = kvcache.NewLatestBatchCache()
	require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 1)
	}))
	head := common.Hash{1}
	reset := newBlockChangeForAccounts(head, 0)
	reset.ChangeBatch[0].BlockHeight = 1
	reset.StateChangesIncomplete = true
	require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
		if _, err := rawdb.IncrementStateVersion(tx); err != nil {
			return err
		}
		return stages.SaveStageProgress(tx, stages.Execution, 2)
	}))

	require.NoError(t, pool.start(ctx))

	require.Equal(t, head, pool.stateBlockHash)
	require.Equal(t, uint64(1), pool.lastSeenBlock.Load(), "head height must advance with its state notification")
}

func TestStartupKeepsAnnouncedFees(t *testing.T) {
	ctx, pool, poolDB, _ := newTestPool(t)
	pool.started.Store(false)
	require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
		fee := make([]byte, 8)
		binary.BigEndian.PutUint64(fee, 999)
		require.NoError(t, tx.Put(kv.PoolInfo, PoolPendingBaseFeeKey, fee))
		return tx.Put(kv.PoolInfo, PoolPendingBlobFeeKey, fee)
	}))
	reset := newBlockChangeForAccounts(common.Hash{1}, 0)
	reset.PendingBlockBaseFee = 7
	reset.PendingBlobFeePerGas = 11
	reset.BlockGasLimit = 25_000_000
	require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.NoError(t, pool.start(ctx))
	require.Equal(t, uint64(7), pool.pendingBaseFee.Load())
	require.Equal(t, uint64(11), pool.pendingBlobFee.Load())
	require.Equal(t, uint64(25_000_000), pool.blockGasLimit.Load())
}

func TestIncompleteBatchRestoresSetCodeAuthorities(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(key.PublicKey)
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 0, 2, 100_000).Txn.(*types.SetCodeTransaction)
	txn.ChainID = pool.chainID
	txn.Authorizations = []types.Authorization{auth}
	var encoded bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&encoded))
	parser := NewTxnParseContext(pool.chainID)
	parser.WithSender(false)
	var slot TxnSlot
	_, err = parser.ParseTransaction(encoded.Bytes(), 0, &slot, nil, false, false, nil)
	require.NoError(t, err)
	require.Empty(t, slot.AuthAndNonces)

	funded := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
	writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&funded), 1)
	reset := newBlockChangeForAccounts(common.Hash{2}, 0)
	reset.StateChangesIncomplete = true
	require.NoError(t, coreDB.View(ctx, func(tx kv.Tx) error {
		var err error
		reset.StateVersionId, err = rawdb.GetStateVersion(tx)
		return err
	}))
	var unwind TxnSlots
	unwind.Append(&slot, sender[:], false)
	require.NoError(t, pool.OnNewBlock(ctx, reset, unwind, TxnSlots{}, TxnSlots{}))
	restored := pool.byHash[string(slot.IDHash[:])]
	require.NotNil(t, restored)
	require.Equal(t, []AuthAndNonce{{authority, 0}}, restored.TxnSlot.AuthAndNonces)
	require.Contains(t, pool.auths, AuthAndNonce{authority, 0})
}
