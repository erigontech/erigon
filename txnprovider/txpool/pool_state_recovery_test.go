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
				err = errors.New("frame admission failed")
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
		unblock()
		require.NoError(t, <-updated)
		require.NoError(t, <-admitted)
		require.Equal(t, PendingSubPool, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	})
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

	require.Error(t, pool.OnNewBlock(ctx, stale, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Contains(t, pool.byHash, string(txn.IDHash[:]), "an obsolete account update must not discard the valid transaction")
	require.Equal(t, common.Hash{1}, pool.stateBlockHash)
	require.True(t, pool.stateViewStale)
}
