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
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func recentRootPoolSlot(t *testing.T, sender common.Address, rootSlot, fee uint64) *TxnSlot {
	t.Helper()
	data := make([]byte, 72)
	data[0], data[71] = 1, 2
	binary.BigEndian.PutUint64(data[32:40], rootSlot)
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Target: &types.FrameRecentRootVerifier, Data: data, Limits: mdgas.MdGas{Execution: 20_000}},
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}},
		},
		Fees: types.FrameFees{TipCap: *uint256.NewInt(fee), FeeCap: *uint256.NewInt(fee)},
	}
	return framePoolSlotFromTransaction(t, tx)
}

func TestFramePoolRecentRootAddressAsPaymaster(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.payer, backend.paymaster = types.FrameRecentRootVerifier, true
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveExecution, Limits: mdgas.MdGas{Execution: 100}},
			{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &types.FrameRecentRootVerifier, Limits: mdgas.MdGas{Execution: 100}},
		},
		Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(1)},
	}
	require.NoError(t, protocol.ValidateFrameTransactionPrefix(tx))
	slot := framePoolSlotFromTransaction(t, tx)
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Empty(t, slot.frameValidation.RecentRoots)
	require.Contains(t, slot.frameValidation.Dependencies, types.FrameRecentRootVerifier)
}

func TestFramePoolRecentRootExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		slot *uint64
		keep bool
	}{
		{"oldest usable", common.NewUint64(10 + protocol.RecentRootHistorySlots - 2), true},
		{"expired", common.NewUint64(10 + protocol.RecentRootHistorySlots - 1), false},
		{"current slot after reorg", common.NewUint64(9), false},
		{"future slot after reorg", common.NewUint64(8), false},
		{"missing slot", nil, false},
		{"slot overflow", common.NewUint64(math.MaxUint64), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			initial := newBlockChangeForAccounts(common.Hash{1}, 0)
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(10)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.calls = nil

			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			change.ChangeBatch[0].SlotNumber = tc.slot
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			if tc.keep {
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			} else {
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
				require.Empty(t, pool.all.senderIDFrameTxn)
			}
			require.Empty(t, backend.calls, "age checks must not require EVM simulation")
		})
	}
}

func TestFramePoolRecentRootForkBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		initialTime uint64
		initialSlot uint64
		direction   remoteproto.Direction
		activeHead  bool
		evict       bool
	}{
		{"before first Bogota block", 76, 19, remoteproto.Direction_FORWARD, false, false},
		{"first activation in batch", 76, 19, remoteproto.Direction_FORWARD, true, false},
		{"head crosses back before activation", 100, 21, remoteproto.Direction_UNWIND, false, true},
		{"activation unwind followed by active head", 100, 21, remoteproto.Direction_UNWIND, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := uint64(time.Now().Unix())
				chainConfig := chain.AllProtocolChanges.Copy()
				chainConfig.BogotaTime = common.NewUint64(now + 100)
				ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
				backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
				pool.ethBackend = backend
				initial := newBlockChangeForAccounts(common.Hash{1}, 0)
				initial.ChangeBatch[0].BlockTime = now + tc.initialTime
				initial.ChangeBatch[0].SlotNumber = common.NewUint64(tc.initialSlot)
				require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				time.Sleep(100 * time.Second)
				slot := recentRootPoolSlot(t, sender, 10, 1)
				var txns TxnSlots
				txns.Append(slot, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

				backend.calls = nil
				change := newBlockChangeForAccounts(common.Hash{2}, 0)
				change.ChangeBatch[0].BlockTime = now + 88
				change.ChangeBatch[0].SlotNumber = common.NewUint64(20)
				change.ChangeBatch[0].Direction = tc.direction
				if tc.activeHead {
					replacement := newBlockChangeForAccounts(common.Hash{3}, 0)
					replacement.ChangeBatch[0].BlockTime = now + 112
					replacement.ChangeBatch[0].SlotNumber = common.NewUint64(22)
					change.ChangeBatch = append(change.ChangeBatch, replacement.ChangeBatch...)
				}
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.Equal(t, tc.activeHead, pool.headIsBogota)
				require.Empty(t, backend.calls, "activation and age checks do not require simulation")
				if tc.evict {
					require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
					require.Empty(t, pool.frameReservations)
					require.Empty(t, pool.all.senderIDFrameTxn)
				} else {
					require.Contains(t, pool.byHash, string(slot.IDHash[:]))
					require.Equal(t, slot.frameValidation.MaxCost, pool.frameReservations[sender].cost)
				}
			})
		})
	}
}

type frameEvictionErrorView struct {
	kvcache.CacheView
	pool *TxPool
	hash string
	err  error
}

func (v frameEvictionErrorView) Get(address []byte) ([]byte, error) {
	if v.pool.byHash[v.hash] == nil {
		return nil, v.err
	}
	return v.CacheView.Get(address)
}

func TestFramePoolRecentRootActivationUnwindRetry(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now - 1)
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	pool.ethBackend = backend
	initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	initial.ChangeBatch[0].BlockTime = now
	initial.ChangeBatch[0].SlotNumber = common.NewUint64(21)
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	frame := recentRootPoolSlot(t, sender, 10, 1)
	successor := encodeRemoteTxn(t, newTestTxnSlot(1, 0, 1, 1, 21_000))
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	txns.Append(successor, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	backend.calls = nil

	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].BlockTime = now - 2
	change.ChangeBatch[0].SlotNumber = common.NewUint64(20)
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	var unwind TxnSlots
	unwind.Append(recentRootPoolSlot(t, sender, 10, 1), sender[:], false)
	readErr := errors.New("promotion account read failed")
	require.NoError(t, coreDB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		view, err := pool._stateCache.View(ctx, tx)
		require.NoError(t, err)
		failing := frameEvictionErrorView{view, pool, string(frame.IDHash[:]), readErr}
		_, err = pool.applyBlockUpdate(change, unwind, TxnSlots{}, TxnSlots{}, failing, nil)
		require.ErrorIs(t, err, readErr)
		return nil
	}))
	require.NotContains(t, pool.byHash, string(frame.IDHash[:]), "eviction must precede the injected promotion failure")
	require.True(t, pool.stateViewStale)

	require.NoError(t, pool.OnNewBlock(ctx, change, unwind, TxnSlots{}, TxnSlots{}))
	require.NotContains(t, pool.byHash, string(frame.IDHash[:]), "retry must preserve activation-unwind eviction")
	require.Empty(t, pool.frameReservations)
	require.Empty(t, pool.all.senderIDFrameTxn)
	require.Empty(t, backend.calls)
	require.False(t, pool.headIsBogota)
}

func TestFramePoolActivationAfterFailedRevalidation(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now - 1)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	pool.ethBackend = backend
	initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	initial.ChangeBatch[0].BlockTime = now
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	updateCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	backend.onValidate = cancel
	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].BlockTime = now - 2
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.ErrorIs(t, pool.OnNewBlock(updateCtx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}), context.Canceled)
	require.True(t, pool.headIsBogota, "failed revalidation must not commit the activation change")

	backend.onValidate = nil
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.False(t, pool.headIsBogota)
}

func TestFramePoolSupersededUpdateKeepsActivation(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now - 1)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	pool.ethBackend = backend
	initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	initial.ChangeBatch[0].BlockTime = now
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	backend.onValidate = func() {
		newer := newBlockChangeForAccounts(common.Hash{3}, 1, sender)
		newer.ChangeBatch[0].BlockTime = now + 12
		require.NoError(t, pool.OnNewBlock(ctx, newer, TxnSlots{}, TxnSlots{}, txns))
	}
	older := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	older.ChangeBatch[0].BlockTime = now - 2
	older.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.NoError(t, pool.OnNewBlock(ctx, older, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, common.Hash{3}, pool.stateBlockHash)
	require.True(t, pool.headIsBogota, "completion of an older update must not overwrite the newer activation")
}

func TestFramePoolRestoredRecentRootEviction(t *testing.T) {
	for _, tc := range []struct {
		name             string
		validated        bool
		activationUnwind bool
		headSlot         uint64
		keep             bool
	}{
		{"validated activation unwind", true, true, 20, false},
		{"parked activation unwind", false, true, 20, false},
		{"parked expired root", false, false, 10 + protocol.RecentRootHistorySlots - 1, false},
		{"parked usable root", false, false, 22, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := uint64(time.Now().Unix())
				chainConfig := chain.AllProtocolChanges.Copy()
				chainConfig.BogotaTime = common.NewUint64(now)
				ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
				pool.chainID.SetUint64(1)
				backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
				if !tc.validated {
					backend.err = errors.New("validation backend unavailable")
				}
				pool.ethBackend = backend
				header := &types.Header{Number: uint256.Int{21}, Time: now, SlotNumber: common.NewUint64(21)}
				require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
					if err := rawdb.WriteHeader(tx, header); err != nil {
						return err
					}
					rawdb.WriteHeadBlockHash(tx, header.Hash())
					return nil
				}))
				slot := recentRootPoolSlot(t, sender, 10, 1)
				require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.PoolTransaction, slot.IDHash[:], append(sender[:], slot.Rlp...))
				}))
				require.NoError(t, pool.start(ctx))
				mt := pool.byHash[string(slot.IDHash[:])]
				require.NotNil(t, mt)
				if tc.validated {
					require.Equal(t, PendingSubPool, mt.currentSubPool)
				} else {
					require.Zero(t, mt.currentSubPool)
					require.Nil(t, mt.TxnSlot.frameValidation)
				}

				backend.calls, backend.err = nil, nil
				change := newBlockChangeForAccounts(common.Hash{2}, 0)
				head := change.ChangeBatch[0]
				head.BlockTime = now + 12
				head.SlotNumber = common.NewUint64(tc.headSlot)
				if tc.activationUnwind {
					head.BlockTime = now - 12
					head.Direction = remoteproto.Direction_UNWIND
				}
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				if tc.keep {
					require.Contains(t, pool.byHash, string(slot.IDHash[:]))
					require.Equal(t, PendingSubPool, mt.currentSubPool)
					require.Len(t, backend.calls, 1)
				} else {
					require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
					require.Empty(t, pool.frameReservations)
					require.Empty(t, pool.all.senderIDFrameTxn)
					require.Empty(t, backend.calls, "activation and age checks do not require simulation")
				}
			})
		})
	}
}

func TestFramePoolRecentRootDependencies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		action     remoteproto.Action
		selected   bool
		unwind     bool
		revalidate bool
	}{
		{"unrelated root", remoteproto.Action_STORAGE, false, false, false},
		{"verifier balance changed", remoteproto.Action_UPSERT, false, false, false},
		{"verifier balance unwound", remoteproto.Action_UPSERT, false, true, false},
		{"referenced root changed", remoteproto.Action_STORAGE, true, false, true},
		{"referenced root changed with balance", remoteproto.Action_UPSERT, true, false, true},
		{"referenced root cleared by unwind", remoteproto.Action_STORAGE, true, true, true},
		{"verifier code changed", remoteproto.Action_CODE, false, false, true},
		{"verifier account and code changed", remoteproto.Action_UPSERT_CODE, false, false, true},
		{"verifier removed", remoteproto.Action_REMOVE, false, false, true},
		{"verifier deployment unwound", remoteproto.Action_REMOVE, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			require.Len(t, slot.frameValidation.RecentRoots, 1)
			ref := slot.frameValidation.RecentRoots[0]
			key := common.Hash{99}
			if tc.selected {
				key = ref.StorageKey
			}
			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			head := change.ChangeBatch[0]
			head.SlotNumber = common.NewUint64(11)
			head.Changes = []*remoteproto.AccountChange{{
				Address: gointerfaces.ConvertAddressToH160(types.FrameRecentRootVerifier), Action: tc.action,
				StorageChanges: []*remoteproto.StorageChange{{Location: gointerfaces.ConvertHashToH256(key)}},
			}}
			if tc.action == remoteproto.Action_UPSERT || tc.action == remoteproto.Action_UPSERT_CODE {
				account := accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.NewCode(protocol.RecentRootCode).Hash}
				if tc.action == remoteproto.Action_UPSERT_CODE {
					head.Changes[0].Code = []byte{0xfe}
					account.CodeHash = accounts.NewCode(head.Changes[0].Code).Hash
				}
				head.Changes[0].Data = accounts.SerialiseV3(&account)
				if !tc.selected {
					head.Changes[0].StorageChanges = nil
				}
			}
			if tc.unwind {
				head.Direction = remoteproto.Direction_UNWIND
			}
			backend.valid, backend.calls = false, nil
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			if tc.revalidate {
				require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
			} else {
				require.Empty(t, backend.calls)
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			}
		})
	}
}

func TestFramePoolRecentRootDeploymentUnwindWithBalance(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	slot := recentRootPoolSlot(t, sender, 10, 1)
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Len(t, slot.frameValidation.RecentRoots, 1)

	account := accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	change := newBlockChangeForAccounts(common.Hash{2}, 0)
	head := change.ChangeBatch[0]
	head.Direction = remoteproto.Direction_UNWIND
	head.SlotNumber = common.NewUint64(11)
	// The funded account survives deployment rollback; its code and roots do not.
	head.Changes = []*remoteproto.AccountChange{{
		Address: gointerfaces.ConvertAddressToH160(types.FrameRecentRootVerifier),
		Action:  remoteproto.Action_UPSERT,
		Data:    accounts.SerialiseV3(&account),
		StorageChanges: []*remoteproto.StorageChange{{
			Location: gointerfaces.ConvertHashToH256(slot.frameValidation.RecentRoots[0].StorageKey),
		}},
	}}
	backend.valid, backend.calls = false, nil
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
	require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
	require.Empty(t, pool.frameReservations)
}

func TestFramePoolRecentRootReplacement(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	first := recentRootPoolSlot(t, sender, 10, 1)
	replacement := recentRootPoolSlot(t, sender, 20, 2)
	for _, slot := range []*TxnSlot{first, replacement} {
		var txns TxnSlots
		txns.Append(slot, sender[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	}
	require.NotContains(t, pool.byHash, string(first.IDHash[:]))
	require.Contains(t, pool.byHash, string(replacement.IDHash[:]))
	require.Len(t, pool.all.senderIDFrameTxn, 1)
	require.Len(t, replacement.frameValidation.RecentRoots, 1)
	require.Equal(t, uint64(20), replacement.frameValidation.RecentRoots[0].Slot)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.valid, backend.calls = false, nil
	for _, slot := range []*TxnSlot{first, replacement} {
		change := newBlockChangeForAccounts(common.Hash{3}, 0)
		change.ChangeBatch[0].SlotNumber = common.NewUint64(30)
		change.ChangeBatch[0].Changes = []*remoteproto.AccountChange{{
			Address: gointerfaces.ConvertAddressToH160(types.FrameRecentRootVerifier), Action: remoteproto.Action_STORAGE,
			StorageChanges: []*remoteproto.StorageChange{{Location: gointerfaces.ConvertHashToH256(slot.frameValidation.RecentRoots[0].StorageKey)}},
		}}
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		if slot == first {
			require.Empty(t, backend.calls)
			require.Contains(t, pool.byHash, string(replacement.IDHash[:]))
		} else {
			require.Equal(t, []common.Hash{replacement.IDHash}, backend.calls)
			require.NotContains(t, pool.byHash, string(replacement.IDHash[:]))
			require.Empty(t, pool.frameReservations)
		}
	}
}

func TestFramePoolRecentRootIncompleteNotifications(t *testing.T) {
	for _, tc := range []struct {
		name       string
		height     uint64
		incomplete bool
	}{
		{"complete empty block", 1_012, false},
		{"deep reorg without root rewrite", 11, true},
		{"head-only catch-up", 1_200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			initial := newBlockChangeForAccounts(common.Hash{1}, 0)
			initial.ChangeBatch[0].BlockHeight = 1_011
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(1_011)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			backend.valid, backend.calls = false, nil
			backend.onValidate = func() {
				require.Zero(t, pool.byHash[string(slot.IDHash[:])].currentSubPool, "park before simulation")
			}
			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			change.StateChangesIncomplete = tc.incomplete
			change.ChangeBatch[0].BlockHeight = tc.height
			change.ChangeBatch[0].SlotNumber = common.NewUint64(tc.height)
			require.True(t, slot.frameValidation.RecentRoots[0].ValidAt(tc.height+1), "age checks alone cannot detect the missing root")
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			if tc.incomplete {
				require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
			} else {
				require.Empty(t, backend.calls)
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			}
		})
	}
}
