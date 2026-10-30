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
	"encoding/binary"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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
	var encoded bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&encoded))
	return &TxnSlot{Txn: tx, IDHash: tx.Hash(), Rlp: encoded.Bytes(), Size: uint32(encoded.Len())}
}

func TestFramePoolRecentRootAddressAsPaymaster(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.payer, backend.paymaster = types.FrameRecentRootVerifier, true
	backend.dependencies = [][]byte{types.FrameRecentRootVerifier[:]}
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveExecution, Limits: mdgas.MdGas{Execution: 100}},
			{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &types.FrameRecentRootVerifier, Limits: mdgas.MdGas{Execution: 100}},
		},
		Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(1)},
	}
	require.NoError(t, protocol.ValidateFrameTransactionPrefix(tx))
	var encoded bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&encoded))
	slot := &TxnSlot{Txn: tx, IDHash: tx.Hash(), Rlp: encoded.Bytes(), Size: uint32(encoded.Len())}
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
		{"oldest usable", common.NewUint64(10 + protocol.RecentRootLength - 2), true},
		{"expired", common.NewUint64(10 + protocol.RecentRootLength - 1), false},
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
		valid       bool
	}{
		{"before first Bogota block", 76, 19, remoteproto.Direction_FORWARD, true},
		{"activation unwound", 100, 21, remoteproto.Direction_UNWIND, true},
		{"invalid after unwind", 100, 21, remoteproto.Direction_UNWIND, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			pool.chainConfig.BogotaTime = common.NewUint64(100)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.dependencies = [][]byte{types.FrameRecentRootVerifier[:]}
			initial := newBlockChangeForAccounts(common.Hash{1}, 0)
			initial.ChangeBatch[0].BlockTime = tc.initialTime
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(tc.initialSlot)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			backend.valid, backend.calls = tc.valid, nil
			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			change.ChangeBatch[0].BlockTime = 88
			change.ChangeBatch[0].SlotNumber = common.NewUint64(20)
			change.ChangeBatch[0].Direction = tc.direction
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.Equal(t, []common.Hash{slot.IDHash}, backend.calls, "simulation decides activation using the next payload's rules")
			if tc.valid {
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			} else {
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
				require.Empty(t, pool.all.senderIDFrameTxn)
			}
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
			backend.dependencies = [][]byte{types.FrameRecentRootVerifier[:]}
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
