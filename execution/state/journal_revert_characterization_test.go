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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Characterization suite pinning the observable revert behaviour of every journal
// entry kind on the versioned path: snapshot, mutate through the public IBS API,
// assert the EVM-observable state changed, revert, assert it is restored exactly.
// Observables (not internal cells) are asserted on purpose — the cell bookkeeping is
// what later changes when the journal becomes cell-aware and the stateObject path is
// removed, so this suite must stay green across that change as proof of no behaviour
// change. See TestJournalDirtySymmetry for the complementary per-kind dirty accounting.

func versionedRevertIBS(reader StateReader) *IntraBlockState {
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)
	return ibs
}

func committedAccountReader(addr accounts.Address, nonce uint64, balance uint64, code []byte) *fieldReader {
	acc := accounts.NewAccount()
	acc.Nonce = nonce
	acc.Balance = *uint256.NewInt(balance)
	if len(code) > 0 {
		acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	}
	return &fieldReader{addr: addr, account: &acc, code: code}
}

func TestJournalRevert_Balance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x01})
	ibs := versionedRevertIBS(committedAccountReader(addr, 1, 1000, nil))

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(500), tracing.BalanceChangeUnspecified))
	got, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1500), got, "AddBalance raised the balance")

	ibs.RevertToSnapshot(snap, nil)
	got, err = ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1000), got, "revert restores the committed balance")
}

func TestJournalRevert_Nonce(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x02})
	ibs := versionedRevertIBS(committedAccountReader(addr, 5, 10, nil))

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.SetNonce(addr, 9, tracing.NonceChangeUnspecified))
	got, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(9), got, "SetNonce changed the nonce")

	ibs.RevertToSnapshot(snap, nil)
	got, err = ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(5), got, "revert restores the committed nonce")
}

func TestJournalRevert_Storage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x03})
	key := accounts.InternKey([32]byte{0x01})
	acc := accounts.NewAccount()
	acc.Nonce, acc.Incarnation = 1, 1
	reader := &storageReader{addr: addr, account: &acc, storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(7)}}
	ibs := versionedRevertIBS(reader)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(42)))
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(42), got, "SetState changed the slot")

	ibs.RevertToSnapshot(snap, nil)
	got, err = ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), got, "revert restores the committed slot value")
}

func TestJournalRevert_Code(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x04})
	oldCode := []byte{0x60, 0x01}
	ibs := versionedRevertIBS(committedAccountReader(addr, 1, 1, oldCode))
	oldHash := accounts.InternCodeHash(crypto.Keccak256Hash(oldCode))

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x02, 0x60, 0x03}, tracing.CodeChangeUnspecified))
	hash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.NotEqual(t, oldHash, hash, "SetCode changed the code hash")

	ibs.RevertToSnapshot(snap, nil)
	hash, err = ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, oldHash, hash, "revert restores the committed code hash")
	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, oldCode, code, "revert restores the committed code")
}

func TestJournalRevert_Refund(t *testing.T) {
	t.Parallel()
	ibs := versionedRevertIBS(&emptyReader{})

	snap := ibs.PushSnapshot()
	ibs.AddRefund(7)
	require.Equal(t, uint64(7), ibs.GetRefund(), "AddRefund raised the refund")

	ibs.RevertToSnapshot(snap, nil)
	require.Equal(t, uint64(0), ibs.GetRefund(), "revert restores the refund counter")
}

func TestJournalRevert_AccessListAccount(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x05})
	ibs := versionedRevertIBS(&emptyReader{})

	snap := ibs.PushSnapshot()
	ibs.AddAddressToAccessList(addr)
	require.True(t, ibs.AddressInAccessList(addr), "address added to the access list")

	ibs.RevertToSnapshot(snap, nil)
	require.False(t, ibs.AddressInAccessList(addr), "revert removes the address from the access list")
}

func TestJournalRevert_AccessListSlot(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x06})
	slot := accounts.InternKey([32]byte{0x02})
	ibs := versionedRevertIBS(&emptyReader{})
	ibs.AddAddressToAccessList(addr)

	snap := ibs.PushSnapshot()
	ibs.AddSlotToAccessList(addr, slot)
	_, slotPresent := ibs.SlotInAccessList(addr, slot)
	require.True(t, slotPresent, "slot added to the access list")

	ibs.RevertToSnapshot(snap, nil)
	_, slotPresent = ibs.SlotInAccessList(addr, slot)
	require.False(t, slotPresent, "revert removes the slot from the access list")
}

func TestJournalRevert_TransientStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x07})
	key := TransientKey(*uint256.NewInt(1))
	ibs := versionedRevertIBS(&emptyReader{})

	snap := ibs.PushSnapshot()
	ibs.SetTransientState(addr, &key, uint256.NewInt(99))
	require.Equal(t, *uint256.NewInt(99), ibs.GetTransientState(addr, key), "SetTransientState set the slot")

	ibs.RevertToSnapshot(snap, nil)
	require.Equal(t, uint256.Int{}, ibs.GetTransientState(addr, key), "revert clears the transient slot")
}

func TestJournalRevert_Selfdestruct(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xba, 0x08})
	ibs := versionedRevertIBS(committedAccountReader(addr, 1, 1000, []byte{0x60, 0x01}))

	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	destructed, err := ibs.HasSelfdestructed(addr)
	require.NoError(t, err)
	require.True(t, destructed, "Selfdestruct marked the account")

	ibs.RevertToSnapshot(snap, nil)
	destructed, err = ibs.HasSelfdestructed(addr)
	require.NoError(t, err)
	require.False(t, destructed, "revert clears the self-destruct")
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1000), bal, "revert restores the pre-destruct balance")
}

func TestJournalRevert_FrameApproval(t *testing.T) {
	t.Parallel()
	ibs := versionedRevertIBS(&emptyReader{})
	payer := accounts.InternAddress([20]byte{0xfa, 0x01})
	ctx := ibs.NewFrameContext(&types.FrameTransaction{}, uint256.Int{})
	prevPayer, prevApproved := ctx.Payer, ctx.SenderApproved

	snap := ibs.PushSnapshot()
	ctx.SetApproval(true, payer)
	require.True(t, ctx.SenderApproved, "SetApproval set approval")
	require.Equal(t, payer, ctx.Payer, "SetApproval set the payer")

	ibs.RevertToSnapshot(snap, nil)
	require.Equal(t, prevApproved, ctx.SenderApproved, "revert restores frame approval")
	require.Equal(t, prevPayer, ctx.Payer, "revert restores the frame payer")
}

func TestJournalRevert_FrameStateGas(t *testing.T) {
	t.Parallel()
	ibs := versionedRevertIBS(&emptyReader{})
	ctx := ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Limits: mdgas.MdGas{State: 10}}}}, uint256.Int{})

	snap := ibs.PushSnapshot()
	require.True(t, ctx.ChargeStateGas(3))
	require.Equal(t, uint64(3), ctx.Receipts[0].GasUsed.State, "ChargeStateGas charged state gas")

	ibs.RevertToSnapshot(snap, nil)
	require.Equal(t, uint64(0), ctx.Receipts[0].GasUsed.State, "revert restores the charged state gas")
}

func TestJournalRevert_FrameTransient(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xfa, 0x03})
	key := TransientKey(*uint256.NewInt(1))
	ibs := versionedRevertIBS(&emptyReader{})
	ibs.SetTransientState(addr, &key, uint256.NewInt(55))

	snap := ibs.PushSnapshot()
	ibs.ResetFrameTransientStorage()
	require.Equal(t, uint256.Int{}, ibs.GetTransientState(addr, key), "frame reset cleared transient storage")

	ibs.RevertToSnapshot(snap, nil)
	require.Equal(t, *uint256.NewInt(55), ibs.GetTransientState(addr, key), "revert restores transient storage after a frame reset")
}

// journalKindRevertCoverage maps every journal entry kind to the test that pins its
// revert behaviour (or the reason it needs none). TestJournalRevertCoverageComplete
// asserts the map covers every kind in [0, kindEnd) so a newly added kind cannot land
// without a revert test — the guard that keeps the stateObject removal verifiable by
// fast unit tests rather than a long integration pass.
var journalKindRevertCoverage = map[entryKind]string{
	kindBalance:              "TestJournalRevert_Balance",
	kindNonce:                "TestJournalRevert_Nonce",
	kindStorage:              "TestJournalRevert_Storage",
	kindCode:                 "TestJournalRevert_Code",
	kindRefund:               "TestJournalRevert_Refund",
	kindAccessListAddAccount: "TestJournalRevert_AccessListAccount",
	kindAccessListAddSlot:    "TestJournalRevert_AccessListSlot",
	kindTransientStorage:     "TestJournalRevert_TransientStorage",
	kindSelfdestruct:         "TestJournalRevert_Selfdestruct (+ selfdestruct_revert_versioned_test)",
	kindCreateObject:         "TestNoMaterialize_CreateRevertDropsCells",
	kindResetObject:          "TestNoMaterialize_RecreateRevertRestoresPrior",
	kindTouch:                "TestNoMaterialize_RevertedTouchNotPublished",
	kindFrameApproval:        "TestJournalRevert_FrameApproval",
	kindFrameStateGas:        "TestJournalRevert_FrameStateGas",
	kindFrameTransient:       "TestJournalRevert_FrameTransient",
	kindFrameStorageOwner:    "TestJournalDirtySymmetry (frame cell; dirty-symmetry covered)",
}

func TestJournalRevertCoverageComplete(t *testing.T) {
	t.Parallel()
	for k := range kindEnd {
		if _, ok := journalKindRevertCoverage[k]; !ok {
			t.Fatalf("journal kind %d has no revert-coverage entry: add a characterization test or document why it needs none", k)
		}
	}
	require.Len(t, journalKindRevertCoverage, int(kindEnd), "coverage map has an entry for a kind that no longer exists")
}
