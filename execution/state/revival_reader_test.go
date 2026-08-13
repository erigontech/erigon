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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// preBlockStateReader serves one slot and one code blob from before the block,
// so a test can assert the reader stops falling through to it.
type preBlockStateReader struct {
	*accountStateReader
	slot accounts.StorageKey
	val  uint256.Int
	code []byte
}

func (r *preBlockStateReader) ReadAccountStorage(_ accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if key == r.slot {
		return r.val, true, nil
	}
	return uint256.Int{}, false, nil
}

func (r *preBlockStateReader) ReadAccountCode(accounts.Address) ([]byte, error) { return r.code, nil }

func (r *preBlockStateReader) HasStorage(accounts.Address) (bool, error) { return true, nil }

// destructThenRevive seeds a version map with a value at tx0, a destruct at tx1
// and a revival at tx2 — the shape where the latest SelfDestruct entry is the
// revival and so cannot answer "was this wiped".
func destructThenRevive(vm *VersionMap, addr accounts.Address) {
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, false, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 2}, uint64(2), true)
}

// A value-transfer revival writes no Nonce or CodeHash, so the account record
// still floors on the pre-destruct cells. Both have to be wiped, or the record
// contradicts the code and nonce versionedReadCore serves the EVM.
func TestVersionedStateReader_AccountCodeHashWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(105)

	vm := NewVersionMap(nil)
	code := accounts.NewCode([]byte{0x60, 0x00})
	vm.WriteCode(addr, Version{TxIndex: 0}, code, true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 0}, code.Hash, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0}, uint64(7), true)
	destructThenRevive(vm, addr)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(1000), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash, "code hash must agree with the code the reader reports")
	require.Zero(t, acc.Nonce, "the destruct deleted the account the nonce belonged to")
	require.Equal(t, uint64(1000), acc.Balance.Uint64(), "the revival's balance is the live value")
}

// A pre-block contract records no CodeHash cell at all, and a value-transfer
// revival adds no code. The wipe still has to reach the account the
// fall-through serves.
func TestVersionedStateReader_PreBlockCodeHashWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(111)

	reader := newAccountStateReader(addr)
	pre := reader.accounts[addr]
	pre.CodeHash = accounts.NewCode([]byte{0x60, 0x00}).Hash
	pre.Nonce = 1

	vm := NewVersionMap(nil)
	destructThenRevive(vm, addr)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(1000), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, reader)
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash, "the destruct erased the pre-block code")
	require.Zero(t, acc.Nonce, "and the domain record the pre-block nonce came from")
}

func TestVersionedStateReader_AccountFieldsSurviveWhenRevivalRewrote(t *testing.T) {
	t.Parallel()
	addr := getAddress(106)

	vm := NewVersionMap(nil)
	old := accounts.NewCode([]byte{0x60, 0x00})
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 0}, old.Hash, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0}, uint64(7), true)
	destructThenRevive(vm, addr)
	fresh := accounts.NewCode([]byte{0x60, 0x01})
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 2}, fresh.Hash, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 2}, uint64(1), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(1), acc.Nonce, "a write above the destruct is the live value")
	require.Equal(t, fresh.Hash, acc.CodeHash, "and so is the recreated contract's code hash")
}

func TestVersionedStateReader_StorageWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(101)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	destructThenRevive(vm, addr)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "the destruct wiped the slot; the revival did not restore it")
	require.False(t, found, "a wiped slot is absent, not a slot that happens to hold zero")
}

func TestVersionedStateReader_StorageSurvivesWhenRevivalRewrote(t *testing.T) {
	t.Parallel()
	addr := getAddress(102)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	destructThenRevive(vm, addr)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 2}, *uint256.NewInt(9), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(9), val.Uint64(), "a write above the destruct is the live value")
	require.True(t, found, "and it is present, not absent")
}

// The pre-block domain holds nothing, so falling through to it answers false for
// an account whose storage was created and rewritten entirely in this block. A
// slot rewritten above the destruct still makes the account hold storage, and
// reporting otherwise would clear the EIP-684/7610 CREATE-collision check.
func TestVersionedStateReader_HasStorageSeesInBlockOnlyStorage(t *testing.T) {
	t.Parallel()
	addr := getAddress(114)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	destructThenRevive(vm, addr)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 2}, *uint256.NewInt(9), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	has, err := vr.HasStorage(addr)
	require.NoError(t, err)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), val.Uint64())
	require.True(t, has, "HasStorage must agree with the slot ReadAccountStorage serves")
}

// A zero write above the destruct leaves the account with no storage, so the
// erased pre-block slots must not answer for it either.
func TestVersionedStateReader_HasStorageZeroWriteAboveDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(115)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 2}, uint256.Int{}, true)

	reader := &preBlockStateReader{
		accountStateReader: newAccountStateReader(addr),
		slot:               key,
		val:                *uint256.NewInt(5),
	}

	vr := NewVersionedStateReader(3, ReadSet{}, vm, reader)
	has, err := vr.HasStorage(addr)
	require.NoError(t, err)
	require.False(t, has, "the only live slot is zero; the pre-block ones are gone")
}

// recordWipedRead stores the zero a destruct left behind, so a recorded storage
// read is not on its own proof that the account holds storage.
func TestVersionedStateReader_HasStorageIgnoresWipedReadSetEntry(t *testing.T) {
	t.Parallel()
	addr := getAddress(116)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)

	reads := ReadSet{}
	reads.SetStorage(addr, key, VersionedRead[uint256.Int]{})

	reader := &preBlockStateReader{
		accountStateReader: newAccountStateReader(addr),
		slot:               key,
		val:                *uint256.NewInt(5),
	}

	vr := NewVersionedStateReader(3, reads, vm, reader)
	has, err := vr.HasStorage(addr)
	require.NoError(t, err)
	require.False(t, has, "the recorded read is the wipe's zero, not a live slot")
}

func TestVersionedStateReader_CodeWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(103)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, CodePath, accounts.NilKey, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}), true)
	destructThenRevive(vm, addr)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	code, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "the revived account has no code until something writes it")

	size, err := vr.ReadAccountCodeSize(addr)
	require.NoError(t, err)
	require.Zero(t, size, "code size must agree with code")
}

// A destruct with no revival above it must not wipe a cell written after it. BAL
// pre-population writes value paths only, so the latest SelfDestruct entry stays
// the destruct while the value above it is live.
func TestVersionedStateReader_StorageSurvivesDestructWithoutRevivalEntry(t *testing.T) {
	t.Parallel()
	addr := getAddress(107)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 4}, *uint256.NewInt(9), true)

	vr := NewVersionedStateReader(5, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(9), val.Uint64(), "the tx4 write sits above the destruct")
	require.True(t, found)
}

// With no cell of its own the reader would fall through to the pre-block state,
// which the destruct erased.
func TestVersionedStateReader_PreBlockValueWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(108)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	destructThenRevive(vm, addr)

	reader := &preBlockStateReader{
		accountStateReader: newAccountStateReader(addr),
		slot:               key,
		val:                *uint256.NewInt(5),
		code:               []byte{0x60, 0x00},
	}

	vr := NewVersionedStateReader(3, ReadSet{}, vm, reader)

	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "the pre-block slot must not survive the destruct")
	require.False(t, found)

	code, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "nor the pre-block code")

	has, err := vr.HasStorage(addr)
	require.NoError(t, err)
	require.False(t, has, "HasStorage must agree with ReadAccountStorage")
}

// An Estimate destruct belongs to an in-flight incarnation, and these readers
// record no read, so nothing re-checks the verdict once that tx re-executes
// without the destruct.
func TestVersionedStateReader_EstimateDestructDoesNotWipe(t *testing.T) {
	t.Parallel()
	addr := getAddress(110)
	key := accounts.InternKey(common.HexToHash("0x01"))

	reader := newAccountStateReader(addr)
	pre := reader.accounts[addr]
	pre.Nonce = 1
	pre.CodeHash = accounts.NewCode([]byte{0x60, 0x00}).Hash

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	vm.MarkEstimate(addr, SelfDestructPath, accounts.NilKey, 1)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, reader)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), val.Uint64(), "the destruct has not committed")
	require.True(t, found)

	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(1), acc.Nonce, "nor may the account record read as empty")
	require.Equal(t, pre.CodeHash, acc.CodeHash)
}

// An address whose only cell is an Estimate destruct holds no in-block value at
// all, so the record reads as absent rather than as a synthesized empty account.
func TestVersionedStateReader_EstimateDestructAloneLeavesNoRecord(t *testing.T) {
	t.Parallel()
	addr := getAddress(117)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	vm.MarkEstimate(addr, SelfDestructPath, accounts.NilKey, 1)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader())
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Nil(t, acc)
}

// Every newly created account writes SelfDestruct=false, so an invalidated
// creating tx leaves an Estimate cell holding false. Reading that as a destruct
// would report a live account's storage as absent.
func TestVersionedStateReader_EstimateCreationIsNotADestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(113)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, false, true)
	vm.MarkEstimate(addr, SelfDestructPath, accounts.NilKey, 2)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), val.Uint64(), "no destruct ever happened here")
	require.True(t, found)
}

// A recreate above the destruct restores the account, and the record and the code
// readers have to serve the same contract. It cannot land at the destruct's own
// TxIndex — see TestSelfdestructWriteSetShape.
func TestVersionedStateReader_RecreateAboveDestructIsServedWhole(t *testing.T) {
	t.Parallel()
	addr := getAddress(109)
	recreated := accounts.NewCode([]byte{0x60, 0x01})

	vm := NewVersionMap(nil)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0}, uint64(7), true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}).Hash, true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)

	fresh := accounts.NewAccount()
	fresh.CodeHash = recreated.Hash
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, false, true)
	writeFor(vm, addr, AddressPath, accounts.NilKey, Version{TxIndex: 2}, &fresh, true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 2}, recreated.Hash, true)
	writeFor(vm, addr, CodePath, accounts.NilKey, Version{TxIndex: 2}, recreated, true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, recreated.Hash, acc.CodeHash, "the recreate's code hash is not what the destruct erased")

	code, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Equal(t, recreated.Bytes, code, "and the code readers must serve the same contract")

	size, err := vr.ReadAccountCodeSize(addr)
	require.NoError(t, err)
	require.Equal(t, len(recreated.Bytes), size)
}

// applySubFieldWrites scans for Nonce and CodeHash but applies Balance and
// Incarnation off their floor cells alone. That split holds only while the
// destructing tx's own write set drops the first pair and carries the second,
// so pin the writer's shape.
func TestSelfdestructWriteSetShape(t *testing.T) {
	t.Parallel()
	for _, preserveBalance := range []bool{false, true} {
		t.Run(map[bool]string{false: "burn", true: "eip8246"}[preserveBalance], func(t *testing.T) {
			t.Parallel()
			addr := getAddress(119)
			base := accounts.NewAccount()
			base.Balance = *uint256.NewInt(100)
			base.Nonce = 1
			base.Incarnation = 3

			ibs := NewWithVersionMap(&sdAccountReader{addr: addr, account: &base}, NewVersionMap(nil))
			ibs.SetTxContext(1, 2)
			ibs.SetVersion(0)
			require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetNonce(addr, 9, tracing.NonceChangeUnspecified))
			destroyed, err := ibs.Selfdestruct(addr, preserveBalance)
			require.NoError(t, err)
			require.True(t, destroyed)

			writes := ibs.FinalizedWrites(&chain.Rules{})
			for _, path := range []AccountPath{AddressPath, NoncePath, CodePath, CodeHashPath, CodeSizePath} {
				require.False(t, writes.Has(WriteHeader{Address: addr, Path: path}), "%s must not survive the destruct", path)
			}

			bal, hasBalance := writes.GetBalance(addr)
			require.Equal(t, !preserveBalance, hasBalance, "a burning destruct zeroes the balance, EIP-8246 keeps the prior cell")
			if hasBalance {
				require.True(t, bal.Val.IsZero())
			}
			inc, hasIncarnation := writes.GetIncarnation(addr)
			require.True(t, hasIncarnation, "the destruct always writes an incarnation of its own")
			if preserveBalance {
				require.Zero(t, inc.Val, "EIP-8246 leaves a balance-only account a re-creation bumps from 0")
			} else {
				require.Equal(t, uint64(3), inc.Val, "the storage-delete cascade needs the pre-destruct incarnation")
			}
		})
	}
}

// Every path the account record is drawn from has to answer, since an EIP-161
// verdict resting on any one of them is provisional.
func TestAnyEstimateAccountCell(t *testing.T) {
	t.Parallel()
	acc := accounts.NewAccount()

	for _, path := range []AccountPath{AddressPath, BalancePath, NoncePath, CodeHashPath} {
		t.Run(path.String(), func(t *testing.T) {
			t.Parallel()
			addr := getAddress(120)
			var value any = &acc
			switch path {
			case BalancePath:
				value = *uint256.NewInt(1)
			case NoncePath:
				value = uint64(1)
			case CodeHashPath:
				value = accounts.EmptyCodeHash
			}

			vm := NewVersionMap(nil)
			writeFor(vm, addr, path, accounts.NilKey, Version{TxIndex: 1}, value, true)
			require.False(t, vm.AnyEstimateAccountCell(addr, 3), "a committed cell is not provisional")

			vm.MarkEstimate(addr, path, accounts.NilKey, 1)
			require.True(t, vm.AnyEstimateAccountCell(addr, 3))
			require.False(t, vm.AnyEstimateAccountCell(addr, 1), "the cell is above this reader")
		})
	}
}

// Same shape through IntraBlockState, which resolves code via versionedReadCore
// rather than these readers.
func TestGetCodeAfterDestructThenRevivalIsEmpty(t *testing.T) {
	t.Parallel()
	addr := getAddress(104)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, CodePath, accounts.NilKey, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}), true)
	destructThenRevive(vm, addr)

	reader := newAccountStateReader(addr)
	ibs := NewWithVersionMap(NewVersionedStateReader(3, ReadSet{}, vm, reader), vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 3)
	ibs.SetVersion(0)

	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "the destruct removed the code and the revival did not write any")

	size, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	require.Zero(t, size, "code size must agree with code")
}
