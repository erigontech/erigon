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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var amsterdamRules = &chain.Rules{IsAmsterdam: true, IsSpuriousDragon: true}

func newAmsterdamIBS(reader *accountStateReader, vm *VersionMap, txIdx int) *IntraBlockState {
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(1, txIdx)
	ibs.SetVersion(0)
	return ibs
}

// destroyThenRecreate runs two txs serially: tx 0 CREATE2-deploys addr with a
// constructor that self-destructs to another address, tx 1 CREATE2-deploys code
// at addr again. It returns both write sets and the block access list they
// produce, which carries tx 1's nonce and code but no account record.
func destroyThenRecreate(t *testing.T, reader *accountStateReader, addr accounts.Address, code []byte) (*WriteSet, *WriteSet, types.BlockAccessList) {
	t.Helper()
	dead := accounts.InternAddress(common.HexToAddress("0xdead"))
	vm := NewVersionMap(nil)

	destroyer := newAmsterdamIBS(reader, vm, 0)
	t.Cleanup(destroyer.Close)
	_, err := destroyer.GetCodeHash(addr)
	require.NoError(t, err)
	_, err = destroyer.GetNonce(addr)
	require.NoError(t, err)
	require.NoError(t, destroyer.CreateAccount(addr, true))
	require.NoError(t, destroyer.SetNonce(addr, 1, tracing.NonceChangeNewContract))
	balance, err := destroyer.GetBalance(addr)
	require.NoError(t, err)
	require.NoError(t, destroyer.SubBalance(addr, balance, tracing.BalanceDecreaseSelfdestruct))
	require.NoError(t, destroyer.AddBalance(dead, balance, tracing.BalanceIncreaseSelfdestruct))
	destroyed, err := destroyer.Selfdestruct(addr, true)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.NoError(t, destroyer.FinalizeTx(amsterdamRules, NewNoopWriter()))
	destroyWrites := finalizedWrites(t, destroyer, amsterdamRules)
	t.Cleanup(destroyWrites.ReleaseAndReset)
	vm.FlushVersionedWrites(destroyWrites, true)

	recreator := newAmsterdamIBS(reader, vm, 1)
	t.Cleanup(recreator.Close)
	_, err = recreator.GetCodeHash(addr)
	require.NoError(t, err)
	_, err = recreator.GetNonce(addr)
	require.NoError(t, err)
	require.NoError(t, recreator.CreateAccount(addr, true))
	require.NoError(t, recreator.SetNonce(addr, 1, tracing.NonceChangeNewContract))
	require.NoError(t, recreator.SetCode(addr, code, tracing.CodeChangeContractCreation))
	require.NoError(t, recreator.FinalizeTx(amsterdamRules, NewNoopWriter()))
	recreateWrites := finalizedWrites(t, recreator, amsterdamRules)
	t.Cleanup(recreateWrites.ReleaseAndReset)

	io := NewVersionedIO(3)
	destroyer.MergeTxIOInto(io, destroyWrites)
	recreator.MergeTxIOInto(io, recreateWrites)
	changes := io.AsBlockAccessList()
	for i := range changes {
		if changes[i].Address == addr.Value() {
			require.NotEmpty(t, changes[i].NonceChanges)
			require.NotEmpty(t, changes[i].CodeChanges)
		}
	}
	return destroyWrites, recreateWrites, changes
}

func validateReads(vm *VersionMap, ibs *IntraBlockState, txIdx int) VersionValidity {
	io := NewVersionedIO(txIdx + 1)
	io.RecordReads(Version{TxIndex: txIdx}, ibs.VersionedReads())
	return vm.ValidateVersion(txIdx, io, validateEqualVersion, true, "")
}

// A later tx's CREATE2 at a destroyed address is visible to a concurrent
// reader only through the block access list's nonce and code cells. The
// account-level verdict must agree with those field reads: the account exists
// with the re-created code, before and after the record is loaded for a call.
func TestEIP8246_RecreationSeededByFieldCellsReadsAsAlive(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246AA"))
	code := []byte{0x60, 0x01}
	codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Balance = *uint256.NewInt(1)
	destroyWrites, recreateWrites, changes := destroyThenRecreate(t, reader, addr, code)

	vm := NewVersionMap(changes)
	vm.FlushVersionedWrites(destroyWrites, true)
	ibs := newAmsterdamIBS(reader, vm, 2)
	defer ibs.Close()

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.False(t, empty)
	hash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, codeHash, hash)
	nonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)

	resolved, err := ibs.ResolveCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, resolved.Bytes)
	require.NoError(t, ibs.TouchAccount(addr))

	vm.FlushVersionedWrites(recreateWrites, true)
	rerun := newAmsterdamIBS(reader, vm, 2)
	defer rerun.Close()
	exists, err = rerun.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	nonce, err = rerun.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)
	require.Equal(t, VersionValid, validateReads(vm, rerun, 2))
}

// Once a tx has concluded that a destroyed address is absent, a re-creation
// flushed afterwards by a lower tx must not be adopted mid-execution through
// the committed record: the reads keep the absence and fail validation, so the
// tx re-executes with a consistent view.
func TestConsumedAbsenceOutlivesRecreationFlush(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246AB"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Balance = *uint256.NewInt(1)
	destroyWrites, recreateWrites, _ := destroyThenRecreate(t, reader, addr, []byte{0x60, 0x01})

	vm := NewVersionMap(nil)
	vm.FlushVersionedWrites(destroyWrites, true)
	ibs := newAmsterdamIBS(reader, vm, 2)
	defer ibs.Close()

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)

	vm.FlushVersionedWrites(recreateWrites, true)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists, "the record fallback must not revive an address this tx already read as absent")
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.Nil(t, so)
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 2))
}

// TestEIP8246PreserveCommitmentMatchesCodelessEOA pins the commitment projection of an
// EIP-8246 balance-preserving self-destruct of a PRE-EXISTING contract: at end-of-tx the
// account resolves to a codeless EOA {balance, nonce 0, EmptyCodeHash}, the same thing the
// domain apply writes (NewAccount+balance). The why-free projection reads raw cells over
// base, so Finalize must stamp the cleared nonce+codeHash, else it inherits the base
// contract's and the commitment leaf diverges from the domain (wrong state root).
func TestEIP8246PreserveCommitmentMatchesCodelessEOA(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246c0de"))
	base := accounts.NewAccount()
	base.Nonce = 5
	base.Balance = *uint256.NewInt(100)
	base.CodeHash = accounts.NewCode([]byte{0x60, 0x00, 0x60, 0x00, 0xf3}).Hash
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{addr: &base}}

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)

	destroyed, err := ibs.Selfdestruct(addr, true) // preserveBalance = EIP-8246 no-burn
	require.NoError(t, err)
	require.True(t, destroyed)
	require.NoError(t, ibs.FinalizeTx(amsterdamRules, NewNoopWriter()))
	writes := finalizedWrites(t, ibs, amsterdamRules)
	vm.FlushVersionedWrites(writes, true)

	acc, ok := vm.View(1, reader).GetAccount(addr)
	require.True(t, ok, "a balance-preserving destruct survives as an EOA")
	require.Equal(t, *uint256.NewInt(100), acc.Balance, "balance is preserved")
	require.Equal(t, uint64(0), acc.Nonce, "the codeless EOA's nonce is 0, not the base contract's")
	require.True(t, acc.CodeHash.IsEmpty() || acc.CodeHash == accounts.EmptyCodeHash, "the codeless EOA has no code, not the base contract's")
}

// TestEIP8246PreserveRevertRestoresContract pins that reverting the preserve destruct
// leaves the pre-existing contract intact (the destruct's cell changes are undone), so
// the commitment projects the original contract, not a cleared codeless EOA.
func TestEIP8246PreserveRevertRestoresContract(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246res"))
	codeHash := accounts.NewCode([]byte{0x60, 0x00, 0x60, 0x00, 0xf3}).Hash
	base := accounts.NewAccount()
	base.Nonce = 5
	base.Balance = *uint256.NewInt(100)
	base.CodeHash = codeHash
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{addr: &base}}

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	destroyed, err := ibs.Selfdestruct(addr, true)
	require.NoError(t, err)
	require.True(t, destroyed)
	ibs.RevertToSnapshot(snap, nil)

	require.NoError(t, ibs.FinalizeTx(amsterdamRules, NewNoopWriter()))
	writes := finalizedWrites(t, ibs, amsterdamRules)
	vm.FlushVersionedWrites(writes, true)
	acc, ok := vm.View(1, reader).GetAccount(addr)
	require.True(t, ok)
	require.Equal(t, uint64(5), acc.Nonce, "revert leaves the original contract nonce")
	require.Equal(t, codeHash, acc.CodeHash, "revert leaves the original contract code")
}
