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
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestGetAccountVerdict_SelfDestructAndEmpty asks the pivotal write-side-completeness
// question: once a tx's FinalizedWrites are flushed to the version map, does the
// GetAccount projection return the correct present/absent verdict for every
// self-destruct and EIP-161 shape — with NO commitment-side re-derivation? If yes,
// the version map is a complete ledger and the calculator can drop calcState +
// ApplyEIP161Removal in favour of GetAccount. Each case flushes at TxIndex 0, so the
// block-end projection is GetAccount(addr, 1, base) (txNum = baseTxNum(0) + 0 + 1).
func TestGetAccountVerdict_SelfDestructAndEmpty(t *testing.T) {
	t.Parallel()

	sd := &chain.Rules{IsSpuriousDragon: true}

	t.Run("pre-existing contract self-destruct projects absent", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0x5de570"))
		reader := newAccountStateReader(addr)
		reader.accounts[addr].Nonce = 1
		reader.accounts[addr].CodeHash = accounts.InternCodeHash(common.HexToHash("0xc0de"))

		vm := NewVersionMap(nil)
		ibs := NewWithVersionMap(reader, vm)
		t.Cleanup(ibs.Close)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)

		destroyed, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true, "")

		_, ok := vm.GetAccount(addr, 1, reader)
		require.False(t, ok, "a destroyed pre-existing contract must project as absent")
	})

	t.Run("create and self-destruct same tx projects absent", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0xc5de"))
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}

		vm := NewVersionMap(nil)
		ibs := NewWithVersionMap(reader, vm)
		t.Cleanup(ibs.Close)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)

		require.NoError(t, ibs.CreateAccount(addr, true))
		require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeNewContract))
		destroyed, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true, "")

		_, ok := vm.GetAccount(addr, 1, reader)
		require.False(t, ok, "an account created and destroyed in one tx must project as absent")
	})

	t.Run("pre-existing account left EIP-161 empty projects absent", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0xe3917"))
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
		empty := accounts.NewAccount() // balance 0, nonce 0, EmptyCodeHash — a pre-existing empty EOA
		reader.accounts[addr] = &empty

		vm := NewVersionMap(nil)
		ibs := NewWithVersionMap(reader, vm)
		t.Cleanup(ibs.Close)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)

		// Touch it with a zero-value add — stays empty, so EIP-161 removes it.
		require.NoError(t, ibs.AddBalance(addr, uint256.Int{}, tracing.BalanceChangeUnspecified))
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true, "")

		_, ok := vm.GetAccount(addr, 1, reader)
		require.False(t, ok, "a pre-existing account left empty must project as absent (EIP-161)")
	})

	t.Run("self-destruct then recreate in a later tx projects live", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0x5de5c0de"))
		code := []byte{0x60, 0x01}
		reader := newAccountStateReader(addr)
		reader.accounts[addr].Nonce = 1

		vm := NewVersionMap(nil)

		destroyer := NewWithVersionMap(reader, vm)
		t.Cleanup(destroyer.Close)
		destroyer.SetTxContext(1, 0)
		destroyer.SetVersion(0)
		destroyed, err := destroyer.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
		require.NoError(t, destroyer.FinalizeTx(sd, NewNoopWriter()))
		vm.FlushVersionedWrites(finalizedWrites(t, destroyer, sd), true, "")

		recreator := NewWithVersionMap(reader, vm)
		t.Cleanup(recreator.Close)
		recreator.SetTxContext(1, 1)
		recreator.SetVersion(0)
		require.NoError(t, recreator.CreateAccount(addr, true))
		require.NoError(t, recreator.SetNonce(addr, 1, tracing.NonceChangeNewContract))
		require.NoError(t, recreator.SetCode(addr, code, tracing.CodeChangeContractCreation))
		require.NoError(t, recreator.FinalizeTx(sd, NewNoopWriter()))
		vm.FlushVersionedWrites(finalizedWrites(t, recreator, sd), true, "")

		acc, ok := vm.GetAccount(addr, 2, reader)
		require.True(t, ok, "a recreated account must project as live after the recreate tx")
		require.Equal(t, crypto.Keccak256Hash(code), acc.CodeHash.Value(),
			"the recreated account carries the new code hash")
	})

	t.Run("funded account survives as live", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0xa11e"))
		reader := newAccountStateReader(addr) // balance 100

		vm := NewVersionMap(nil)
		ibs := NewWithVersionMap(reader, vm)
		t.Cleanup(ibs.Close)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)

		require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(50), tracing.BalanceChangeUnspecified))
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true, "")

		acc, ok := vm.GetAccount(addr, 1, reader)
		require.True(t, ok, "a funded account must project as live")
		require.True(t, acc.Balance.Eq(uint256.NewInt(150)), "live account carries its final balance")
	})
}
