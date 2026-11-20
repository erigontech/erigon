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

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestLiveReadMatchesInterpreter pins that the live IBS read and the version-map
// interpreter (ApplyView.ReadAccountData) agree on account existence and fields across
// the self-destruct, preserve, revival and recreate shapes.
func TestLiveReadMatchesInterpreter(t *testing.T) {
	t.Parallel()

	const txIdx = 4
	code := accounts.NewCode([]byte{0x60, 0x00}).Hash

	cases := []struct {
		name  string
		base  func() *accountStateReader
		setup func(vm *VersionMap, addr accounts.Address)
	}{
		{"in-block create then destruct, balance kept", func() *accountStateReader { return newAccountStateReader() }, func(vm *VersionMap, addr accounts.Address) {
			sd := Version{TxIndex: 0}
			vm.WriteSelfDestruct(addr, sd, true, true)
			vm.WriteBalance(addr, sd, *uint256.NewInt(5), true)
			vm.WriteIncarnation(addr, sd, 1, true)
		}},
		{"preserve then later balance credit", func() *accountStateReader { return newAccountStateReader() }, func(vm *VersionMap, addr accounts.Address) {
			sd := Version{TxIndex: 0}
			vm.WriteSelfDestruct(addr, sd, true, true)
			vm.WriteBalance(addr, sd, *uint256.NewInt(5), true)
			vm.WriteIncarnation(addr, sd, 1, true)
			vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(9), true)
		}},
		{"burn (balance ValueDeleted)", func() *accountStateReader { return newAccountStateReader() }, func(vm *VersionMap, addr accounts.Address) {
			ws := &WriteSet{}
			ws.SetContractRemoval(addr, Version{TxIndex: 0})
			vm.FlushVersionedWrites(ws, true)
		}},
		{"destruct then recreate", func() *accountStateReader { return newAccountStateReader() }, func(vm *VersionMap, addr accounts.Address) {
			vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
			// A real recreation writes the AddressPath record and the field cells the
			// per-field live readers consume.
			fresh := accounts.NewAccount()
			fresh.Nonce, fresh.Balance, fresh.Incarnation = 1, *uint256.NewInt(7), 2
			vm.WriteAddress(addr, Version{TxIndex: 1}, &fresh, true)
			vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(7), true)
			vm.WriteNonce(addr, Version{TxIndex: 1}, 1, true)
			vm.WriteIncarnation(addr, Version{TxIndex: 1}, 2, true)
			writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 1}, code, true)
		}},
		{"recreate seen only through field cells above a burn", func() *accountStateReader { return newAccountStateReader() }, func(vm *VersionMap, addr accounts.Address) {
			ws := &WriteSet{}
			ws.SetContractRemoval(addr, Version{TxIndex: 0})
			vm.FlushVersionedWrites(ws, true)
			vm.WriteNonce(addr, Version{TxIndex: 1}, 1, true)
			writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 1}, code, true)
		}},
		{"self-destruct over a balance credited below it", func() *accountStateReader { return newAccountStateReader() }, func(vm *VersionMap, addr accounts.Address) {
			vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(5), true)
			vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
		}},
		{"nonce-only revival above a balance credited below the destruct", func() *accountStateReader {
			r := newAccountStateReader()
			a := accounts.NewAccount()
			a.Nonce, a.Balance = 3, *uint256.NewInt(100)
			r.accounts[getAddress(1)] = &a
			return r
		}, func(vm *VersionMap, addr accounts.Address) {
			vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(5), true)
			vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
			vm.WriteNonce(addr, Version{TxIndex: 2}, 1, true)
		}},
		{"bare self-destruct over a committed account", func() *accountStateReader {
			r := newAccountStateReader()
			a := accounts.NewAccount()
			a.Nonce, a.Balance, a.CodeHash = 3, *uint256.NewInt(100), code
			r.accounts[getAddress(1)] = &a
			return r
		}, func(vm *VersionMap, addr accounts.Address) {
			vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
		}},
		{"live account, cells over base", func() *accountStateReader {
			r := newAccountStateReader()
			a := accounts.NewAccount()
			a.Nonce, a.Balance, a.CodeHash = 3, *uint256.NewInt(100), code
			r.accounts[getAddress(1)] = &a
			return r
		}, func(vm *VersionMap, addr accounts.Address) {
			writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 1}, *uint256.NewInt(250), true)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr := getAddress(1)

			vmLive, vmView := NewVersionMap(nil), NewVersionMap(nil)
			tc.setup(vmLive, addr)
			tc.setup(vmView, addr)

			ibs := New(tc.base())
			defer ibs.Close()
			ibs.SetTxContext(0, txIdx)
			ibs.SetVersion(0)
			ibs.SetVersionMap(vmLive)

			liveExists, err := ibs.Exist(addr)
			require.NoError(t, err)
			liveBal, err := ibs.GetBalance(addr)
			require.NoError(t, err)
			liveNonce, err := ibs.GetNonce(addr)
			require.NoError(t, err)
			liveEmptyCode, err := ibs.GetCodeHash(addr)
			require.NoError(t, err)

			got, err := vmView.ApplyView(txIdx, tc.base()).ReadAccountData(addr)
			require.NoError(t, err)

			require.Equal(t, liveExists, got != nil, "existence")
			if !liveExists {
				return
			}
			require.Equal(t, liveBal, got.Balance, "balance")
			require.Equal(t, liveNonce, got.Nonce, "nonce")
			// EmptyCodeHash vs zero CodeHash is the benign representation the commitment
			// boundary folds; compare via IsEmpty when both are empty.
			if liveEmptyCode.IsEmpty() && got.CodeHash.IsEmpty() {
				return
			}
			require.Equal(t, liveEmptyCode, got.CodeHash, "codehash")
		})
	}
}
