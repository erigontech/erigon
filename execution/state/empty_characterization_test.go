// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Characterization of Empty() over the parallel (versionMap) path, pinning the
// EIP-161 emptiness verdict across the cases that make Empty subtle — before
// rationalizing the whole-account refresh it drives. Empty keys off AddressPath
// existence and the {Balance,Nonce,CodeHash} fields; it never consults
// Incarnation, and (EIP-6780) a same-tx self-destruct leaves the account alive.
func emptyCharIBS(reader *accountStateReader, vm *VersionMap, txIndex int) *IntraBlockState {
	ibs := New(NewVersionedStateReader(txIndex, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, txIndex)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	return ibs
}

func TestEmpty_Characterization(t *testing.T) {
	t.Parallel()

	t.Run("warm non-empty (balance>0)", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(1)
		reader := newAccountStateReader(addr) // balance 100
		empty, err := emptyCharIBS(reader, NewVersionMap(nil), 3).Empty(addr)
		require.NoError(t, err)
		require.False(t, empty)
	})

	t.Run("warm empty (all zero)", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(2)
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
		zero := accounts.NewAccount() // balance 0, nonce 0, empty code hash
		reader.accounts[addr] = &zero
		empty, err := emptyCharIBS(reader, NewVersionMap(nil), 3).Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
	})

	t.Run("absent account is empty and records a nil AddressPath read", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(3)
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
		ibs := emptyCharIBS(reader, NewVersionMap(nil), 3)
		empty, err := ibs.Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
		tr, ok := ibs.versionedReads.GetAddress(addr)
		require.True(t, ok, "absent Empty must record an AddressPath read for OCC create/absent detection")
		require.Nil(t, tr.Val, "the recorded AddressPath read must carry a nil account")
	})

	t.Run("cross-tx self-destruct (no revival) is empty", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(4)
		reader := newAccountStateReader(addr) // balance 100 in DB
		vm := NewVersionMap(nil)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
		empty, err := emptyCharIBS(reader, vm, 3).Empty(addr)
		require.NoError(t, err)
		require.True(t, empty, "a prior-tx self-destruct with no revival reads as empty")
	})

	t.Run("field write without AddressPath zeroes balance -> empty", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(5)
		reader := newAccountStateReader(addr) // balance 100 in DB
		vm := NewVersionMap(nil)
		// A prior tx wrote only BalancePath=0 (no AddressPath). Empty overlays
		// the field on the base record: balance 0, nonce 0, empty code -> empty.
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 1}, uint256.Int{}, true)
		empty, err := emptyCharIBS(reader, vm, 3).Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
	})
}

func TestEmptyOnAbsentAccountOnlyRecordsTheRead(t *testing.T) {
	t.Parallel()

	addr := getAddress(90)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	ibs := emptyCharIBS(reader, NewVersionMap(nil), 3)

	for range 3 {
		empty, err := ibs.Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
	}

	require.Empty(t, ibs.journal.entries, "a read must not grow the journal")
	require.True(t, ibs.warmReadable(addr), "a read must not disable the read-once path")
	read, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok, "the absence must stay in the read set for validation")
	require.Nil(t, read.Val)
}

func TestEmpty_WarmReadsAnswerWithoutFullRead(t *testing.T) {
	t.Parallel()

	code := accounts.InternCodeHash([32]byte{0xaa})
	cases := []struct {
		name  string
		set   func(a *accounts.Account)
		empty bool
	}{
		{"balance", func(a *accounts.Account) { a.Balance = *uint256.NewInt(100) }, false},
		{"nonce", func(a *accounts.Account) { a.Nonce = 1 }, false},
		{"code", func(a *accounts.Account) { a.CodeHash = code }, false},
		{"truly empty", func(a *accounts.Account) {}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			addr := getAddress(40)
			account := accounts.NewAccount()
			c.set(&account)
			reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{addr: &account}}
			ibs := emptyCharIBS(reader, NewVersionMap(nil), 3)

			_, ok := ibs.emptyFromWarmReads(addr)
			require.False(t, ok, "a first read has nothing recorded to answer from")

			first, err := ibs.Empty(addr)
			require.NoError(t, err)
			require.Equal(t, c.empty, first)

			warm, ok := ibs.emptyFromWarmReads(addr)
			require.True(t, ok)
			require.Equal(t, c.empty, warm)
		})
	}
}

func TestEmpty_WarmReadsKeepConflictTracking(t *testing.T) {
	t.Parallel()

	code := accounts.InternCodeHash([32]byte{0xaa})
	cases := []struct {
		name  string
		set   func(a *accounts.Account)
		path  AccountPath
		value any
	}{
		{"balance", func(a *accounts.Account) { a.Balance = *uint256.NewInt(100) }, BalancePath, *uint256.NewInt(7)},
		{"nonce", func(a *accounts.Account) { a.Nonce = 1 }, NoncePath, uint64(2)},
		{"code hash", func(a *accounts.Account) { a.CodeHash = code }, CodeHashPath, accounts.EmptyCodeHash},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			addr := getAddress(41)
			account := accounts.NewAccount()
			c.set(&account)
			reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{addr: &account}}
			vm := NewVersionMap(nil)
			ibs := emptyCharIBS(reader, vm, 3)

			for range 2 {
				empty, err := ibs.Empty(addr)
				require.NoError(t, err)
				require.False(t, empty)
			}
			ibs.PublishOrigins()
			require.Equal(t, VersionValid, vm.ValidateReadSet(3, ibs.versionedReads, validateEqualVersion, false, ""))

			writeFor(vm, addr, c.path, accounts.NilKey, Version{TxIndex: 1}, c.value, true)
			require.Equal(t, VersionInvalid, vm.ValidateReadSet(3, ibs.versionedReads, validateEqualVersion, false, ""))
		})
	}
}

func TestEmpty_OwnWriteIsNotAnsweredFromWarmReads(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	ibs := emptyCharIBS(newAccountStateReader(addr), NewVersionMap(nil), 3) // balance 100
	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.False(t, empty)

	require.NoError(t, ibs.SubBalance(addr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))

	empty, err = ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty, "the own write spent the balance the first Empty read")
}

func TestEmpty_BalanceCellWithoutAccountStaysOnFullPath(t *testing.T) {
	t.Parallel()

	addr := getAddress(43)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 1}, *uint256.NewInt(5), true)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetTxContext(0, 3)

	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(5), balance)

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)

	_, ok := ibs.emptyFromWarmReads(addr)
	require.False(t, ok)
	empty, err = ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)
}
