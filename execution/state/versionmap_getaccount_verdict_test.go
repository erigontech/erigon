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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestGetAccountVerdict_SelfDestructAndEmpty pins write-side completeness: once a tx's
// FinalizedWrites are flushed to the version map, the view's GetAccount returns the
// correct present/absent verdict for every self-destruct and EIP-161 shape with no
// commitment-side re-derivation — the version map is the complete commitment ledger.
// Each case flushes at TxIndex 0, so the block-end projection is
// View(1, base).GetAccount(addr) (txNum = baseTxNum(0) + 0 + 1).
func TestGetAccountVerdict_SelfDestructAndEmpty(t *testing.T) {
	t.Parallel()

	sd := &chain.Rules{IsSpuriousDragon: true}

	t.Run("pre-existing contract self-destruct projects absent", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0x5de570"))
		reader := newAccountStateReader(addr)
		reader.accounts[addr].Nonce = 1
		reader.accounts[addr].CodeHash = accounts.InternCodeHash(common.HexToHash("0xc0de"))

		vm := NewVersionMap(nil)
		ibs := newAmsterdamIBS(reader, vm, 0)
		t.Cleanup(ibs.Close)

		destroyed, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true)

		_, ok, _ := vm.View(1, reader).GetAccount(addr)
		require.False(t, ok, "a destroyed pre-existing contract must project as absent")
	})

	t.Run("create and self-destruct same tx projects absent", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0xc5de"))
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}

		vm := NewVersionMap(nil)
		ibs := newAmsterdamIBS(reader, vm, 0)
		t.Cleanup(ibs.Close)

		require.NoError(t, ibs.CreateAccount(addr, true))
		require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeNewContract))
		destroyed, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true)

		_, ok, _ := vm.View(1, reader).GetAccount(addr)
		require.False(t, ok, "an account created and destroyed in one tx must project as absent")
	})

	t.Run("self-destruct then recreate in a later tx projects live", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0x5de5c0de"))
		code := []byte{0x60, 0x01}
		reader := newAccountStateReader(addr)
		reader.accounts[addr].Nonce = 1

		vm := NewVersionMap(nil)

		destroyer := newAmsterdamIBS(reader, vm, 0)
		t.Cleanup(destroyer.Close)
		destroyed, err := destroyer.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, destroyed)
		require.NoError(t, destroyer.FinalizeTx(sd, NewNoopWriter()))
		vm.FlushVersionedWrites(finalizedWrites(t, destroyer, sd), true)

		recreator := newAmsterdamIBS(reader, vm, 1)
		t.Cleanup(recreator.Close)
		require.NoError(t, recreator.CreateAccount(addr, true))
		require.NoError(t, recreator.SetNonce(addr, 1, tracing.NonceChangeNewContract))
		require.NoError(t, recreator.SetCode(addr, code, tracing.CodeChangeContractCreation))
		require.NoError(t, recreator.FinalizeTx(sd, NewNoopWriter()))
		vm.FlushVersionedWrites(finalizedWrites(t, recreator, sd), true)

		acc, ok, _ := vm.View(2, reader).GetAccount(addr)
		require.True(t, ok, "a recreated account must project as live after the recreate tx")
		require.Equal(t, crypto.Keccak256Hash(code), acc.CodeHash.Value(),
			"the recreated account carries the new code hash")
	})

	// The recreating tx runs before the lower tx's removal of the touched account is flushed,
	// and creates the account from an absent base as an EIP-7702 authority.
	t.Run("recreate run before a lower tx's EIP-161 removal projects live", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0x7004c4"))
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}

		vm := NewVersionMap(nil)

		recreator := newAmsterdamIBS(reader, vm, 1)
		t.Cleanup(recreator.Close)
		require.NoError(t, recreator.SetNonce(addr, 1, tracing.NonceChangeAuthorization))
		require.NoError(t, recreator.FinalizeTx(sd, NewNoopWriter()))
		vm.FlushVersionedWrites(finalizedWrites(t, recreator, sd), true)

		toucher := newAmsterdamIBS(reader, vm, 0)
		t.Cleanup(toucher.Close)
		require.NoError(t, toucher.AddBalance(addr, uint256.Int{}, tracing.BalanceChangeUnspecified))
		require.NoError(t, toucher.FinalizeTx(sd, NewNoopWriter()))
		vm.FlushVersionedWrites(finalizedWrites(t, toucher, sd), true)

		acc, ok, _ := vm.View(2, reader).GetAccount(addr)
		require.True(t, ok, "the recreated account must project as live")
		require.Equal(t, uint64(1), acc.Nonce)
	})

	t.Run("funded account survives as live", func(t *testing.T) {
		addr := accounts.InternAddress(common.HexToAddress("0xa11e"))
		reader := newAccountStateReader(addr) // balance 100

		vm := NewVersionMap(nil)
		ibs := newAmsterdamIBS(reader, vm, 0)
		t.Cleanup(ibs.Close)

		require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(50), tracing.BalanceChangeUnspecified))
		require.NoError(t, ibs.FinalizeTx(sd, NewNoopWriter()))
		writes := finalizedWrites(t, ibs, sd)
		vm.FlushVersionedWrites(writes, true)

		acc, ok, _ := vm.View(1, reader).GetAccount(addr)
		require.True(t, ok, "a funded account must project as live")
		require.True(t, acc.Balance.Eq(uint256.NewInt(150)), "live account carries its final balance")
	})
}

// TestVersionMapView_GetAccountCrossBlockExistence pins the batch-fold existence verdict:
// the touched union carries accounts whose writes are in an earlier batch block, so they
// have no cell in the current block's version map and are resolved from the base. An
// account the base reports absent (removed in an earlier block) must project as absent —
// not as a resurrected empty leaf — while one the base still carries projects as present.
func TestVersionMapView_GetAccountCrossBlockExistence(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xc0b10c"))

	t.Run("no cell and absent base projects absent", func(t *testing.T) {
		vm := NewVersionMap(nil)
		base := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
		_, ok, _ := vm.View(1, base).GetAccount(addr)
		require.False(t, ok, "no version-map cell and no base account must project as absent")
	})

	t.Run("no cell and present base projects present", func(t *testing.T) {
		vm := NewVersionMap(nil)
		base := newAccountStateReader(addr) // balance 100
		acc, ok, _ := vm.View(1, base).GetAccount(addr)
		require.True(t, ok, "an account the base still carries must project as present")
		require.True(t, acc.Balance.Eq(uint256.NewInt(100)), "present account takes its value from the base")
	})
}

type getAccountBase struct {
	minimalStateReader
	acc   *accounts.Account
	err   error
	reads int
}

func (r *getAccountBase) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	r.reads++
	return r.acc, r.err
}

// TestVersionMapView_GetAccountTable pins the composed account, the presence and
// destroyed verdicts, the error and the number of base reads for each cell shape the
// commitment fold meets. The view reads at txIdx 2, so cells at TxIndex 0 and 1 are visible.
func TestVersionMapView_GetAccountTable(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x9e7acc"))
	slot := accounts.InternKey(common.HexToHash("0x01"))
	cellCode := accounts.InternCodeHash(common.HexToHash("0xc0de"))
	readErr := errors.New("base read failed")
	v := func(i int) Version { return Version{TxIndex: i} }
	with := func(a accounts.Account, f func(*accounts.Account)) accounts.Account { f(&a); return a }
	bal := func(n uint64) func(*accounts.Account) {
		return func(a *accounts.Account) { a.Balance = *uint256.NewInt(n) }
	}

	fresh := accounts.NewAccount()
	committed := with(fresh, func(a *accounts.Account) {
		a.Balance, a.Nonce, a.Incarnation = *uint256.NewInt(100), 4, 2
		a.CodeHash = accounts.InternCodeHash(common.HexToHash("0xba5e"))
	})
	marker := with(fresh, func(a *accounts.Account) { a.Nonce = 77 })
	addrAcc := with(fresh, func(a *accounts.Account) { a.Balance, a.Nonce, a.Incarnation = *uint256.NewInt(7), 1, 1 })
	originAcc := with(fresh, func(a *accounts.Account) { a.Balance, a.Nonce = *uint256.NewInt(8), 2 })
	removal := func(complete bool) func(vm *VersionMap) {
		return func(vm *VersionMap) {
			ws := &WriteSet{}
			ws.SetEmptyRemoval(addr, v(0))
			vm.FlushVersionedWrites(ws, complete)
		}
	}

	cases := []struct {
		name       string
		setup      func(vm *VersionMap)
		base       *accounts.Account
		noBase     bool
		baseErr    error
		err        error
		acc        accounts.Account
		ok         bool
		wasDeleted bool
		baseReads  int
	}{
		{name: "no entry, base present", base: &committed, acc: committed, ok: true, baseReads: 1},
		{name: "no entry, base absent", baseReads: 1},
		{name: "no entry, no base reader", noBase: true},
		{
			name: "storage cell only, base present", setup: func(vm *VersionMap) { vm.WriteStorage(addr, slot, v(0), *uint256.NewInt(7), true) },
			base: &committed, acc: committed, ok: true, baseReads: 1,
		},
		{
			name: "storage cell only, base absent", setup: func(vm *VersionMap) { vm.WriteStorage(addr, slot, v(0), *uint256.NewInt(7), true) },
			baseReads: 1,
		},
		{
			name: "balance cell over base", setup: func(vm *VersionMap) { vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true) },
			base: &committed, acc: with(committed, bal(50)), ok: true, baseReads: 1,
		},
		{name: "every field cell over base", setup: func(vm *VersionMap) {
			vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true)
			vm.WriteNonce(addr, v(1), 9, true)
			vm.WriteIncarnation(addr, v(0), 3, true)
			vm.WriteCodeHash(addr, v(1), cellCode, true)
		}, base: &committed, acc: with(committed, func(a *accounts.Account) {
			a.Balance, a.Nonce, a.Incarnation, a.CodeHash = *uint256.NewInt(50), 9, 3, cellCode
		}), ok: true, baseReads: 1},
		{
			name: "balance cell only, base absent", setup: func(vm *VersionMap) { vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true) },
			acc: with(fresh, bal(50)), ok: true, baseReads: 1,
		},
		{
			name: "nonce cell only, base absent", setup: func(vm *VersionMap) { vm.WriteNonce(addr, v(0), 9, true) },
			acc: with(fresh, func(a *accounts.Account) { a.Nonce = 9 }), ok: true, baseReads: 1,
		},
		{
			name: "incarnation cell only, base absent", setup: func(vm *VersionMap) { vm.WriteIncarnation(addr, v(0), 3, true) },
			acc: with(fresh, func(a *accounts.Account) { a.Incarnation = 3 }), ok: true, baseReads: 1,
		},
		{
			name: "code-hash cell only, base absent", setup: func(vm *VersionMap) { vm.WriteCodeHash(addr, v(0), cellCode, true) },
			acc: with(fresh, func(a *accounts.Account) { a.CodeHash = cellCode }), ok: true, baseReads: 1,
		},
		{
			name: "estimate cell counts", setup: func(vm *VersionMap) { vm.WriteNonce(addr, v(0), 9, false) },
			acc: with(fresh, func(a *accounts.Account) { a.Nonce = 9 }), ok: true, baseReads: 1,
		},
		{
			name: "cell at the view's txIdx is not visible", setup: func(vm *VersionMap) { vm.WriteBalance(addr, v(2), *uint256.NewInt(50), true) },
			base: &committed, acc: committed, ok: true, baseReads: 1,
		},
		{
			name: "balance cell, no base reader", setup: func(vm *VersionMap) { vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true) },
			noBase: true, acc: with(fresh, bal(50)), ok: true,
		},
		{name: "EIP-161 removal", setup: removal(true), base: &committed, wasDeleted: true},
		{name: "EIP-161 removal as estimate", setup: removal(false), base: &committed},
		{name: "revived above an EIP-161 removal", setup: func(vm *VersionMap) {
			removal(true)(vm)
			vm.WriteBalance(addr, v(1), *uint256.NewInt(5), true)
		}, base: &committed, acc: with(committed, bal(5)), ok: true, wasDeleted: true, baseReads: 1},
		{name: "recreated above an EIP-161 removal", setup: func(vm *VersionMap) {
			removal(true)(vm)
			vm.WriteSelfDestruct(addr, v(1), false, true)
			vm.WriteAddress(addr, v(1), &fresh, true)
			vm.WriteBalance(addr, v(1), *uint256.NewInt(5), true)
			vm.WriteNonce(addr, v(1), 1, true)
		}, base: &committed, acc: with(fresh, func(a *accounts.Account) { a.Balance, a.Nonce = *uint256.NewInt(5), 1 }), ok: true, wasDeleted: true},
		{name: "EIP-161 removal at the view's txIdx is not visible", setup: func(vm *VersionMap) {
			ws := &WriteSet{}
			ws.SetEmptyRemoval(addr, v(2))
			vm.FlushVersionedWrites(ws, true)
		}, base: &committed, acc: committed, ok: true, baseReads: 1},
		{
			name: "self-destruct cell only", setup: func(vm *VersionMap) { vm.WriteSelfDestruct(addr, v(0), true, true) },
			base: &committed, acc: committed, ok: true, wasDeleted: true, baseReads: 1,
		},
		{name: "EIP-8246 destruct keeps raw floors below it", setup: func(vm *VersionMap) {
			vm.WriteNonce(addr, v(0), 9, true)
			vm.WriteCodeHash(addr, v(0), cellCode, true)
			vm.WriteSelfDestruct(addr, v(1), true, true)
			vm.WriteBalance(addr, v(1), *uint256.NewInt(3), true)
		}, base: &committed, acc: with(committed, func(a *accounts.Account) {
			a.Balance, a.Nonce, a.CodeHash = *uint256.NewInt(3), 9, cellCode
		}), ok: true, wasDeleted: true, baseReads: 1},
		{name: "BAL marker over base", setup: func(vm *VersionMap) {
			vm.WriteAddressExists(addr, v(0), &marker)
			vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true)
		}, base: &committed, acc: with(committed, bal(50)), ok: true, baseReads: 1},
		{name: "BAL marker, base absent", setup: func(vm *VersionMap) {
			vm.WriteAddressExists(addr, v(0), &marker)
			vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true)
		}, acc: with(fresh, bal(50)), ok: true, baseReads: 1},
		{
			name: "BAL marker alone, base absent", setup: func(vm *VersionMap) { vm.WriteAddressExists(addr, v(0), &marker) },
			baseReads: 1,
		},
		{name: "BAL marker hides the origin", setup: func(vm *VersionMap) {
			vm.StoreOriginAccount(addr, &originAcc)
			vm.WriteAddressExists(addr, v(0), &marker)
			vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true)
		}, base: &committed, acc: with(committed, bal(50)), ok: true, baseReads: 1},
		{name: "address cell is the base", setup: func(vm *VersionMap) {
			vm.WriteAddress(addr, v(0), &addrAcc, true)
			vm.WriteBalance(addr, v(1), *uint256.NewInt(50), true)
		}, base: &committed, acc: with(addrAcc, bal(50)), ok: true},
		{
			name: "nil address cell falls back to base", setup: func(vm *VersionMap) { vm.WriteAddress(addr, v(0), nil, true) },
			base: &committed, acc: committed, ok: true, baseReads: 1,
		},
		{
			name: "origin is the base", setup: func(vm *VersionMap) { vm.StoreOriginAccount(addr, &originAcc) },
			base: &committed, acc: originAcc, ok: true,
		},
		{
			name: "absent origin falls back to base", setup: func(vm *VersionMap) { vm.StoreOriginAccount(addr, nil) },
			base: &committed, acc: committed, ok: true, baseReads: 1,
		},
		{name: "address cell wins over origin", setup: func(vm *VersionMap) {
			vm.StoreOriginAccount(addr, &originAcc)
			vm.WriteAddress(addr, v(0), &addrAcc, true)
		}, base: &committed, acc: addrAcc, ok: true},
		{name: "base read error", baseErr: readErr, err: readErr, baseReads: 1},
		{
			name: "base read error under a balance cell", setup: func(vm *VersionMap) { vm.WriteBalance(addr, v(0), *uint256.NewInt(50), true) },
			baseErr: readErr, err: readErr, baseReads: 1,
		},
		{name: "EIP-161 removal skips the failing base", setup: removal(true), baseErr: readErr, wasDeleted: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vm := NewVersionMap(nil)
			if tc.setup != nil {
				tc.setup(vm)
			}
			reader := &getAccountBase{acc: tc.base, err: tc.baseErr}
			var base StateReader = reader
			if tc.noBase {
				base = nil
			}
			view := vm.View(2, base)

			acc, ok, wasDeleted := view.GetAccount(addr)
			require.Equal(t, tc.acc, acc, "account")
			require.Equal(t, tc.ok, ok, "present")
			require.Equal(t, tc.wasDeleted, wasDeleted, "wasDeleted")
			require.Equal(t, tc.baseReads, reader.reads, "base reads")
			require.Equal(t, tc.err, view.Err(), "error")
		})
	}
}
