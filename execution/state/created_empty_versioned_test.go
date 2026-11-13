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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSetEmptyRemoval(t *testing.T) {
	t.Parallel()
	ver := Version{TxIndex: 1}

	t.Run("removal stamps the self-destruct marker and clears the footprint", func(t *testing.T) {
		addr := accounts.InternAddress([20]byte{0xe0})
		ws := &WriteSet{}
		ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: 1})
		ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: ver}})

		ws.SetEmptyRemoval(addr, ver)

		bal, ok := ws.GetBalance(addr)
		require.True(t, ok && bal.Deletes(), "removal stamps the balance-deleted verdict")
		sd, ok := ws.GetSelfDestruct(addr)
		require.True(t, ok && sd.Val, "the removal carries the self-destruct wipe marker")
		_, ok = ws.GetAddress(addr)
		require.False(t, ok, "the whole-account AddressPath cell is cleared (no metamorphic false-revival)")
		_, ok = ws.GetNonce(addr)
		require.False(t, ok, "the footprint is cleared")
		_, ok = ws.GetCodeHash(addr)
		require.False(t, ok, "a touched-empty EOA was not a contract, so no code-deleted marker")
	})

	t.Run("contract removal stamps the code-deleted marker", func(t *testing.T) {
		addr := accounts.InternAddress([20]byte{0xc0})
		ws := &WriteSet{}
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})

		ws.SetContractRemoval(addr, ver)

		bal, ok := ws.GetBalance(addr)
		require.True(t, ok && bal.Deletes(), "removal stamps the balance-deleted verdict")
		sd, ok := ws.GetSelfDestruct(addr)
		require.True(t, ok && sd.Val, "the removal carries the self-destruct wipe marker")
		ch, ok := ws.GetCodeHash(addr)
		require.True(t, ok && ch.Deletes(), "a destroyed contract stamps the code-deleted marker (was a contract, code removed)")
	})
}

// A created-then-destructed account decides emptiness from its own writes: a failing base read proves no base read ran.
func TestEncodeExistingEmptyRemovals_CreatedSelfDestructSkipsBaseRead(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc1})
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, &failingBaseReader{err: errors.New("domain read failed")}))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ver := ibs.Version()
	writes := newWriteSet(
		&VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath, Version: ver}, Val: true},
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}},
	)

	ibs.encodeExistingEmptyRemovals(&chain.Rules{IsSpuriousDragon: true}, writes, map[accounts.Address]struct{}{addr: {}}, nil)

	require.NoError(t, ibs.StateReadError())
	ch, ok := writes.GetCodeHash(addr)
	require.True(t, ok && ch.Deletes(), "the code-deleted marker is present")
}

func TestFinalizedWritesWithholdCreatedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	vm.WriteBalance(addr, Version{TxIndex: 0}, uint256.Int{}, false)

	require.NoError(t, ibs.TouchAccount(addr))

	writes := finalizedWrites(t, ibs, &chain.Rules{IsSpuriousDragon: true})
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress)
	bal, hasBalance := writes.GetBalance(addr)
	require.True(t, hasBalance && bal.Deletes(),
		"a withheld created-empty account emits the balance-deleted removal verdict, overriding its lingering during-tx cells")

	_, hasSelfDestruct := writes.GetSelfDestruct(addr)
	require.True(t, hasSelfDestruct, "the removal carries the self-destruct marker")

	vm.FlushVersionedWrites(writes, true)
	published, err := WithoutVanished(writes, vm, 0, false, &minimalStateReader{})
	require.NoError(t, err)
	_, hasSelfDestruct = published.GetSelfDestruct(addr)
	_, hasBalance = published.GetBalance(addr)
	require.False(t, hasSelfDestruct || hasBalance, "the removal of an absent account publishes no writes")

	next := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(next.Close)
	next.SetTxContext(1, 1)
	exists, err := next.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

// An account that never existed and that each tx touches and leaves empty (a call to an absent
// precompile) must not chain the txs: a tx that touches it again while the previous toucher is
// still in flight records no dependency on it.
func TestCreatedEmptyAccountTouchedByInFlightTxIsNoDependency(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x02})
	rules := &chain.Rules{IsSpuriousDragon: true}
	vm := NewVersionMap(nil)

	first := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(first.Close)
	first.SetTxContext(1, 0)
	require.NoError(t, first.TouchAccount(addr))
	vm.FlushVersionedWrites(finalizedWrites(t, first, rules), false)

	next := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(next.Close)
	next.SetTxContext(1, 1)
	exists, err := next.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, next.TouchAccount(addr))
	require.False(t, next.HadInvalidRead(), "dependency on tx %d", next.DepTxIndex())
}

// TestFinalizedWritesEmptyRemovalClearsWholeAccountCell pins item 1's write-side
// invariant: when an EIP-161 account that existed at base is (re)created empty and
// removed, the removal must drop the whole-account AddressPath write too — not only the
// account fields. Otherwise the tx flushes both a SelfDestruct(true) and an AddressPath
// cell at the SAME tx index, which is the exact cell layout of a same-tx SD+CREATE2
// metamorphic recreate. AccountLifecycleAt then reads the removed account as Revived
// (its AddressPath-at-destroyedAt branch), undoing the removal and diverging the root.
// The fix must be write-side: the two cases are indistinguishable from cells, so
// tightening AccountLifecycleAt's >= would instead regress genuine metamorphic recreate.
func TestFinalizedWritesEmptyRemovalClearsWholeAccountCell(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe2})
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	base := accounts.NewAccount() // existing empty EOA: bal 0, nonce 0, EmptyCodeHash
	reader.accounts[addr] = &base

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.CreateAccount(addr, false))

	writes := finalizedWrites(t, ibs, &chain.Rules{IsSpuriousDragon: true})

	bal, hasBalance := writes.GetBalance(addr)
	require.True(t, hasBalance && bal.Deletes(), "empty removal must emit the balance-deleted removal verdict")
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress,
		"empty removal must not leave a whole-account AddressPath cell alongside the removal "+
			"(same layout as a metamorphic recreate -> AccountLifecycleAt would read it as Revived)")
}

func TestEmptyAccountTouchInvalidatedByFunding(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xe2})
	vm := NewVersionMap(nil)
	// Model a real create+SELFDESTRUCT: the destroying tx marks the account
	// destructed and zeroes its balance at the same version.
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
	vm.WriteBalance(addr, Version{TxIndex: 0}, uint256.Int{}, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 2)

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)
	require.NoError(t, ibs.TouchAccount(addr))

	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, ibs.VersionedReads())

	account := accounts.NewAccount()
	account.Balance = *uint256.NewInt(32649)
	vm.WriteAddress(addr, Version{TxIndex: 1}, &account, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, account.Balance, true)

	require.Equal(t, VersionInvalid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
}

func TestDestroyedAccountReadRemainsValid(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xe3})
	vm := NewVersionMap(nil)
	account := accounts.NewAccount()
	account.Balance = *uint256.NewInt(1)
	vm.WriteAddress(addr, Version{TxIndex: 0}, &account, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 2)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)

	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
}

// An account that exists before a tx and that the tx touches and leaves empty is
// removed under EIP-161: the commitment verdict drops its trie leaf, and a higher tx
// that already read it must fail validation.
func TestFinalizedWritesRemoveTouchedEmptyAccount(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xe4})
	empty := accounts.NewAccount()
	reader := &emptyAccountReader{addr: addr, acc: &empty}
	vm := NewVersionMap(nil)
	newTx := func(txIndex int) *IntraBlockState {
		ibs := NewWithVersionMap(reader, vm)
		t.Cleanup(ibs.Close)
		ibs.SetTxContext(1, txIndex)
		return ibs
	}

	early := newTx(2)
	exists, err := early.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, early.VersionedReads())

	toucher := newTx(1)
	exists, err = toucher.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, toucher.AddBalance(addr, uint256.Int{}, 0))
	vm.FlushVersionedWrites(finalizedWrites(t, toucher, &chain.Rules{IsSpuriousDragon: true}), true)

	// The emptied pre-existing account is removed: a later tx reads it absent, and
	// an earlier tx's existence read predating the removal is stale (invalidated).
	_, ok, _ := vm.View(2, reader).GetAccount(addr)
	require.False(t, ok, "an EIP-161-emptied account is removed from the trie")
	exists, err = newTx(2).Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
}

// Sequential executors run FinalizeTx, which clears the journal, before
// FinalizedWrites. The removal verdict must still see the tx's own writes.
func TestFinalizedWritesAfterFinalizeTx(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		apply   func(ibs *IntraBlockState, addr accounts.Address) error
		removed bool
	}{
		{"zero-value call", func(ibs *IntraBlockState, addr accounts.Address) error {
			if _, err := ibs.Exist(addr); err != nil {
				return err
			}
			return ibs.AddBalance(addr, uint256.Int{}, 0)
		}, true},
		{"zero tip", func(ibs *IntraBlockState, addr accounts.Address) error {
			if _, err := ibs.GetBalance(addr); err != nil {
				return err
			}
			return ibs.AddBalance(addr, uint256.Int{}, 0)
		}, true},
		{"value call", func(ibs *IntraBlockState, addr accounts.Address) error {
			if _, err := ibs.Empty(addr); err != nil {
				return err
			}
			if _, err := ibs.Exist(addr); err != nil {
				return err
			}
			return ibs.AddBalance(addr, *uint256.NewInt(1), 0)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr := accounts.InternAddress([20]byte{0xe6})
			empty := accounts.NewAccount()
			ibs := NewWithVersionMap(&emptyAccountReader{addr: addr, acc: &empty}, NewVersionMap(nil))
			t.Cleanup(ibs.Close)
			ibs.SetTxContext(1, 0)
			rules := &chain.Rules{IsSpuriousDragon: true}

			require.NoError(t, tc.apply(ibs, addr))
			require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))

			bal, ok := finalizedWrites(t, ibs, rules).GetBalance(addr)
			require.Equal(t, tc.removed, ok && bal.Deletes())
		})
	}
}

// EIP-161 removes a touched empty account only from Spurious Dragon on, and AuRa
// keeps the system address, which every system call touches.
func TestFinalizedWritesRemovalFollowsEIP161(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xe7})
	for _, tc := range []struct {
		name    string
		addr    accounts.Address
		rules   *chain.Rules
		removed bool
	}{
		{"from Spurious Dragon", addr, &chain.Rules{IsSpuriousDragon: true}, true},
		{"before Spurious Dragon", addr, &chain.Rules{}, false},
		{"AuRa system address", params.SystemAddress, &chain.Rules{IsSpuriousDragon: true, IsAura: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			empty := accounts.NewAccount()
			ibs := NewWithVersionMap(&emptyAccountReader{addr: tc.addr, acc: &empty}, NewVersionMap(nil))
			t.Cleanup(ibs.Close)
			ibs.SetTxContext(1, 0)
			_, err := ibs.Exist(tc.addr)
			require.NoError(t, err)
			require.NoError(t, ibs.AddBalance(tc.addr, uint256.Int{}, 0))

			bal, ok := finalizedWrites(t, ibs, tc.rules).GetBalance(tc.addr)
			require.Equal(t, tc.removed, ok && bal.Deletes())
		})
	}
}

func TestFinalizedWritesLeavesVersionMapForApplyLoop(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(addr))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), false)

	writes := finalizedWrites(t, ibs, &chain.Rules{IsSpuriousDragon: true})
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress)

	_, result, ok := vm.ReadAddress(addr, 1)
	require.True(t, ok)
	require.Equal(t, MVReadResultDependency, result.Status())
	require.Equal(t, 0, result.DepIdx())
}

func TestCreatedEmptyRequiresNoOtherWrites(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	newWrites := func() *WriteSet {
		account := accounts.NewAccount()
		writes := &WriteSet{}
		writes.SetAddress(addr, &VersionedWrite[*accounts.Account]{Val: &account})
		return writes
	}
	tests := []struct {
		name string
		add  func(*WriteSet)
	}{
		{
			name: "balance",
			add: func(writes *WriteSet) {
				writes.SetBalance(addr, &VersionedWrite[uint256.Int]{Val: *uint256.NewInt(1)})
			},
		},
		{
			name: "nonce",
			add: func(writes *WriteSet) {
				writes.SetNonce(addr, &VersionedWrite[uint64]{Val: 1})
			},
		},
		{
			name: "code hash",
			add: func(writes *WriteSet) {
				writes.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{Val: accounts.InternCodeHash(common.Hash{1})})
			},
		},
		{
			name: "code",
			add: func(writes *WriteSet) {
				writes.SetCode(addr, &VersionedWrite[accounts.Code]{})
			},
		},
		{
			name: "code size",
			add: func(writes *WriteSet) {
				writes.SetCodeSize(addr, &VersionedWrite[int]{})
			},
		},
		{
			name: "storage",
			add: func(writes *WriteSet) {
				writes.SetStorage(addr, accounts.InternKey([32]byte{1}), &VersionedWrite[uint256.Int]{})
			},
		},
		{
			name: "self-destruct",
			add: func(writes *WriteSet) {
				writes.SetSelfDestruct(addr, &VersionedWrite[bool]{})
			},
		},
		{
			name: "contract creation",
			add: func(writes *WriteSet) {
				writes.SetCreateContract(addr, &VersionedWrite[bool]{})
			},
		},
		{
			name: "incarnation",
			add: func(writes *WriteSet) {
				writes.SetIncarnation(addr, &VersionedWrite[uint64]{})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writes := newWrites()
			require.True(t, writes.createdEmpty(addr))
			test.add(writes)
			require.False(t, writes.createdEmpty(addr))
		})
	}
}

func TestFinalizedWritesKeepCreatedEmptyBeforeEIP161(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress([20]byte{0xe1})
	require.NoError(t, ibs.TouchAccount(addr))

	writes := finalizedWrites(t, ibs, &chain.Rules{})
	_, ok := writes.GetAddress(addr)
	require.True(t, ok)
}

func TestFinalizedWritesKeepCreatedEmptyAtGenesis(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(0, 0)

	addr := accounts.InternAddress([20]byte{0xe1})
	require.NoError(t, ibs.TouchAccount(addr))

	writes := finalizedWrites(t, ibs, &chain.Rules{})
	_, ok := writes.GetAddress(addr)
	require.True(t, ok)
}

func TestFinalizedWritesKeepCreatedEmptyAuraSystemAccount(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(params.SystemAddress))

	writes := finalizedWrites(t, ibs, &chain.Rules{
		IsSpuriousDragon: true,
		IsAura:           true,
	})
	_, ok := writes.GetAddress(params.SystemAddress)
	require.True(t, ok)
}

// TestRecreateAfterEmptyRemovalShadowsBalanceDelete pins that an account removed as an
// EIP-161 empty and re-created by a later tx with no balance write reads present: the
// re-creation must supersede the removal's ValueDeleted balance.
func TestRecreateAfterEmptyRemovalShadowsBalanceDelete(t *testing.T) {
	t.Parallel()
	rules := &chain.Rules{IsSpuriousDragon: true}
	delegation := types.AddressToDelegation(accounts.InternAddress(common.Address{0xde}))

	for _, tc := range []struct {
		name     string
		recreate func(*IntraBlockState, accounts.Address) error
	}{
		{"nonce only", func(ibs *IntraBlockState, addr accounts.Address) error {
			return ibs.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
		}},
		{"delegation then nonce", func(ibs *IntraBlockState, addr accounts.Address) error {
			if err := ibs.SetCode(addr, delegation, tracing.CodeChangeAuthorization); err != nil {
				return err
			}
			return ibs.SetNonce(addr, 1, tracing.NonceChangeAuthorization)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := accounts.InternAddress([20]byte{0xe4})
			vm := NewVersionMap(nil)

			touch := NewWithVersionMap(&minimalStateReader{}, vm)
			t.Cleanup(touch.Close)
			touch.SetTxContext(1, 0)
			vm.WriteBalance(addr, Version{TxIndex: 0}, uint256.Int{}, false)
			require.NoError(t, touch.TouchAccount(addr))
			removal := finalizedWrites(t, touch, rules)
			bal, ok := removal.GetBalance(addr)
			require.True(t, ok && bal.Deletes(), "the touching tx removes the account")
			vm.FlushVersionedWrites(removal, true)

			recreate := NewWithVersionMap(&minimalStateReader{}, vm)
			t.Cleanup(recreate.Close)
			recreate.SetTxContext(1, 1)
			require.NoError(t, tc.recreate(recreate, addr))
			vm.FlushVersionedWrites(finalizedWrites(t, recreate, rules), true)

			acc, ok, _ := vm.View(2, &minimalStateReader{}).GetAccount(addr)
			require.True(t, ok, "the re-created account must read present to commitment")
			require.Equal(t, uint64(1), acc.Nonce)
		})
	}
}

func TestCreatedSelfDestructEmpty(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd0})
	realSD := map[accounts.Address]struct{}{addr: {}}
	writes := func(created bool, bal uint64) *WriteSet {
		ws := &WriteSet{}
		if created {
			ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath}, Val: true})
		}
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(bal)})
		return ws
	}

	empty, known := createdSelfDestructEmpty(writes(true, 0), realSD, addr)
	require.True(t, known)
	require.True(t, empty, "created, self-destructed, no balance left: removed")

	empty, known = createdSelfDestructEmpty(writes(true, 7), realSD, addr)
	require.True(t, known)
	require.False(t, empty, "a balance left after the self-destruct keeps the account")

	_, known = createdSelfDestructEmpty(writes(false, 0), realSD, addr)
	require.False(t, known, "an account that existed before the tx needs the account view")

	_, known = createdSelfDestructEmpty(writes(true, 0), nil, addr)
	require.False(t, known, "without a real self-destruct the account view decides")
}

func TestFinalizedWritesCreatedSelfDestructKeepsCodeDeletedMarker(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xd1})
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)

	writes := finalizedWrites(t, ibs, &chain.Rules{IsSpuriousDragon: true, IsCancun: true})

	sd, ok := writes.GetSelfDestruct(addr)
	require.True(t, ok && sd.Val, "the created contract is removed")
	ch, ok := writes.GetCodeHash(addr)
	require.True(t, ok && ch.Deletes(), "a destroyed contract keeps the code-deleted marker, so the fee calc burns its delayed fee")
}
