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
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSetEmptyRemoval pins how the two kinds of account removal are encoded. Both stamp
// the balance ValueDeleted verdict (commitment and the domain apply read it to drop the
// leaf) and clear the whole footprint — but a genuine self-destruct keeps its
// SelfDestruct marker (calcFees burns its delayed fee, the read path pins the absence),
// while an EIP-161 touched-empty EOA carries none and must not gain one.
func TestSetEmptyRemoval(t *testing.T) {
	t.Parallel()
	ver := Version{TxIndex: 1}

	t.Run("self-destruct keeps its marker", func(t *testing.T) {
		addr := accounts.InternAddress([20]byte{0x5d})
		ws := &WriteSet{}
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})
		ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: 1})
		ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: ver}})

		ws.SetEmptyRemoval(addr, ver)

		bal, ok := ws.GetBalance(addr)
		require.True(t, ok && bal.Deletes(), "removal stamps the balance-deleted verdict")
		sd, ok := ws.GetSelfDestruct(addr)
		require.True(t, ok && sd.Val, "a self-destruct keeps its marker across the removal")
		_, ok = ws.GetAddress(addr)
		require.False(t, ok, "the whole-account AddressPath cell is cleared (no metamorphic false-revival)")
		_, ok = ws.GetNonce(addr)
		require.False(t, ok, "the footprint is cleared")
	})

	t.Run("EIP-161 empty gains no self-destruct marker", func(t *testing.T) {
		addr := accounts.InternAddress([20]byte{0xe0})
		ws := &WriteSet{}
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}})
		ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}})

		ws.SetEmptyRemoval(addr, ver)

		bal, ok := ws.GetBalance(addr)
		require.True(t, ok && bal.Deletes(), "removal stamps the balance-deleted verdict")
		_, ok = ws.GetSelfDestruct(addr)
		require.False(t, ok, "a touched-empty EOA carries no self-destruct marker")
	})
}

// TestSnapshotSelfDestructPreserveNonceRestamp pins the re-stamp gate for a
// balance-preserving self-destruct (EIP-8246): at end-of-tx the account resolves to a
// codeless EOA, so Snapshot drops the per-field writes and the domain apply rebuilds
// NewAccount+balance. For a PRE-EXISTING account the commitment would otherwise read the
// stale base nonce/codeHash, so Snapshot re-stamps nonce 0 / EmptyCodeHash. For an account
// CREATED this block there is no base — the commitment composes the codeless-EOA defaults
// itself, and re-stamping nonce 0 would leak a net-zero nonce change (0->1->0) into the
// block access list, diverging the BAL hash and the trie root.
func TestSnapshotSelfDestructPreserveNonceRestamp(t *testing.T) {
	t.Parallel()
	ver := Version{TxIndex: 1}

	build := func(created bool) *WriteSet {
		addr := accounts.InternAddress([20]byte{0x82})
		ws := &WriteSet{}
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: ver}, Val: true})
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: *uint256.NewInt(7)})
		ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: 1})
		ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: accounts.EmptyCodeHash})
		if created {
			ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath, Version: ver}, Val: true})
		}
		return ws
	}

	t.Run("pre-existing re-stamps cleared nonce and codeHash", func(t *testing.T) {
		addr := accounts.InternAddress([20]byte{0x82})
		out := build(false).Snapshot()

		n, ok := out.GetNonce(addr)
		require.True(t, ok && n.Val == 0, "a pre-existing SD-preserve account re-stamps nonce 0 over the stale base")
		ch, ok := out.GetCodeHash(addr)
		require.True(t, ok && ch.Val == accounts.EmptyCodeHash, "and re-stamps EmptyCodeHash over the stale base code")
	})

	t.Run("created-this-block emits no nonce or codeHash change", func(t *testing.T) {
		addr := accounts.InternAddress([20]byte{0x82})
		out := build(true).Snapshot()

		_, ok := out.GetNonce(addr)
		require.False(t, ok, "a created-this-block SD-preserve account has no base, so no nonce re-stamp -> no net-zero BAL nonceChange")
		_, ok = out.GetCodeHash(addr)
		require.False(t, ok, "and no codeHash re-stamp")
	})
}

func TestFinalizedWritesWithholdCreatedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(addr))

	writes := finalizedWrites(t, ibs, &chain.Rules{IsSpuriousDragon: true})
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress)
	bal, hasBalance := writes.GetBalance(addr)
	require.True(t, hasBalance && bal.Deletes(),
		"a withheld created-empty account emits the balance-deleted removal verdict, overriding its lingering during-tx cells")
	_, hasDelete := writes.GetSelfDestruct(addr)
	require.False(t, hasDelete)

	vm.FlushVersionedWrites(writes, true)
	next := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(next.Close)
	next.SetTxContext(1, 1)
	exists, err := next.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
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
	_, hasDelete := writes.GetSelfDestruct(addr)
	require.False(t, hasDelete, "the removal is encoded as a balance-deleted verdict, not a self-destruct cell")
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

	// The emptied pre-existing account is dropped from the trie: the commitment
	// verdict reads it absent (its balance write is stamped ValueDeleted).
	_, ok := vm.View(2, reader).GetAccount(addr)
	require.False(t, ok, "an EIP-161-emptied account is removed from the trie")
	// The removal is a balance write to zero (the value the early reader already
	// saw on the empty base), so a higher tx's balance read stays valid — an empty
	// account and a removed one are observationally identical post EIP-161.
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
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
