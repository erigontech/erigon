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

	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestAccountLifecycleAt pins the enumerated lifecycle resolver — the single
// verdict readers, the validator, and the create decision converge on.
//
// # States (at a reading txIdx, from the versionMap SelfDestruct + revival signals)
//
//	Live     no Done SelfDestruct=true below txIdx. Normal floor reads.
//	Absent   destroyed and net-absent: no revival above the wipe AND no EIP-8246
//	         balance/nonce preserved. Reads gone; a base (pre-block) read is not stale.
//	Revived  destroyed but re-created above the wipe: AddressPath >= wipe (same-tx
//	         metamorphic) or Balance/Nonce/CodeHash > wipe. Storage from on/before
//	         the wipe reads zero; stale base reads are invalidated. (EIP-8246
//	         balance-preserve is NOT resolved here — see the resolver doc — it is a
//	         fork-aware caller's decision.)
//
// # canonicalVer — the point of reader/validator agreement
//
// canonicalVer is always the LATEST SelfDestruct cell (what the validator's
// ReadStatus(SelfDestructPath) resolves), NOT the wipe. When a revival flips
// SelfDestruct=false above the wipe, canonicalVer is that flip; a reader recording
// the wipe instead would disagree with the validator forever and livelock (the
// CREATE2-recreate-then-use bug). destroyedAt stays the wipe, for the reader's
// "did this slot's last write predate the wipe" decision.
//
// # Transitions (across txs in a block) and the fork that produces each
//
//	Live -> Absent    a SELFDESTRUCT with no revival. Pre-Cancun: any plain destruct.
//	                  Cancun+ (EIP-6780): only a same-tx create+destruct nets absent.
//	Live -> Revived   metamorphic SELFDESTRUCT then CREATE2 re-create (Constantinople+
//	                  for CREATE2; pre-Cancun the destruct wipes storage).
//	Absent -> Revived a later tx re-creates the net-absent account.
//
// The resolver is fork-agnostic: it reads whatever signals execution wrote. The
// fork determines which signal pattern appears; these cases construct each pattern.
func TestAccountLifecycleAt(t *testing.T) {
	t.Parallel()

	t.Run("Live: no self-destruct", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(1)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(10), true)
		state, canonicalVer, _ := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleLive, state)
		require.Equal(t, Version{}, canonicalVer)
	})

	t.Run("Absent: destroyed, no revival, zero balance", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(2)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		state, canonicalVer, destroyedAt := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleAbsent, state)
		require.Equal(t, 2, destroyedAt)
		require.Equal(t, 2, canonicalVer.TxIndex, "no revival: canonicalVer is the wipe")
	})

	t.Run("Revived via same-tx metamorphic (AddressPath >= wipe)", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(3)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 3}, true, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, &accounts.Account{Nonce: 1}, true)
		state, canonicalVer, destroyedAt := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleRevived, state)
		require.Equal(t, 3, destroyedAt)
		require.Equal(t, 3, canonicalVer.TxIndex, "latest SD cell is the wipe@3 (no later flip)")
	})

	t.Run("Revived via CREATE2 flip above wipe: canonicalVer is the flip, not the wipe", func(t *testing.T) {
		t.Parallel()
		// The CREATE2-recreate-then-use case: wipe@2, re-create@3 (AddressPath +
		// SelfDestruct=false). canonicalVer MUST be 3 (what the validator resolves);
		// recording the wipe@2 is the livelock bug.
		vm := NewVersionMap(nil)
		addr := getAddress(4)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 3}, false, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, &accounts.Account{Nonce: 1}, true)
		state, canonicalVer, destroyedAt := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleRevived, state)
		require.Equal(t, 2, destroyedAt, "destroyedAt stays the wipe for the slot-predates-wipe check")
		require.Equal(t, 3, canonicalVer.TxIndex, "canonicalVer is the latest SD cell (the flip), never the wipe")
	})

	t.Run("EIP-8246 balance-preserve signature resolves Absent (fork-aware caller decides existence)", func(t *testing.T) {
		t.Parallel()
		// A non-zero balance at the destruct with no revival above it is
		// indistinguishable from a pre-Cancun credit to a doomed account, so the
		// fork-agnostic resolver reports Absent; only a fork-aware caller applies
		// EIP-8246's "still exists".
		vm := NewVersionMap(nil)
		addr := getAddress(5)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(9), true)
		state, _, destroyedAt := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleAbsent, state)
		require.Equal(t, 2, destroyedAt)
	})

	// Negative cases: signals that must NOT flip the verdict.

	t.Run("negative: the destruct's zero-balance write governs, not stale history", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(6)
		// Pre-wipe non-zero balance, then a plain (pre-EIP-8246) destruct that zeroes
		// the balance in its own tx. The zero at the wipe must win over the stale
		// history → Absent, not a spurious balance-preserve Revived.
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 1}, *uint256.NewInt(9), true)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, uint256.Int{}, true)
		state, _, _ := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleAbsent, state)
	})

	t.Run("negative: same-tx zero-balance write is not a preserve", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(7)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, uint256.Int{}, true)
		state, _, _ := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleAbsent, state, "a same-tx SD-zero balance is not an EIP-8246 preserve")
	})

	t.Run("negative: self-destruct AT the reading tx is not yet in effect", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(8)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 5}, true, true)
		state, _, _ := vm.AccountLifecycleAt(addr, 5)
		require.Equal(t, LifecycleLive, state, "the reading tx does not observe its own/equal-tx destruct via the floor")
	})
}

// accountLifecycle layers the tx's own field-level SelfDestruct write over the
// versionMap floor, without the stateObject. Pin the layering: own write wins
// (true after same-tx SD, false after same-tx recreate); else the floor verdict.
func TestAccountLifecycle_LayersOwnTxWrites(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)

	newIBS := func() (*IntraBlockState, *VersionMap) {
		vm := NewVersionMap(nil)
		ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), vm)
		ibs.SetTxContext(0, 5)
		return ibs, vm
	}
	ownSD := func(ibs *IntraBlockState, addr accounts.Address, val bool) {
		ibs.versionedWrites.SetSelfDestruct(addr, &VersionedWrite[bool]{
			WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 5}}, Val: val})
		ibs.journal.dirties[addr] = 1
	}

	t.Run("own-tx SD wins over floor", func(t *testing.T) {
		ibs, _ := newIBS()
		a := getAddress(1)
		ownSD(ibs, a, true)
		require.True(t, ibs.accountLifecycle(a))
	})
	t.Run("own-tx recreate (SD=false) wins", func(t *testing.T) {
		ibs, vm := newIBS()
		a := getAddress(2)
		writeFor(vm, a, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true) // floor says destroyed
		ownSD(ibs, a, false)                                                                // own recreate
		require.False(t, ibs.accountLifecycle(a), "own recreate must override the floor destruct")
	})
	t.Run("no own write -> floor destroyed-no-revival", func(t *testing.T) {
		ibs, vm := newIBS()
		a := getAddress(3)
		writeFor(vm, a, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		require.True(t, ibs.accountLifecycle(a))
	})
	t.Run("no own write -> floor revived", func(t *testing.T) {
		ibs, vm := newIBS()
		a := getAddress(4)
		writeFor(vm, a, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, a, BalancePath, accounts.NilKey, Version{TxIndex: 3}, *uint256.NewInt(1), true)
		require.False(t, ibs.accountLifecycle(a))
	})
}
