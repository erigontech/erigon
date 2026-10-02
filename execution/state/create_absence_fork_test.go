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

	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestCreateOverAbsenceConsumedBeforeDestructFlush pins the OCC invariant
// that an execution which consumed an account's absence (e.g. for the
// EIP-8037 new-account gas charge) must not silently adopt a destruct flushed
// since: the attempt has to abort with ErrDependency or fail commit-time
// validation, else its gas view forks from its state view. The
// collisionCheckFirst variant interleaves the CREATE2 collision reads between
// the probe and the account load — their destruct-wipe scan records a
// SelfDestruct=true read that must not count as the absence having been
// concluded from the destruct.
func TestCreateOverAbsenceConsumedBeforeDestructFlush(t *testing.T) {
	t.Parallel()
	for _, collisionCheck := range []bool{false, true} {
		name := "createOnly"
		if collisionCheck {
			name = "collisionCheckFirst"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, tx, domains := NewTestRwTx(t)
			vm := NewVersionMap(nil)
			ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), vm)
			defer ibs.Close()
			ibs.SetTxContext(0, 1)
			ibs.SetNoMaterialize(true)
			ibs.SetVersion(0)
			ibs.eip8246 = true
			ibs.eip161 = true

			addr := getAddress(8246)

			// tx1's gas probe consumes the account's absence before tx0 flushes.
			empty, err := ibs.Empty(addr)
			require.NoError(t, err)
			require.True(t, empty)

			// tx0's writes flush: CREATE2 whose constructor self-destructed to
			// itself. EIP-8246 preserves the balance; the worker flush carries no
			// AddressPath cell, so only the destruct probe can reveal the account
			// to tx1.
			writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000_000), true)
			writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(1), true)
			writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(0), true)
			writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, true, true)
			writeFor(vm, addr, CreateContractPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, true, true)

			// The CREATE2 flow re-reads the account it is about to create over.
			diverged := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						if err, ok := r.(error); ok && errors.Is(err, ErrDependency) {
							diverged = true
							return
						}
						panic(r)
					}
				}()
				if collisionCheck {
					codeHash, err := ibs.GetCodeHash(addr)
					require.NoError(t, err)
					require.True(t, codeHash.IsEmpty() || codeHash.IsZero())
					nonce, err := ibs.GetNonce(addr)
					require.NoError(t, err)
					require.Zero(t, nonce)
				}
				require.NoError(t, ibs.CreateAccount(addr, true))
			}()

			if !diverged {
				io := NewVersionedIO(2)
				io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.VersionedReads())
				diverged = vm.ValidateVersion(1, io, validateEqualVersion, true, false, false, "") != VersionValid
			}
			require.True(t, diverged,
				"consuming the account's absence and then adopting the flushed destruct forks the gas view from the state view")
		})
	}
}

// Net-zero changes leave no BAL cells, so only the worker flush exposes the
// destruct marker. The retained reads must reject a later preserved balance.
func TestBALAbsenceRemainsValidAfterEmptyDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(8247)
	reader := newAccountStateReader()
	creator := NewWithVersionMap(reader, NewVersionMap(nil))
	defer creator.Close()
	creator.SetTxContext(1, 0)
	creator.SetNoMaterialize(true)
	creator.SetVersion(0)
	creator.eip8246 = true
	creator.eip161 = true
	require.NoError(t, creator.CreateAccount(addr, true))
	destroyed, err := creator.Selfdestruct(addr, true)
	require.NoError(t, err)
	require.True(t, destroyed)
	writes := creator.FinalizedWrites(&chain.Rules{IsAmsterdam: true, IsSpuriousDragon: true})
	defer writes.ReleaseAndReset()

	creatorIO := NewVersionedIO(2)
	creator.MergeTxIOInto(creatorIO, writes)
	changes := creatorIO.AsBlockAccessList()
	require.Len(t, changes, 1)
	require.Equal(t, addr, changes[0].Address)
	require.Empty(t, changes[0].BalanceChanges)
	require.Empty(t, changes[0].NonceChanges)
	require.Empty(t, changes[0].CodeChanges)

	vm := NewVersionMap(changes)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	ibs.SetNoMaterialize(true)
	ibs.SetVersion(0)
	ibs.eip8246 = true
	ibs.eip161 = true

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)
	require.True(t, ibs.consumedAddressAbsence(addr))

	vm.FlushVersionedWrites(writes, true, "")
	require.True(t, vm.destroyedAndUnrevived(addr, 1))

	require.NotPanics(t, func() {
		empty, err := ibs.Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
	}, "a zero-balance destruct leaves the consumed absence unchanged")

	io := NewVersionedIO(2)
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(1, io, validateEqualVersion, true, false, false, ""))

	reexecuted := Version{TxIndex: 0, Incarnation: 1}
	vm.WriteSelfDestruct(addr, reexecuted, true, true)
	vm.WriteBalance(addr, reexecuted, *uint256.NewInt(1), true)
	vm.WriteIncarnation(addr, reexecuted, 0, true)
	vm.WriteCreateContract(addr, reexecuted, true, true)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(1, io, validateEqualVersion, true, false, false, ""))
}
