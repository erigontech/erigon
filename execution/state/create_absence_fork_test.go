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

// TestCreateOverAbsenceConsumedBeforeDestructFlush pins the two-view fork the
// EIP-8246 reconstruction in versionedAccountBase can otherwise smuggle past
// validation: tx1's new-account gas probe (EIP-8037 chargeNewAccount) reads
// the CREATE2 target as absent before tx0 — which created the account and
// self-destructed it to itself, leaving a balance-only account — has flushed
// its cells. When the create flow later re-reads the account, the flushed
// destruct must NOT be silently adopted: the absence was already consumed
// (new-account state gas was charged on it), so the tx must abort with
// ErrDependency (or fail commit-time validation) and re-execute. Silent
// adoption keeps the state view correct but leaves the inflated gas charge
// standing, so the proposer and validator diverge on fees.
//
// The collisionCheck variant interleaves the CREATE2 collision reads between
// the stale probe and the account load: the nonce read's destruct-wipe scan
// records a SelfDestruct=true read, which must not be mistaken for the
// absence having been concluded from the destruct.
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
			ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), vm)
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
						if r == ErrDependency {
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
