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

// An OCC read that consumed an account's absence (e.g. the EIP-8037 new-account gas charge) must not silently
// adopt a destruct flushed afterward: it must abort with ErrDependency or fail validation, or gas view forks from state view.
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

			empty, err := ibs.Empty(addr)
			require.NoError(t, err)
			require.True(t, empty)

			writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000_000), true)
			writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(1), true)
			writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(0), true)
			writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, true, true)
			writeFor(vm, addr, CreateContractPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, true, true)

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
