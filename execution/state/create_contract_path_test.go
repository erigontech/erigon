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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/stretchr/testify/require"
)

// CreateContractPath is not redundant with IncarnationPath: it is the signal a
// contract (not a plain account) was created, and the apply path consumes it to
// clear stale storage before re-creation (rw_v3 DomainDelPrefix, mirroring
// Writer.CreateContract). This pins the distinct write-side signal — contract
// creation records CreateContractPath, a plain account creation does not — so a
// future "fold it into IncarnationPath" simplification can't silently drop the
// storage-clear trigger.
func TestCreateContractPath_ContractOnlySignal(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})), vm)
	ibs.SetTxContext(0, 1)

	contract := getAddress(1)
	require.NoError(t, ibs.CreateAccount(contract, true))
	if _, ok := ibs.versionedWrites.GetCreateContract(contract); !ok {
		t.Fatal("contract creation must record CreateContractPath (drives apply-side storage clear)")
	}
	if _, ok := ibs.versionedWrites.GetIncarnation(contract); !ok {
		t.Fatal("contract creation also bumps IncarnationPath — the two are co-emitted, distinct signals")
	}

	plain := getAddress(2)
	require.NoError(t, ibs.CreateAccount(plain, false))
	if _, ok := ibs.versionedWrites.GetCreateContract(plain); ok {
		t.Fatal("non-contract account creation must not set CreateContractPath")
	}
}

// CreateAccount records its balance and incarnation reads only for conflict
// detection; Reset turns detection back on.
func TestNoConflictDetectionCreateAccountRecordsNoConflictReads(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	ibs, vm := newNoMaterializeIBS(NewNoopReader())
	defer ibs.Close()
	conflictReads := func() (balance, incarnation bool) {
		reads := ibs.VersionedReads()
		_, balance = reads.GetBalance(addr)
		_, incarnation = reads.GetIncarnation(addr)
		return balance, incarnation
	}

	startNoMaterializeTx(ibs, vm, 0)
	ibs.SetNoConflictDetection()
	require.NoError(t, ibs.CreateAccount(addr, true))
	balance, incarnation := conflictReads()
	require.False(t, balance, "no conflict detection records no balance read")
	require.False(t, incarnation, "no conflict detection records no incarnation read")

	startNoMaterializeTx(ibs, vm, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	balance, incarnation = conflictReads()
	require.True(t, balance, "after Reset the balance read is recorded again")
	require.True(t, incarnation, "after Reset the incarnation read is recorded again")
}

// countingAccountReader serves one committed account and counts base reads.
type countingAccountReader struct {
	NoopReader
	addr  accounts.Address
	acc   *accounts.Account
	reads int
}

func (r *countingAccountReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	r.reads++
	if address == r.addr {
		return r.acc, nil
	}
	return nil, nil
}

func (r *countingAccountReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

// Exist must give the same answer with and without conflict detection: the
// unvalidated path only drops records a validator would have read.
func TestExistAgreesWithoutConflictDetection(t *testing.T) {
	committed := accounts.InternAddress(common.HexToAddress("0xc0de"))
	absent := accounts.InternAddress(common.HexToAddress("0xbeef"))
	created := accounts.InternAddress(common.HexToAddress("0xf00d"))
	destructed := accounts.InternAddress(common.HexToAddress("0xdead"))

	for _, tc := range []struct {
		name string
		addr accounts.Address
		seed func(*IntraBlockState)
	}{
		{name: "committed", addr: committed},
		{name: "absent", addr: absent},
		{name: "created in this call", addr: created, seed: func(ibs *IntraBlockState) {
			require.NoError(t, ibs.CreateAccount(created, true))
		}},
		{name: "self-destructed in this call", addr: destructed, seed: func(ibs *IntraBlockState) {
			require.NoError(t, ibs.CreateAccount(destructed, true))
			_, err := ibs.Selfdestruct(destructed, false)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]bool, 2)
			for i, noConflict := range []bool{false, true} {
				reader := &countingAccountReader{addr: committed, acc: &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}}
				ibs, vm := newNoMaterializeIBS(reader)
				startNoMaterializeTx(ibs, vm, 0)
				if noConflict {
					ibs.SetNoConflictDetection()
				}
				if tc.seed != nil {
					tc.seed(ibs)
				}
				exists, err := ibs.Exist(tc.addr)
				require.NoError(t, err)
				got[i] = exists
			}
			require.Equal(t, got[0], got[1], "conflict detection must not change existence")
		})
	}
}

// The committed record is read once per address, however often Exist asks.
func TestExistUnvalidatedMemoizesTheCommittedRead(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	reader := &countingAccountReader{addr: addr, acc: &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}}
	ibs, vm := newNoMaterializeIBS(reader)
	startNoMaterializeTx(ibs, vm, 0)
	ibs.SetNoConflictDetection()

	for range 4 {
		exists, err := ibs.Exist(addr)
		require.NoError(t, err)
		require.True(t, exists)
	}
	require.Equal(t, 1, reader.reads)
}
