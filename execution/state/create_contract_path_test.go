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

	"github.com/stretchr/testify/require"
)

// CreateContractPath is not redundant with IncarnationPath: it signals contract (vs plain-account) creation
// and drives the apply-side stale-storage clear before re-creation — don't fold it into IncarnationPath.
func TestCreateContractPath_ContractOnlySignal(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), vm)
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
