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
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A CREATE2 over a pre-existing storage-only account clears its storage. EIP-7928 records
// the code deploy but not the per-slot clear, so the cleared slots appear nowhere in the
// BAL as a value. SeedStorageWipes derives the wipe at construction, so a later tx reading
// a cleared slot resolves zero from the seed — without the creating tx having executed, and
// so without a re-run. On main the read falls through to the stale pre-block value.
func TestSeedStorageWipesCreate2OverStorageOnly(t *testing.T) {
	t.Parallel()

	created := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	wiped := accounts.InternKey(common.HexToHash("0x01"))   // base 9, not rewritten -> cleared to 0
	written := accounts.InternKey(common.HexToHash("0x02")) // the init code writes 1

	bal := types.BlockAccessList{{
		Address:      created.Value(),
		CodeChanges:  []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60, 0x00}}},
		NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}},
		StorageChanges: []types.SlotChanges{{
			Slot:    written,
			Changes: []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(1)}},
		}},
	}}

	base := accounts.NewAccount() // nonce 0, empty code -> storage-only
	reader := &storageReader{
		addr:    created,
		account: &base,
		storage: map[accounts.StorageKey]uint256.Int{wiped: *uint256.NewInt(9)},
	}

	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
	vm.SeedStorageWipes()

	r := NewVersionedStateReader(2, ReadSet{}, vm, reader)

	got, _, err := r.ReadAccountStorage(created, wiped)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{}, got, "a slot cleared by the CREATE2 reads zero at a later tx, not the stale base value 9")

	got, _, err = r.ReadAccountStorage(created, written)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1), got, "the created contract's own slot write survives the wipe")
}
