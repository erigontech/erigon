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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSlotOnlyAccessList(t *testing.T) {
	address := accounts.InternAddress(common.HexToAddress("0x2000"))
	slot := accounts.InternKey(common.HexToHash("0x07"))
	newState := func(t *testing.T) *IntraBlockState {
		ibs := New(NewNoopReader())
		t.Cleanup(ibs.Close)
		return ibs
	}

	t.Run("address stays cold", func(t *testing.T) {
		ibs := newState(t)
		ibs.AddSlotOnlyToAccessList(address, slot)
		addressWarm, slotWarm := ibs.SlotInAccessList(address, slot)
		require.True(t, slotWarm)
		require.False(t, addressWarm)
		require.False(t, ibs.AddressInAccessList(address))
	})

	t.Run("revert", func(t *testing.T) {
		ibs := newState(t)
		snapshot := ibs.PushSnapshot()
		ibs.AddSlotOnlyToAccessList(address, slot)
		ibs.RevertToSnapshot(snapshot, nil)
		ibs.PopSnapshot(snapshot)
		_, slotWarm := ibs.SlotInAccessList(address, slot)
		require.False(t, slotWarm)
	})

	t.Run("next transaction", func(t *testing.T) {
		ibs := newState(t)
		ibs.AddSlotOnlyToAccessList(address, slot)
		ibs.Prepare(&chain.Rules{IsBerlin: true}, accounts.NilAddress, accounts.NilAddress, accounts.NilAddress, nil, nil)
		_, slotWarm := ibs.SlotInAccessList(address, slot)
		require.False(t, slotWarm)
	})

	t.Run("later storage access", func(t *testing.T) {
		ibs := newState(t)
		ibs.AddSlotOnlyToAccessList(address, slot)
		addressAdded, slotAdded := ibs.AddSlotToAccessList(address, slot)
		require.True(t, addressAdded)
		require.False(t, slotAdded)
		addressWarm, slotWarm := ibs.SlotInAccessList(address, slot)
		require.True(t, addressWarm)
		require.True(t, slotWarm)
	})
}
