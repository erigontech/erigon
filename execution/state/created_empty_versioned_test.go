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

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestFinalizedWritesWithholdCreatedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(func() { ibs.Release(false) })
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(addr))

	writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	require.NoError(t, err)
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress)
	_, hasBalance := writes.GetBalance(addr)
	require.False(t, hasBalance)
	_, hasDelete := writes.GetSelfDestruct(addr)
	require.False(t, hasDelete)

	vm.FlushVersionedWrites(writes, true, "")
	next := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(func() { next.Release(false) })
	next.SetNoMaterialize(true)
	next.SetTxContext(1, 1)
	exists, err := next.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestFinalizedWritesKeepCreatedEmptyBeforeEIP161(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(func() { ibs.Release(false) })
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress([20]byte{0xe1})
	require.NoError(t, ibs.TouchAccount(addr))

	writes, err := ibs.FinalizedWrites(&chain.Rules{})
	require.NoError(t, err)
	_, ok := writes.GetAddress(addr)
	require.True(t, ok)
}

func TestFinalizedWritesKeepCreatedEmptyAtGenesis(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(func() { ibs.Release(false) })
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)

	addr := accounts.InternAddress([20]byte{0xe1})
	require.NoError(t, ibs.TouchAccount(addr))

	writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	require.NoError(t, err)
	_, ok := writes.GetAddress(addr)
	require.True(t, ok)
}

func TestFinalizedWritesKeepCreatedEmptyAuraSystemAccount(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(func() { ibs.Release(false) })
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(params.SystemAddress))

	writes, err := ibs.FinalizedWrites(&chain.Rules{
		IsSpuriousDragon: true,
		IsAura:           true,
	})
	require.NoError(t, err)
	_, ok := writes.GetAddress(params.SystemAddress)
	require.True(t, ok)
}
