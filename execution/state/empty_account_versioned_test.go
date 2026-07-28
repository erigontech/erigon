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
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// An account a tx leaves empty must be published as a delete on the versioned
// path too, not as an existing empty record: the write-set is flushed to the
// version map, so a later tx in the same block reads it. Before EIP-161 the
// touched account is created and persists, so the clearing is rules-gated.
func TestVersionedWritesClearTouchedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xe1"))
	spuriousDragon := &chain.Rules{IsSpuriousDragon: true}

	touchedWrites := func(reader StateReader, rules *chain.Rules) *WriteSet {
		t.Helper()
		ibs := NewWithVersionMap(reader, NewVersionMap(nil))
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)
		require.NoError(t, ibs.TouchAccount(addr))
		return ibs.FinalizedWrites(rules)
	}
	existingReader := func() StateReader {
		empty := accounts.NewAccount()
		return &accountStateReader{
			accounts: map[accounts.Address]*accounts.Account{addr: &empty},
		}
	}

	t.Run("serial baseline emits a delete", func(t *testing.T) {
		ibs := New(&minimalStateReader{})
		require.NoError(t, ibs.TouchAccount(addr))
		ibs.SoftFinalise()
		collector := NewLightCollector()
		require.NoError(t, ibs.MakeWriteSet(spuriousDragon, collector))
		sd, ok := collector.TakeWrites().GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, sd.Val)
	})

	t.Run("versioned path withholds the created account", func(t *testing.T) {
		writes := touchedWrites(&minimalStateReader{}, spuriousDragon)
		_, hasAddress := writes.GetAddress(addr)
		require.False(t, hasAddress, "the create must not reach the write-set")
		_, hasBalance := writes.GetBalance(addr)
		require.False(t, hasBalance)
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete, "nothing was published, so nothing to delete")
	})

	t.Run("next tx does not observe the account", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.FlushVersionedWrites(touchedWrites(&minimalStateReader{}, spuriousDragon), true, "")

		next := NewWithVersionMap(&minimalStateReader{}, vm)
		next.SetNoMaterialize(true)
		next.SetTxContext(1, 1)
		next.SetVersion(0)
		exists, err := next.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("versioned path deletes an existing empty account", func(t *testing.T) {
		writes := touchedWrites(existingReader(), spuriousDragon)
		_, hasBalance := writes.GetBalance(addr)
		require.False(t, hasBalance)
		deleted, ok := writes.GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, deleted.Val)
	})

	t.Run("next tx does not observe an existing empty account", func(t *testing.T) {
		reader := existingReader()
		vm := NewVersionMap(nil)
		vm.FlushVersionedWrites(touchedWrites(reader, spuriousDragon), true, "")

		next := NewWithVersionMap(reader, vm)
		next.SetNoMaterialize(true)
		next.SetTxContext(1, 1)
		next.SetVersion(0)
		exists, err := next.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("existing account reset to empty emits a delete", func(t *testing.T) {
		ibs := NewWithVersionMap(existingReader(), NewVersionMap(nil))
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)
		require.NoError(t, ibs.CreateAccount(addr, false))

		writes := ibs.FinalizedWrites(spuriousDragon)
		deleted, ok := writes.GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, deleted.Val)
	})

	t.Run("existing account funded after the touch is retained", func(t *testing.T) {
		ibs := NewWithVersionMap(existingReader(), NewVersionMap(nil))
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(1, 0)
		ibs.SetVersion(0)
		require.NoError(t, ibs.TouchAccount(addr))
		require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(1), tracing.BalanceChangeTransfer))

		writes := ibs.FinalizedWrites(spuriousDragon)
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		balance, ok := writes.GetBalance(addr)
		require.True(t, ok)
		require.Equal(t, uint64(1), balance.Val.Uint64())
	})

	t.Run("account from an earlier tx emits a delete", func(t *testing.T) {
		empty := accounts.NewAccount()
		vm := NewVersionMap(nil)
		previous := &WriteSet{}
		previous.SetAddress(addr, &VersionedWrite[*accounts.Account]{
			WriteHeader: WriteHeader{
				Address: addr,
				Path:    AddressPath,
				Version: Version{TxIndex: 0},
			},
			Val: &empty,
		})
		vm.FlushVersionedWrites(previous, true, "")

		ibs := NewWithVersionMap(&minimalStateReader{}, vm)
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(1, 1)
		ibs.SetVersion(0)
		require.NoError(t, ibs.TouchAccount(addr))

		writes := ibs.FinalizedWrites(spuriousDragon)
		deleted, ok := writes.GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, deleted.Val)
	})

	t.Run("pre-SpuriousDragon keeps the touched account", func(t *testing.T) {
		writes := touchedWrites(&minimalStateReader{}, &chain.Rules{})
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		_, hasAddress := writes.GetAddress(addr)
		require.True(t, hasAddress)
	})

	t.Run("pre-SpuriousDragon keeps an existing empty account", func(t *testing.T) {
		writes := touchedWrites(existingReader(), &chain.Rules{})
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		_, hasBalance := writes.GetBalance(addr)
		require.True(t, hasBalance)
	})
}
