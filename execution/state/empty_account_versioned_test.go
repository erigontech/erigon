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

func newVersionedTestState(t *testing.T, reader StateReader, versionMap *VersionMap, txIndex int) *IntraBlockState {
	t.Helper()
	ibs := NewWithVersionMap(reader, versionMap)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, txIndex)
	ibs.SetVersion(0)
	t.Cleanup(func() { ibs.Release(false) })
	return ibs
}

type deleteRecordingWriter struct {
	NoopWriter
	deleted []accounts.Address
}

func (w *deleteRecordingWriter) DeleteAccount(addr accounts.Address, _ *accounts.Account) error {
	w.deleted = append(w.deleted, addr)
	return nil
}

// An account a tx leaves empty must be published as a delete on the versioned
// path too, not as an existing empty record: the write-set is flushed to the
// version map, so a later tx in the same block reads it. Before EIP-161 the
// touched account is created and persists, so the clearing is rules-gated.
func TestVersionedWritesClearTouchedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xe1"))
	spuriousDragon := &chain.Rules{IsSpuriousDragon: true}

	finalizedWrites := func(t *testing.T, ibs *IntraBlockState, rules *chain.Rules) *WriteSet {
		t.Helper()
		writes, err := ibs.FinalizedWrites(rules)
		require.NoError(t, err)
		return writes
	}
	touchedWrites := func(t *testing.T, reader StateReader, rules *chain.Rules) *WriteSet {
		t.Helper()
		ibs := newVersionedTestState(t, reader, NewVersionMap(nil), 0)
		require.NoError(t, ibs.TouchAccount(addr))
		return finalizedWrites(t, ibs, rules)
	}
	existingReader := func() StateReader {
		empty := accounts.NewAccount()
		return &accountStateReader{
			accounts: map[accounts.Address]*accounts.Account{addr: &empty},
		}
	}
	afterPriorWrites := func(t *testing.T, prior *WriteSet) *IntraBlockState {
		t.Helper()
		vm := NewVersionMap(nil)
		vm.FlushVersionedWrites(prior, true, "")
		return newVersionedTestState(t, existingReader(), vm, 1)
	}

	t.Run("serial baseline emits a delete", func(t *testing.T) {
		ibs := New(&minimalStateReader{})
		t.Cleanup(func() { ibs.Release(false) })
		require.NoError(t, ibs.TouchAccount(addr))
		ibs.SoftFinalise()
		writer := new(deleteRecordingWriter)
		require.NoError(t, ibs.MakeWriteSet(spuriousDragon, writer))
		require.Contains(t, writer.deleted, addr)
	})

	t.Run("versioned path withholds the created account", func(t *testing.T) {
		writes := touchedWrites(t, &minimalStateReader{}, spuriousDragon)
		_, hasAddress := writes.GetAddress(addr)
		require.False(t, hasAddress, "the create must not reach the write-set")
		_, hasBalance := writes.GetBalance(addr)
		require.False(t, hasBalance)
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete, "nothing was published, so nothing to delete")
	})

	t.Run("next tx does not observe the account", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.FlushVersionedWrites(touchedWrites(t, &minimalStateReader{}, spuriousDragon), true, "")

		next := newVersionedTestState(t, &minimalStateReader{}, vm, 1)
		exists, err := next.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("versioned path deletes an existing empty account", func(t *testing.T) {
		writes := touchedWrites(t, existingReader(), spuriousDragon)
		_, hasBalance := writes.GetBalance(addr)
		require.False(t, hasBalance)
		deleted, ok := writes.GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, deleted.Val)
	})

	t.Run("next tx does not observe an existing empty account", func(t *testing.T) {
		reader := existingReader()
		vm := NewVersionMap(nil)
		vm.FlushVersionedWrites(touchedWrites(t, reader, spuriousDragon), true, "")

		next := newVersionedTestState(t, reader, vm, 1)
		exists, err := next.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("existing account reset to empty emits a delete", func(t *testing.T) {
		ibs := newVersionedTestState(t, existingReader(), NewVersionMap(nil), 0)
		require.NoError(t, ibs.CreateAccount(addr, false))

		writes := finalizedWrites(t, ibs, spuriousDragon)
		deleted, ok := writes.GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, deleted.Val)
	})

	t.Run("existing account funded after the touch is retained", func(t *testing.T) {
		ibs := newVersionedTestState(t, existingReader(), NewVersionMap(nil), 0)
		require.NoError(t, ibs.TouchAccount(addr))
		require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(1), tracing.BalanceChangeTransfer))

		writes := finalizedWrites(t, ibs, spuriousDragon)
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		balance, ok := writes.GetBalance(addr)
		require.True(t, ok)
		require.Equal(t, uint64(1), balance.Val.Uint64())
	})

	t.Run("prior balance survives a current nonce write", func(t *testing.T) {
		prior := &WriteSet{}
		prior.SetBalance(addr, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}},
			Val:         *uint256.NewInt(1),
		})
		ibs := afterPriorWrites(t, prior)
		require.NoError(t, ibs.SetNonce(addr, 0, tracing.NonceChangeUnspecified))

		writes := finalizedWrites(t, ibs, spuriousDragon)
		_, deleted := writes.GetSelfDestruct(addr)
		require.False(t, deleted)
		reads := ibs.VersionedReads()
		balanceRead, ok := reads.GetBalance(addr)
		require.True(t, ok)
		require.Equal(t, MapRead, balanceRead.Source)
		require.Equal(t, Version{TxIndex: 0}, balanceRead.Version)
		nonce, ok := writes.GetNonce(addr)
		require.True(t, ok)
		require.Zero(t, nonce.Val)
	})

	t.Run("prior nonce survives a current balance write", func(t *testing.T) {
		prior := &WriteSet{}
		prior.SetNonce(addr, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: Version{TxIndex: 0}},
			Val:         1,
		})
		ibs := afterPriorWrites(t, prior)
		require.NoError(t, ibs.SetBalance(addr, uint256.Int{}, tracing.BalanceChangeUnspecified))

		writes := finalizedWrites(t, ibs, spuriousDragon)
		_, deleted := writes.GetSelfDestruct(addr)
		require.False(t, deleted)
		reads := ibs.VersionedReads()
		nonceRead, ok := reads.GetNonce(addr)
		require.True(t, ok)
		require.Equal(t, MapRead, nonceRead.Source)
		require.Equal(t, Version{TxIndex: 0}, nonceRead.Version)
		balance, ok := writes.GetBalance(addr)
		require.True(t, ok)
		require.True(t, balance.Val.IsZero())
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

		ibs := newVersionedTestState(t, &minimalStateReader{}, vm, 1)
		require.NoError(t, ibs.TouchAccount(addr))

		writes := finalizedWrites(t, ibs, spuriousDragon)
		deleted, ok := writes.GetSelfDestruct(addr)
		require.True(t, ok)
		require.True(t, deleted.Val)
	})

	t.Run("pre-SpuriousDragon keeps the touched account", func(t *testing.T) {
		writes := touchedWrites(t, &minimalStateReader{}, &chain.Rules{})
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		_, hasAddress := writes.GetAddress(addr)
		require.True(t, hasAddress)
	})

	t.Run("pre-SpuriousDragon keeps an existing empty account", func(t *testing.T) {
		writes := touchedWrites(t, existingReader(), &chain.Rules{})
		_, hasDelete := writes.GetSelfDestruct(addr)
		require.False(t, hasDelete)
		_, hasBalance := writes.GetBalance(addr)
		require.True(t, hasBalance)
	})
}

// A non-empty code hash or nonce decides EIP-161 emptiness without reading the
// balance, avoiding conflicts with unrelated balance updates.
func TestVersionedWritesEmptinessKeepsReadFootprint(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	key := accounts.InternKey(common.HexToHash("0x01"))

	contract := accounts.NewAccount()
	contract.Balance = *uint256.NewInt(1000)
	contract.Nonce = 1
	reader := &accountStateReader{
		accounts: map[accounts.Address]*accounts.Account{addr: &contract},
	}

	ibs := newVersionedTestState(t, reader, NewVersionMap(nil), 1)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(7)))

	writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	require.NoError(t, err)
	_, deleted := writes.GetSelfDestruct(addr)
	require.False(t, deleted)

	reads := ibs.VersionedReads()
	_, hasBalanceRead := reads.GetBalance(addr)
	require.False(t, hasBalanceRead, "a storage write must not depend on the account's balance")
}

func TestVersionedWritesStorageOnlyClearsExistingEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	key := accounts.InternKey(common.HexToHash("0x01"))

	empty := accounts.NewAccount()
	reader := &accountStateReader{
		accounts: map[accounts.Address]*accounts.Account{addr: &empty},
	}

	ibs := newVersionedTestState(t, reader, NewVersionMap(nil), 1)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(7)))

	writes, err := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	require.NoError(t, err)
	deleted, ok := writes.GetSelfDestruct(addr)
	require.True(t, ok)
	require.True(t, deleted.Val)
}

func TestLateAccountDeleteInvalidatesExistenceRead(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xfee"))
	empty := accounts.NewAccount()
	reader := &accountStateReader{
		accounts: map[accounts.Address]*accounts.Account{addr: &empty},
	}
	versionMap := NewVersionMap(nil)
	ibs := newVersionedTestState(t, reader, versionMap, 1)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)

	io := NewVersionedIO(1)
	io.RecordReads(Version{BlockNum: 1, TxIndex: 1}, ibs.VersionedReads())

	deleteWrites := &WriteSet{}
	deleteWrites.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{
			Address: addr,
			Path:    SelfDestructPath,
			Version: Version{BlockNum: 1, TxIndex: 0},
		},
		Val: true,
	})
	versionMap.FlushVersionedWrites(deleteWrites, true, "")

	require.Equal(t, VersionInvalid, versionMap.ValidateVersion(1, io, validateEqualVersion, false, ""))
}
