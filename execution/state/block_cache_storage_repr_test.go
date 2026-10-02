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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockStateCacheStorageRepresentation(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	token := accounts.InternAddress(common.HexToAddress("0x70ce"))
	present := accounts.InternKey(common.HexToHash("0x01"))
	absent := accounts.InternKey(common.HexToHash("0x02"))
	updated := accounts.InternKey(common.HexToHash("0x03"))
	cleared := accounts.InternKey(common.HexToHash("0x04"))
	created := accounts.InternKey(common.HexToHash("0x05"))

	put := func(k accounts.StorageKey, v uint64, txNum uint64) {
		t.Helper()
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, []byte(storageCacheKey(token, k)), uint256.NewInt(v).Bytes(), txNum, nil))
	}
	put(present, 0x1122, 1)
	put(updated, 0x3344, 1)
	put(cleared, 0x5566, 1)

	cache := NewBlockStateCache()
	getter := domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	worker := NewCachedReaderV3(getter, cache)
	current := NewCurrentCachedReaderV3(getter, cache)
	history := NewHistoryReaderV3WithBlockCache(tx, domains, cache, 100)

	type slot struct {
		val uint64
		ok  bool
	}
	read := func(r StateReader, k accounts.StorageKey) slot {
		t.Helper()
		v, ok, err := r.ReadAccountStorage(token, k)
		require.NoError(t, err)
		require.True(t, v.IsUint64())
		return slot{v.Uint64(), ok}
	}

	require.Equal(t, slot{0x1122, true}, read(worker, present))
	require.Equal(t, slot{0, false}, read(worker, absent))
	require.Equal(t, slot{0x3344, true}, read(worker, updated))
	put(present, 0x7777, 2)
	put(absent, 0x8888, 2)
	require.Equal(t, slot{0x1122, true}, read(worker, present))
	require.Equal(t, slot{0, false}, read(worker, absent))
	require.Equal(t, slot{0x1122, true}, read(current, present))
	require.Equal(t, slot{0, false}, read(current, absent))

	apply := func(txNum uint64, writes ...any) {
		t.Helper()
		require.NoError(t, newWriteSet(writes...).Apply(domains, tx, 1, txNum, nil, &chain.Rules{}, cache, false))
	}
	requireCurrent := func(k accounts.StorageKey, want slot) {
		t.Helper()
		require.Equal(t, want, read(current, k))
		require.Equal(t, want, read(history, k))
	}

	apply(10, storageWrite(token, updated, 0x10), storageWrite(token, cleared, 0), storageWrite(token, created, 0x50))
	requireCurrent(updated, slot{0x10, true})
	requireCurrent(cleared, slot{0, false})
	requireCurrent(created, slot{0x50, true})
	require.Equal(t, slot{0x3344, true}, read(worker, updated))

	apply(11, storageWrite(token, updated, 0x11))
	requireCurrent(updated, slot{0x11, true})

	require.NoError(t, cache.Flush(domains, tx))

	asOf := func(k accounts.StorageKey, txNum uint64) []byte {
		t.Helper()
		enc, _, err := domains.GetAsOf(kv.StorageDomain, []byte(storageCacheKey(token, k)), txNum)
		require.NoError(t, err)
		return enc
	}
	require.Equal(t, uint256.NewInt(0x10).Bytes(), asOf(updated, 11))
	require.Equal(t, uint256.NewInt(0x11).Bytes(), asOf(updated, 12))
	require.Empty(t, asOf(cleared, 12))
	require.Equal(t, uint256.NewInt(0x50).Bytes(), asOf(created, 12))
}
