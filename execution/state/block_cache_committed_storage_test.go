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
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockStateCache_CommittedStorage_Semantics(t *testing.T) {
	t.Parallel()

	cache := NewBlockStateCache()
	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	key := accounts.InternKey(common.Hash{0x22})

	got, ok := cache.GetCommittedStorage(addr, key)
	require.False(t, ok, "uncached slot must miss")
	require.True(t, got.IsZero())

	cache.PutCommittedStorage(addr, key, *uint256.NewInt(0x0102))
	got, ok = cache.GetCommittedStorage(addr, key)
	require.True(t, ok)
	require.Equal(t, *uint256.NewInt(0x0102), got)

	emptyKey := accounts.InternKey(common.Hash{0x33})
	cache.PutCommittedStorage(addr, emptyKey, uint256.Int{})
	got, ok = cache.GetCommittedStorage(addr, emptyKey)
	require.True(t, ok, "a cached empty slot must report ok=true, not a miss")
	require.True(t, got.IsZero())

	got, ok = cache.GetCurrentStorage(addr, key)
	require.True(t, ok, "GetCurrentStorage must fall back to committed when unwritten")
	require.Equal(t, *uint256.NewInt(0x0102), got)
}

// All worker goroutines of a block share one cache and read committed storage
// concurrently; the lock-free path must be race-free and never lose a value.
func TestBlockStateCache_CommittedStorage_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	const nAddrs, nKeys = 8, 32
	addrList := make([]accounts.Address, nAddrs)
	keyList := make([]accounts.StorageKey, nKeys)
	for i := range addrList {
		addrList[i] = accounts.InternAddress(common.Address{byte(i + 1)})
	}
	for i := range keyList {
		keyList[i] = accounts.InternKey(common.Hash{byte(i + 1)})
	}
	// write-once value, deterministic in (addr,key) so reads can be checked.
	val := func(ai, ki int) uint256.Int { return *uint256.NewInt(uint64(ai+1)<<8 | uint64(ki+1)) }

	cache := NewBlockStateCache()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 2000 {
				ai := (g + i) % nAddrs
				ki := (g*7 + i) % nKeys
				a, k := addrList[ai], keyList[ki]
				cache.PutCommittedStorage(a, k, val(ai, ki))
				if got, ok := cache.GetCommittedStorage(a, k); ok {
					require.Equal(t, val(ai, ki), got)
				}
				cache.GetCurrentStorage(a, k)
			}
		}(g)
	}
	wg.Wait()
}
