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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// committedStorage is a write-once, immutable pre-block view backed by a
// lock-free sync.Map. These pin the value semantics the reader relies on — in
// particular a cached empty slot (ok=true, nil value) must stay distinct from
// an uncached miss (ok=false).
func TestBlockStateCache_CommittedStorage_Semantics(t *testing.T) {
	t.Parallel()

	cache := NewBlockStateCache()
	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	key := accounts.InternKey(common.Hash{0x22})

	got, ok := cache.GetCommittedStorage(addr, key)
	require.False(t, ok, "uncached slot must miss")
	require.Nil(t, got)

	cache.PutCommittedStorage(addr, key, []byte{0x01, 0x02})
	got, ok = cache.GetCommittedStorage(addr, key)
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02}, got)

	emptyKey := accounts.InternKey(common.Hash{0x33})
	cache.PutCommittedStorage(addr, emptyKey, nil)
	got, ok = cache.GetCommittedStorage(addr, emptyKey)
	require.True(t, ok, "a cached empty slot must report ok=true, not a miss")
	require.Nil(t, got)

	got, ok = cache.GetCurrentStorage(addr, key)
	require.True(t, ok, "GetCurrentStorage must fall back to committed when unwritten")
	require.Equal(t, []byte{0x01, 0x02}, got)
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
	val := func(ai, ki int) []byte { return []byte{byte(ai + 1), byte(ki + 1)} }

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
