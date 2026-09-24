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

package commitment

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func makeV4CacheKey(tag byte, addrHash []byte, path []byte) []byte {
	key := append([]byte{tag}, addrHash...)
	key = append(key, make([]byte, (len(path)+1)/2)...)
	for i, nibble := range path {
		if i&1 == 0 {
			key[1+len(addrHash)+i/2] = nibble << 4
		} else {
			key[1+len(addrHash)+i/2] |= nibble
		}
	}
	return append(key, byte(len(path)))
}

func v4CachePath(depth int) []byte {
	path := make([]byte, depth)
	for i := range path {
		path[i] = byte((i*3 + depth + 1) & 0x0f)
	}
	return path
}

func TestBranchCacheV4RoutingTiers(t *testing.T) {
	addrHash := bytes.Repeat([]byte{0x5a}, 32)
	for _, tc := range []struct {
		name string
		tag  byte
		addr []byte
	}{
		{name: "account", tag: 0x40},
		{name: "storage", tag: 0x41, addr: addrHash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for depth := range 7 {
				t.Run(string(rune('0'+depth)), func(t *testing.T) {
					c := NewBranchCache(100)
					defer c.Close()

					key := makeV4CacheKey(tc.tag, tc.addr, v4CachePath(depth))
					value := []byte{tc.tag, byte(depth), 0xa5}
					c.Put(key, value, 0, 0)

					got, _, ok := c.Get(key)
					require.True(t, ok)
					require.Equal(t, value, got)

					path := v4CachePath(depth)
					if tc.tag == 0x40 && depth <= 4 && depth <= int(c.maxDepth) {
						var packed [4]byte
						for i := 0; i < len(path) && i < len(packed); i++ {
							packed[i] = path[i]
						}
						slot := c.v4AccountTrunk.slot(&packed, depth, false)
						require.NotNil(t, slot)
						if slot != nil {
							require.NotNil(t, slot.Load())
						}
						return
					}
					require.Equal(t, 1, c.tailLen())
					require.NotNil(t, c.tail.Load())
					_, ok = c.tail.Load().Get(maphash.Hash(key))
					require.True(t, ok)
					require.Nil(t, c.pinned.Load())
				})
			}
		})
	}
}

func TestBranchCacheV4RoutingCollision(t *testing.T) {
	addrHash := bytes.Repeat([]byte{0x37}, 32)
	c := NewBranchCache(100)
	defer c.Close()

	type entry struct {
		key    []byte
		value  []byte
		pinned bool
	}
	var entries []entry
	for _, tc := range []struct {
		tag  byte
		addr []byte
	}{
		{tag: 0x40},
		{tag: 0x41, addr: addrHash},
	} {
		for depth := range 7 {
			key := makeV4CacheKey(tc.tag, tc.addr, v4CachePath(depth))
			value := []byte{tc.tag, byte(depth), byte(depth + 17)}
			entries = append(entries, entry{key: key, value: value, pinned: tc.tag == 0x41})
			if tc.tag == 0x41 {
				c.PinEntry(key, value, 0, 0)
			} else {
				c.Put(key, value, 0, 0)
			}
		}
	}

	for _, want := range entries {
		got, _, ok := c.Get(want.key)
		require.True(t, ok)
		require.Equal(t, want.value, got)
	}
}

func TestBranchCacheV4DispatchPrecondition(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		c := NewBranchCache(100)
		defer c.Close()

		v1Key := nibbles.HexToCompact(nil)
		v4Key := makeV4CacheKey(0x40, nil, nil)
		c.Put(v1Key, []byte("v1-root"), 0, 0)
		c.Put(v4Key, []byte("v4-root"), 0, 0)

		got, _, ok := c.Get(v1Key)
		require.True(t, ok)
		require.Equal(t, []byte("v1-root"), got)
		got, _, ok = c.Get(v4Key)
		require.True(t, ok)
		require.Equal(t, []byte("v4-root"), got)
	})

	t.Run("account-trunk", func(t *testing.T) {
		c := NewBranchCache(100)
		defer c.Close()

		path := []byte{0xa, 0xb}
		v1Key := nibbles.HexToCompact(path)
		v4Key := makeV4CacheKey(0x40, nil, path)
		c.Put(v1Key, []byte("v1-account"), 0, 0)
		c.Put(v4Key, []byte("v4-account"), 0, 0)

		got, _, ok := c.Get(v1Key)
		require.True(t, ok)
		require.Equal(t, []byte("v1-account"), got)
		got, _, ok = c.Get(v4Key)
		require.True(t, ok)
		require.Equal(t, []byte("v4-account"), got)
	})

	t.Run("storage-trunk", func(t *testing.T) {
		c := NewBranchCache(100)
		defer c.Close()

		addrHash := bytes.Repeat([]byte{0x37}, 32)
		path := []byte{0xa, 0xb}
		v1Nibbles := make([]byte, 0, 66)
		for _, b := range addrHash {
			v1Nibbles = append(v1Nibbles, b>>4, b&0x0f)
		}
		v1Key := nibbles.HexToCompact(append(v1Nibbles, path...))
		v4Key := makeV4CacheKey(0x41, addrHash, path)
		c.PinEntry(v1Key, []byte("v1-storage"), 0, 0)
		c.PinEntry(v4Key, []byte("v4-storage"), 0, 0)

		got, _, ok := c.Get(v1Key)
		require.True(t, ok)
		require.Equal(t, []byte("v1-storage"), got)
		got, _, ok = c.Get(v4Key)
		require.True(t, ok)
		require.Equal(t, []byte("v4-storage"), got)
	})

	for depth := range 65 {
		path := make([]byte, depth)
		for first := range 16 {
			if len(path) > 0 {
				path[0] = byte(first)
			}
			compact := nibbles.HexToCompact(path)
			require.NotEqual(t, byte(0x40), compact[0])
			require.NotEqual(t, byte(0x41), compact[0])
			require.NotEqual(t, byte(0x42), compact[0])
		}
	}
	require.True(t, IsCommitmentStateKey([]byte{0x42}))
	require.False(t, IsCommitmentStateKey([]byte{0x42, 0x00}))
}
