package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBranchCache_TwoTier(t *testing.T) {
	cache := NewBranchCache(1024)

	// Depth 0 (root): compact key [0x00]
	rootKey := HexNibblesToCompactBytes(nil)
	require.Equal(t, []byte{0x00}, rootKey)
	cache.Put(rootKey, []byte("root-data"))

	data, found := cache.Get(rootKey)
	require.True(t, found)
	require.Equal(t, []byte("root-data"), data)

	// Depth 1: nibble 0xA → compact key [0x1A]
	key1 := HexNibblesToCompactBytes([]byte{0xa})
	require.Equal(t, []byte{0x1a}, key1)
	cache.Put(key1, []byte("depth1-data"))

	data, found = cache.Get(key1)
	require.True(t, found)
	require.Equal(t, []byte("depth1-data"), data)

	// Depth 2: nibbles [3,7] → compact key [0x00, 0x37]
	key2 := HexNibblesToCompactBytes([]byte{3, 7})
	require.Equal(t, []byte{0x00, 0x37}, key2)
	cache.Put(key2, []byte("depth2-data"))

	data, found = cache.Get(key2)
	require.True(t, found)
	require.Equal(t, []byte("depth2-data"), data)

	// Depth 3: nibbles [1,2,3] → compact key [0x11, 0x23]
	key3 := HexNibblesToCompactBytes([]byte{1, 2, 3})
	cache.Put(key3, []byte("depth3-data"))

	data, found = cache.Get(key3)
	require.True(t, found)
	require.Equal(t, []byte("depth3-data"), data)

	// Depth 4: nibbles [1,2,3,4] → compact key [0x00, 0x12, 0x34]
	key4 := HexNibblesToCompactBytes([]byte{1, 2, 3, 4})
	cache.Put(key4, []byte("depth4-data"))

	data, found = cache.Get(key4)
	require.True(t, found)
	require.Equal(t, []byte("depth4-data"), data)

	// Depth 5: nibbles [1,2,3,4,5] → goes to LRU (compact key 3 bytes, odd → not pinned)
	key5 := HexNibblesToCompactBytes([]byte{1, 2, 3, 4, 5})
	cache.Put(key5, []byte("lru-data"))

	data, found = cache.Get(key5)
	require.True(t, found)
	require.Equal(t, []byte("lru-data"), data)

	// Verify counters (6 Gets above, all hits)
	require.Equal(t, uint64(6), cache.Hits())
	require.Equal(t, uint64(0), cache.Misses())

	// Miss on non-existent pinned key
	_, found = cache.Get(HexNibblesToCompactBytes([]byte{0xb}))
	require.False(t, found)
	require.Equal(t, uint64(1), cache.Misses())

	// Invalidate pinned key
	cache.Invalidate(key1)
	_, found = cache.Get(key1)
	require.False(t, found)

	// Contains
	require.True(t, cache.Contains(key2))
	require.False(t, cache.Contains(key1))

	// Pinned len: root + depth2 + depth3 + depth4 = 4 (depth1 was invalidated)
	require.Equal(t, 4, cache.PinnedLen())

	// Total len: 4 pinned + 1 LRU (depth5) = 5
	require.Equal(t, 5, cache.Len())

	// Clear
	cache.Clear()
	require.Equal(t, 0, cache.Len())
	require.Equal(t, uint64(0), cache.Hits())
}

func TestBranchCache_AllDepth1Nibbles(t *testing.T) {
	cache := NewBranchCache(1024)

	// All 16 depth-1 entries should be independently addressable
	for nibble := byte(0); nibble < 16; nibble++ {
		key := HexNibblesToCompactBytes([]byte{nibble})
		cache.Put(key, []byte{nibble})
	}

	for nibble := byte(0); nibble < 16; nibble++ {
		key := HexNibblesToCompactBytes([]byte{nibble})
		data, found := cache.Get(key)
		require.True(t, found, "nibble %d not found", nibble)
		require.Equal(t, []byte{nibble}, data)
	}

	require.Equal(t, 16, cache.PinnedLen())
}

func TestBranchCache_DataCopy(t *testing.T) {
	cache := NewBranchCache(1024)

	// Verify Put makes a copy (caller can reuse buffer)
	key := HexNibblesToCompactBytes([]byte{5})
	buf := []byte("original")
	cache.Put(key, buf)

	buf[0] = 'X' // modify original buffer
	data, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, []byte("original"), data) // cache should have the copy
}

func TestBranchCache_Depth4Indexing(t *testing.T) {
	cache := NewBranchCache(1024)

	// Depth 4 even: nibbles [a,b,c,d] → compact [0x00, 0xab, 0xcd]
	// Index into t4: (0xab << 8) | 0xcd = 0xabcd
	key := HexNibblesToCompactBytes([]byte{0xa, 0xb, 0xc, 0xd})
	require.Equal(t, []byte{0x00, 0xab, 0xcd}, key)
	cache.Put(key, []byte("abcd"))

	data, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, []byte("abcd"), data)

	// Different depth-4 key should not collide
	key2 := HexNibblesToCompactBytes([]byte{0xf, 0xe, 0xd, 0xc})
	cache.Put(key2, []byte("fedc"))

	data, found = cache.Get(key2)
	require.True(t, found)
	require.Equal(t, []byte("fedc"), data)

	// Original still intact
	data, found = cache.Get(key)
	require.True(t, found)
	require.Equal(t, []byte("abcd"), data)
}
