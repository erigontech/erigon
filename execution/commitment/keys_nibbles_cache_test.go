package commitment

import (
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKeyToHexNibbleHashCached_MatchesUncached verifies the cached variant is
// byte-identical to KeyToHexNibbleHash regardless of key type or ordering — a
// cache hit and a cache miss must both reproduce the uncached result.
func TestKeyToHexNibbleHashCached_MatchesUncached(t *testing.T) {
	t.Parallel()

	t.Run("account_keys", func(t *testing.T) {
		var c addrHashCache
		for i := 0; i < 100; i++ {
			addr := make([]byte, length.Addr)
			addr[0] = byte(i)
			addr[19] = byte(i * 7)
			assert.Equal(t, KeyToHexNibbleHash(addr), keyToHexNibbleHashCached(addr, &c), "account key %d", i)
		}
	})

	t.Run("storage_keys", func(t *testing.T) {
		var c addrHashCache
		for i := 0; i < 100; i++ {
			key := make([]byte, 52)
			key[0] = byte(i % 30)
			key[19] = byte(i)
			key[20] = byte(i)
			key[51] = byte(i * 3)
			assert.Equal(t, KeyToHexNibbleHash(key), keyToHexNibbleHashCached(key, &c), "storage key %d", i)
		}
	})

	// Whale: one address, many slots — the reuse target.
	t.Run("whale_storage", func(t *testing.T) {
		var c addrHashCache
		addr := make([]byte, length.Addr)
		addr[0], addr[1], addr[19] = 0xDE, 0xAD, 0xBE
		for slot := 0; slot < 1000; slot++ {
			key := make([]byte, 52)
			copy(key[:20], addr)
			key[20] = byte(slot >> 8)
			key[51] = byte(slot)
			assert.Equal(t, KeyToHexNibbleHash(key), keyToHexNibbleHashCached(key, &c), "whale slot %d", slot)
		}
	})

	// Account/storage interleaving forces cache misses and address changes;
	// the cache must never leak a stale prefix across an address change.
	t.Run("interleaved", func(t *testing.T) {
		var c addrHashCache
		for i := 0; i < 200; i++ {
			addr := make([]byte, length.Addr)
			addr[0] = byte(i % 4) // only 4 distinct addresses, non-consecutive
			addr[19] = byte(i % 4)
			assert.Equal(t, KeyToHexNibbleHash(addr), keyToHexNibbleHashCached(addr, &c), "acct %d", i)

			key := make([]byte, 52)
			copy(key[:20], addr)
			key[20] = byte(i)
			key[51] = byte(i)
			assert.Equal(t, KeyToHexNibbleHash(key), keyToHexNibbleHashCached(key, &c), "storage %d", i)
		}
	})
}

// TestAddrHashCache_ReuseAndInvalidation pins the cache state transitions the
// reuse depends on: populated on first storage slot, retained across same-addr
// slots, replaced on an address change, cleared by reset.
func TestAddrHashCache_ReuseAndInvalidation(t *testing.T) {
	t.Parallel()
	var c addrHashCache
	require.False(t, c.valid)

	mkKey := func(addrByte, slot byte) []byte {
		key := make([]byte, 52)
		key[0] = addrByte
		key[51] = slot
		return key
	}

	keyToHexNibbleHashCached(mkKey(0xAA, 0), &c)
	require.True(t, c.valid)
	require.Equal(t, byte(0xAA), c.addr[0])
	firstNibs := c.nibs

	// Same address, different slot: prefix retained unchanged.
	keyToHexNibbleHashCached(mkKey(0xAA, 1), &c)
	require.Equal(t, firstNibs, c.nibs)

	// Different address: prefix replaced.
	keyToHexNibbleHashCached(mkKey(0xBB, 0), &c)
	require.Equal(t, byte(0xBB), c.addr[0])
	require.NotEqual(t, firstNibs, c.nibs)

	// Account key does not touch the cache.
	acctBefore := c.addr
	keyToHexNibbleHashCached(make([]byte, length.Addr), &c)
	require.Equal(t, acctBefore, c.addr)

	c.reset()
	require.False(t, c.valid)
}

// TestUpdatesHashKey_MatchesHasher verifies hashKey reproduces the configured
// hasher across every mode that hashes plain keys.
func TestUpdatesHashKey_MatchesHasher(t *testing.T) {
	t.Parallel()
	keys := [][]byte{
		{0x01, 0x02},
		make([]byte, length.Addr),
		func() []byte { k := make([]byte, 52); k[0], k[51] = 0x11, 0x22; return k }(),
	}
	for _, mode := range []Mode{ModeDirect, ModeUpdate, ModeParallel} {
		u := NewUpdates(mode, t.TempDir(), KeyToHexNibbleHash)
		require.True(t, u.addrCacheReuse, "cache must be enabled for the nibblizing hasher")
		for _, k := range keys {
			assert.Equal(t, KeyToHexNibbleHash(k), u.hashKey(k), "mode=%d key=%x", mode, k)
		}
	}
}

func TestHasherReusesAddrPrefix(t *testing.T) {
	t.Parallel()
	assert.True(t, hasherReusesAddrPrefix(KeyToHexNibbleHash))
	assert.False(t, hasherReusesAddrPrefix(keyHasherNoop))
}

func benchKeys(numAddr, slotsPer int) [][]byte {
	keys := make([][]byte, 0, numAddr*slotsPer)
	for a := 0; a < numAddr; a++ {
		for s := 0; s < slotsPer; s++ {
			k := make([]byte, 52)
			k[0] = byte(a)
			k[1] = byte(a >> 8)
			k[19] = byte(a * 7)
			k[20] = byte(s >> 8)
			k[51] = byte(s)
			keys = append(keys, k)
		}
	}
	return keys
}

var benchWorkloads = []struct {
	name    string
	numAddr int
	slots   int
}{
	{"whale_1x1000", 1, 1000},
	{"spread5_5x200", 5, 200},
	{"spread100_100x10", 100, 10},
	{"scatter1000_1000x1", 1000, 1},
}

func Benchmark_KeyNibbleHash_NoCache(b *testing.B) {
	for _, w := range benchWorkloads {
		keys := benchKeys(w.numAddr, w.slots)
		b.Run(w.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, k := range keys {
					_ = KeyToHexNibbleHash(k)
				}
			}
		})
	}
}

func Benchmark_KeyNibbleHash_Cached(b *testing.B) {
	for _, w := range benchWorkloads {
		keys := benchKeys(w.numAddr, w.slots)
		b.Run(w.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var c addrHashCache
				for _, k := range keys {
					_ = keyToHexNibbleHashCached(k, &c)
				}
			}
		})
	}
}
