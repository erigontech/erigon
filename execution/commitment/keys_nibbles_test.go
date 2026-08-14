package commitment

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

func TestAccountKey(t *testing.T) {
	t.Parallel()
	accKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233")
	nibblizedHashedKey := KeyToHexNibbleHash(accKey)

	require.Equal(t, hexutil.MustDecode("0x0b070f0f040d05000b0d01080705010601060800020a0400060c09040b0109000f010a030f0d040f0c08020b00060d0b04000904030e000101090c050e080b0c"), nibblizedHashedKey)
}

func TestStorageKey(t *testing.T) {
	t.Parallel()
	accKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233")
	storageKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
	nibblizedHashedKey := KeyToHexNibbleHash(append(accKey, storageKey...))

	require.Equal(t, hexutil.MustDecode(
		"0x0b070f0f040d05000b0d01080705010601060800020a0400060c09040b0109000f010a030f0d040f0c08020b00060d0b04000904030e000101090c050e080b0c"+
			"020d040906010f0e0803000401080b02000a070601050b0d060003030f0e01040100060a090303090500060c0c0905020c0b0b040e0d0007030a03000807030c"), nibblizedHashedKey)
}

func BenchmarkKeyToHexNibbleHash(b *testing.B) {
	key := make([]byte, 20)
	for i := range key {
		key[i] = byte(i)
	}
	for b.Loop() {
		KeyToHexNibbleHash(key)
	}
}

// TestKeyToHexNibbleHashCached_MatchesUncached verifies the cached variant is
// byte-identical to KeyToHexNibbleHash regardless of key type or ordering — a
// cache hit and a cache miss must both reproduce the uncached result.
func TestKeyToHexNibbleHashCached_MatchesUncached(t *testing.T) {
	t.Parallel()

	t.Run("account_keys", func(t *testing.T) {
		var c addrHashCache
		for i := range 100 {
			addr := make([]byte, length.Addr)
			addr[0] = byte(i)
			addr[19] = byte(i * 7)
			assert.Equal(t, KeyToHexNibbleHash(addr), keyToHexNibbleHashCached(addr, &c), "account key %d", i)
		}
	})

	t.Run("storage_keys", func(t *testing.T) {
		var c addrHashCache
		for i := range 100 {
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
		for slot := range 1000 {
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
		for i := range 200 {
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
	for a := range numAddr {
		for s := range slotsPer {
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

// Shared helpers for commitment tests: brute-force address generation keyed by
// hashed-key nibble and a mock trie-context factory.

// maxAddrSearchIters bounds the brute-force address search helpers below so a
// broken search space (e.g. hash function change) produces a descriptive panic
// instead of an infinite hang. 1M iterations is well above the expected work:
// a single-nibble hit averages ~16 iters; a 16-bit shared-prefix hit averages
// ~65k, both comfortably under the cap.
const maxAddrSearchIters = 1 << 20

// nibbleSeedKey is the composite cache key for findAddressForNibble.
type nibbleSeedKey struct{ nibble, seed int }

// nibbleAddressCache caches brute-forced addresses keyed by (nibble, seed) to
// avoid repeated keccak work across tests and ensure each seed always returns
// the same deterministic address regardless of call order.
var (
	nibbleAddressCacheMu sync.Mutex
	nibbleAddressCache   = make(map[nibbleSeedKey][]byte)
)

// findAddressForNibble brute-force searches for a 20-byte address whose
// keccak256 first nibble (upper 4 bits of hash[0]) matches targetNibble.
// seed controls the starting point for the search; each unique seed produces
// a different address. Results are cached globally.
func findAddressForNibble(targetNibble int, seed int) []byte {
	if targetNibble < 0 || targetNibble > 0xf {
		panic(fmt.Sprintf("findAddressForNibble: nibble %d out of range [0,15]", targetNibble))
	}
	key := nibbleSeedKey{targetNibble, seed}

	nibbleAddressCacheMu.Lock()
	if cached, ok := nibbleAddressCache[key]; ok {
		nibbleAddressCacheMu.Unlock()
		return append([]byte(nil), cached...) // copy so callers can't mutate the shared cache
	}
	nibbleAddressCacheMu.Unlock()

	// Brute force: we encode a counter into the first 8 bytes of a 20-byte
	// address and increment until keccak(addr)[0] >> 4 == targetNibble.
	var addr [20]byte
	// Use seed * large prime to separate search spaces for different seeds.
	counter := uint64(seed) * 1_000_003
	for range maxAddrSearchIters {
		binary.BigEndian.PutUint64(addr[:8], counter)
		h := crypto.Keccak256(addr[:])
		if int(h[0]>>4) == targetNibble {
			result := make([]byte, 20)
			copy(result, addr[:])

			nibbleAddressCacheMu.Lock()
			nibbleAddressCache[key] = result
			nibbleAddressCacheMu.Unlock()
			return append([]byte(nil), result...)
		}
		counter++
	}
	panic(fmt.Sprintf("findAddressForNibble(nibble=%d, seed=%d): exceeded %d iterations", targetNibble, seed, maxAddrSearchIters))
}

// findAddressForHexPrefix brute-force searches for a 20-byte address whose keccak256
// hashed-key nibbles start with the given nibble prefix (each entry in [0,15]). seed
// separates search spaces. Used to force accounts to share a multi-nibble hashed prefix
// (e.g. an extension-topped subtree under one root nibble).
func findAddressForHexPrefix(nibblePrefix []byte, seed int) []byte {
	for i, n := range nibblePrefix {
		if n > 0xf {
			panic(fmt.Sprintf("findAddressForHexPrefix: nibble %d at %d out of range [0,15]", n, i))
		}
	}
	var addr [20]byte
	counter := uint64(seed)*2_654_435_761 + 1
	for range maxAddrSearchIters {
		binary.BigEndian.PutUint64(addr[:8], counter)
		h := crypto.Keccak256(addr[:])
		match := true
		for i, n := range nibblePrefix {
			var hn byte
			if i%2 == 0 {
				hn = h[i/2] >> 4
			} else {
				hn = h[i/2] & 0xf
			}
			if hn != n {
				match = false
				break
			}
		}
		if match {
			result := make([]byte, 20)
			copy(result, addr[:])
			return result
		}
		counter++
	}
	panic(fmt.Sprintf("findAddressForHexPrefix(%v, seed=%d): exceeded %d iterations", nibblePrefix, seed, maxAddrSearchIters))
}

// mockTrieCtxFactory returns a TrieContextFactory that always returns the
// given MockState and a no-op cleanup.
func mockTrieCtxFactory(ms *MockState) TrieContextFactory {
	return func(context.Context) (PatriciaContext, func()) {
		return ms, func() {}
	}
}

// addrHex returns the hex-encoded string of a 20-byte address (no 0x prefix),
// suitable for passing to UpdateBuilder methods.
func addrHex(addr []byte) string {
	return hex.EncodeToString(addr)
}
