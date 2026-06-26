package commitment

import (
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKeyToHexNibbleHashWithCache_Correctness verifies that the cached variant
// produces byte-identical output to the uncached KeyToHexNibbleHash for
// account keys, storage keys, and whale storage patterns.
func TestKeyToHexNibbleHashWithCache_Correctness(t *testing.T) {
	t.Parallel()

	cache := make(map[[20]byte][64]byte)

	// Account keys (len <= 20)
	t.Run("account_keys", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			addr := make([]byte, length.Addr)
			addr[0] = byte(i)
			addr[19] = byte(i * 7)
			got := KeyToHexNibbleHashWithCache(addr, cache)
			want := KeyToHexNibbleHash(addr)
			assert.Equal(t, want, got, "account key %d mismatch", i)
		}
	})

	// Storage keys (len > 20) — various addresses and slots
	t.Run("storage_keys", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := make([]byte, 52) // 20 addr + 32 slot
			key[0] = byte(i % 30)   // some address reuse
			key[19] = byte(i)
			key[20] = byte(i) // slot
			key[51] = byte(i * 3)
			got := KeyToHexNibbleHashWithCache(key, cache)
			want := KeyToHexNibbleHash(key)
			assert.Equal(t, want, got, "storage key %d mismatch", i)
		}
	})

	// Whale storage: one address, many slots — primary target of the optimization
	t.Run("whale_storage", func(t *testing.T) {
		whaleCache := make(map[[20]byte][64]byte)
		addr := make([]byte, length.Addr)
		addr[0] = 0xDE
		addr[1] = 0xAD
		addr[19] = 0xBE

		for slot := 0; slot < 1000; slot++ {
			key := make([]byte, 52)
			copy(key[:20], addr)
			key[20] = byte(slot >> 8)
			key[51] = byte(slot)

			got := KeyToHexNibbleHashWithCache(key, whaleCache)
			want := KeyToHexNibbleHash(key)
			assert.Equal(t, want, got, "whale storage slot %d mismatch", slot)

			// First slot populates cache; subsequent should hit
			if slot == 0 {
				require.Len(t, whaleCache, 1, "cache should have exactly 1 entry after first slot")
			}
		}
		// Only 1 addr cached despite 1000 calls
		assert.Equal(t, 1, len(whaleCache), "whale cache should have exactly 1 entry")
	})

	// Mixed: 50 addresses × 50 slots each
	t.Run("mixed_keys", func(t *testing.T) {
		cache2 := make(map[[20]byte][64]byte)
		for a := 0; a < 50; a++ {
			for s := 0; s < 50; s++ {
				key := make([]byte, 52)
				key[0] = byte(a)
				key[19] = byte(a * 3)
				key[20] = byte(s >> 8)
				key[51] = byte(s)

				got := KeyToHexNibbleHashWithCache(key, cache2)
				want := KeyToHexNibbleHash(key)
				assert.Equal(t, want, got, "mixed addr=%d slot=%d mismatch", a, s)
			}
		}
		assert.Equal(t, 50, len(cache2), "should cache exactly 50 unique addresses")
	})
}

func Benchmark_KeyNibbleHash_NoCache(b *testing.B) {
	// Whale storage: 1 addr × 1000 slots
	keys := make([][]byte, 1000)
	addr := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	for i := range keys {
		keys[i] = make([]byte, 52)
		copy(keys[i][:20], addr)
		keys[i][20] = byte(i >> 8)
		keys[i][51] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = KeyToHexNibbleHash(key)
		}
	}
}

func Benchmark_KeyNibbleHash_WithCache_Whale(b *testing.B) {
	// Whale storage: 1 addr × 1000 slots
	keys := make([][]byte, 1000)
	addr := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	for i := range keys {
		keys[i] = make([]byte, 52)
		copy(keys[i][:20], addr)
		keys[i][20] = byte(i >> 8)
		keys[i][51] = byte(i)
	}
	cache := make(map[[20]byte][64]byte)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = KeyToHexNibbleHashWithCache(key, cache)
		}
	}
}

func Benchmark_KeyNibbleHash_WithCache_Spread5(b *testing.B) {
	// 5 addresses × 200 slots each
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = make([]byte, 52)
		addrIdx := byte(i / 200)
		slotIdx := i % 200
		keys[i][0] = addrIdx
		keys[i][19] = addrIdx * 7
		keys[i][20] = byte(slotIdx >> 8)
		keys[i][51] = byte(slotIdx)
	}
	cache := make(map[[20]byte][64]byte)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = KeyToHexNibbleHashWithCache(key, cache)
		}
	}
}

func Benchmark_KeyNibbleHash_WithCache_Spread100(b *testing.B) {
	// 100 addresses × 10 slots each
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = make([]byte, 52)
		addrIdx := byte(i / 10)
		slotIdx := i % 10
		keys[i][0] = addrIdx
		keys[i][19] = addrIdx * 3
		keys[i][20] = byte(slotIdx >> 8)
		keys[i][51] = byte(slotIdx)
	}
	cache := make(map[[20]byte][64]byte)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = KeyToHexNibbleHashWithCache(key, cache)
		}
	}
}

func Benchmark_KeyNibbleHash_NoCache_Spread100(b *testing.B) {
	// 100 addresses × 10 slots each — baseline
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = make([]byte, 52)
		addrIdx := byte(i / 10)
		slotIdx := i % 10
		keys[i][0] = addrIdx
		keys[i][19] = addrIdx * 3
		keys[i][20] = byte(slotIdx >> 8)
		keys[i][51] = byte(slotIdx)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = KeyToHexNibbleHash(key)
		}
	}
}
