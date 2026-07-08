package commitment

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkTouchCollectCarried_ModeDirect times value-carrying collection alone
// (TouchPlainKeyDirect with a pre-built update, keyHasherNoop).
func BenchmarkTouchCollectCarried_ModeDirect(b *testing.B) {
	for _, n := range []int{5_000, 100_000, 500_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			rnd := rand.New(rand.NewSource(1))
			keys := make([]string, n)
			for i := range keys {
				k := make([]byte, 128)
				for j := 0; j < 128; j += 8 {
					binary.BigEndian.PutUint64(k[j:], rnd.Uint64())
				}
				for j := range k {
					k[j] &= 0x0f
				}
				keys[i] = string(k)
			}
			upd := NewCarriedStorageUpdate([]byte("v"))

			ut := NewUpdates(ModeDirect, b.TempDir(), keyHasherNoop)
			defer ut.Close()

			b.ReportAllocs()
			for b.Loop() {
				for _, k := range keys {
					ut.TouchPlainKeyDirect(k, upd)
				}
				b.StopTimer()
				ut.Reset()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkTouchCollect_ModeDirect times the collection side alone (TouchPlainKey
// with a pre-hashed key, keyHasherNoop) so the backend cost is not drowned by keccak.
func BenchmarkTouchCollect_ModeDirect(b *testing.B) {
	for _, n := range []int{5_000, 100_000, 500_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			rnd := rand.New(rand.NewSource(1))
			keys := make([]string, n)
			for i := range keys {
				k := make([]byte, 128)
				for j := 0; j < 128; j += 8 {
					binary.BigEndian.PutUint64(k[j:], rnd.Uint64())
				}
				for j := range k {
					k[j] &= 0x0f
				}
				keys[i] = string(k)
			}
			val := []byte("v")

			// Long-lived Updates, reset between rounds: steady-state collection like
			// a node reusing one SharedDomains across commitment cycles.
			ut := NewUpdates(ModeDirect, b.TempDir(), keyHasherNoop)
			defer ut.Close()

			b.ReportAllocs()
			for b.Loop() {
				for _, k := range keys {
					ut.TouchPlainKey(k, val, ut.TouchStorage)
				}
				b.StopTimer()
				ut.Reset()
				b.StartTimer()
			}
		})
	}
}
