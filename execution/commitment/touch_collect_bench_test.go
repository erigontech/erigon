package commitment

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
)

func benchTouchKeys(n int) []string {
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
	return keys
}

func BenchmarkTouchCollect_ModeDirect(b *testing.B) {
	for _, n := range []int{5_000, 100_000, 500_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			keys := benchTouchKeys(n)
			val := []byte("v")

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

func BenchmarkTouchDirect(b *testing.B) {
	for _, mode := range []Mode{ModeUpdate, ModeDirect, ModeParallel} {
		for _, n := range []int{5_000, 100_000} {
			b.Run(fmt.Sprintf("%s/n=%d", mode, n), func(b *testing.B) {
				keys := benchTouchKeys(n)
				ut := NewUpdates(mode, b.TempDir(), keyHasherNoop)
				defer ut.Close()

				b.ReportAllocs()
				for b.Loop() {
					// Update is built inline: hoisting it out of the loop would stop
					// this benchmark from measuring what it exists to measure.
					for i, k := range keys {
						ut.TouchPlainKeyDirect(k, &Update{
							Flags:   BalanceUpdate,
							Balance: *uint256.NewInt(uint64(i)),
						})
					}
					b.StopTimer()
					ut.Reset()
					b.StartTimer()
				}
			})
		}
	}
}

func BenchmarkTouchDirectMerge(b *testing.B) {
	for _, n := range []int{5_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			keys := benchTouchKeys(n)
			ut := NewUpdates(ModeUpdate, b.TempDir(), keyHasherNoop)
			defer ut.Close()

			b.ReportAllocs()
			for b.Loop() {
				for i, k := range keys {
					ut.TouchPlainKeyDirect(k, &Update{
						Flags:   BalanceUpdate,
						Balance: *uint256.NewInt(uint64(i)),
					})
					ut.TouchPlainKeyDirect(k, &Update{
						Flags: NonceUpdate,
						Nonce: uint64(i),
					})
				}
				b.StopTimer()
				ut.Reset()
				b.StartTimer()
			}
		})
	}
}
