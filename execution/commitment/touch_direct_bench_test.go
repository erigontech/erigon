package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
)

// benchDirectKeys builds n pre-nibblized keys (keyHasherNoop is used so keccak
// does not drown the collection cost).
func benchDirectKeys(n int) []string {
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

// BenchmarkTouchDirect mimics WriteSet.TouchUpdates: one Update built at the
// call site per write. Building it inline is deliberate — the caller-side
// escape behaviour is part of what is measured.
func BenchmarkTouchDirect(b *testing.B) {
	for _, mode := range []Mode{ModeUpdate, ModeDirect, ModeParallel} {
		for _, n := range []int{5_000, 100_000} {
			b.Run(fmt.Sprintf("%s/n=%d", mode, n), func(b *testing.B) {
				keys := benchDirectKeys(n)
				ut := NewUpdates(mode, b.TempDir(), keyHasherNoop)
				defer ut.Close()

				b.ReportAllocs()
				for b.Loop() {
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

// BenchmarkTouchDirectMerge touches every key twice, exercising the ModeUpdate
// merge-into-existing branch as well as the insert branch.
func BenchmarkTouchDirectMerge(b *testing.B) {
	for _, n := range []int{5_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			keys := benchDirectKeys(n)
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

// TestTouchPlainKeyDirect_UpdateDoesNotEscape pins the escape-analysis property
// this call site depends on: the caller builds an Update per write, so the
// parameter must not escape or every write costs a heap allocation. Taking the
// address of any field of update outside a value-taking helper breaks this.
func TestTouchPlainKeyDirect_UpdateDoesNotEscape(t *testing.T) {
	ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	defer ut.Close()

	key := string(bytes.Repeat([]byte{1}, 128))
	// Warm the dedup map so the measured calls hit the already-touched path and
	// only the caller's Update is left to allocate.
	ut.TouchPlainKeyDirect(key, &Update{Flags: BalanceUpdate})

	allocs := testing.AllocsPerRun(100, func() {
		ut.TouchPlainKeyDirect(key, &Update{
			Flags:   BalanceUpdate,
			Balance: *uint256.NewInt(7),
		})
	})
	if allocs != 0 {
		t.Fatalf("TouchPlainKeyDirect allocated %v times per call, want 0", allocs)
	}
}
