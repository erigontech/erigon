package keccakx4

import (
	"maps"
	"math/rand/v2"
	"strconv"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
)

func randomMsgs(r *rand.Rand, n, maxLen int) [][]byte {
	msgs := make([][]byte, n)
	for i := range msgs {
		msgs[i] = make([]byte, r.IntN(maxLen+1))
		for j := range msgs[i] {
			msgs[i][j] = byte(r.Uint32())
		}
	}
	return msgs
}

func checkSumMany(t *testing.T, msgs [][]byte) {
	t.Helper()
	out := make([][32]byte, len(msgs))
	SumMany256(msgs, out)
	for i, msg := range msgs {
		if want := keccak.Sum256(msg); out[i] != want {
			t.Fatalf("msg %d of %d (len %d): got %x, want %x", i, len(msgs), len(msg), out[i], want)
		}
	}
}

// forEachX4 runs f once per four-lane implementation this CPU can run, and once without any.
func forEachX4(t *testing.T, f func(t *testing.T)) {
	impls := map[string]func(*[100]uint64, *[4]*byte){"scalar": nil}
	maps.Copy(impls, availableX4())
	for name, impl := range impls {
		t.Run(name, func(t *testing.T) {
			old := x4
			x4 = impl
			t.Cleanup(func() { x4 = old })
			f(t)
		})
	}
}

func TestSumMany256(t *testing.T) {
	forEachX4(t, func(t *testing.T) {
		r := rand.New(rand.NewPCG(1, 2))
		for n := range 21 {
			t.Run(strconv.Itoa(n), func(t *testing.T) {
				for range 20 {
					checkSumMany(t, randomMsgs(r, n, 4*rate))
				}
			})
		}
		edges := [][]byte{
			nil,
			{},
			make([]byte, rate-1), make([]byte, rate), make([]byte, rate+1),
			make([]byte, 2*rate), make([]byte, 100*rate+3), make([]byte, 1),
		}
		checkSumMany(t, edges)
		checkSumMany(t, append(edges, edges...))
	})
}

// A message much longer than the rest keeps one lane busy while the others take new messages.
func TestSumMany256MixedLengths(t *testing.T) {
	forEachX4(t, func(t *testing.T) {
		r := rand.New(rand.NewPCG(3, 4))
		msgs := randomMsgs(r, 50, 2*rate)
		msgs[7] = make([]byte, 300*rate+11)
		msgs[30] = make([]byte, 40*rate)
		checkSumMany(t, msgs)
	})
}

func TestX4SelfTestDetectsWrongPermutation(t *testing.T) {
	broken := func(a *[100]uint64, blocks *[4]*byte) { a[0] ^= 1 }
	if x4SelfTest(broken) {
		t.Fatal("self-test accepted a wrong permutation")
	}
}

func FuzzSumMany256(f *testing.F) {
	f.Add([]byte("seed"), uint16(5))
	f.Fuzz(func(t *testing.T, data []byte, split uint16) {
		var msgs [][]byte
		step := int(split%300) + 1
		for len(data) > 0 {
			n := min(step, len(data))
			msgs = append(msgs, data[:n])
			data = data[n:]
			step = step*7%301 + 1
		}
		checkSumMany(t, msgs)
	})
}

func BenchmarkSumMany256(b *testing.B) {
	r := rand.New(rand.NewPCG(5, 6))
	msgs := randomMsgs(r, 300, 4*rate)
	out := make([][32]byte, len(msgs))
	b.Run("Sum256", func(b *testing.B) {
		for b.Loop() {
			for i, m := range msgs {
				out[i] = keccak.Sum256(m)
			}
		}
	})
	b.Run("SumMany256", func(b *testing.B) {
		for b.Loop() {
			SumMany256(msgs, out)
		}
	})
}
