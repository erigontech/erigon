package blake2b

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"
)

func TestF(t *testing.T) {
	for i, test := range testVectorsF {
		t.Run(fmt.Sprintf("test vector %v", i), func(t *testing.T) {
			//toEthereumTestCase(test)

			h := test.hIn
			F(&h, test.m, test.c, test.f, test.rounds)

			if !reflect.DeepEqual(test.hOut, h) {
				t.Errorf("Unexpected result\nExpected: [%#x]\nActual:   [%#x]\n", test.hOut, h)
			}
		})
	}
}

type testVector struct {
	hIn    [8]uint64
	m      [16]uint64
	c      [2]uint64
	f      bool
	rounds uint32
	hOut   [8]uint64
}

// https://tools.ietf.org/html/rfc7693#appendix-A
func randomF(r *rand.Rand) (h [8]uint64, m [16]uint64, c [2]uint64, final bool) {
	for i := range h {
		h[i] = r.Uint64()
	}
	for i := range m {
		m[i] = r.Uint64()
	}
	c[0], c[1] = r.Uint64(), r.Uint64()
	return h, m, c, r.Intn(2) == 0
}

func TestFChunkedMatchesGeneric(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	rounds := []uint32{
		maxAsmRounds - 1, maxAsmRounds, maxAsmRounds + 1,
		2*maxAsmRounds - 1, 2 * maxAsmRounds, 2*maxAsmRounds + 1,
		3*maxAsmRounds + 7, 100003,
	}
	for _, n := range rounds {
		for trial := range 4 {
			h, m, c, final := randomF(r)
			var flag uint64
			if final {
				flag = 0xFFFFFFFFFFFFFFFF
			}
			want := h
			fGeneric(&want, &m, c[0], c[1], flag, uint64(n))

			got := h
			F(&got, m, c, final, n)

			if got != want {
				t.Fatalf("rounds=%d trial=%d: got %#x, want %#x", n, trial, got, want)
			}
		}
	}
}

func maxStopTheWorld() time.Duration {
	s := []metrics.Sample{{Name: "/sched/pauses/stopping/gc:seconds"}}
	metrics.Read(s)
	h := s[0].Value.Float64Histogram()
	var max float64
	for i, count := range h.Counts {
		if count > 0 {
			max = h.Buckets[i+1]
		}
	}
	return time.Duration(max * float64(time.Second))
}

// The rounds argument of the BLAKE2b F precompile comes from calldata and is
// priced at one gas per round, so a single transaction can ask for tens of
// millions of rounds. Assembly is never preemptible, so an unchunked call
// blocks every stop-the-world for as long as it runs.
func TestFLongRoundsIsPreemptible(t *testing.T) {
	if testing.Short() {
		t.Skip("runs several million rounds")
	}
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("needs GOMAXPROCS >= 2 to observe a stopping pause")
	}
	if !useAVX2 && !useAVX && !useSSE4 {
		t.Skip("no assembly on this machine, so the round loop is already preemptible Go")
	}

	var garbage []byte
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			garbage = make([]byte, 1<<20)
			runtime.GC()
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(200 * time.Millisecond)

	var h [8]uint64
	var m [16]uint64
	var c [2]uint64
	const rounds = 8_000_000
	for range 8 {
		F(&h, m, c, false, rounds)
	}

	time.Sleep(300 * time.Millisecond) // let a blocked stop-the-world finish and be recorded
	got := maxStopTheWorld()
	close(stop)
	<-done
	_ = garbage

	const budget = 5 * time.Millisecond
	if got > budget {
		t.Fatalf("max GC stop-the-world stopping pause %v over budget %v: F(rounds=%d) is not preemptible",
			got, budget, rounds)
	}
	t.Logf("max GC stop-the-world stopping pause: %v", got)
}

var testVectorsF = []testVector{
	{
		hIn: [8]uint64{
			0x6a09e667f2bdc948, 0xbb67ae8584caa73b,
			0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
			0x510e527fade682d1, 0x9b05688c2b3e6c1f,
			0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
		},
		m: [16]uint64{
			0x0000000000636261, 0x0000000000000000, 0x0000000000000000,
			0x0000000000000000, 0x0000000000000000, 0x0000000000000000,
			0x0000000000000000, 0x0000000000000000, 0x0000000000000000,
			0x0000000000000000, 0x0000000000000000, 0x0000000000000000,
			0x0000000000000000, 0x0000000000000000, 0x0000000000000000,
			0x0000000000000000,
		},
		c:      [2]uint64{3, 0},
		f:      true,
		rounds: 12,
		hOut: [8]uint64{
			0x0D4D1C983FA580BA, 0xE9F6129FB697276A, 0xB7C45A68142F214C,
			0xD1A2FFDB6FBB124B, 0x2D79AB2A39C5877D, 0x95CC3345DED552C2,
			0x5A92F1DBA88AD318, 0x239900D4ED8623B9,
		},
	},
}
