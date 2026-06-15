package existence_test

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/db/datastruct/existence"
)

func benchContainsHash(b *testing.B, n int, useFuse bool) {
	b.Helper()
	fp := filepath.Join(b.TempDir(), "filter")

	f, err := existence.NewFilter(uint64(n), fp, useFuse)
	if err != nil {
		b.Fatal(err)
	}
	f.DisableFsync()
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < n; i++ {
		if err := f.AddHash(rng.Uint64()); err != nil {
			b.Fatal(err)
		}
	}
	if err := f.Build(); err != nil {
		b.Fatal(err)
	}

	r, err := existence.OpenFilter(fp, useFuse)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	const probesN = 16 * 1024
	probes := make([]uint64, probesN)
	rng2 := rand.New(rand.NewPCG(7, 11))
	for i := range probes {
		probes[i] = rng2.Uint64()
	}
	for _, k := range probes[:1024] {
		_ = r.ContainsHash(k)
	}

	b.ResetTimer()
	b.ReportAllocs()
	var hits int
	for i := 0; i < b.N; i++ {
		if r.ContainsHash(probes[i%probesN]) {
			hits++
		}
	}
	b.StopTimer()
	_ = hits
}

func BenchmarkContainsHash(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		for _, useFuse := range []bool{false, true} {
			label := "bloom"
			if useFuse {
				label = "fuse"
			}
			b.Run(fmt.Sprintf("%s/n=%d", label, n), func(b *testing.B) {
				benchContainsHash(b, n, useFuse)
			})
		}
	}
}
