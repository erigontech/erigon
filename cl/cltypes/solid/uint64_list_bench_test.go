package solid

import (
	"math/rand"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

const benchmarkUint64ListLength = 1_140_000

var benchmarkUint64ListRoot [32]byte

func BenchmarkUint64ListWarmHash(b *testing.B) {
	indices := make([]int, 600)
	rng := rand.New(rand.NewSource(1))
	for i := range indices {
		indices[i] = rng.Intn(benchmarkUint64ListLength)
	}

	b.Run("fulu_cached", func(b *testing.B) {
		list := newBenchmarkUint64List()
		benchmarkUint64ListRoot = benchmarkUint64ListHash(b, list.HashSSZ)
		b.ReportAllocs()
		b.ResetTimer()
		for n := range b.N {
			setBenchmarkUint64ListValues(list, indices, n)
			benchmarkUint64ListRoot = benchmarkUint64ListHash(b, list.HashSSZ)
		}
	})

	b.Run("gloas_cached", func(b *testing.B) {
		list := newBenchmarkUint64List()
		benchmarkUint64ListRoot = benchmarkUint64ListHash(b, list.HashSSZProgressive)
		b.ReportAllocs()
		b.ResetTimer()
		for n := range b.N {
			setBenchmarkUint64ListValues(list, indices, n)
			benchmarkUint64ListRoot = benchmarkUint64ListHash(b, list.HashSSZProgressive)
		}
	})

	b.Run("gloas_uncached", func(b *testing.B) {
		list := newBenchmarkUint64List()
		b.ReportAllocs()
		b.ResetTimer()
		for n := range b.N {
			setBenchmarkUint64ListValues(list, indices, n)
			benchmarkUint64ListRoot = benchmarkUint64ListHash(b, func() ([32]byte, error) {
				return merkle_tree.ProgressiveBasicListRoot(list.Bytes(), uint64(list.Length()))
			})
		}
	})
}

func BenchmarkUint64ListProgressiveCopy(b *testing.B) {
	source := newBenchmarkUint64List()
	benchmarkUint64ListRoot = benchmarkUint64ListHash(b, source.HashSSZProgressive)
	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			target := NewUint64ListSSZ(1 << 40)
			source.CopyTo(target)
			runtime.KeepAlive(target)
		}
	})
	b.Run("reused", func(b *testing.B) {
		target := NewUint64ListSSZ(1 << 40)
		source.CopyTo(target)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			source.CopyTo(target)
		}
		runtime.KeepAlive(target)
	})
}

func BenchmarkUint64ListProgressiveCacheMemory(b *testing.B) {
	b.StopTimer()
	list := newBenchmarkUint64List()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	benchmarkUint64ListRoot = benchmarkUint64ListHash(b, list.HashSSZProgressive)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(list)
	b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "cache-bytes/list")
	b.StartTimer()
	for range b.N {
		runtime.KeepAlive(list)
	}
}

func newBenchmarkUint64List() *uint64ListSSZ {
	list := NewUint64ListSSZ(1 << 40).(*uint64ListSSZ)
	for i := range benchmarkUint64ListLength {
		list.Append(uint64(i))
	}
	return list
}

func setBenchmarkUint64ListValues(list *uint64ListSSZ, indices []int, iteration int) {
	for i, index := range indices {
		list.Set(index, uint64(iteration+i+1))
	}
}

func benchmarkUint64ListHash(b *testing.B, hash func() ([32]byte, error)) [32]byte {
	b.Helper()
	root, err := hash()
	if err != nil {
		b.Fatal(err)
	}
	return root
}
