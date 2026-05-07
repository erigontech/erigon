package fusefilter

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// peakSampler polls runtime.MemStats.HeapAlloc in a tight loop and tracks the maximum
// observed value. It exists so the benchmark can attribute the well-known OOM in
// xorfilter.NewBinaryFuse (t2hash + reverseOrder buffers, ~10x bytes-per-key at peak)
// to a specific Build call rather than relying on b.AllocedBytesPerOp which only
// reflects total allocs, not concurrent peak residency.
type peakSampler struct {
	stop chan struct{}
	done chan struct{}
	max  atomic.Uint64
}

func startPeakSampler(every time.Duration) *peakSampler {
	p := &peakSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		var ms runtime.MemStats
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-p.stop:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > p.max.Load() {
					p.max.Store(ms.HeapAlloc)
				}
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > p.max.Load() {
					p.max.Store(ms.HeapAlloc)
				}
			}
		}
	}()
	return p
}

func (p *peakSampler) Stop() uint64 {
	close(p.stop)
	<-p.done
	return p.max.Load()
}

func benchKeys(n int, seed1, seed2 uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed1, seed2))
	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = rng.Uint64()
	}
	return keys
}

// benchBuild runs a single Build of either Writer or WriterSharded over n random keys,
// reporting wall time, peak heap, allocs and resulting file size as benchmark metrics.
//
// Each iteration of b.N starts on a freshly-GC'd heap and a fresh peak sampler, so
// the reported peak_heap_mb is the max heap residency observed during that single
// Build call (not aggregated across iterations).
func benchBuild(b *testing.B, sharded bool, n int) {
	b.Helper()
	keys := benchKeys(n, 1, 2)

	var totalFileBytes uint64
	var totalPeakHeap uint64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		fp := filepath.Join(dir, "f")
		runtime.GC() // stable heap baseline before the sampler starts
		sampler := startPeakSampler(500 * time.Microsecond)
		b.StartTimer()

		if sharded {
			w, err := NewWriterSharded(fp)
			if err != nil {
				b.Fatal(err)
			}
			w.DisableFsync()
			for _, k := range keys {
				if err := w.AddHash(k); err != nil {
					b.Fatal(err)
				}
			}
			if err := w.Build(); err != nil {
				b.Fatal(err)
			}
			w.Close()
		} else {
			w, err := NewWriter(fp)
			if err != nil {
				b.Fatal(err)
			}
			w.DisableFsync()
			for _, k := range keys {
				if err := w.AddHash(k); err != nil {
					b.Fatal(err)
				}
			}
			if err := w.Build(); err != nil {
				b.Fatal(err)
			}
			w.Close()
		}

		b.StopTimer()
		peak := sampler.Stop()
		totalPeakHeap += peak

		st, err := os.Stat(fp)
		if err != nil {
			b.Fatal(err)
		}
		totalFileBytes += uint64(st.Size())
		b.StartTimer()
	}
	b.StopTimer()

	if b.N > 0 {
		b.ReportMetric(float64(totalPeakHeap)/float64(b.N)/(1<<20), "peak_heap_mb")
		b.ReportMetric(float64(totalFileBytes)/float64(b.N)/(1<<20), "file_size_mb")
		b.ReportMetric(float64(n), "keys")
	}
}

// benchBuildOnly isolates the Build() call from key generation and AddHash
// ingestion, so peak_heap_mb reflects only what Build itself allocates on the
// Go heap. The pre-generated `keys []uint64` slice (8 MB at n=1M) is dropped
// before Build runs — otherwise it dominates peak_heap and hides the actual
// builder cost we care about.
func benchBuildOnly(b *testing.B, sharded bool, n int) {
	b.Helper()

	var totalFileBytes uint64
	var totalPeakHeap uint64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		fp := filepath.Join(dir, "f")
		keys := benchKeys(n, 1, 2)

		// Ingest keys into the writer's off-heap temp file outside the timer/sampler.
		var (
			plainW *Writer
			shardW *WriterSharded
		)
		if sharded {
			w, err := NewWriterSharded(fp)
			if err != nil {
				b.Fatal(err)
			}
			w.DisableFsync()
			for _, k := range keys {
				if err := w.AddHash(k); err != nil {
					b.Fatal(err)
				}
			}
			shardW = w
		} else {
			w, err := NewWriter(fp)
			if err != nil {
				b.Fatal(err)
			}
			w.DisableFsync()
			for _, k := range keys {
				if err := w.AddHash(k); err != nil {
					b.Fatal(err)
				}
			}
			plainW = w
		}

		// Drop the input slice before Build so it doesn't pollute peak_heap.
		keys = nil
		_ = keys
		runtime.GC()
		sampler := startPeakSampler(500 * time.Microsecond)
		b.StartTimer()

		if sharded {
			if err := shardW.Build(); err != nil {
				b.Fatal(err)
			}
			shardW.Close()
		} else {
			if err := plainW.Build(); err != nil {
				b.Fatal(err)
			}
			plainW.Close()
		}

		b.StopTimer()
		peak := sampler.Stop()
		totalPeakHeap += peak

		st, err := os.Stat(fp)
		if err != nil {
			b.Fatal(err)
		}
		totalFileBytes += uint64(st.Size())
		b.StartTimer()
	}
	b.StopTimer()

	if b.N > 0 {
		b.ReportMetric(float64(totalPeakHeap)/float64(b.N)/(1<<20), "peak_heap_mb")
		b.ReportMetric(float64(totalFileBytes)/float64(b.N)/(1<<20), "file_size_mb")
		b.ReportMetric(float64(n), "keys")
	}
}

// BenchmarkBuildOnly is the apples-to-apples Build-phase comparison: input
// keys are not on the Go heap when the timer/sampler runs. This is what you
// want when reasoning about the OOM, since the OOM is inside Build, not
// AddHash (AddHash writes to a mmap-backed temp file, off-heap).
func BenchmarkBuildOnly(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		for _, sharded := range []bool{false, true} {
			label := "writer"
			if sharded {
				label = "sharded"
			}
			b.Run(label+"/n="+itoaUnderscored(n), func(b *testing.B) {
				benchBuildOnly(b, sharded, n)
			})
		}
	}
}

// Build benchmarks. Run a single size with `-bench=BenchmarkBuild/sharded=false/n=1000000`
// or compare with `go test -bench=BenchmarkBuild -run=^$ -benchtime=3x`.
//
// Sizes are deliberately small enough to fit on a dev box; the goal is to demonstrate
// the *ratio* of peak heap (Writer vs WriterSharded) rather than reproduce the
// 3B-key OOM directly.
func BenchmarkBuild(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		for _, sharded := range []bool{false, true} {
			label := "writer"
			if sharded {
				label = "sharded"
			}
			b.Run(label+"/n="+itoaUnderscored(n), func(b *testing.B) {
				benchBuild(b, sharded, n)
			})
		}
	}
}

// BenchmarkBuildLarge is gated behind -short so `make test-short` skips it.
// Run explicitly with `go test -bench=BenchmarkBuildLarge -run=^$ -benchtime=1x`.
func BenchmarkBuildLarge(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large build benchmark in -short mode")
	}
	const n = 10_000_000
	for _, sharded := range []bool{false, true} {
		label := "writer"
		if sharded {
			label = "sharded"
		}
		b.Run(label+"/n="+itoaUnderscored(n), func(b *testing.B) {
			benchBuild(b, sharded, n)
		})
	}
}

// benchLookup measures the cost of N random ContainsHash probes (mix of present and
// absent keys) on a prebuilt filter. Distinguishes hot-cache (mmap pages already
// resident) from cold; we only run hot here since the cold path is dominated by
// IO, not by Reader vs ReaderSharded code.
func benchLookup(b *testing.B, sharded bool, n int) {
	b.Helper()
	keys := benchKeys(n, 1, 2)
	dir := b.TempDir()
	fp := filepath.Join(dir, "f")

	if sharded {
		w, err := NewWriterSharded(fp)
		if err != nil {
			b.Fatal(err)
		}
		w.DisableFsync()
		for _, k := range keys {
			if err := w.AddHash(k); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Build(); err != nil {
			b.Fatal(err)
		}
		w.Close()
	} else {
		w, err := NewWriter(fp)
		if err != nil {
			b.Fatal(err)
		}
		w.DisableFsync()
		for _, k := range keys {
			if err := w.AddHash(k); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Build(); err != nil {
			b.Fatal(err)
		}
		w.Close()
	}

	probes := benchKeys(2*n, 7, 11)

	type containsReader interface {
		ContainsHash(uint64) bool
	}
	var r containsReader
	if sharded {
		rs, err := NewReaderSharded(fp)
		if err != nil {
			b.Fatal(err)
		}
		defer rs.Close()
		r = rs
	} else {
		rr, err := NewReader(fp)
		if err != nil {
			b.Fatal(err)
		}
		defer rr.Close()
		r = rr
	}

	// Warm pages.
	for _, k := range probes[:min(len(probes), 1024)] {
		_ = r.ContainsHash(k)
	}

	b.ResetTimer()
	b.ReportAllocs()
	var hits int
	for i := 0; i < b.N; i++ {
		if r.ContainsHash(probes[i%len(probes)]) {
			hits++
		}
	}
	b.StopTimer()
	_ = hits
}

func BenchmarkLookup(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		for _, sharded := range []bool{false, true} {
			label := "writer"
			if sharded {
				label = "sharded"
			}
			b.Run(label+"/n="+itoaUnderscored(n), func(b *testing.B) {
				benchLookup(b, sharded, n)
			})
		}
	}
}

// itoaUnderscored renders 1000000 as "1_000_000" for readable bench names.
func itoaUnderscored(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 16)
	for i := 0; n > 0; i++ {
		if i > 0 && i%3 == 0 {
			buf = append(buf, '_')
		}
		buf = append(buf, byte('0'+n%10))
		n /= 10
	}
	if neg {
		buf = append(buf, '-')
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}
