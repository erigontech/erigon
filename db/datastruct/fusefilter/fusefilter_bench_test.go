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

// benchBuild streams n random uint64 hashes through the writer's AddHash
// (matching the production recsplit path: hashes flow one-at-a-time into the
// off-heap mmap temp file), then drops the rng + AddHash state, runs a GC,
// and only THEN starts the peak sampler and the bench timer for Build().
//
// Why no pre-allocated []uint64 slice: if the bench keeps a slice of all keys
// alive, that slice (8 MB at 1M keys, 80 MB at 10M) becomes a fixed floor
// shared by both paths and masks the real Build-internal heap delta. After
// `keys = nil; runtime.GC()` the compiler may still keep the backing array
// reachable from a stack slot until the surrounding function returns, which
// is exactly what made earlier readings show ~78 MB peak at n=10M sharded
// (a number the real-file tooltest at 339M keys then disproved by reporting
// 32 MB — i.e. ~1/256 of plain). Streaming kills the floor entirely.
func benchBuild(b *testing.B, sharded bool, n int) {
	b.Helper()

	var totalFileBytes uint64
	var totalPeakHeap uint64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		fp := filepath.Join(dir, "f")

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
			rng := rand.New(rand.NewPCG(1, 2))
			for j := 0; j < n; j++ {
				if err := w.AddHash(rng.Uint64()); err != nil {
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
			rng := rand.New(rand.NewPCG(1, 2))
			for j := 0; j < n; j++ {
				if err := w.AddHash(rng.Uint64()); err != nil {
					b.Fatal(err)
				}
			}
			plainW = w
		}

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

// BenchmarkBuild isolates the Build phase: input is streamed into the writer
// (off-heap mmap), then dropped before the sampler starts. peak_heap_mb is
// the heap residency of Build itself — exactly what causes the OOM in
// production.
//
// Compare paths with `go test -bench=BenchmarkBuild -run=^$ -benchtime=3x -count=5`
// and pipe through benchstat. Sizes deliberately cap at 10M so it runs on a
// dev box; absolute numbers extrapolate ~linearly to 3B (the bloatnet OOM
// scenario).
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

// BenchmarkBuildLarge runs the 10M-key variant. Gated by -short so the regular
// test-short suite skips it. Run with `go test -bench=BenchmarkBuildLarge
// -run=^$ -benchtime=1x ./db/datastruct/fusefilter/`.
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
// absent keys) on a prebuilt filter. Hot-cache only — the cold path is dominated by
// IO, not by Reader vs ReaderSharded code.
func benchLookup(b *testing.B, sharded bool, n int) {
	b.Helper()
	dir := b.TempDir()
	fp := filepath.Join(dir, "f")

	// Build the filter by streaming hashes (no materialised []uint64).
	if sharded {
		w, err := NewWriterSharded(fp)
		if err != nil {
			b.Fatal(err)
		}
		w.DisableFsync()
		rng := rand.New(rand.NewPCG(1, 2))
		for j := 0; j < n; j++ {
			if err := w.AddHash(rng.Uint64()); err != nil {
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
		rng := rand.New(rand.NewPCG(1, 2))
		for j := 0; j < n; j++ {
			if err := w.AddHash(rng.Uint64()); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Build(); err != nil {
			b.Fatal(err)
		}
		w.Close()
	}

	// Small fixed-size probe buffer (16k uint64 = 128 KB) is enough to defeat
	// branch-predictor caching of the inner ContainsHash and re-used cyclically
	// via `i%len(probes)`. Keeping it bounded means lookup bench heap stays tiny
	// regardless of n.
	const probesN = 16 * 1024
	probes := make([]uint64, probesN)
	{
		rng := rand.New(rand.NewPCG(7, 11))
		for i := range probes {
			probes[i] = rng.Uint64()
		}
	}

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
