package commitment

// Measurement-only (uncommitted): regular single-threaded trie (ModeDirect) vs
// the streaming committer on the SAME corpora, for an apples-to-apples report.

import (
	"runtime"
	"testing"
)

func Benchmark_RegularVsStreaming(b *testing.B) {
	for _, c := range streamingBenchCorpora() {
		pk, upds := c.pk, c.upds
		// regular single-threaded sequential trie
		b.Run(c.name+"/Direct", func(b *testing.B) { runDirectBench(b, pk, upds) })
		// streaming, fold-at-Process (production path: no background scheduler)
		b.Run(c.name+"/Streaming-batch", func(b *testing.B) {
			runStreamingOverlapBench(b, pk, upds, 0, false)
		})
		// streaming, background overlap (test-only scheduler) under a synthetic exec cost
		b.Run(c.name+"/Streaming-overlap-cpu2000", func(b *testing.B) {
			runStreamingOverlapBench(b, pk, upds, 2000, true)
		})
	}
}

// Benchmark_TrieVariants_1MWhale: regular / parallel / streaming, all on the same
// ~1M-key corpus where a few accounts hold huge storage shards (3 whales
// 750k/150k/5k + 95k single-slot accounts). Run with
// `ERIGON_CMT_MOUNT=1 ERIGON_CMT_DEEP=1` so the Parallel arm takes the mount +
// deep-storage-fan-out path.
func Benchmark_TrieVariants_1MWhale(b *testing.B) {
	pk, upds := build1MWhaleCorpus(b)
	b.Logf("corpus keys=%d", len(pk))
	ncpu := runtime.NumCPU()
	b.Run("Regular-Direct", func(b *testing.B) { runDirectBench(b, pk, upds) })
	b.Run("Parallel-mount-deep", func(b *testing.B) { runParallelBench(b, pk, upds, ncpu) })
	b.Run("Streaming-batch", func(b *testing.B) { runStreamingOverlapBench(b, pk, upds, 0, false) })
	b.Run("Streaming-overlap-cpu2000", func(b *testing.B) { runStreamingOverlapBench(b, pk, upds, 2000, true) })
}
