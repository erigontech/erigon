package commitment

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

type whaleCase struct {
	name string
	opts whaleOpts
}

func whaleCases() []whaleCase {
	return []whaleCase{
		{"100K", bigAccountWhale(100_000)},
		{"1M", whale1M()},
	}
}

func Benchmark_LegacyV2_vs_Hex_Whale(b *testing.B) {
	for _, c := range whaleCases() {
		pk, upds := buildWhaleCorpus(c.opts)
		b.Run(c.name, func(b *testing.B) {
			b.Run("LegacyV2", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = buildLegacyTrie(pk, upds).Hash()
				}
			})

			b.Run("HexSeq-foldOnly", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					ms := NewMockState(b)
					require.NoError(b, ms.applyPlainUpdates(pk, upds))
					hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
					u := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, upds)
					b.StartTimer()
					_, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
					b.StopTimer()
					require.NoError(b, err)
					u.Close()
					b.StartTimer()
				}
			})

			b.Run("HexSeq-touchAndFold", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					ms := NewMockState(b)
					require.NoError(b, ms.applyPlainUpdates(pk, upds))
					hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
					b.StartTimer()
					u := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, upds)
					_, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
					b.StopTimer()
					require.NoError(b, err)
					u.Close()
					b.StartTimer()
				}
			})

			for _, w := range []int{1, runtime.NumCPU()} {
				b.Run(fmt.Sprintf("HexPar-w%d-touchAndFold", w), func(b *testing.B) {
					b.ReportAllocs()
					var pph *ParallelPatriciaHashed
					defer func() {
						if pph != nil {
							pph.Release()
						}
					}()
					for b.Loop() {
						b.StopTimer()
						ms := NewMockState(b)
						ms.SetConcurrentCommitment(true)
						require.NoError(b, ms.applyPlainUpdates(pk, upds))
						if pph == nil {
							pph = NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
							pph.SetNumWorkers(w)
						} else {
							pph.SetTrieContextFactory(mockTrieCtxFactory(ms))
							pph.ResetContext(ms)
							pph.SetNumWorkers(w)
						}
						pph.RootTrie().Reset()
						b.StartTimer()
						u := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, upds)
						_, err := pph.Process(context.Background(), u, "", nil, WarmupConfig{})
						b.StopTimer()
						require.NoError(b, err)
						u.Close()
						b.StartTimer()
					}
				})
			}
		})
	}
}

func Benchmark_LegacyV2_Resident(b *testing.B) {
	for _, c := range whaleCases() {
		pk, upds := buildWhaleCorpus(c.opts)
		b.Run(c.name, func(b *testing.B) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			tr := buildLegacyTrie(pk, upds)
			root := tr.Hash()
			runtime.GC()
			runtime.ReadMemStats(&after)
			b.Logf("keys=%d root=%x legacyResidentHeapMB=%.1f", len(pk), root,
				float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))
			runtime.KeepAlive(tr)
		})
	}
}
