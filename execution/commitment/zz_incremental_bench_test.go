package commitment

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

func Benchmark_Incremental_LegacyV2_vs_Hex(b *testing.B) {
	for _, c := range whaleCases() {
		pk, upds := buildWhaleCorpus(c.opts)
		dk, du := buildDelta(pk, upds, 500, 4242)
		b.Run(fmt.Sprintf("%s/delta%d", c.name, len(dk)), func(b *testing.B) {

			b.Run("LegacyV2", func(b *testing.B) {
				tr := buildLegacyTrie(pk, upds)
				tr.Hash()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_ = applyDeltaLegacy(tr, dk, du)
				}
			})

			b.Run("HexSeq", func(b *testing.B) {
				ctx := context.Background()
				ms := NewMockState(b)
				require.NoError(b, ms.applyPlainUpdates(pk, upds))
				hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
				u1 := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, pk, upds)
				_, err := hph.Process(ctx, u1, "", nil, WarmupConfig{})
				require.NoError(b, err)
				u1.Close()
				require.NoError(b, ms.applyPlainUpdates(dk, du))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					u := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, dk, du)
					_, err := hph.Process(ctx, u, "", nil, WarmupConfig{})
					require.NoError(b, err)
					u.Close()
				}
			})

			for _, w := range []int{1, runtime.NumCPU()} {
				b.Run(fmt.Sprintf("HexPar-w%d", w), func(b *testing.B) {
					ctx := context.Background()
					ms := NewMockState(b)
					ms.SetConcurrentCommitment(true)
					require.NoError(b, ms.applyPlainUpdates(pk, upds))
					pph := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
					defer pph.Release()
					pph.SetNumWorkers(w)
					u1 := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, pk, upds)
					_, err := pph.Process(ctx, u1, "", nil, WarmupConfig{})
					require.NoError(b, err)
					u1.Close()
					require.NoError(b, ms.applyPlainUpdates(dk, du))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						u := WrapKeyUpdates(b, ModeParallel, KeyToHexNibbleHash, dk, du)
						_, err := pph.Process(ctx, u, "", nil, WarmupConfig{})
						require.NoError(b, err)
						u.Close()
					}
				})
			}
		})
	}
}
