// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

type legacyTrieBenchmarkCase struct {
	name string
	opts whaleOpts
}

func legacyTrieBenchmarkCases() []legacyTrieBenchmarkCase {
	return []legacyTrieBenchmarkCase{
		{"100K", bigAccountWhale(100_000)},
		{"1M", whale1M()},
	}
}

func Benchmark_LegacyTrie_vs_HexCorpus(b *testing.B) {
	for _, c := range legacyTrieBenchmarkCases() {
		pk, upds := buildWhaleCorpus(c.opts)
		b.Run(c.name, func(b *testing.B) {
			b.Run("LegacyTrie", func(b *testing.B) {
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

func Benchmark_LegacyTrie_ResidentMemory(b *testing.B) {
	for _, c := range legacyTrieBenchmarkCases() {
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

func Benchmark_LegacyTrie_vs_HexDelta(b *testing.B) {
	for _, c := range legacyTrieBenchmarkCases() {
		pk, upds := buildWhaleCorpus(c.opts)
		dk, du := buildDelta(pk, upds, 500, 4242)
		b.Run(fmt.Sprintf("%s/delta%d", c.name, len(dk)), func(b *testing.B) {
			b.Run("LegacyTrie", func(b *testing.B) {
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
