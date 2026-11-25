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

package state

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func BenchmarkBeaconStateEncodeSSZ2400k(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			dst := make([]byte, 0, source.EncodingSizeSSZ())
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				dst, err = source.EncodeSSZ(dst[:0])
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkBeaconStateDecodeSSZ2400k(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			encoded, err := source.EncodeSSZ(nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				decoded := New(source.BeaconConfig())
				require.NoError(b, decoded.DecodeSSZ(encoded, int(version.version)))
			}
		})
	}
}

// BenchmarkBeaconStateDecodeSSZ2400kWithReference decodes the source with and without a reference
// that is a few slots ahead of it. retained-MB is the heap one decoded state keeps alive on its own.
// The adversarial case differs from the source in one validator of every registry chunk but chunk 0.
func BenchmarkBeaconStateDecodeSSZ2400kWithReference(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			encoded, err := source.EncodeSSZ(nil)
			require.NoError(b, err)
			ref, err := source.Copy()
			require.NoError(b, err)
			for i := range 64 {
				require.NoError(b, ref.SetEffectiveBalanceForValidatorAtIndex(i*30_000, 31_000_000_000))
			}
			for i := range 16 {
				require.NoError(b, ref.AddValidator(activeValidatorWithIndex(uint64(benchmarkLargeValidatorCount+benchmarkValidatorDelta+i)), 32_000_000_000))
			}
			_, err = ref.HashSSZ()
			require.NoError(b, err)
			adversarial, err := source.Copy()
			require.NoError(b, err)
			for i := 100 + 256; i < adversarial.ValidatorLength(); i += 256 {
				require.NoError(b, adversarial.SetEffectiveBalanceForValidatorAtIndex(i, 31_000_000_000))
			}
			_, err = adversarial.HashSSZ()
			require.NoError(b, err)

			var viewTime time.Duration
			decodeShared := func(ref *CachingBeaconState) *CachingBeaconState {
				view := func(use func(*CachingBeaconState)) {
					start := time.Now()
					use(ref)
					viewTime += time.Since(start)
				}
				decoded := New(source.BeaconConfig())
				require.NoError(b, decoded.BeaconState.DecodeSSZ(encoded, int(version.version)))
				_, err := decoded.InitBeaconStateSharing(view)
				require.NoError(b, err)
				return decoded
			}
			decodePlain := func() *CachingBeaconState {
				decoded := New(source.BeaconConfig())
				require.NoError(b, decoded.DecodeSSZ(encoded, int(version.version)))
				return decoded
			}
			for _, reference := range []struct {
				name string
				ref  *CachingBeaconState
			}{{"shared", ref}, {"adversarial", adversarial}} {
				b.Run(reference.name, func(b *testing.B) {
					viewTime = 0
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						decodeShared(reference.ref)
					}
					b.StopTimer()
					b.ReportMetric(float64(viewTime)/float64(time.Microsecond)/float64(b.N), "view-us")
					b.ReportMetric(retainedMB(func() *CachingBeaconState { return decodeShared(reference.ref) }), "retained-MB")
				})
			}
			b.Run("plain", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					decodePlain()
				}
				b.StopTimer()
				b.ReportMetric(retainedMB(decodePlain), "retained-MB")
			})
		})
	}
}

func retainedMB(build func() *CachingBeaconState) float64 {
	var alive, dropped runtime.MemStats
	func() {
		decoded := build()
		runtime.GC()
		runtime.ReadMemStats(&alive)
		runtime.KeepAlive(decoded)
	}()
	runtime.GC()
	runtime.ReadMemStats(&dropped)
	return float64(int64(alive.HeapAlloc)-int64(dropped.HeapAlloc)) / 1e6
}

// BenchmarkUpgradeToGloas2400k times UpgradeToGloas and the first HashSSZ on a fresh Fulu state per iteration.
func BenchmarkUpgradeToGloas2400k(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		fulu := benchmarkBeaconStateWithSize(b, clparams.FuluVersion, benchmarkLargeValidatorCount)
		b.StartTimer()
		require.NoError(b, fulu.UpgradeToGloas())
		_, err := fulu.HashSSZ()
		require.NoError(b, err)
	}
}
