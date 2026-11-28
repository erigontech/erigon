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

package solid

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	benchRegistrySize  = 2_400_000
	benchRegistryLimit = 1 << 40
)

var benchLayouts = []struct {
	name        string
	progressive bool
}{
	{"fulu", false},
	{"gloas", true},
}

var benchRegistryBytes = sync.OnceValue(func() []byte {
	validators := NewValidatorSet(benchRegistryLimit)
	for i := range benchRegistrySize {
		var publicKey [48]byte
		binary.LittleEndian.PutUint64(publicKey[:], uint64(i)+1)
		var credentials [32]byte
		credentials[0] = 1
		binary.LittleEndian.PutUint64(credentials[12:], uint64(i)*2654435761)
		validators.Append(NewValidatorFromParameters(
			publicKey, credentials, 32_000_000_000+uint64(i%32)*1_000_000_000, i%251 == 0,
			uint64(i%7), uint64(i%5), ^uint64(0), ^uint64(0),
		))
	}
	encoded, err := validators.EncodeSSZ(nil)
	if err != nil {
		panic(err)
	}
	return encoded
})

func newBenchValidatorSet(b testing.TB, progressive, warm bool) *ValidatorSet {
	b.Helper()
	validators := NewValidatorSet(benchRegistryLimit)
	loadBenchValidatorSet(b, validators, progressive, warm)
	return validators
}

func loadBenchValidatorSet(b testing.TB, validators *ValidatorSet, progressive, warm bool) {
	b.Helper()
	validators.SetProgressiveHashing(progressive)
	if err := validators.DecodeSSZ(benchRegistryBytes(), 0); err != nil {
		b.Fatal(err)
	}
	if warm {
		hashBenchValidatorSet(b, validators, progressive)
	}
}

func hashBenchValidatorSet(b testing.TB, validators *ValidatorSet, progressive bool) [32]byte {
	b.Helper()
	var root [32]byte
	var err error
	if progressive {
		root, err = validators.HashSSZProgressive()
	} else {
		root, err = validators.HashSSZ()
	}
	if err != nil {
		b.Fatal(err)
	}
	return root
}

func writeBenchValidator(validators *ValidatorSet, index int, salt uint64) {
	validators.SetEffectiveBalanceForValidatorAtIndex(index, 31_000_000_000+salt%2_000_000_000)
}

// spreadIndices returns count indices spaced evenly over the registry, so each one falls in another chunk.
func spreadIndices(count int) []int {
	indices := make([]int, count)
	for i := range indices {
		indices[i] = i * (benchRegistrySize / count)
	}
	return indices
}

func BenchmarkValidatorSetCopyTo(b *testing.B) {
	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			source := newBenchValidatorSet(b, layout.progressive, true)
			b.Run("fresh", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					source.CopyTo(NewValidatorSet(benchRegistryLimit))
				}
			})
			b.Run("reused_spare", func(b *testing.B) {
				spare := NewValidatorSet(benchRegistryLimit)
				source.CopyTo(spare)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					source.CopyTo(spare)
				}
			})
		})
	}
}

func BenchmarkValidatorSetHashSSZAfterCopyAndWrite(b *testing.B) {
	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			source := newBenchValidatorSet(b, layout.progressive, true)
			spare := NewValidatorSet(benchRegistryLimit)
			source.CopyTo(spare)
			b.ReportAllocs()
			b.ResetTimer()
			var copyTime, writeTime, hashTime time.Duration
			for i := range b.N {
				start := time.Now()
				source.CopyTo(spare)
				copied := time.Now()
				writeBenchValidator(spare, (i*7919)%benchRegistrySize, uint64(i))
				written := time.Now()
				hashBenchValidatorSet(b, spare, layout.progressive)
				copyTime += copied.Sub(start)
				writeTime += written.Sub(copied)
				hashTime += time.Since(written)
			}
			b.ReportMetric(float64(copyTime.Nanoseconds())/float64(b.N), "copy-ns/op")
			b.ReportMetric(float64(writeTime.Nanoseconds())/float64(b.N), "write-ns/op")
			b.ReportMetric(float64(hashTime.Nanoseconds())/float64(b.N), "hash-ns/op")
		})
	}
}

func BenchmarkValidatorSetFirstWriteAfterCopy(b *testing.B) {
	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			source := newBenchValidatorSet(b, layout.progressive, true)
			spare := NewValidatorSet(benchRegistryLimit)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				b.StopTimer()
				source.CopyTo(spare)
				b.StartTimer()
				writeBenchValidator(spare, (i*7919)%benchRegistrySize, uint64(i))
			}
		})
	}
}

func BenchmarkValidatorSetColdHash(b *testing.B) {
	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			validators := NewValidatorSet(benchRegistryLimit)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				loadBenchValidatorSet(b, validators, layout.progressive, false)
				b.StartTimer()
				hashBenchValidatorSet(b, validators, layout.progressive)
			}
		})
	}
}

func BenchmarkValidatorSetConcurrentCopyHash(b *testing.B) {
	for _, layout := range benchLayouts {
		for _, goroutines := range []int{4, 16} {
			for _, dirty := range []int{1, 64, 1024} {
				for _, source := range []struct {
					name string
					warm bool
				}{{"cold", false}, {"warm", true}} {
					name := fmt.Sprintf("%s/G=%d/dirty=%d/%s", layout.name, goroutines, dirty, source.name)
					b.Run(name, func(b *testing.B) {
						benchmarkConcurrentCopyHash(b, layout.progressive, goroutines, dirty, source.warm)
					})
				}
			}
		}
	}
}

func benchmarkConcurrentCopyHash(b *testing.B, progressive bool, goroutines, dirty int, warm bool) {
	source := NewValidatorSet(benchRegistryLimit)
	spares := make([]*ValidatorSet, goroutines)
	for g := range spares {
		spares[g] = NewValidatorSet(benchRegistryLimit)
	}
	dirtyIndices := spreadIndices(dirty)
	errs := make([]error, goroutines)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		loadBenchValidatorSet(b, source, progressive, warm)
		for _, index := range dirtyIndices {
			writeBenchValidator(source, index, uint64(i)+1)
		}
		var ready, done sync.WaitGroup
		start := make(chan struct{})
		for g, spare := range spares {
			ready.Add(1)
			done.Go(func() {
				ready.Done()
				<-start
				source.CopyTo(spare)
				writeBenchValidator(spare, (g*104729+13)%benchRegistrySize, uint64(i))
				var err error
				if progressive {
					_, err = spare.HashSSZProgressive()
				} else {
					_, err = spare.HashSSZ()
				}
				errs[g] = err
			})
		}
		ready.Wait()
		b.StartTimer()
		close(start)
		done.Wait()
		b.StopTimer()
		for _, err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
