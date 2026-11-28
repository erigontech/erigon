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
	"math/rand"
	"testing"
)

var benchSink uint64

func BenchmarkValidatorSetGet(b *testing.B) {
	validators := newBenchValidatorSet(b, false, false)
	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := range b.N {
			sum += validators.Get(i % benchRegistrySize).EffectiveBalance()
		}
		benchSink = sum
	})
	b.Run("random", func(b *testing.B) {
		rng := rand.New(rand.NewSource(1))
		indices := make([]int, 1<<16)
		for i := range indices {
			indices[i] = rng.Intn(benchRegistrySize)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sum uint64
		for i := range b.N {
			sum += validators.Get(indices[i%len(indices)]).EffectiveBalance()
		}
		benchSink = sum
	})
}

func BenchmarkValidatorSetAll(b *testing.B) {
	validators := newBenchValidatorSet(b, false, false)
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint64
	for range b.N {
		for _, validator := range validators.All(0, benchRegistrySize) {
			sum += validator.EffectiveBalance()
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/benchRegistrySize, "ns/validator")
	benchSink = sum
}

func BenchmarkValidatorViewPublicKey(b *testing.B) {
	validators := newBenchValidatorSet(b, false, false)
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint64
	for i := range b.N {
		publicKey := validators.Get(i % benchRegistrySize).PublicKey()
		sum += uint64(publicKey[0])
	}
	benchSink = sum
}
