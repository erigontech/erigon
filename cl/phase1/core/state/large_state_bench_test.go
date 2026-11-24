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
	"testing"

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
