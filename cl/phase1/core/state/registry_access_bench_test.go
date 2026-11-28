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

var proposerBenchmarkVersions = append([]benchmarkVersion{{name: "deneb", version: clparams.DenebVersion}}, benchmarkVersions...)

// BenchmarkInitiateValidatorExit2400k exercises the pre-Electra exit-queue scan over the whole registry.
func BenchmarkInitiateValidatorExit2400k(b *testing.B) {
	template := sharedBenchmarkState(b, clparams.DenebVersion, benchmarkLargeValidatorCount)
	exiting, err := template.Copy()
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		require.NoError(b, exiting.InitiateValidatorExit(uint64(i%benchmarkLargeValidatorCount)))
	}
}

// BenchmarkValidatorsSweepWithdrawals2400k sweeps the maximum window, which wraps around the end of the registry.
func BenchmarkValidatorsSweepWithdrawals2400k(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			template := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			sweeping, err := template.Copy()
			require.NoError(b, err)
			window := sweeping.BeaconConfig().MaxValidatorsPerWithdrawalsSweep
			sweeping.SetNextWithdrawalValidatorIndex(uint64(sweeping.ValidatorLength()) - window/2)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, processed, err := GetValidatorsSweepWithdrawals(sweeping, 0, Epoch(sweeping), nil)
				require.NoError(b, err)
				require.Equal(b, window, processed)
			}
		})
	}
}

// BenchmarkProposerIndexForSlot2400k computes a proposer outside the Fulu lookahead window.
func BenchmarkProposerIndexForSlot2400k(b *testing.B) {
	for _, version := range proposerBenchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			slot := (source.Slot()/source.BeaconConfig().SlotsPerEpoch + 3) * source.BeaconConfig().SlotsPerEpoch
			_, err := source.GetBeaconProposerIndexForSlot(slot)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				_, err := source.GetBeaconProposerIndexForSlot(slot + uint64(i%int(source.BeaconConfig().SlotsPerEpoch)))
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkProposerIndices2400k fills one epoch of the Fulu proposer lookahead on a copy with empty caches, so the
// active-validator scan is measured.
func BenchmarkProposerIndices2400k(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			epoch := Epoch(source) + 2
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				s, err := source.Copy()
				require.NoError(b, err)
				b.StartTimer()
				_, err = s.GetBeaconProposerIndices(epoch)
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkProposerIndicesWarmCache2400k repeats the lookahead epoch with the active-validator cache already filled.
func BenchmarkProposerIndicesWarmCache2400k(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			epoch := Epoch(source) + 2
			_, err := source.GetBeaconProposerIndices(epoch)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := source.GetBeaconProposerIndices(epoch)
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkNextSyncCommitteeIndices2400k selects the sync committee members; the BLS aggregation is not measured
// because the synthetic public keys are not curve points.
func BenchmarkNextSyncCommitteeIndices2400k(b *testing.B) {
	for _, version := range benchmarkVersions {
		b.Run(version.name, func(b *testing.B) {
			source := sharedBenchmarkState(b, version.version, benchmarkLargeValidatorCount)
			_, err := GetNextSyncCommitteeIndices(source)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := GetNextSyncCommitteeIndices(source)
				require.NoError(b, err)
			}
		})
	}
}
