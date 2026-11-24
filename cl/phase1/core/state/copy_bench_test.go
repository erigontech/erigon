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
	"fmt"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkValidatorCount      = 1_140_000
	benchmarkLargeValidatorCount = 2_400_000
	benchmarkValidatorDelta      = 1_000
)

type benchmarkVersion struct {
	name    string
	version clparams.StateVersion
}

var benchmarkVersions = []benchmarkVersion{
	{name: "fulu", version: clparams.FuluVersion},
	{name: "gloas", version: clparams.GloasVersion},
}

type benchmarkStateKey struct {
	version clparams.StateVersion
	count   int
}

var (
	sharedBenchmarkStatesMu sync.Mutex
	sharedBenchmarkStates   = map[benchmarkStateKey]*CachingBeaconState{}
)

// sharedBenchmarkState builds each (version, count) state once per process. Callers must not mutate it; use Copy.
func sharedBenchmarkState(b *testing.B, version clparams.StateVersion, count int) *CachingBeaconState {
	b.Helper()
	sharedBenchmarkStatesMu.Lock()
	defer sharedBenchmarkStatesMu.Unlock()
	key := benchmarkStateKey{version, count}
	if state, ok := sharedBenchmarkStates[key]; ok {
		return state
	}
	state := benchmarkBeaconStateWithSize(b, version, count)
	sharedBenchmarkStates[key] = state
	return state
}

func BenchmarkBeaconStateCopyInto(b *testing.B) {
	for _, size := range []int{benchmarkValidatorCount, benchmarkLargeValidatorCount} {
		for _, version := range benchmarkVersions {
			b.Run(fmt.Sprintf("%d/%s", size, version.name), func(b *testing.B) {
				source := sharedBenchmarkState(b, version.version, size)
				target := New(source.BeaconConfig())
				target.SetVersion(version.version)
				require.NoError(b, source.CopyInto(target))

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					require.NoError(b, source.CopyInto(target))
				}
			})
		}
	}
}

func BenchmarkValidatorIndexLookup(b *testing.B) {
	state := benchmarkBeaconState(b, clparams.FuluVersion)
	keys := [][48]byte{
		activeValidatorWithIndex(0).PublicKey(),
		activeValidatorWithIndex(benchmarkValidatorCount / 2).PublicKey(),
		activeValidatorWithIndex(benchmarkValidatorCount + benchmarkValidatorDelta - 1).PublicKey(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		_, _ = state.ValidatorIndexByPubkey(keys[i%len(keys)])
	}
}

func benchmarkBeaconState(b *testing.B, version clparams.StateVersion) *CachingBeaconState {
	b.Helper()
	return benchmarkBeaconStateWithSize(b, version, benchmarkValidatorCount)
}

func benchmarkBeaconStateWithSize(b *testing.B, version clparams.StateVersion, count int) *CachingBeaconState {
	b.Helper()
	state := New(&clparams.MainnetBeaconConfig)
	state.SetVersion(version)
	if version < clparams.GloasVersion {
		state.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(version))
	}
	validators := solid.NewValidatorSet(int(state.BeaconConfig().ValidatorRegistryLimit))
	balances := make([]uint64, count)
	inactivityScores := make([]uint64, count)
	for i := range count {
		validators.Append(activeValidatorWithIndex(uint64(i)))
		balances[i] = 32_000_000_000
		inactivityScores[i] = uint64(i % 4)
	}
	state.SetValidators(validators)
	state.SetBalances(solid.NewUint64ListSSZFromSlice(int(state.BeaconConfig().ValidatorRegistryLimit), balances))
	state.SetInactivityScores(inactivityScores)
	previousParticipation := solid.NewParticipationBitList(count, int(state.BeaconConfig().ValidatorRegistryLimit))
	currentParticipation := solid.NewParticipationBitList(count, int(state.BeaconConfig().ValidatorRegistryLimit))
	previousParticipation.Set(0, 1)
	currentParticipation.Set(count-1, 2)
	state.SetPreviousEpochParticipation(previousParticipation)
	state.SetCurrentEpochParticipation(currentParticipation)
	require.NoError(b, state.InitBeaconState())
	for i := range benchmarkValidatorDelta {
		require.NoError(b, state.AddValidator(activeValidatorWithIndex(uint64(count+i)), 32_000_000_000))
	}
	_, err := state.HashSSZ()
	require.NoError(b, err)
	return state
}
