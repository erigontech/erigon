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

package fork_graph

import (
	"math"
	"math/rand"
	"runtime"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

const mainnetSizeValidators = 2_400_000

func mainnetSizedState(b *testing.B) *state.CachingBeaconState {
	b.Helper()
	_, st, _ := tests.GetElectraRandom()
	limit := int(st.BeaconConfig().ValidatorRegistryLimit)
	rng := rand.New(rand.NewSource(1))
	validators := solid.NewValidatorSet(limit)
	balances := make([]uint64, mainnetSizeValidators)
	participation := make([]byte, mainnetSizeValidators)
	for i := range mainnetSizeValidators {
		validator := solid.NewValidator()
		var pubkey [48]byte
		var credentials common.Hash
		rng.Read(pubkey[:])
		rng.Read(credentials[:])
		validator.SetPublicKey(pubkey)
		validator.SetWithdrawalCredentials(credentials)
		validator.SetEffectiveBalance(32_000_000_000)
		validator.SetExitEpoch(math.MaxUint64)
		validator.SetWithdrawableEpoch(math.MaxUint64)
		validators.Append(validator)
		balances[i] = 32_000_000_000 + uint64(rng.Intn(1_000_000))
		participation[i] = byte(rng.Intn(8))
	}
	st.SetValidators(validators)
	st.SetBalances(solid.NewUint64ListSSZFromSlice(limit, balances))
	st.SetInactivityScores(make([]uint64, mainnetSizeValidators))
	st.SetPreviousEpochParticipation(solid.ParticipationBitListFromBytes(participation, limit))
	st.SetCurrentEpochParticipation(solid.ParticipationBitListFromBytes(participation, limit))
	require.NoError(b, st.InitBeaconState())
	return st
}

func heapAfterGC() uint64 {
	var stats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func BenchmarkForkGraphStateFile2400k(b *testing.B) {
	st := mainnetSizedState(b)
	newGraph := func(b *testing.B) *forkGraphDisk {
		graph, err := NewForkGraphDisk(st, nil, afero.NewBasePathFs(afero.NewOsFs(), b.TempDir()), beacon_router_configuration.RouterConfiguration{})
		require.NoError(b, err)
		return graph.(*forkGraphDisk)
	}
	root := common.Hash{0xbe}
	b.Run("dump", func(b *testing.B) {
		graph := newGraph(b)
		b.ReportAllocs()
		for b.Loop() {
			graph.lastDumpedRoot = common.Hash{}
			if err := graph.DumpBeaconStateOnDisk(root, st, true); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("read", func(b *testing.B) {
		graph := newGraph(b)
		require.NoError(b, graph.DumpBeaconStateOnDisk(root, st, true))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := graph.readBeaconStateFromDisk(root); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("retained", func(b *testing.B) {
		for b.Loop() {
			graphs := []*forkGraphDisk{newGraph(b)}
			require.NoError(b, graphs[0].DumpBeaconStateOnDisk(root, st, true))
			read, err := graphs[0].readBeaconStateFromDisk(root)
			require.NoError(b, err)
			with := heapAfterGC()
			graphs[0] = nil
			without := heapAfterGC()
			runtime.KeepAlive(read)
			b.ReportMetric(float64(int64(with)-int64(without))/(1<<20), "graph-retained-MB")
		}
	})
}
