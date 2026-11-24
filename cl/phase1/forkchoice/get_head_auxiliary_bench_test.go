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

package forkchoice

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

const benchAuxiliaryRegistry = 2_400_000

var benchAuxiliaryState = sync.OnceValue(func() *state.CachingBeaconState {
	cfg := &clparams.MainnetBeaconConfig
	s := state.New(cfg)
	s.SetVersion(clparams.FuluVersion)
	validators := solid.NewValidatorSet(int(cfg.ValidatorRegistryLimit))
	for i := range benchAuxiliaryRegistry {
		var publicKey [48]byte
		binary.LittleEndian.PutUint64(publicKey[:], uint64(i)+1)
		activationEpoch := uint64(0)
		if i%97 == 0 {
			activationEpoch = 100
		}
		validators.Append(solid.NewValidatorFromParameters(
			publicKey, [32]byte{1}, benchBalance, i%64 == 0, 0, activationEpoch, cfg.FarFutureEpoch, cfg.FarFutureEpoch,
		))
	}
	s.SetValidators(validators)
	return s
})

// The justified epoch is 10, so computeVotes reads every registry entry through the auxiliary state.
func benchmarkGetHeadAuxiliary(b *testing.B, probabilistic bool) {
	f, justified, recent := newPreGloasHeadBenchStore()
	f.probabilisticHeadGetter = probabilistic
	f.latestMessages = newLatestMessagesStore(benchAuxiliaryRegistry)
	rng := rand.New(rand.NewSource(1))
	for i := range benchAuxiliaryRegistry {
		f.latestMessages.set(i, LatestMessage{Epoch: 11, Root: recent[rng.Intn(len(recent))]})
	}
	f.checkpointStates.Store(justified, &checkpointState{
		beaconConfig:           f.beaconCfg,
		unslashedActiveBalance: benchAuxiliaryRegistry * benchBalance,
	})

	auxiliary := benchAuxiliaryState()
	calls := 0
	view := func(fn synced_data.ViewHeadStateFn) error {
		calls++
		return fn(auxiliary)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		f.headHash = common.Hash{}
		gen := rand.New(rand.NewSource(int64(i)))
		b.StartTimer()
		if _, _, err := f.getHead(view, false, gen); err != nil {
			b.Fatal(err)
		}
	}
	if calls != b.N {
		b.Fatalf("auxiliary state view called %d times, want %d: computeVotes did not read the registry", calls, b.N)
	}
}

func BenchmarkHeadVotesExact(b *testing.B) {
	benchmarkGetHeadAuxiliary(b, false)
}

func BenchmarkHeadVotesProbabilistic(b *testing.B) {
	benchmarkGetHeadAuxiliary(b, true)
}
