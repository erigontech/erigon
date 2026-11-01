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

package stages

import (
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func useFreshPrecacheValidatorCaches(t *testing.T) {
	t.Helper()
	previousShuffled := caches.ShuffledIndiciesCacheGlobal
	previousLookaheadShuffled := caches.LookaheadShuffledIndiciesCacheGlobal
	previousActive := caches.ActiveValidatorsCacheGlobal
	previousLookaheadActive := caches.LookaheadActiveValidatorsCacheGlobal
	caches.ShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("shuffled_indicies", caches.MaxShuffledIndiciesCacheSize)
	caches.LookaheadShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("lookahead_shuffled_indicies", caches.MaxLookaheadShuffledIndiciesCacheSize)
	caches.ActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("active_validators", caches.MaxActiveValidatorsCacheSize)
	caches.LookaheadActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("lookahead_active_validators", caches.MaxLookaheadActiveValidatorsCacheSize)
	t.Cleanup(func() {
		caches.ShuffledIndiciesCacheGlobal = previousShuffled
		caches.LookaheadShuffledIndiciesCacheGlobal = previousLookaheadShuffled
		caches.ActiveValidatorsCacheGlobal = previousActive
		caches.LookaheadActiveValidatorsCacheGlobal = previousLookaheadActive
	})
}

func preCacheTestState(t *testing.T, cfg *clparams.BeaconChainConfig, validators int, seed byte) *state.CachingBeaconState {
	t.Helper()
	s := state.New(cfg)
	for i := range validators {
		var pk [48]byte
		binary.BigEndian.PutUint64(pk[:], uint64(i))
		pk[47] = seed
		v := solid.NewValidator()
		v.SetActivationEpoch(0)
		v.SetExitEpoch(^uint64(0))
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(32_000_000_000)
		require.NoError(t, s.AddValidator(v, 32_000_000_000))
	}
	require.NoError(t, s.SetSlot(3*cfg.SlotsPerEpoch+1))
	for i := range int(cfg.SlotsPerHistoricalRoot) {
		require.NoError(t, s.SetBlockRootAt(i, common.Hash{seed, byte(i), byte(i >> 8)}))
	}
	for i := range int(cfg.EpochsPerHistoricalVector) {
		require.NoError(t, s.SetRandaoMixAt(i, common.Hash{seed, byte(i), byte(i >> 8)}))
	}
	return s
}

func TestPreCacheNextShuffledValidatorSetReadsTheStateBeforeReturning(t *testing.T) {
	useFreshPrecacheValidatorCaches(t)
	cfg := &clparams.MainnetBeaconConfig
	head := preCacheTestState(t, cfg, 50_000, 0xa1)
	other := preCacheTestState(t, cfg, 40_000, 0xb2)

	ref, err := head.Copy()
	require.NoError(t, err)
	epoch := state.Epoch(ref) + 1
	root, err := ref.GetBlockRootAtSlot((epoch-1)*cfg.SlotsPerEpoch - 1)
	require.NoError(t, err)
	mix := ref.GetRandaoMix(int((epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector))
	indicies := ref.GetActiveValidatorsIndices(epoch)
	want := shuffling.ComputeShuffledIndicies(cfg, mix, make([]uint64, len(indicies)), indicies, epoch*cfg.SlotsPerEpoch)

	require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)
	preCacheNextShuffledValidatorSet(log.Root(), &Cfg{beaconCfg: cfg}, head)
	require.NoError(t, other.CopyInto(head))
	require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)

	got, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, root)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestPrecacheDecisionRootMatchesBoundaryState(t *testing.T) {
	useFreshPrecacheValidatorCaches(t)
	cfg := &clparams.MainnetBeaconConfig
	boundary := preCacheTestState(t, cfg, 2_048, 0xc3)
	require.NoError(t, boundary.SetSlot(4*cfg.SlotsPerEpoch-1))
	header := boundary.LatestBlockHeader()
	header.Root[0] = 1
	boundary.SetLatestBlockHeader(&header)
	stateRoot, err := boundary.HashSSZ()
	require.NoError(t, err)
	wantRoot, err := header.HashSSZ()
	require.NoError(t, err)
	index := int(boundary.Slot() % cfg.SlotsPerHistoricalRoot)
	require.NoError(t, boundary.SetBlockRootAt(index, wantRoot))
	require.NoError(t, boundary.SetStateRootAt(index, stateRoot))
	fresh, err := boundary.Copy()
	require.NoError(t, err)
	advanced, err := boundary.Copy()
	require.NoError(t, err)
	require.NoError(t, advanced.SetSlot(boundary.Slot()+1))
	epoch := state.Epoch(boundary) + 2

	root, err := advanced.GetBlockRootAtSlot(boundary.Slot())
	require.NoError(t, err)
	require.Equal(t, common.Hash(wantRoot), root)
	in, ok := shuffleInputForEpoch(log.Root(), cfg, advanced, epoch)
	require.True(t, ok)
	preCacheShuffledSet(in)

	cachedActive, ok := caches.ActiveValidatorsCacheGlobal.Get(epoch, root)
	require.True(t, ok)
	active := fresh.GetActiveValidatorsIndices(epoch)
	require.Same(t, unsafe.SliceData(cachedActive), unsafe.SliceData(active))
	cachedShuffle, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, root)
	require.True(t, ok)
	committee, err := fresh.GetBeaconCommitee(epoch*cfg.SlotsPerEpoch, 0)
	require.NoError(t, err)
	require.Same(t, unsafe.SliceData(cachedShuffle), unsafe.SliceData(committee))
}

func TestPrecachePromotesLookaheadShuffle(t *testing.T) {
	useFreshPrecacheValidatorCaches(t)
	cfg := &clparams.MainnetBeaconConfig
	head := preCacheTestState(t, cfg, 2_048, 0xd4)
	head.SetVersion(clparams.GloasVersion)
	boundarySlot := 4*cfg.SlotsPerEpoch - 1
	require.NoError(t, head.SetSlot(boundarySlot-1))
	head.SetCurrentEpochParticipation(solid.NewParticipationBitList(head.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	head.SetPreviousEpochParticipation(solid.NewParticipationBitList(head.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	head.SetInactivityScores(make([]uint64, head.ValidatorLength()))
	require.NoError(t, transition.DefaultMachine.ProcessSlots(head, boundarySlot+1))
	epoch := state.Epoch(head) + 1
	stateRoot := head.StateRoots().Get(int(boundarySlot % cfg.SlotsPerHistoricalRoot))
	lookahead, ok := caches.LookaheadShuffledIndiciesCacheGlobal.Get(epoch, stateRoot)
	require.True(t, ok)
	blockRoot, err := head.GetBlockRootAtSlot(boundarySlot)
	require.NoError(t, err)

	_, ok = shuffleInputForEpoch(log.Root(), cfg, head, epoch)
	require.False(t, ok)
	main, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, blockRoot)
	require.True(t, ok)
	require.Same(t, unsafe.SliceData(lookahead), unsafe.SliceData(main))
}

func TestTotalActiveBalanceIsNotSharedAcrossBranchesForkedInPreviousEpoch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	epoch := uint64(4)

	type branchPair struct {
		higher      *state.CachingBeaconState
		lower       *state.CachingBeaconState
		cacheRoot   common.Hash
		higherTotal uint64
		lowerTotal  uint64
	}

	newBranches := func(t *testing.T) branchPair {
		t.Helper()
		ancestor := preCacheTestState(t, cfg, 64, 0xe5)
		higher, err := ancestor.Copy()
		require.NoError(t, err)
		lower, err := ancestor.Copy()
		require.NoError(t, err)

		higherHeader := higher.LatestBlockHeader()
		higherHeader.Root[0] = 1
		higher.SetLatestBlockHeader(&higherHeader)
		lowerHeader := lower.LatestBlockHeader()
		lowerHeader.Root[0] = 2
		lower.SetLatestBlockHeader(&lowerHeader)
		require.NoError(t, lower.SetValidatorBalance(0, 31_000_000_000))

		require.NoError(t, transition.DefaultMachine.ProcessSlots(higher, epoch*cfg.SlotsPerEpoch))
		require.NoError(t, transition.DefaultMachine.ProcessSlots(lower, epoch*cfg.SlotsPerEpoch))

		forkSlot := (epoch-1)*cfg.SlotsPerEpoch + 1
		higherForkRoot, err := higher.GetBlockRootAtSlot(forkSlot)
		require.NoError(t, err)
		lowerForkRoot, err := lower.GetBlockRootAtSlot(forkSlot)
		require.NoError(t, err)
		require.NotEqual(t, higherForkRoot, lowerForkRoot)

		decisionSlot := (epoch-1)*cfg.SlotsPerEpoch - 1
		higherCacheRoot, err := higher.GetBlockRootAtSlot(decisionSlot)
		require.NoError(t, err)
		lowerCacheRoot, err := lower.GetBlockRootAtSlot(decisionSlot)
		require.NoError(t, err)
		require.Equal(t, higherCacheRoot, lowerCacheRoot)
		require.Equal(t, higher.GetActiveValidatorsIndices(epoch), lower.GetActiveValidatorsIndices(epoch))

		trueTotal := func(s *state.CachingBeaconState) uint64 {
			var total uint64
			for _, index := range s.GetActiveValidatorsIndices(epoch) {
				total += s.ValidatorSet().Get(int(index)).EffectiveBalance()
			}
			return total
		}
		higherTotal := trueTotal(higher)
		lowerTotal := trueTotal(lower)
		require.NotEqual(t, higherTotal, lowerTotal)

		return branchPair{
			higher:      higher,
			lower:       lower,
			cacheRoot:   higherCacheRoot,
			higherTotal: higherTotal,
			lowerTotal:  lowerTotal,
		}
	}

	for _, tc := range []struct {
		name         string
		polluterHigh bool
		copyVictim   bool
	}{
		{name: "higher_pollutes_lower/state", polluterHigh: true},
		{name: "higher_pollutes_lower/copy", polluterHigh: true, copyVictim: true},
		{name: "lower_pollutes_higher/state"},
		{name: "lower_pollutes_higher/copy", copyVictim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useFreshPrecacheValidatorCaches(t)
			branches := newBranches(t)
			polluter, victim := branches.lower, branches.higher
			want := branches.higherTotal
			if tc.polluterHigh {
				polluter, victim = branches.higher, branches.lower
				want = branches.lowerTotal
			}

			preCacheActiveValidatorsForEpoch(polluter, epoch, branches.cacheRoot)
			if tc.copyVictim {
				var err error
				victim, err = victim.Copy()
				require.NoError(t, err)
			}
			require.Equal(t, want, victim.GetTotalActiveBalance())
		})
	}
}
