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
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor/shuffling_metrics"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const activeListTestValidators = 2_048

// Validators whose activity flips around the boundary epoch E, so lists for E-1, E and E+1 all differ.
const (
	exitsAtBoundaryEpoch        = 3
	activatesAtBoundaryEpoch    = 5
	activatesAfterBoundaryEpoch = 7
)

// boundaryHead is a state one slot past an epoch boundary; processing the boundary slot filled the
// lookahead caches for `epoch` under stateRoot from Fulu on.
type boundaryHead struct {
	cfg         *clparams.BeaconChainConfig
	preBoundary *state.CachingBeaconState
	head        *state.CachingBeaconState
	epoch       uint64
	refIndex    int
	blockRoot   common.Hash
	stateRoot   common.Hash
}

func newBoundaryHead(t *testing.T) *boundaryHead {
	t.Helper()
	return newBoundaryHeadAt(t, &clparams.MainnetBeaconConfig, clparams.GloasVersion)
}

func newBoundaryHeadAt(t *testing.T, cfg *clparams.BeaconChainConfig, version clparams.StateVersion) *boundaryHead {
	t.Helper()
	useFreshPrecacheValidatorCaches(t)
	s := preCacheTestState(t, cfg, activeListTestValidators, 0xd4)
	s.SetVersion(version)
	boundarySlot := 4*cfg.SlotsPerEpoch - 1
	epoch := boundarySlot/cfg.SlotsPerEpoch + 2
	require.NoError(t, s.SetExitEpochForValidatorAtIndex(exitsAtBoundaryEpoch, epoch))
	require.NoError(t, s.SetActivationEpochForValidatorAtIndex(activatesAtBoundaryEpoch, epoch))
	require.NoError(t, s.SetActivationEpochForValidatorAtIndex(activatesAfterBoundaryEpoch, epoch+1))
	require.NoError(t, s.SetSlot(boundarySlot-1))
	s.SetCurrentEpochParticipation(solid.NewParticipationBitList(s.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	s.SetPreviousEpochParticipation(solid.NewParticipationBitList(s.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	s.SetInactivityScores(make([]uint64, s.ValidatorLength()))
	pre, err := s.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(s, boundarySlot+1))
	require.Equal(t, epoch, state.Epoch(s)+1)
	refIndex := int(boundarySlot % cfg.SlotsPerHistoricalRoot)
	blockRoot, err := s.GetBlockRootAtSlot(boundarySlot)
	require.NoError(t, err)
	return &boundaryHead{
		cfg:         cfg,
		preBoundary: pre,
		head:        s,
		epoch:       epoch,
		refIndex:    refIndex,
		blockRoot:   blockRoot,
		stateRoot:   s.StateRoots().Get(refIndex),
	}
}

func (h *boundaryHead) freshHead(t *testing.T) *state.CachingBeaconState {
	t.Helper()
	c, err := h.head.Copy()
	require.NoError(t, err)
	return c
}

func expectedActiveIndices(boundaryEpoch, epoch uint64) []uint32 {
	want := make([]uint32, 0, activeListTestValidators)
	for i := range uint32(activeListTestValidators) {
		switch {
		case i == exitsAtBoundaryEpoch && epoch >= boundaryEpoch,
			i == activatesAtBoundaryEpoch && epoch < boundaryEpoch,
			i == activatesAfterBoundaryEpoch && epoch <= boundaryEpoch:
			continue
		}
		want = append(want, i)
	}
	return want
}

func scansDuring(f func()) uint64 {
	before := shuffling_metrics.ActiveValidatorsIndicesComputed.GetValueUint64()
	f()
	return shuffling_metrics.ActiveValidatorsIndicesComputed.GetValueUint64() - before
}

func TestPrecachePromotionAcrossForkVersions(t *testing.T) {
	gloasAfterBoundary := clparams.MainnetBeaconConfig
	gloasAfterBoundary.GloasForkEpoch = 4

	for _, tc := range []struct {
		name        string
		cfg         *clparams.BeaconChainConfig
		version     clparams.StateVersion
		headVersion clparams.StateVersion
		promoted    bool
	}{
		{"electra", &clparams.MainnetBeaconConfig, clparams.ElectraVersion, clparams.ElectraVersion, false},
		{"fulu", &clparams.MainnetBeaconConfig, clparams.FuluVersion, clparams.FuluVersion, true},
		{"gloas", &clparams.MainnetBeaconConfig, clparams.GloasVersion, clparams.GloasVersion, true},
		{"fulu_to_gloas", &gloasAfterBoundary, clparams.FuluVersion, clparams.GloasVersion, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBoundaryHeadAt(t, tc.cfg, tc.version)
			require.Equal(t, tc.headVersion, h.head.Version())
			lookahead, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(h.epoch, h.stateRoot)
			require.Equal(t, tc.promoted, ok)
			head := h.freshHead(t)

			scans := scansDuring(func() { shuffleInputForEpoch(log.Root(), h.cfg, head, h.epoch) })

			main, ok := caches.ActiveValidatorsCacheGlobal.Get(h.epoch, h.blockRoot)
			require.True(t, ok)
			require.Equal(t, expectedActiveIndices(h.epoch, h.epoch), main)
			if !tc.promoted {
				require.Equal(t, uint64(1), scans)
				return
			}
			require.Zero(t, scans)
			require.Same(t, unsafe.SliceData(lookahead), unsafe.SliceData(main))
		})
	}
}

func TestPrecacheDoesNotPromoteForeignLookaheadActiveValidators(t *testing.T) {
	foreign := []uint32{7, 8, 9}
	otherRoot := common.Hash{0xee}

	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) (foreignList []uint32)
	}{
		{"different_state_root", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint32 {
			caches.LookaheadActiveValidatorsCacheGlobal.Put(h.epoch, h.stateRoot, foreign)
			require.NoError(t, head.SetStateRootAt(h.refIndex, otherRoot))
			return foreign
		}},
		{"zero_state_root", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint32 {
			caches.LookaheadActiveValidatorsCacheGlobal.Put(h.epoch, common.Hash{}, foreign)
			require.NoError(t, head.SetStateRootAt(h.refIndex, common.Hash{}))
			return foreign
		}},
		{"absent_lookahead", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint32 {
			caches.LookaheadActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("lookahead_active_validators", caches.MaxLookaheadActiveValidatorsCacheSize)
			return nil
		}},
		{"evicted_lookahead", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint32 {
			evicted, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(h.epoch, h.stateRoot)
			require.True(t, ok)
			for i := range caches.MaxLookaheadActiveValidatorsCacheSize + 1 {
				caches.LookaheadActiveValidatorsCacheGlobal.Put(h.epoch+1+uint64(i), h.stateRoot, foreign)
			}
			_, ok = caches.LookaheadActiveValidatorsCacheGlobal.Get(h.epoch, h.stateRoot)
			require.False(t, ok)
			return evicted
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBoundaryHead(t)
			head := h.freshHead(t)
			foreignList := tc.setup(t, h, head)

			scans := scansDuring(func() { shuffleInputForEpoch(log.Root(), h.cfg, head, h.epoch) })

			require.Equal(t, uint64(1), scans)
			main, ok := caches.ActiveValidatorsCacheGlobal.Get(h.epoch, h.blockRoot)
			require.True(t, ok)
			require.Equal(t, expectedActiveIndices(h.epoch, h.epoch), main)
			if foreignList != nil {
				require.NotSame(t, unsafe.SliceData(foreignList), unsafe.SliceData(main))
			}
			fresh := h.freshHead(t)
			require.NoError(t, fresh.SetStateRootAt(h.refIndex, head.StateRoots().Get(h.refIndex)))
			require.Same(t, unsafe.SliceData(main), unsafe.SliceData(fresh.GetActiveValidatorsIndices(h.epoch)))
		})
	}
}

func TestPrecachePromotesEachBranchItsOwnLookaheadActiveValidators(t *testing.T) {
	h := newBoundaryHead(t)
	rootA, rootB := h.stateRoot, common.Hash{0xbb}
	listA, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(h.epoch, rootA)
	require.True(t, ok)
	listB := []uint32{1, 2, 3}
	caches.LookaheadActiveValidatorsCacheGlobal.Put(h.epoch, rootB, listB)
	headA := h.freshHead(t)
	headB := h.freshHead(t)
	blockRootB := common.Hash{0xcc}
	require.NoError(t, headB.SetStateRootAt(h.refIndex, rootB))
	require.NoError(t, headB.SetBlockRootAt(h.refIndex, blockRootB))

	scans := scansDuring(func() {
		shuffleInputForEpoch(log.Root(), h.cfg, headA, h.epoch)
		shuffleInputForEpoch(log.Root(), h.cfg, headB, h.epoch)
	})

	require.Zero(t, scans)
	mainA, ok := caches.ActiveValidatorsCacheGlobal.Get(h.epoch, h.blockRoot)
	require.True(t, ok)
	require.Same(t, unsafe.SliceData(listA), unsafe.SliceData(mainA))
	mainB, ok := caches.ActiveValidatorsCacheGlobal.Get(h.epoch, blockRootB)
	require.True(t, ok)
	require.Same(t, unsafe.SliceData(listB), unsafe.SliceData(mainB))
}

func TestPrecacheScansActiveValidatorsOncePerEpochBoundary(t *testing.T) {
	h := newBoundaryHead(t)
	useFreshPrecacheValidatorCaches(t)
	preCacheCfg := &Cfg{beaconCfg: h.cfg}
	waitPreCacheIdle := func() {
		require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)
	}

	waitPreCacheIdle()
	preCacheNextShuffledValidatorSet(log.Root(), preCacheCfg, h.preBoundary)
	waitPreCacheIdle()
	next, err := h.preBoundary.Copy()
	require.NoError(t, err)
	published, err := h.preBoundary.Copy()
	require.NoError(t, err)

	scans := scansDuring(func() {
		require.NoError(t, transition.DefaultMachine.ProcessSlots(next, h.head.Slot()))
		require.NoError(t, next.CopyInto(published))
		preCacheNextShuffledValidatorSet(log.Root(), preCacheCfg, published)
	})
	waitPreCacheIdle()

	require.Equal(t, uint64(1), scans)
}
