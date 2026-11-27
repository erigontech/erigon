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

// boundaryHead is a Gloas state one slot past an epoch boundary; processing the boundary slot filled the
// lookahead caches for `epoch` under stateRoot.
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
	useFreshPrecacheValidatorCaches(t)
	cfg := &clparams.MainnetBeaconConfig
	s := preCacheTestState(t, cfg, activeListTestValidators, 0xd4)
	s.SetVersion(clparams.GloasVersion)
	boundarySlot := 4*cfg.SlotsPerEpoch - 1
	require.NoError(t, s.SetSlot(boundarySlot-1))
	s.SetCurrentEpochParticipation(solid.NewParticipationBitList(s.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	s.SetPreviousEpochParticipation(solid.NewParticipationBitList(s.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	s.SetInactivityScores(make([]uint64, s.ValidatorLength()))
	pre, err := s.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(s, boundarySlot+1))
	refIndex := int(boundarySlot % cfg.SlotsPerHistoricalRoot)
	blockRoot, err := s.GetBlockRootAtSlot(boundarySlot)
	require.NoError(t, err)
	return &boundaryHead{
		cfg:         cfg,
		preBoundary: pre,
		head:        s,
		epoch:       state.Epoch(s) + 1,
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

func (h *boundaryHead) allActive() []uint64 {
	want := make([]uint64, activeListTestValidators)
	for i := range want {
		want[i] = uint64(i)
	}
	return want
}

func scansDuring(f func()) uint64 {
	before := shuffling_metrics.ActiveValidatorsIndicesComputed.GetValueUint64()
	f()
	return shuffling_metrics.ActiveValidatorsIndicesComputed.GetValueUint64() - before
}

func TestPrecachePromotesLookaheadActiveValidators(t *testing.T) {
	h := newBoundaryHead(t)
	lookahead, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(h.epoch, h.stateRoot)
	require.True(t, ok)
	head := h.freshHead(t)

	scans := scansDuring(func() { shuffleInputForEpoch(log.Root(), h.cfg, head, h.epoch) })

	require.Zero(t, scans)
	main, ok := caches.ActiveValidatorsCacheGlobal.Get(h.epoch, h.blockRoot)
	require.True(t, ok)
	require.Same(t, unsafe.SliceData(lookahead), unsafe.SliceData(main))
}

func TestPrecacheDoesNotPromoteForeignLookaheadActiveValidators(t *testing.T) {
	foreign := []uint64{7, 8, 9}
	otherRoot := common.Hash{0xee}

	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) (foreignList []uint64)
	}{
		{"different_state_root", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint64 {
			caches.LookaheadActiveValidatorsCacheGlobal.Put(h.epoch, h.stateRoot, foreign)
			require.NoError(t, head.SetStateRootAt(h.refIndex, otherRoot))
			return foreign
		}},
		{"zero_state_root", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint64 {
			caches.LookaheadActiveValidatorsCacheGlobal.Put(h.epoch, common.Hash{}, foreign)
			require.NoError(t, head.SetStateRootAt(h.refIndex, common.Hash{}))
			return foreign
		}},
		{"absent_lookahead", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint64 {
			caches.LookaheadActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("lookahead_active_validators", caches.MaxLookaheadActiveValidatorsCacheSize)
			return nil
		}},
		{"evicted_lookahead", func(t *testing.T, h *boundaryHead, head *state.CachingBeaconState) []uint64 {
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
			require.Equal(t, h.allActive(), main)
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
	listB := []uint64{1, 2, 3}
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
	caches.ActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("active_validators", caches.MaxActiveValidatorsCacheSize)
	caches.LookaheadActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("lookahead_active_validators", caches.MaxLookaheadActiveValidatorsCacheSize)
	caches.ShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("shuffled_indicies", caches.MaxShuffledIndiciesCacheSize)
	caches.LookaheadShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("lookahead_shuffled_indicies", caches.MaxLookaheadShuffledIndiciesCacheSize)

	preBoundaryEpoch := state.Epoch(h.preBoundary)
	for epoch := uint64(1); epoch <= preBoundaryEpoch+1; epoch++ {
		shuffleInputForEpoch(log.Root(), h.cfg, h.preBoundary, epoch)
	}
	next, err := h.preBoundary.Copy()
	require.NoError(t, err)
	published, err := h.preBoundary.Copy()
	require.NoError(t, err)

	scans := scansDuring(func() {
		require.NoError(t, transition.DefaultMachine.ProcessSlots(next, h.head.Slot()))
		require.NoError(t, next.CopyInto(published))
		current := state.Epoch(published)
		for epoch := current - 2; epoch <= current+1; epoch++ {
			shuffleInputForEpoch(log.Root(), h.cfg, published, epoch)
		}
	})

	require.Equal(t, uint64(1), scans)
}
