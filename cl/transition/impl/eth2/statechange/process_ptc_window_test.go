package statechange_test

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func ptcWindowTestState(tb testing.TB, validatorCount int) *state.CachingBeaconState {
	tb.Helper()
	cfg := &clparams.MainnetBeaconConfig
	s := state.New(cfg)
	s.SetVersion(clparams.GloasVersion)
	for i := range validatorCount {
		validator := solid.NewValidator()
		var publicKey [48]byte
		binary.LittleEndian.PutUint64(publicKey[:8], uint64(i))
		validator.SetPublicKey(publicKey)
		validator.SetActivationEpoch(0)
		validator.SetExitEpoch(cfg.FarFutureEpoch)
		validator.SetEffectiveBalance(cfg.MaxEffectiveBalance)
		require.NoError(tb, s.AddValidator(validator, cfg.MaxEffectiveBalance))
	}
	require.NoError(tb, s.SetSlot(4*cfg.SlotsPerEpoch-1))
	header := s.LatestBlockHeader()
	header.Root[0] = 1
	s.SetLatestBlockHeader(&header)
	stateRoot, err := s.HashSSZ()
	require.NoError(tb, err)
	root, err := header.HashSSZ()
	require.NoError(tb, err)
	index := int(s.Slot() % cfg.SlotsPerHistoricalRoot)
	require.NoError(tb, s.SetBlockRootAt(index, root))
	require.NoError(tb, s.SetStateRootAt(index, stateRoot))
	return s
}

func useFreshValidatorCaches(tb testing.TB) func() {
	tb.Helper()
	previousShuffled := caches.ShuffledIndiciesCacheGlobal
	previousLookaheadShuffled := caches.LookaheadShuffledIndiciesCacheGlobal
	previousActive := caches.ActiveValidatorsCacheGlobal
	previousLookaheadActive := caches.LookaheadActiveValidatorsCacheGlobal
	reset := func() {
		caches.ShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("shuffled_indicies", caches.MaxShuffledIndiciesCacheSize)
		caches.LookaheadShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("lookahead_shuffled_indicies", caches.MaxLookaheadShuffledIndiciesCacheSize)
		caches.ActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("active_validators", caches.MaxActiveValidatorsCacheSize)
		caches.LookaheadActiveValidatorsCacheGlobal = caches.NewActiveValidatorsCache("lookahead_active_validators", caches.MaxLookaheadActiveValidatorsCacheSize)
	}
	reset()
	tb.Cleanup(func() {
		caches.ShuffledIndiciesCacheGlobal = previousShuffled
		caches.LookaheadShuffledIndiciesCacheGlobal = previousLookaheadShuffled
		caches.ActiveValidatorsCacheGlobal = previousActive
		caches.LookaheadActiveValidatorsCacheGlobal = previousLookaheadActive
	})
	return reset
}

func ptcWindowValues(s *state.CachingBeaconState) [][]uint64 {
	window := s.GetPtcWindow()
	values := make([][]uint64, window.Length())
	for i := range values {
		entry := window.Get(i)
		values[i] = make([]uint64, entry.Length())
		for j := range values[i] {
			values[i][j] = entry.Get(j)
		}
	}
	return values
}

func coldPtcWindow(tb testing.TB, source *state.CachingBeaconState, resetCaches func()) [][]uint64 {
	tb.Helper()
	resetCaches()
	return ptcWindowValues(processPtcWindowCopy(tb, source))
}

func nextPtcEpochFirstSlot(s *state.CachingBeaconState) uint64 {
	cfg := s.BeaconConfig()
	return (state.Epoch(s) + cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch
}

func ptcWindowSlotTransitionState(tb testing.TB, validatorCount int) (*state.CachingBeaconState, uint64) {
	tb.Helper()
	s := ptcWindowTestState(tb, validatorCount)
	cfg := s.BeaconConfig()
	boundarySlot := 4*cfg.SlotsPerEpoch - 1
	require.NoError(tb, s.SetSlot(boundarySlot-1))
	s.SetCurrentEpochParticipation(solid.NewParticipationBitList(s.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	s.SetPreviousEpochParticipation(solid.NewParticipationBitList(s.ValidatorLength(), int(cfg.ValidatorRegistryLimit)))
	s.SetInactivityScores(make([]uint64, s.ValidatorLength()))
	header := s.LatestBlockHeader()
	header.Slot = s.Slot()
	s.SetLatestBlockHeader(&header)
	index := int(boundarySlot % cfg.SlotsPerHistoricalRoot)
	require.NoError(tb, s.SetBlockRootAt(index, common.Hash{}))
	require.NoError(tb, s.SetStateRootAt(index, common.Hash{}))
	return s, boundarySlot
}

func TestProcessPtcWindowSharesShuffleAcrossStateCopies(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 16_384)
	want := coldPtcWindow(t, base, resetCaches)
	a, err := base.Copy()
	require.NoError(t, err)
	b, err := base.Copy()
	require.NoError(t, err)
	resetCaches()

	require.NoError(t, statechange.ProcessPtcWindow(a))
	require.NoError(t, statechange.ProcessPtcWindow(b))
	require.Equal(t, want, ptcWindowValues(a))
	require.Equal(t, want, ptcWindowValues(b))

	firstSlot := nextPtcEpochFirstSlot(base)
	activeA := a.GetActiveValidatorsIndices(firstSlot / base.BeaconConfig().SlotsPerEpoch)
	activeB := b.GetActiveValidatorsIndices(firstSlot / base.BeaconConfig().SlotsPerEpoch)
	require.Same(t, unsafe.SliceData(activeA), unsafe.SliceData(activeB))
	committeeA, err := a.GetBeaconCommitee(firstSlot, 0)
	require.NoError(t, err)
	committeeB, err := b.GetBeaconCommitee(firstSlot, 0)
	require.NoError(t, err)
	require.Same(t, unsafe.SliceData(committeeA), unsafe.SliceData(committeeB))
	require.Equal(t, cap(committeeA), len(committeeA))
	require.Equal(t, cap(committeeB), len(committeeB))
}

func TestProcessSlotsCachesPtcWindowAtBoundaryRoot(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base, boundarySlot := ptcWindowSlotTransitionState(t, 16_384)
	cfg := base.BeaconConfig()
	header := base.LatestBlockHeader()
	header.Root = common.Hash{}
	base.SetLatestBlockHeader(&header)
	targetEpoch := state.Epoch(base) + cfg.MinSeedLookahead + 1

	cold, err := base.Copy()
	require.NoError(t, err)
	resetCaches()
	require.NoError(t, transition.DefaultMachine.ProcessSlots(cold, boundarySlot+1))
	want := ptcWindowValues(cold)

	first, err := base.Copy()
	require.NoError(t, err)
	second, err := base.Copy()
	require.NoError(t, err)
	resetCaches()
	require.NoError(t, transition.DefaultMachine.ProcessSlots(first, boundarySlot+1))
	root, err := first.GetBlockRootAtSlot(boundarySlot)
	require.NoError(t, err)
	resolvedHeader := first.LatestBlockHeader()
	require.NotEqual(t, common.Hash{}, resolvedHeader.Root)
	headerRoot, err := resolvedHeader.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, common.Hash(headerRoot))
	lookaheadRoot := first.StateRoots().Get(int(boundarySlot % cfg.SlotsPerHistoricalRoot))
	require.NotEqual(t, common.Hash{}, lookaheadRoot)
	activeCache, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(targetEpoch, lookaheadRoot)
	require.True(t, ok)
	shuffleCache, ok := caches.LookaheadShuffledIndiciesCacheGlobal.Get(targetEpoch, lookaheadRoot)
	require.True(t, ok)

	require.NoError(t, transition.DefaultMachine.ProcessSlots(second, boundarySlot+1))
	require.Equal(t, want, ptcWindowValues(first))
	require.Equal(t, want, ptcWindowValues(second))
	activeFirst := first.GetActiveValidatorsIndices(targetEpoch)
	activeSecond := second.GetActiveValidatorsIndices(targetEpoch)
	require.Same(t, unsafe.SliceData(activeCache), unsafe.SliceData(activeFirst))
	require.Same(t, unsafe.SliceData(activeFirst), unsafe.SliceData(activeSecond))
	committeeFirst, err := first.GetBeaconCommitee(targetEpoch*cfg.SlotsPerEpoch, 0)
	require.NoError(t, err)
	committeeSecond, err := second.GetBeaconCommitee(targetEpoch*cfg.SlotsPerEpoch, 0)
	require.NoError(t, err)
	require.Same(t, unsafe.SliceData(shuffleCache), unsafe.SliceData(committeeFirst))
	require.Same(t, unsafe.SliceData(committeeFirst), unsafe.SliceData(committeeSecond))
}

func TestProcessPtcWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := &clparams.MainnetBeaconConfig
	slotsPerEpoch := cfg.SlotsPerEpoch                                // 32
	totalSlots := int((2 + cfg.MinSeedLookahead) * cfg.SlotsPerEpoch) // (2+1)*32 = 96
	lastEpochStart := totalSlots - int(slotsPerEpoch)                 // 64
	ptcSize := int(clparams.PtcSize)                                  // 512

	// Build an initial PTC window with distinguishable values per slot.
	// Epoch 0 slots [0..31]:  validator indices all = slot*10
	// Epoch 1 slots [32..63]: validator indices all = slot*10
	// Epoch 2 slots [64..95]: validator indices all = slot*10
	initial := solid.NewUint64VectorOfVectors(totalSlots, ptcSize)
	for i := range totalSlots {
		vec := solid.NewUint64VectorSSZ(ptcSize)
		for j := range ptcSize {
			vec.Set(j, uint64(i*10+j))
		}
		initial.Set(i, vec)
	}

	// The current epoch = slot / slotsPerEpoch. Set slot at epoch boundary.
	currentEpoch := uint64(5)
	slot := currentEpoch * slotsPerEpoch                 // slot 160
	nextEpoch := currentEpoch + cfg.MinSeedLookahead + 1 // epoch 7
	nextEpochStartSlot := nextEpoch * slotsPerEpoch      // slot 224

	mockState := mock_services.NewMockBeaconState(ctrl)
	mockState.EXPECT().BeaconConfig().Return(cfg).AnyTimes()
	mockState.EXPECT().Slot().Return(slot).AnyTimes()
	mockState.EXPECT().GetPtcWindow().Return(initial)

	// Mock ComputePTC: for each slot in next epoch, return predictable values.
	for i := range slotsPerEpoch {
		s := nextEpochStartSlot + i
		ptc := make([]uint64, ptcSize)
		for j := range ptcSize {
			ptc[j] = s*1000 + uint64(j) // unique per slot
		}
		mockState.EXPECT().ComputePTC(s).Return(ptc, nil)
	}

	// Capture the new window passed to SetPtcWindow.
	var captured *solid.VectorSSZ[solid.Uint64VectorSSZ]
	mockState.EXPECT().SetPtcWindow(gomock.Any()).Do(func(w *solid.VectorSSZ[solid.Uint64VectorSSZ]) {
		captured = w
	})

	// Execute
	err := statechange.ProcessPtcWindow(mockState)
	require.NoError(t, err)
	require.NotNil(t, captured)
	require.Equal(t, totalSlots, captured.Length())

	// Verify shift: positions [0, lastEpochStart) should equal old positions
	// [slotsPerEpoch, totalSlots), i.e., old epochs 1 and 2.
	for i := range lastEpochStart {
		oldIdx := i + int(slotsPerEpoch)
		got := captured.Get(i)
		// Check first element to verify correct shift
		require.Equal(t, uint64(oldIdx*10+0), got.Get(0),
			"shifted slot %d should match old slot %d", i, oldIdx)
		// Spot-check last element
		require.Equal(t, uint64(oldIdx*10+ptcSize-1), got.Get(ptcSize-1),
			"shifted slot %d last element should match old slot %d", i, oldIdx)
	}

	// Verify fill: positions [lastEpochStart, totalSlots) should be from ComputePTC.
	for i := 0; i < int(slotsPerEpoch); i++ {
		idx := lastEpochStart + i
		s := nextEpochStartSlot + uint64(i)
		got := captured.Get(idx)
		require.Equal(t, s*1000+0, got.Get(0),
			"new epoch slot %d first element mismatch", s)
		require.Equal(t, s*1000+uint64(ptcSize-1), got.Get(ptcSize-1),
			"new epoch slot %d last element mismatch", s)
	}
}
