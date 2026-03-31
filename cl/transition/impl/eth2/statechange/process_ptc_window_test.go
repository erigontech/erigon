package statechange_test

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/stretchr/testify/require"
)

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
	for i := 0; i < totalSlots; i++ {
		vec := solid.NewUint64VectorSSZ(ptcSize)
		for j := 0; j < ptcSize; j++ {
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
	for i := uint64(0); i < slotsPerEpoch; i++ {
		s := nextEpochStartSlot + i
		ptc := make([]uint64, ptcSize)
		for j := 0; j < ptcSize; j++ {
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
	for i := 0; i < lastEpochStart; i++ {
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
