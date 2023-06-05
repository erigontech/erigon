package transition

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/transition/machine"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

func TransitionState(s *state.BeaconState, block *cltypes.SignedBeaconBlock, fullValidation bool) error {
	cvm := &impl{FullValidation: fullValidation}
	return machine.TransitionState(cvm, s, block)
}

// transitionSlot is called each time there is a new slot to process
func transitionSlot(s *state.BeaconState) error {
	slot := s.Slot()
	previousStateRoot := s.PreviousStateRoot()
	var err error
	if previousStateRoot == (libcommon.Hash{}) {
		previousStateRoot, err = s.HashSSZ()
		if err != nil {
			return err
		}
	}

	beaconConfig := s.BeaconConfig()

	s.SetStateRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousStateRoot)

	latestBlockHeader := s.LatestBlockHeader()
	if latestBlockHeader.Root == [32]byte{} {
		latestBlockHeader.Root = previousStateRoot
		s.SetLatestBlockHeader(&latestBlockHeader)
	}
	blockHeader := s.LatestBlockHeader()

	previousBlockRoot, err := (&blockHeader).HashSSZ()
	if err != nil {
		return err
	}
	s.SetBlockRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousBlockRoot)
	return nil
}

func (I *impl) ProcessSlots(s *state.BeaconState, slot uint64) error {
	beaconConfig := s.BeaconConfig()
	sSlot := s.Slot()
	if slot <= sSlot {
		return fmt.Errorf("new slot: %d not greater than s slot: %d", slot, sSlot)
	}
	// Process each slot.
	for i := sSlot; i < slot; i++ {
		err := transitionSlot(s)
		if err != nil {
			return fmt.Errorf("unable to process slot transition: %v", err)
		}
		// TODO(Someone): Add epoch transition.
		if (sSlot+1)%beaconConfig.SlotsPerEpoch == 0 {
			start := time.Now()
			if err := ProcessEpoch(s); err != nil {
				return err
			}
			log.Debug("Processed new epoch successfully", "epoch", state.Epoch(s.BeaconState), "process_epoch_elpsed", time.Since(start))
		}
		// TODO: add logic to process epoch updates.
		sSlot += 1
		s.SetSlot(sSlot)
		if sSlot%beaconConfig.SlotsPerEpoch != 0 {
			continue
		}
		if state.Epoch(s.BeaconState) == beaconConfig.AltairForkEpoch {
			if err := s.UpgradeToAltair(); err != nil {
				return err
			}
		}
		if state.Epoch(s.BeaconState) == beaconConfig.BellatrixForkEpoch {
			if err := s.UpgradeToBellatrix(); err != nil {
				return err
			}
		}
		if state.Epoch(s.BeaconState) == beaconConfig.CapellaForkEpoch {
			if err := s.UpgradeToCapella(); err != nil {
				return err
			}
		}
		if state.Epoch(s.BeaconState) == beaconConfig.DenebForkEpoch {
			if err := s.UpgradeToDeneb(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ProcessHistoricalRootsUpdate updates the historical root data structure by computing a new historical root batch when it is time to do so.
func ProcessHistoricalRootsUpdate(s *state.BeaconState) error {
	nextEpoch := state.Epoch(s.BeaconState) + 1
	beaconConfig := s.BeaconConfig()
	blockRoots := s.BlockRoots()
	stateRoots := s.StateRoots()

	// Check if it's time to compute the historical root batch.
	if nextEpoch%(beaconConfig.SlotsPerHistoricalRoot/beaconConfig.SlotsPerEpoch) != 0 {
		return nil
	}

	// Compute historical root batch.
	blockRootsLeaf, err := blockRoots.HashSSZ()
	if err != nil {
		return err
	}
	stateRootsLeaf, err := stateRoots.HashSSZ()
	if err != nil {
		return err
	}

	// Add the historical summary or root to the s.
	if s.Version() >= clparams.CapellaVersion {
		s.AddHistoricalSummary(&cltypes.HistoricalSummary{
			BlockSummaryRoot: blockRootsLeaf,
			StateSummaryRoot: stateRootsLeaf,
		})
	} else {
		historicalRoot := utils.Keccak256(blockRootsLeaf[:], stateRootsLeaf[:])
		s.AddHistoricalRoot(historicalRoot)
	}

	return nil
}
