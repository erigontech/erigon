package transition

import (
	"fmt"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/log/v3"
)

func TransitionState(s *state.BeaconState, block *cltypes.SignedBeaconBlock, fullValidation bool) error {
	currentBlock := block.Block
	if err := ProcessSlots(s, currentBlock.Slot); err != nil {
		return err
	}

	if fullValidation {
		valid, err := verifyBlockSignature(s, block)
		if err != nil {
			return fmt.Errorf("error validating block signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("block not valid")
		}
	}
	// Transition block
	if err := processBlock(s, block, fullValidation); err != nil {
		return err
	}
	if fullValidation {
		expectedStateRoot, err := s.HashSSZ()
		if err != nil {
			return fmt.Errorf("unable to generate state root: %v", err)
		}
		if expectedStateRoot != currentBlock.StateRoot {
			return fmt.Errorf("expected state root differs from received state root")
		}
	}

	s.SetPreviousStateRoot(currentBlock.StateRoot)
	return nil
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

func ProcessSlots(s *state.BeaconState, slot uint64) error {
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

func verifyBlockSignature(s *state.BeaconState, block *cltypes.SignedBeaconBlock) (bool, error) {
	proposer, err := s.ValidatorForValidatorIndex(int(block.Block.ProposerIndex))
	if err != nil {
		return false, err
	}
	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconProposer, state.Epoch(s.BeaconState))
	if err != nil {
		return false, err
	}
	sigRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	if err != nil {
		return false, err
	}
	return bls.Verify(block.Signature[:], sigRoot[:], proposer.PublicKey[:])
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
	blockRootsLeaf, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(blockRoots[:]), state_encoding.BlockRootsLength)
	if err != nil {
		return err
	}
	stateRootsLeaf, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(stateRoots[:]), state_encoding.StateRootsLength)
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
