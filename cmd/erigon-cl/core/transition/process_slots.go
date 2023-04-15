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

func TransitionState(state *state.BeaconState, block *cltypes.SignedBeaconBlock, fullValidation bool) error {
	currentBlock := block.Block
	if err := ProcessSlots(state, currentBlock.Slot); err != nil {
		return err
	}

	if fullValidation {
		valid, err := verifyBlockSignature(state, block)
		if err != nil {
			return fmt.Errorf("error validating block signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("block not valid")
		}
	}
	// Transition block
	if err := processBlock(state, block, fullValidation); err != nil {
		return err
	}
	if fullValidation {
		expectedStateRoot, err := state.HashSSZ()
		if err != nil {
			return fmt.Errorf("unable to generate state root: %v", err)
		}
		if expectedStateRoot != currentBlock.StateRoot {
			return fmt.Errorf("expected state root differs from received state root")
		}
	}

	state.SetPreviousStateRoot(currentBlock.StateRoot)
	return nil
}

// transitionSlot is called each time there is a new slot to process
func transitionSlot(state *state.BeaconState) error {
	slot := state.Slot()
	previousStateRoot := state.PreviousStateRoot()
	var err error
	if previousStateRoot == (libcommon.Hash{}) {
		previousStateRoot, err = state.HashSSZ()
		if err != nil {
			return err
		}
	}

	beaconConfig := state.BeaconConfig()

	state.SetStateRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousStateRoot)

	latestBlockHeader := state.LatestBlockHeader()
	if latestBlockHeader.Root == [32]byte{} {
		latestBlockHeader.Root = previousStateRoot
		state.SetLatestBlockHeader(&latestBlockHeader)
	}
	blockHeader := state.LatestBlockHeader()

	previousBlockRoot, err := (&blockHeader).HashSSZ()
	if err != nil {
		return err
	}
	state.SetBlockRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousBlockRoot)
	return nil
}

func ProcessSlots(state *state.BeaconState, slot uint64) error {
	beaconConfig := state.BeaconConfig()
	stateSlot := state.Slot()
	if slot <= stateSlot {
		return fmt.Errorf("new slot: %d not greater than state slot: %d", slot, stateSlot)
	}
	// Process each slot.
	for i := stateSlot; i < slot; i++ {
		err := transitionSlot(state)
		if err != nil {
			return fmt.Errorf("unable to process slot transition: %v", err)
		}
		// TODO(Someone): Add epoch transition.
		if (stateSlot+1)%beaconConfig.SlotsPerEpoch == 0 {
			start := time.Now()
			if err := ProcessEpoch(state); err != nil {
				return err
			}
			log.Info("Processed new epoch successfully", "epoch", state.Epoch(), "process_epoch_elpsed", time.Since(start))
		}
		// TODO: add logic to process epoch updates.
		stateSlot += 1
		state.SetSlot(stateSlot)
		if stateSlot%beaconConfig.SlotsPerEpoch != 0 {
			continue
		}
		if state.Epoch() == beaconConfig.AltairForkEpoch {
			if err := state.UpgradeToAltair(); err != nil {
				return err
			}
		}
		if state.Epoch() == beaconConfig.BellatrixForkEpoch {
			if err := state.UpgradeToBellatrix(); err != nil {
				return err
			}
		}
		if state.Epoch() == beaconConfig.CapellaForkEpoch {
			if err := state.UpgradeToCapella(); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyBlockSignature(state *state.BeaconState, block *cltypes.SignedBeaconBlock) (bool, error) {
	proposer, err := state.ValidatorForValidatorIndex(int(block.Block.ProposerIndex))
	if err != nil {
		return false, err
	}
	domain, err := state.GetDomain(state.BeaconConfig().DomainBeaconProposer, state.Epoch())
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
func ProcessHistoricalRootsUpdate(state *state.BeaconState) error {
	nextEpoch := state.Epoch() + 1
	beaconConfig := state.BeaconConfig()
	blockRoots := state.BlockRoots()
	stateRoots := state.StateRoots()

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

	// Add the historical summary or root to the state.
	if state.Version() >= clparams.CapellaVersion {
		state.AddHistoricalSummary(&cltypes.HistoricalSummary{
			BlockSummaryRoot: blockRootsLeaf,
			StateSummaryRoot: stateRootsLeaf,
		})
	} else {
		historicalRoot := utils.Keccak256(blockRootsLeaf[:], stateRootsLeaf[:])
		state.AddHistoricalRoot(historicalRoot)
	}

	return nil
}
