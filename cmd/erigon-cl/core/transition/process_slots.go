package transition

import (
	"fmt"
	"time"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"

	"github.com/ledgerwatch/log/v3"
)

func (s *StateTransistor) TransitionState(block *cltypes.SignedBeaconBlock) error {
	currentBlock := block.Block
	s.processSlots(currentBlock.Slot)
	// Write the block root to the cache
	if !s.noValidate {
		valid, err := s.verifyBlockSignature(block)
		if err != nil {
			return fmt.Errorf("error validating block signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("block not valid")
		}
	}
	// Transition block
	if err := s.processBlock(block); err != nil {
		return err
	}
	if !s.noValidate {
		expectedStateRoot, err := s.state.HashSSZ()
		if err != nil {
			return fmt.Errorf("unable to generate state root: %v", err)
		}
		if expectedStateRoot != currentBlock.StateRoot {
			return fmt.Errorf("expected state root differs from received state root")
		}
	}
	// Write the block root to the cache
	s.stateRootsCache.Add(block.Block.Slot, block.Block.StateRoot)

	return nil
}

// transitionSlot is called each time there is a new slot to process
func (s *StateTransistor) transitionSlot() error {
	slot := s.state.Slot()
	var (
		previousStateRoot libcommon.Hash
		err               error
	)
	if previousStateRootI, ok := s.stateRootsCache.Get(slot); ok {
		previousStateRoot = previousStateRootI.(libcommon.Hash)
	} else {
		previousStateRoot, err = s.state.HashSSZ()
		if err != nil {
			return err
		}
	}
	s.state.SetStateRootAt(int(slot%s.beaconConfig.SlotsPerHistoricalRoot), previousStateRoot)

	latestBlockHeader := s.state.LatestBlockHeader()
	if latestBlockHeader.Root == [32]byte{} {
		latestBlockHeader.Root = previousStateRoot
		s.state.SetLatestBlockHeader(latestBlockHeader)
	}

	previousBlockRoot, err := s.state.LatestBlockHeader().HashSSZ()
	if err != nil {
		return err
	}
	s.state.SetBlockRootAt(int(slot%s.beaconConfig.SlotsPerHistoricalRoot), previousBlockRoot)
	return nil
}

func (s *StateTransistor) processSlots(slot uint64) error {
	stateSlot := s.state.Slot()
	if slot <= stateSlot {
		return fmt.Errorf("new slot: %d not greater than state slot: %d", slot, stateSlot)
	}
	// Process each slot.
	for i := stateSlot; i < slot; i++ {
		err := s.transitionSlot()
		if err != nil {
			return fmt.Errorf("unable to process slot transition: %v", err)
		}
		// TODO(Someone): Add epoch transition.
		if (stateSlot+1)%s.beaconConfig.SlotsPerEpoch == 0 {
			start := time.Now()
			if err := s.ProcessEpoch(); err != nil {
				return err
			}
			log.Info("Processed new epoch successfully", "epoch", s.state.Epoch(), "process_epoch_elpsed", time.Since(start))
		}
		// TODO: add logic to process epoch updates.
		stateSlot += 1
		s.state.SetSlot(stateSlot)
	}
	return nil
}

func (s *StateTransistor) verifyBlockSignature(block *cltypes.SignedBeaconBlock) (bool, error) {
	proposer, err := s.state.ValidatorAt(int(block.Block.ProposerIndex))
	if err != nil {
		return false, err
	}
	domain, err := s.state.GetDomain(s.beaconConfig.DomainBeaconProposer, s.state.Epoch())
	if err != nil {
		return false, err
	}
	sigRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	if err != nil {
		return false, err
	}
	return bls.Verify(block.Signature[:], sigRoot[:], proposer.PublicKey[:])
}

func (s *StateTransistor) ProcessHistoricalRootsUpdate() error {
	nextEpoch := s.state.Epoch() + 1
	if nextEpoch%(s.beaconConfig.SlotsPerHistoricalRoot/s.beaconConfig.SlotsPerEpoch) == 0 {
		var (
			blockRoots = s.state.BlockRoots()
			stateRoots = s.state.StateRoots()
		)

		// Compute historical root batch.
		blockRootsLeaf, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(blockRoots[:]), state_encoding.BlockRootsLength)
		if err != nil {
			return err
		}
		stateRootsLeaf, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(stateRoots[:]), state_encoding.StateRootsLength)
		if err != nil {
			return err
		}

		s.state.AddHistoricalRoot(utils.Keccak256(blockRootsLeaf[:], stateRootsLeaf[:]))
	}
	return nil
}
