package forkchoice

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/phase1/core/transition"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
)

func (f *ForkChoiceStore) OnBlock(block *cltypes.SignedBeaconBlock, newPayload, fullValidation bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	if f.Slot() < block.Block.Slot {
		return fmt.Errorf("block is too late compared to current_slot")
	}
	// Check that block is later than the finalized epoch slot (optimization to reduce calls to get_ancestor)
	finalizedSlot := f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Epoch())
	if block.Block.Slot <= finalizedSlot {
		return fmt.Errorf("block is too late compared to finalized")
	}

	config := f.forkGraph.Config()
	lastProcessedState, status, err := f.forkGraph.AddChainSegment(block, fullValidation)
	if status != fork_graph.Success {
		if status != fork_graph.PreValidated {
			log.Debug("Could not replay block", "slot", block.Block.Slot, "code", status, "reason", err)
			return fmt.Errorf("could not replay block, err: %s, code: %d", err, status)
		}
		return nil
	}
	if newPayload && f.engine != nil {
		if err := f.engine.NewPayload(block.Block.Body.ExecutionPayload); err != nil {
			log.Warn("newPayload failed", "err", err)
			return err
		}
	}
	if block.Block.Body.ExecutionPayload != nil {
		f.eth2Roots.Add(blockRoot, block.Block.Body.ExecutionPayload.BlockHash)
	}
	if block.Block.Slot > f.highestSeen {
		f.highestSeen = block.Block.Slot
	}
	// Add proposer score boost if the block is timely
	timeIntoSlot := (f.time - f.forkGraph.GenesisTime()) % lastProcessedState.BeaconConfig().SecondsPerSlot
	isBeforeAttestingInterval := timeIntoSlot < config.SecondsPerSlot/config.IntervalsPerSlot
	if f.Slot() == block.Block.Slot && isBeforeAttestingInterval {
		f.proposerBoostRoot = blockRoot
	}
	// Update checkpoints
	f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	// First thing save previous values of the checkpoints (avoid memory copy of all states and ensure easy revert)
	var (
		previousJustifiedCheckpoint = lastProcessedState.PreviousJustifiedCheckpoint().Copy()
		currentJustifiedCheckpoint  = lastProcessedState.CurrentJustifiedCheckpoint().Copy()
		finalizedCheckpoint         = lastProcessedState.FinalizedCheckpoint().Copy()
		justificationBits           = lastProcessedState.JustificationBits().Copy()
	)
	// Eagerly compute unrealized justification and finality
	if err := transition.ProcessJustificationBitsAndFinality(lastProcessedState); err != nil {
		return err
	}
	f.updateUnrealizedCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	// Set the changed value pre-simulation
	lastProcessedState.SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint)
	lastProcessedState.SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint)
	lastProcessedState.SetFinalizedCheckpoint(finalizedCheckpoint)
	lastProcessedState.SetJustificationBits(justificationBits)
	// If the block is from a prior epoch, apply the realized values
	blockEpoch := f.computeEpochAtSlot(block.Block.Slot)
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	if blockEpoch < currentEpoch {
		f.updateCheckpoints(lastProcessedState.CurrentJustifiedCheckpoint().Copy(), lastProcessedState.FinalizedCheckpoint().Copy())
	}
	return nil
}
