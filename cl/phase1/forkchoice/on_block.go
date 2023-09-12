package forkchoice

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2/statechange"
)

func (f *ForkChoiceStore) OnBlock(block *cltypes.SignedBeaconBlock, newPayload, fullValidation bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	start := time.Now()
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	if f.Slot() < block.Block.Slot {
		return fmt.Errorf("block is too early compared to current_slot")
	}
	// Check that block is later than the finalized epoch slot (optimization to reduce calls to get_ancestor)
	finalizedSlot := f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Epoch())
	if block.Block.Slot <= finalizedSlot {
		return nil
	}

	config := f.forkGraph.Config()
	lastProcessedState, status, err := f.forkGraph.AddChainSegment(block, fullValidation)
	if err != nil {
		return err
	}
	switch status {
	case fork_graph.PreValidated:
		return nil
	case fork_graph.Success:
	case fork_graph.BelowAnchor:
		log.Debug("replay block", "code", status)
		return nil
	default:
		return fmt.Errorf("replay block, code: %+v", status)
	}
	if block.Block.Body.ExecutionPayload != nil {
		f.eth2Roots.Add(blockRoot, block.Block.Body.ExecutionPayload.BlockHash)
	}
	var invalidBlock bool
	if newPayload && f.engine != nil {
		if invalidBlock, err = f.engine.NewPayload(block.Block.Body.ExecutionPayload, &block.Block.ParentRoot); err != nil {
			log.Warn("newPayload failed", "err", err)
			return err
		}
	}
	if invalidBlock {
		f.forkGraph.MarkHeaderAsInvalid(blockRoot)
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
	if lastProcessedState.Slot()%f.forkGraph.Config().SlotsPerEpoch == 0 {
		if err := freezer.PutObjectSSZIntoFreezer("beaconState", "caplin_core", lastProcessedState.Slot(), lastProcessedState, f.recorder); err != nil {
			return err
		}
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
	if err := statechange.ProcessJustificationBitsAndFinality(lastProcessedState, nil); err != nil {
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
	log.Debug("OnBlock", "elapsed", time.Since(start))
	return nil
}
