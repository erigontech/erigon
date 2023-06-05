// Package machine is the interface for eth2 state transition
package machine

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/metrics/methelp"
)

type Interface interface {
	BlockValidator
	BlockProcessor
}

type BlockValidator interface {
	VerifyBlockSignature(s *state.BeaconState, block *cltypes.SignedBeaconBlock) (bool, error)
	VerifyTransition(s *state.BeaconState, block *cltypes.BeaconBlock) error
}

type BlockProcessor interface {
	ProcessBlockHeader(s *state.BeaconState, block *cltypes.BeaconBlock) error
	ProcessWithdrawals(s *state.BeaconState, withdrawals *solid.ListSSZ[*types.Withdrawal]) error
	ProcessExecutionPayload()
	ProcessRandaoReveal()
	ProcessEth1Data()
	ProcessProposerSlashing()
	ProcessAttesterSlashing()
	ProcessAttestations()
	ProcessSyncAggregate()

	VerifyKzgCommitments()
}

type SlotProcessor interface {
}

func ProcessSlots(impl Interface, s *state.BeaconState, slot uint64) error {
	return nil
}

func processBlock(impl BlockProcessor, s *state.BeaconState, signedBlock *cltypes.SignedBeaconBlock) error {
	block := signedBlock.Block
	version := s.Version()
	// Check the state version is correct.
	if signedBlock.Version() != version {
		return fmt.Errorf("processBlock: wrong state version for block at slot %d", block.Slot)
	}
	h := methelp.NewHistTimer("beacon_process_block")
	c := h.Tag("process_step", "block_header")
	// Process the block header.
	if err := impl.ProcessBlockHeader(s, block); err != nil {
		return fmt.Errorf("processBlock: failed to process block header: %v", err)
	}
	c.PutSince()
	return nil
}

func TransitionState(impl Interface, s *state.BeaconState, block *cltypes.SignedBeaconBlock) error {
	currentBlock := block.Block
	if err := ProcessSlots(impl, s, currentBlock.Slot); err != nil {
		return err
	}

	valid, err := impl.VerifyBlockSignature(s, block)
	if err != nil {
		return fmt.Errorf("error validating block signature: %v", err)
	}
	if !valid {
		return fmt.Errorf("block not valid")
	}

	// Transition block
	if err := processBlock(impl, s, block); err != nil {
		return err
	}

	// perform validation
	if err := impl.VerifyTransition(s, currentBlock); err != nil {
		return err
	}

	// if validation is successful, transition
	s.SetPreviousStateRoot(currentBlock.StateRoot)
	return nil
}
