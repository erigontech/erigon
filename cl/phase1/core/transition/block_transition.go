package transition

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/metrics/methelp"
)

// processBlock takes a block and transitions the state to the next slot, using the provided execution payload if enabled.
func processBlock(state *state2.BeaconState, signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) error {
	block := signedBlock.Block
	version := state.Version()
	// Check the state version is correct.
	if signedBlock.Version() != version {
		return fmt.Errorf("processBlock: wrong state version for block at slot %d", block.Slot)
	}

	h := methelp.NewHistTimer("beacon_process_block")

	c := h.Tag("process_step", "block_header")
	// Process the block header.
	if err := ProcessBlockHeader(state, block, fullValidation); err != nil {
		return fmt.Errorf("processBlock: failed to process block header: %v", err)
	}
	c.PutSince()

	// Process execution payload if enabled.
	if version >= clparams.BellatrixVersion && executionEnabled(state, block.Body.ExecutionPayload) {
		if state.Version() >= clparams.CapellaVersion {
			// Process withdrawals in the execution payload.
			c = h.Tag("process_step", "withdrawals")
			if err := ProcessWithdrawals(state, block.Body.ExecutionPayload.Withdrawals, fullValidation); err != nil {
				return fmt.Errorf("processBlock: failed to process withdrawals: %v", err)
			}
			c.PutSince()
		}

		// Process the execution payload.
		c = h.Tag("process_step", "execution_payload")
		if err := ProcessExecutionPayload(state, block.Body.ExecutionPayload); err != nil {
			return fmt.Errorf("processBlock: failed to process execution payload: %v", err)
		}
		c.PutSince()
	}

	// Process RANDAO reveal.
	c = h.Tag("process_step", "randao_reveal")
	if err := ProcessRandao(state, block.Body.RandaoReveal, block.ProposerIndex, fullValidation); err != nil {
		return fmt.Errorf("processBlock: failed to process RANDAO reveal: %v", err)
	}
	c.PutSince()

	// Process Eth1 data.
	c = h.Tag("process_step", "eth1_data")
	if err := ProcessEth1Data(state, block.Body.Eth1Data); err != nil {
		return fmt.Errorf("processBlock: failed to process Eth1 data: %v", err)
	}
	c.PutSince()

	// Process block body operations.
	c = h.Tag("process_step", "operations")
	if err := processOperations(state, block.Body, fullValidation); err != nil {
		return fmt.Errorf("processBlock: failed to process block body operations: %v", err)
	}
	c.PutSince()

	// Process sync aggregate in case of Altair version.
	if version >= clparams.AltairVersion {
		c = h.Tag("process_step", "sync_aggregate")
		if err := ProcessSyncAggregate(state, block.Body.SyncAggregate, fullValidation); err != nil {
			return fmt.Errorf("processBlock: failed to process sync aggregate: %v", err)
		}
		c.PutSince()
	}

	if version >= clparams.DenebVersion && fullValidation {
		c = h.Tag("process_step", "blob_kzg_commitments")
		verified, err := VerifyKzgCommitmentsAgainstTransactions(block.Body.ExecutionPayload.Transactions, block.Body.BlobKzgCommitments)
		if err != nil {
			return fmt.Errorf("processBlock: failed to process blob kzg commitments: %w", err)
		}
		if !verified {
			return fmt.Errorf("processBlock: failed to process blob kzg commitments: commitments are not equal")
		}
		c.PutSince()
	}

	h.PutSince()
	return nil
}

func processOperations(state *state2.BeaconState, blockBody *cltypes.BeaconBody, fullValidation bool) error {
	if blockBody.Deposits.Len() != int(maximumDeposits(state)) {
		return errors.New("outstanding deposits do not match maximum deposits")
	}
	h := methelp.NewHistTimer("beacon_process_block_operations")

	// Process each proposer slashing
	var err error
	c := h.Tag("operation", "proposer_slashings")
	if err := solid.RangeErr[*cltypes.ProposerSlashing](blockBody.ProposerSlashings, func(index int, slashing *cltypes.ProposerSlashing, length int) error {
		if err = ProcessProposerSlashing(state, slashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	c.PutSince()

	c = h.Tag("operation", "attester_slashings")
	if err := solid.RangeErr[*cltypes.AttesterSlashing](blockBody.AttesterSlashings, func(index int, slashing *cltypes.AttesterSlashing, length int) error {
		if err = ProcessAttesterSlashing(state, slashing); err != nil {
			return fmt.Errorf("ProcessAttesterSlashing: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	c.PutSince()

	// Process each attestations
	c = h.Tag("operation", "attestations", "validation", "false")
	if fullValidation {
		c = h.Tag("operation", "attestations", "validation", "true")
	}
	if err := ProcessAttestations(state, blockBody.Attestations, fullValidation); err != nil {
		return fmt.Errorf("ProcessAttestation: %s", err)
	}
	c.PutSince()

	// Process each deposit
	c = h.Tag("operation", "deposit")
	if err := solid.RangeErr[*cltypes.Deposit](blockBody.Deposits, func(index int, deposit *cltypes.Deposit, length int) error {
		if err = ProcessDeposit(state, deposit, fullValidation); err != nil {
			return fmt.Errorf("ProcessDeposit: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	c.PutSince()

	// Process each voluntary exit.
	c = h.Tag("operation", "voluntary_exit")
	if err := solid.RangeErr[*cltypes.SignedVoluntaryExit](blockBody.VoluntaryExits, func(index int, exit *cltypes.SignedVoluntaryExit, length int) error {
		if err = ProcessVoluntaryExit(state, exit, fullValidation); err != nil {
			return fmt.Errorf("ProcessVoluntaryExit: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	c.PutSince()
	if state.Version() < clparams.CapellaVersion {
		return nil
	}
	// Process each execution change. this will only have entries after the capella fork.
	c = h.Tag("operation", "execution_change")
	if err := solid.RangeErr[*cltypes.SignedBLSToExecutionChange](blockBody.ExecutionChanges, func(index int, addressChange *cltypes.SignedBLSToExecutionChange, length int) error {
		if err := ProcessBlsToExecutionChange(state, addressChange, fullValidation); err != nil {
			return fmt.Errorf("ProcessBlsToExecutionChange: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	c.PutSince()
	return nil
}

func maximumDeposits(state *state2.BeaconState) (maxDeposits uint64) {
	maxDeposits = state.Eth1Data().DepositCount - state.Eth1DepositIndex()
	if maxDeposits > state.BeaconConfig().MaxDeposits {
		maxDeposits = state.BeaconConfig().MaxDeposits
	}
	return
}

// ProcessExecutionPayload sets the latest payload header accordinly.
func ProcessExecutionPayload(s *state2.BeaconState, payload *cltypes.Eth1Block) error {
	if state2.IsMergeTransitionComplete(s.BeaconState) {
		if payload.ParentHash != s.LatestExecutionPayloadHeader().BlockHash {
			return fmt.Errorf("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	if payload.PrevRandao != s.GetRandaoMixes(state2.Epoch(s.BeaconState)) {
		return fmt.Errorf("ProcessExecutionPayload: randao mix mismatches with mix digest")
	}
	if payload.Time != state2.ComputeTimestampAtSlot(s.BeaconState, s.Slot()) {
		return fmt.Errorf("ProcessExecutionPayload: invalid Eth1 timestamp")
	}
	payloadHeader, err := payload.PayloadHeader()
	if err != nil {
		return err
	}
	s.SetLatestExecutionPayloadHeader(payloadHeader)
	return nil
}

func executionEnabled(s *state2.BeaconState, payload *cltypes.Eth1Block) bool {
	return (!state2.IsMergeTransitionComplete(s.BeaconState) && payload.BlockHash != libcommon.Hash{}) || state2.IsMergeTransitionComplete(s.BeaconState)
}
