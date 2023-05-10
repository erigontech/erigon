package transition

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/metrics/methelp"
)

// processBlock takes a block and transitions the state to the next slot, using the provided execution payload if enabled.
func processBlock(state *state.BeaconState, signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) error {
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

	h.PutSince()
	return nil
}

func processOperations(state *state.BeaconState, blockBody *cltypes.BeaconBody, fullValidation bool) error {
	if len(blockBody.Deposits) != int(maximumDeposits(state)) {
		return errors.New("outstanding deposits do not match maximum deposits")
	}
	h := methelp.NewHistTimer("beacon_process_block_operations")

	// Process each proposer slashing
	c := h.Tag("operation", "proposer_slashings")
	for _, slashing := range blockBody.ProposerSlashings {
		if err := ProcessProposerSlashing(state, slashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
	}
	c.PutSince()
	// Process each attester slashing
	c = h.Tag("operation", "attester_slashings")
	for _, slashing := range blockBody.AttesterSlashings {
		if err := ProcessAttesterSlashing(state, slashing); err != nil {
			return fmt.Errorf("ProcessAttesterSlashing: %s", err)
		}
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
	for _, dep := range blockBody.Deposits {
		if err := ProcessDeposit(state, dep, fullValidation); err != nil {
			return fmt.Errorf("ProcessDeposit: %s", err)
		}
	}
	c.PutSince()

	// Process each voluntary exit.
	c = h.Tag("operation", "voluntary_exit")
	for _, exit := range blockBody.VoluntaryExits {
		if err := ProcessVoluntaryExit(state, exit, fullValidation); err != nil {
			return fmt.Errorf("ProcessVoluntaryExit: %s", err)
		}
	}
	c.PutSince()

	// Process each execution change. this will only have entries after the capella fork.
	c = h.Tag("operation", "execution_change")
	for _, addressChange := range blockBody.ExecutionChanges {
		if err := ProcessBlsToExecutionChange(state, addressChange, fullValidation); err != nil {
			return err
		}
	}
	c.PutSince()
	return nil
}

func maximumDeposits(state *state.BeaconState) (maxDeposits uint64) {
	maxDeposits = state.Eth1Data().DepositCount - state.Eth1DepositIndex()
	if maxDeposits > state.BeaconConfig().MaxDeposits {
		maxDeposits = state.BeaconConfig().MaxDeposits
	}
	return
}

// ProcessExecutionPayload sets the latest payload header accordinly.
func ProcessExecutionPayload(s *state.BeaconState, payload *cltypes.Eth1Block) error {
	if state.IsMergeTransitionComplete(s.BeaconState) {
		if payload.ParentHash != s.LatestExecutionPayloadHeader().BlockHash {
			return fmt.Errorf("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	if payload.PrevRandao != s.GetRandaoMixes(state.Epoch(s.BeaconState)) {
		return fmt.Errorf("ProcessExecutionPayload: randao mix mismatches with mix digest")
	}
	if payload.Time != state.ComputeTimestampAtSlot(s.BeaconState, s.Slot()) {
		return fmt.Errorf("ProcessExecutionPayload: invalid Eth1 timestamp")
	}
	payloadHeader, err := payload.PayloadHeader()
	if err != nil {
		return err
	}
	s.SetLatestExecutionPayloadHeader(payloadHeader)
	return nil
}

func executionEnabled(s *state.BeaconState, payload *cltypes.Eth1Block) bool {
	return (!state.IsMergeTransitionComplete(s.BeaconState) && payload.BlockHash != libcommon.Hash{}) || state.IsMergeTransitionComplete(s.BeaconState)
}
