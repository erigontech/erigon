package transition

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// processBlock takes a block and transitions the state to the next slot, using the provided execution payload if enabled.
func processBlock(state *state.BeaconState, signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) error {
	block := signedBlock.Block
	version := state.Version()
	// Check the state version is correct.
	if signedBlock.Version() != version {
		return fmt.Errorf("processBlock: wrong state version for block at slot %d", block.Slot)
	}

	// Process the block header.
	if err := ProcessBlockHeader(state, block, fullValidation); err != nil {
		return fmt.Errorf("processBlock: failed to process block header: %v", err)
	}

	// Process execution payload if enabled.
	if version >= clparams.BellatrixVersion && executionEnabled(state, block.Body.ExecutionPayload) {
		if state.Version() >= clparams.CapellaVersion {
			// Process withdrawals in the execution payload.
			if err := ProcessWithdrawals(state, block.Body.ExecutionPayload.Withdrawals, fullValidation); err != nil {
				return fmt.Errorf("processBlock: failed to process withdrawals: %v", err)
			}
		}

		// Process the execution payload.
		if err := ProcessExecutionPayload(state, block.Body.ExecutionPayload); err != nil {
			return fmt.Errorf("processBlock: failed to process execution payload: %v", err)
		}
	}

	// Process RANDAO reveal.
	if err := ProcessRandao(state, block.Body.RandaoReveal, block.ProposerIndex, fullValidation); err != nil {
		return fmt.Errorf("processBlock: failed to process RANDAO reveal: %v", err)
	}

	// Process Eth1 data.
	if err := ProcessEth1Data(state, block.Body.Eth1Data); err != nil {
		return fmt.Errorf("processBlock: failed to process Eth1 data: %v", err)
	}

	// Process block body operations.
	if err := processOperations(state, block.Body, fullValidation); err != nil {
		return fmt.Errorf("processBlock: failed to process block body operations: %v", err)
	}

	// Process sync aggregate in case of Altair version.
	if version >= clparams.AltairVersion {
		if err := ProcessSyncAggregate(state, block.Body.SyncAggregate, fullValidation); err != nil {
			return fmt.Errorf("processBlock: failed to process sync aggregate: %v", err)
		}
	}

	return nil
}

func processOperations(state *state.BeaconState, blockBody *cltypes.BeaconBody, fullValidation bool) error {
	if len(blockBody.Deposits) != int(maximumDeposits(state)) {
		return errors.New("outstanding deposits do not match maximum deposits")
	}
	// Process each proposer slashing
	for _, slashing := range blockBody.ProposerSlashings {
		if err := ProcessProposerSlashing(state, slashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
	}
	// Process each attester slashing
	for _, slashing := range blockBody.AttesterSlashings {
		if err := ProcessAttesterSlashing(state, slashing); err != nil {
			return fmt.Errorf("ProcessAttesterSlashing: %s", err)
		}
	}
	// Process each attestations
	if err := ProcessAttestations(state, blockBody.Attestations, fullValidation); err != nil {
		return fmt.Errorf("ProcessAttestation: %s", err)
	}
	// Process each deposit
	for _, dep := range blockBody.Deposits {
		if err := ProcessDeposit(state, dep, fullValidation); err != nil {
			return fmt.Errorf("ProcessDeposit: %s", err)
		}
	}
	// Process each voluntary exit.
	for _, exit := range blockBody.VoluntaryExits {
		if err := ProcessVoluntaryExit(state, exit, fullValidation); err != nil {
			return fmt.Errorf("ProcessVoluntaryExit: %s", err)
		}
	}

	// Process each execution change. this will only have entries after the capella fork.
	for _, addressChange := range blockBody.ExecutionChanges {
		if err := ProcessBlsToExecutionChange(state, addressChange, fullValidation); err != nil {
			return err
		}
	}
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
func ProcessExecutionPayload(state *state.BeaconState, payload *cltypes.Eth1Block) error {
	if state.IsMergeTransitionComplete() {
		if payload.ParentHash != state.LatestExecutionPayloadHeader().BlockHash {
			return fmt.Errorf("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	if payload.PrevRandao != state.GetRandaoMixes(state.Epoch()) {
		return fmt.Errorf("ProcessExecutionPayload: randao mix mismatches with mix digest")
	}
	if payload.Time != state.ComputeTimestampAtSlot(state.Slot()) {
		return fmt.Errorf("ProcessExecutionPayload: invalid Eth1 timestamp")
	}
	payloadHeader, err := payload.PayloadHeader()
	if err != nil {
		return err
	}
	state.SetLatestExecutionPayloadHeader(payloadHeader)
	return nil
}

func executionEnabled(state *state.BeaconState, payload *cltypes.Eth1Block) bool {
	return (!state.IsMergeTransitionComplete() && payload.BlockHash != libcommon.Hash{}) || state.IsMergeTransitionComplete()
}
