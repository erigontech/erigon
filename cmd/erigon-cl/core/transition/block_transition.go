package transition

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// processBlock takes a block and transition said block. Important: it assumes execution payload is correct.
func processBlock(state *state.BeaconState, signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) error {
	block := signedBlock.Block
	if signedBlock.Version() != state.Version() {
		return fmt.Errorf("wrong state version for block at slot %d", block.Slot)
	}
	if err := ProcessBlockHeader(state, block, fullValidation); err != nil {
		return fmt.Errorf("ProcessBlockHeader: %s", err)
	}
	if state.Version() >= clparams.BellatrixVersion && executionEnabled(state, block.Body.ExecutionPayload) {
		if err := ProcessExecutionPayload(state, block.Body.ExecutionPayload); err != nil {
			return err
		}
	}
	if err := ProcessRandao(state, block.Body.RandaoReveal, block.ProposerIndex, fullValidation); err != nil {
		return fmt.Errorf("ProcessRandao: %s", err)
	}
	if err := ProcessEth1Data(state, block.Body.Eth1Data); err != nil {
		return fmt.Errorf("ProcessEth1Data: %s", err)
	}
	// Do operationns
	if err := processOperations(state, block.Body, fullValidation); err != nil {
		return fmt.Errorf("processOperations: %s", err)
	}
	// Process altair data
	if state.Version() >= clparams.AltairVersion {
		if err := ProcessSyncAggregate(state, block.Body.SyncAggregate, fullValidation); err != nil {
			return fmt.Errorf("ProcessSyncAggregate: %s", err)
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
		if payload.Header.ParentHash != state.LatestExecutionPayloadHeader().BlockHashCL {
			return fmt.Errorf("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	if payload.Header.MixDigest != state.GetRandaoMixes(state.Epoch()) {
		return fmt.Errorf("ProcessExecutionPayload: randao mix mismatches with mix digest")
	}
	if payload.Header.Time != state.ComputeTimestampAtSlot(state.Slot()) {
		return fmt.Errorf("ProcessExecutionPayload: invalid Eth1 timestamp")
	}
	state.SetLatestExecutionPayloadHeader(payload.Header)
	return nil
}

func executionEnabled(state *state.BeaconState, payload *cltypes.Eth1Block) bool {
	return (!state.IsMergeTransitionComplete() && payload.Header.Root != libcommon.Hash{}) || state.IsMergeTransitionComplete()
}
