package machine

import (
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/cl/abstract"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/metrics/methelp"
)

// ProcessBlock processes a block with the block processor
func ProcessBlock(impl BlockProcessor, s abstract.BeaconState, signedBlock *cltypes.SignedBeaconBlock) error {
	block := signedBlock.Block
	version := s.Version()
	// Check the state version is correct.
	if signedBlock.Version() != version {
		return fmt.Errorf("processBlock: wrong state version for block at slot %d", block.Slot)
	}
	h := methelp.NewHistTimer("beacon_process_block")
	// Process the block header.
	if err := impl.ProcessBlockHeader(s, block); err != nil {
		return fmt.Errorf("processBlock: failed to process block header: %v", err)
	}
	// Process execution payload if enabled.
	if version >= clparams.BellatrixVersion && executionEnabled(s, block.Body.ExecutionPayload) {
		if s.Version() >= clparams.CapellaVersion {
			// Process withdrawals in the execution payload.
			if err := impl.ProcessWithdrawals(s, block.Body.ExecutionPayload.Withdrawals); err != nil {
				return fmt.Errorf("processBlock: failed to process withdrawals: %v", err)
			}
		}
		// Process the execution payload.
		if err := impl.ProcessExecutionPayload(s, block.Body.ExecutionPayload); err != nil {
			return fmt.Errorf("processBlock: failed to process execution payload: %v", err)
		}
	}
	// Process RANDAO reveal.
	if err := impl.ProcessRandao(s, block.Body.RandaoReveal, block.ProposerIndex); err != nil {
		return fmt.Errorf("processBlock: failed to process RANDAO reveal: %v", err)
	}
	// Process Eth1 data.
	if err := impl.ProcessEth1Data(s, block.Body.Eth1Data); err != nil {
		return fmt.Errorf("processBlock: failed to process Eth1 data: %v", err)
	}
	// Process block body operations.
	if err := ProcessOperations(impl, s, block.Body); err != nil {
		return fmt.Errorf("processBlock: failed to process block body operations: %v", err)
	}
	// Process sync aggregate in case of Altair version.
	if version >= clparams.AltairVersion {
		if err := impl.ProcessSyncAggregate(s, block.Body.SyncAggregate); err != nil {
			return fmt.Errorf("processBlock: failed to process sync aggregate: %v", err)
		}
	}
	if version >= clparams.DenebVersion {
		verified, err := impl.VerifyKzgCommitmentsAgainstTransactions(block.Body.ExecutionPayload.Transactions, block.Body.BlobKzgCommitments)
		if err != nil {
			return fmt.Errorf("processBlock: failed to process blob kzg commitments: %w", err)
		}
		if !verified {
			return fmt.Errorf("processBlock: failed to process blob kzg commitments: commitments are not equal")
		}
	}
	h.PutSince()
	return nil
}

// ProcessOperations is called by ProcessBlock and prcesses the block body operations
func ProcessOperations(impl BlockOperationProcessor, s abstract.BeaconState, blockBody *cltypes.BeaconBody) error {
	if blockBody.Deposits.Len() != int(maximumDeposits(s)) {
		return errors.New("outstanding deposits do not match maximum deposits")
	}
	// Process each proposer slashing
	var err error
	if err := solid.RangeErr[*cltypes.ProposerSlashing](blockBody.ProposerSlashings, func(index int, slashing *cltypes.ProposerSlashing, length int) error {
		if err = impl.ProcessProposerSlashing(s, slashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := solid.RangeErr[*cltypes.AttesterSlashing](blockBody.AttesterSlashings, func(index int, slashing *cltypes.AttesterSlashing, length int) error {
		if err = impl.ProcessAttesterSlashing(s, slashing); err != nil {
			return fmt.Errorf("ProcessAttesterSlashing: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	// Process each attestations
	if err := impl.ProcessAttestations(s, blockBody.Attestations); err != nil {
		return fmt.Errorf("ProcessAttestation: %s", err)
	}

	// Process each deposit
	if err := solid.RangeErr[*cltypes.Deposit](blockBody.Deposits, func(index int, deposit *cltypes.Deposit, length int) error {
		if err = impl.ProcessDeposit(s, deposit); err != nil {
			return fmt.Errorf("ProcessDeposit: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	// Process each voluntary exit.
	if err := solid.RangeErr[*cltypes.SignedVoluntaryExit](blockBody.VoluntaryExits, func(index int, exit *cltypes.SignedVoluntaryExit, length int) error {
		if err = impl.ProcessVoluntaryExit(s, exit); err != nil {
			return fmt.Errorf("ProcessVoluntaryExit: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if s.Version() < clparams.CapellaVersion {
		return nil
	}
	// Process each execution change. this will only have entries after the capella fork.
	if err := solid.RangeErr[*cltypes.SignedBLSToExecutionChange](blockBody.ExecutionChanges, func(index int, addressChange *cltypes.SignedBLSToExecutionChange, length int) error {
		if err := impl.ProcessBlsToExecutionChange(s, addressChange); err != nil {
			return fmt.Errorf("ProcessBlsToExecutionChange: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func maximumDeposits(s abstract.BeaconState) (maxDeposits uint64) {
	maxDeposits = s.Eth1Data().DepositCount - s.Eth1DepositIndex()
	if maxDeposits > s.BeaconConfig().MaxDeposits {
		maxDeposits = s.BeaconConfig().MaxDeposits
	}
	return
}
