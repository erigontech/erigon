// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package machine

import (
	"fmt"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/pkg/errors"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// ProcessBlock processes a block with the block processor
func ProcessBlock(impl BlockProcessor, s abstract.BeaconState, block cltypes.GenericBeaconBlock) error {
	var (
		version = s.Version()
		body    = block.GetBody()
	)
	payloadHeader, err := body.GetPayloadHeader()
	if err != nil {
		return errors.WithMessage(err, "processBlock: failed to extract execution payload header")
	}

	// Check the state version is correct.
	if block.Version() != version {
		return fmt.Errorf("processBlindedBlock: wrong state version for block at slot %d", block.GetSlot())
	}
	bodyRoot, err := body.HashSSZ()
	if err != nil {
		return errors.WithMessagef(err, "processBlindedBlock: failed to hash block body")
	}
	if err := impl.ProcessBlockHeader(s, block.GetSlot(), block.GetProposerIndex(), block.GetParentRoot(), bodyRoot); err != nil {
		return fmt.Errorf("processBlindedBlock: failed to process block header: %v", err)
	}
	// Process execution payload if enabled.
	if version >= clparams.BellatrixVersion && executionEnabled(s, payloadHeader.BlockHash) {
		if s.Version() >= clparams.CapellaVersion {
			// Process withdrawals in the execution payload.
			expect := state.ExpectedWithdrawals(s, state.Epoch(s))
			expectWithdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(s.BeaconConfig().MaxWithdrawalsPerPayload), 44)
			for i := range expect {
				expectWithdrawals.Append(expect[i])
			}
			if err := impl.ProcessWithdrawals(s, expectWithdrawals); err != nil {
				return fmt.Errorf("processBlock: failed to process withdrawals: %v", err)
			}
		}
		parentHash := payloadHeader.ParentHash
		prevRandao := payloadHeader.PrevRandao
		time := payloadHeader.Time
		if err := impl.ProcessExecutionPayload(s, parentHash, prevRandao, time, payloadHeader); err != nil {
			return fmt.Errorf("processBlock: failed to process execution payload: %v", err)
		}
	}

	// Process RANDAO reveal.
	if err := impl.ProcessRandao(s, body.GetRandaoReveal(), block.GetProposerIndex()); err != nil {
		return fmt.Errorf("processBlock: failed to process RANDAO reveal: %v", err)
	}
	// Process Eth1 data.
	if err := impl.ProcessEth1Data(s, body.GetEth1Data()); err != nil {
		return fmt.Errorf("processBlock: failed to process Eth1 data: %v", err)
	}
	// Process block body operations.
	if err := ProcessOperations(impl, s, body); err != nil {
		return fmt.Errorf("processBlock: failed to process block body operations: %v", err)
	}
	// Process sync aggregate in case of Altair version.
	if version >= clparams.AltairVersion {
		if err := impl.ProcessSyncAggregate(s, body.GetSyncAggregate()); err != nil {
			return fmt.Errorf("processBlock: failed to process sync aggregate: %v", err)
		}
	}

	return nil
}

// ProcessOperations is called by ProcessBlock and prcesses the block body operations
func ProcessOperations(impl BlockOperationProcessor, s abstract.BeaconState, blockBody cltypes.GenericBeaconBody) error {
	if blockBody.GetDeposits().Len() != int(maximumDeposits(s)) {
		return errors.New("outstanding deposits do not match maximum deposits")
	}
	// Process each proposer slashing
	var err error
	if err := solid.RangeErr[*cltypes.ProposerSlashing](blockBody.GetProposerSlashings(), func(index int, slashing *cltypes.ProposerSlashing, length int) error {
		if err = impl.ProcessProposerSlashing(s, slashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := solid.RangeErr[*cltypes.AttesterSlashing](blockBody.GetAttesterSlashings(), func(index int, slashing *cltypes.AttesterSlashing, length int) error {
		if err = impl.ProcessAttesterSlashing(s, slashing); err != nil {
			return fmt.Errorf("ProcessAttesterSlashing: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	// Process each attestations
	if err := impl.ProcessAttestations(s, blockBody.GetAttestations()); err != nil {
		return fmt.Errorf("ProcessAttestation: %s", err)
	}

	// Process each deposit
	if err := solid.RangeErr[*cltypes.Deposit](blockBody.GetDeposits(), func(index int, deposit *cltypes.Deposit, length int) error {
		if err = impl.ProcessDeposit(s, deposit); err != nil {
			return fmt.Errorf("ProcessDeposit: %s", err)
		}
		return nil
	}); err != nil {
		return err
	}

	// Process each voluntary exit.
	if err := solid.RangeErr[*cltypes.SignedVoluntaryExit](blockBody.GetVoluntaryExits(), func(index int, exit *cltypes.SignedVoluntaryExit, length int) error {
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
	if err := solid.RangeErr[*cltypes.SignedBLSToExecutionChange](blockBody.GetExecutionChanges(), func(index int, addressChange *cltypes.SignedBLSToExecutionChange, length int) error {
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
