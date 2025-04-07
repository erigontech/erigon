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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"

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
		return fmt.Errorf("processBlock: failed to extract execution payload header: %w", err)
	}

	// Check the state version is correct.
	if block.Version() != version {
		return fmt.Errorf("processBlock: wrong state version for block at slot %d. state version %v. block version %v", block.GetSlot(), version, block.Version())
	}
	bodyRoot, err := body.HashSSZ()
	if err != nil {
		return fmt.Errorf("processBlock: failed to hash block body: %w", err)
	}
	if err := impl.ProcessBlockHeader(s, block.GetSlot(), block.GetProposerIndex(), block.GetParentRoot(), bodyRoot); err != nil {
		return fmt.Errorf("processBlock: failed to process block header: %v", err)
	}
	// Process execution payload if enabled.
	if version >= clparams.BellatrixVersion && executionEnabled(s, payloadHeader.BlockHash) {
		if s.Version() >= clparams.CapellaVersion {
			// Process withdrawals in the execution payload.
			expect, _ := state.ExpectedWithdrawals(s, state.Epoch(s))
			expectWithdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(s.BeaconConfig().MaxWithdrawalsPerPayload), 44)
			for i := range expect {
				expectWithdrawals.Append(expect[i])
			}
			if err := impl.ProcessWithdrawals(s, expectWithdrawals); err != nil {
				return fmt.Errorf("processBlock: failed to process withdrawals: %v", err)
			}
		}
		if err := impl.ProcessExecutionPayload(s, body); err != nil {
			return fmt.Errorf("processBlock: failed to process execution payload: %v", err)
		}
	}
	var signatures, messages, publicKeys [][]byte

	// Process each proposer slashing
	sigs, msgs, pubKeys, err := processRandao(impl, s, body, block)
	if err != nil {
		return err
	}
	signatures, messages, publicKeys = append(signatures, sigs...), append(messages, msgs...), append(publicKeys, pubKeys...)

	// Process Eth1 data.
	if err := impl.ProcessEth1Data(s, body.GetEth1Data()); err != nil {
		return fmt.Errorf("processBlock: failed to process Eth1 data: %v", err)
	}

	// Process block body operations.
	sigs, msgs, pubKeys, err = ProcessOperations(impl, s, body)
	if err != nil {
		return fmt.Errorf("processBlock: failed to process block body operations: %v", err)

	}
	signatures, messages, publicKeys = append(signatures, sigs...), append(messages, msgs...), append(publicKeys, pubKeys...)

	// process signature validation
	if len(signatures) != 0 {
		valid, err := bls.VerifyMultipleSignatures(signatures, messages, publicKeys)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("block signature validation failed")
		}
	}

	// Process sync aggregate in case of Altair version.
	if version >= clparams.AltairVersion {
		if err := impl.ProcessSyncAggregate(s, body.GetSyncAggregate()); err != nil {
			return fmt.Errorf("processBlock: failed to process sync aggregate: %v", err)
		}
	}

	return nil
}

// ProcessOperations is called by ProcessBlock and processes the block body operations
func ProcessOperations(impl BlockOperationProcessor, s abstract.BeaconState, blockBody cltypes.GenericBeaconBody) (signatures [][]byte, messages [][]byte, publicKeys [][]byte, err error) {
	maxDepositsAllowed := int(min(s.BeaconConfig().MaxDeposits, s.Eth1Data().DepositCount-s.Eth1DepositIndex()))
	if s.Version() <= clparams.DenebVersion {
		if blockBody.GetDeposits().Len() != maxDepositsAllowed {
			return nil, nil, nil, errors.New("outstanding deposits do not match maximum deposits")
		}
	} else if s.Version() >= clparams.ElectraVersion {
		eth1DepositIndexLimit := min(s.Eth1Data().DepositCount, s.GetDepositRequestsStartIndex())
		if s.Eth1DepositIndex() < eth1DepositIndexLimit {
			if uint64(blockBody.GetDeposits().Len()) != min(s.BeaconConfig().MaxDeposits, eth1DepositIndexLimit-s.Eth1DepositIndex()) {
				return nil, nil, nil, errors.New("outstanding deposits do not match maximum deposits")
			}
		} else {
			if blockBody.GetDeposits().Len() != 0 {
				return nil, nil, nil, errors.New("no deposits allowed after deposit contract limit")
			}
		}
	}

	// Process each proposer slashing
	sigs, msgs, pubKeys, err := processProposerSlashings(impl, s, blockBody)
	if err != nil {
		return
	}
	signatures, messages, publicKeys = append(signatures, sigs...), append(messages, msgs...), append(publicKeys, pubKeys...)

	// attester slashings
	if err := forEachProcess(s, blockBody.GetAttesterSlashings(), impl.ProcessAttesterSlashing); err != nil {
		return nil, nil, nil, fmt.Errorf("ProcessProposerSlashing: %s", err)
	}

	// Process each attestations
	if err := impl.ProcessAttestations(s, blockBody.GetAttestations()); err != nil {
		return nil, nil, nil, fmt.Errorf("ProcessAttestation: %s", err)
	}

	// Process each deposit
	if err := forEachProcess(s, blockBody.GetDeposits(), impl.ProcessDeposit); err != nil {
		return nil, nil, nil, fmt.Errorf("ProcessDeposit: %s", err)
	}

	// Process each voluntary exit.
	sigs, msgs, pubKeys, err = processVoluntaryExits(impl, s, blockBody)
	if err != nil {
		return nil, nil, nil, err
	}
	signatures, messages, publicKeys = append(signatures, sigs...), append(messages, msgs...), append(publicKeys, pubKeys...)

	if s.Version() < clparams.CapellaVersion {
		return
	}

	// Process each execution change. this will only have entries after the capella fork.
	sigs, msgs, pubKeys, err = processBlsToExecutionChanges(impl, s, blockBody)
	if err != nil {
		return nil, nil, nil, err
	}
	signatures, messages, publicKeys = append(signatures, sigs...), append(messages, msgs...), append(publicKeys, pubKeys...)

	if s.Version() >= clparams.ElectraVersion {
		if err := forEachProcess(s, blockBody.GetExecutionRequests().Deposits, impl.ProcessDepositRequest); err != nil {
			return nil, nil, nil, fmt.Errorf("ProcessDepositRequest: %s", err)
		}
		if err := forEachProcess(s, blockBody.GetExecutionRequests().Withdrawals, impl.ProcessWithdrawalRequest); err != nil {
			return nil, nil, nil, fmt.Errorf("ProcessWithdrawalRequest: %s", err)
		}
		if err := forEachProcess(s, blockBody.GetExecutionRequests().Consolidations, impl.ProcessConsolidationRequest); err != nil {
			return nil, nil, nil, fmt.Errorf("ProcessConsolidationRequest: %s", err)
		}
	}

	return
}

func forEachProcess[T solid.EncodableHashableSSZ](
	s abstract.BeaconState,
	list *solid.ListSSZ[T],
	f func(s abstract.BeaconState, item T) error) error {
	return solid.RangeErr(list, func(index int, item T, length int) error {
		return f(s, item)
	})
}

func processRandao(impl BlockProcessor, s abstract.BeaconState, body cltypes.GenericBeaconBody, block cltypes.GenericBeaconBlock) (sigs [][]byte, msgs [][]byte, pubKeys [][]byte, err error) {
	// Process RANDAO reveal.
	proposerIndex := block.GetProposerIndex()
	randao := body.GetRandaoReveal()
	epoch := state.Epoch(s)
	proposer, err := s.ValidatorForValidatorIndex(int(proposerIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	if impl.FullValidate() {
		domain, err := s.GetDomain(s.BeaconConfig().DomainRandao, epoch)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("ProcessRandao: unable to get domain: %v", err)
		}
		// compute signing root epoch
		b := make([]byte, 32)
		binary.LittleEndian.PutUint64(b, epoch)
		signingRoot := utils.Sha256(b, domain)

		pk := proposer.PublicKey()
		sigs, msgs, pubKeys = append(sigs, randao[:]), append(msgs, signingRoot[:]), append(pubKeys, pk[:])
	}
	if err = impl.ProcessRandao(s, randao, proposerIndex); err != nil {
		return nil, nil, nil, fmt.Errorf("processBlock: failed to process RANDAO reveal: %v", err)
	}
	return
}

func processProposerSlashings(impl BlockOperationProcessor, s abstract.BeaconState, blockBody cltypes.GenericBeaconBody) (sigs [][]byte, msgs [][]byte, pubKeys [][]byte, err error) {
	// Process each proposer slashing
	err = solid.RangeErr[*cltypes.ProposerSlashing](blockBody.GetProposerSlashings(), func(index int, propSlashing *cltypes.ProposerSlashing, length int) error {
		for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{propSlashing.Header1, propSlashing.Header2} {
			proposer, err := s.ValidatorForValidatorIndex(int(propSlashing.Header1.Header.ProposerIndex))
			if err != nil {
				return err
			}

			domain, err := s.GetDomain(
				s.BeaconConfig().DomainBeaconProposer,
				state.GetEpochAtSlot(s.BeaconConfig(), signedHeader.Header.Slot),
			)
			if err != nil {
				return fmt.Errorf("unable to get domain: %v", err)
			}
			signingRoot, err := fork.ComputeSigningRoot(signedHeader.Header, domain)
			if err != nil {
				return fmt.Errorf("unable to compute signing root: %v", err)
			}
			pk := proposer.PublicKey()
			sigs, msgs, pubKeys = append(sigs, signedHeader.Signature[:]), append(msgs, signingRoot[:]), append(pubKeys, pk[:])
		}

		if err = impl.ProcessProposerSlashing(s, propSlashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
		return nil
	})

	return
}

func processVoluntaryExits(impl BlockOperationProcessor, s abstract.BeaconState, blockBody cltypes.GenericBeaconBody) (sigs [][]byte, msgs [][]byte, pubKeys [][]byte, err error) {
	// Process each voluntary exit.
	err = solid.RangeErr[*cltypes.SignedVoluntaryExit](blockBody.GetVoluntaryExits(), func(index int, exit *cltypes.SignedVoluntaryExit, length int) error {
		voluntaryExit := exit.VoluntaryExit
		validator, err := s.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
		if err != nil {
			return err
		}

		// We can skip it in some instances if we want to optimistically sync up.
		if impl.FullValidate() {
			var domain []byte
			if s.Version() < clparams.DenebVersion {
				domain, err = s.GetDomain(s.BeaconConfig().DomainVoluntaryExit, voluntaryExit.Epoch)
			} else if s.Version() >= clparams.DenebVersion {
				domain, err = fork.ComputeDomain(s.BeaconConfig().DomainVoluntaryExit[:], utils.Uint32ToBytes4(uint32(s.BeaconConfig().CapellaForkVersion)), s.GenesisValidatorsRoot())
			}
			if err != nil {
				return err
			}
			signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
			if err != nil {
				return err
			}
			pk := validator.PublicKey()
			sigs, msgs, pubKeys = append(sigs, exit.Signature[:]), append(msgs, signingRoot[:]), append(pubKeys, pk[:])
		}

		if err = impl.ProcessVoluntaryExit(s, exit); err != nil {
			return fmt.Errorf("ProcessVoluntaryExit: %s", err)
		}
		return nil
	})

	return
}

func processBlsToExecutionChanges(impl BlockOperationProcessor, s abstract.BeaconState, blockBody cltypes.GenericBeaconBody) (sigs [][]byte, msgs [][]byte, pubKeys [][]byte, err error) {
	// Process each execution change. this will only have entries after the capella fork.
	err = solid.RangeErr[*cltypes.SignedBLSToExecutionChange](blockBody.GetExecutionChanges(), func(index int, addressChange *cltypes.SignedBLSToExecutionChange, length int) error {
		change := addressChange.Message

		beaconConfig := s.BeaconConfig()
		validator, err := s.ValidatorForValidatorIndex(int(change.ValidatorIndex))
		if err != nil {
			return err
		}

		if impl.FullValidate() {
			// Perform full validation if requested.
			wc := validator.WithdrawalCredentials()
			// Check the validator's withdrawal credentials prefix.
			if wc[0] != byte(beaconConfig.BLSWithdrawalPrefixByte) {
				return errors.New("invalid withdrawal credentials prefix")
			}

			// Check the validator's withdrawal credentials against the provided message.
			hashedFrom := utils.Sha256(change.From[:])
			if !bytes.Equal(hashedFrom[1:], wc[1:]) {
				return errors.New("invalid withdrawal credentials")
			}

			// Compute the signing domain and verify the message signature.
			domain, err := fork.ComputeDomain(
				beaconConfig.DomainBLSToExecutionChange[:],
				utils.Uint32ToBytes4(uint32(beaconConfig.GenesisForkVersion)),
				s.GenesisValidatorsRoot(),
			)
			if err != nil {
				return err
			}
			signedRoot, err := fork.ComputeSigningRoot(change, domain)
			if err != nil {
				return err
			}
			sigs, msgs, pubKeys = append(sigs, addressChange.Signature[:]), append(msgs, signedRoot[:]), append(pubKeys, change.From[:])
		}

		if err := impl.ProcessBlsToExecutionChange(s, addressChange); err != nil {
			return fmt.Errorf("ProcessBlsToExecutionChange: %s", err)
		}
		return nil
	})

	return
}
