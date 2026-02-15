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
	"github.com/erigontech/erigon/common"

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

	if version >= clparams.GloasVersion {
		// [Modified in Gloas:EIP7732] process_withdrawals(state) — no payload parameter
		if err := processGloasWithdrawals(impl, s); err != nil {
			return fmt.Errorf("processBlock: failed to process withdrawals: %v", err)
		}
		// [New in Gloas:EIP7732] process_execution_payload_header(state, block)
		if err := processExecutionPayloadHeader(s, body, block.GetProposerIndex(), block.GetSlot(), block.GetParentRoot()); err != nil {
			return fmt.Errorf("processBlock: failed to process execution payload header: %v", err)
		}
	} else {
		// Pre-Gloas: process execution payload if enabled.
		payloadHeader, err := body.GetPayloadHeader()
		if err != nil {
			return fmt.Errorf("processBlock: failed to extract execution payload header: %w", err)
		}
		if version >= clparams.BellatrixVersion && executionEnabled(s, payloadHeader.BlockHash) {
			if version >= clparams.CapellaVersion {
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

	if s.Version() >= clparams.ElectraVersion && s.Version() < clparams.GloasVersion {
		// [Modified in Gloas:EIP7732] Removed execution requests processing
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

	// [New in Gloas:EIP7732] Process payload attestations
	if s.Version() >= clparams.GloasVersion {
		if err := processPayloadAttestations(s, blockBody); err != nil {
			return nil, nil, nil, fmt.Errorf("ProcessPayloadAttestation: %s", err)
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

// processGloasWithdrawals implements the Gloas process_withdrawals(state) which takes no payload parameter.
// It returns early if the parent block is not full (is_parent_block_full check).
func processGloasWithdrawals(impl BlockProcessor, s abstract.BeaconState) error {
	// [New in Gloas:EIP7732] Return early if the parent block is empty
	// is_parent_block_full: latest_execution_payload_bid.block_hash == latest_block_hash
	bid := s.LatestExecutionPayloadBid()
	if bid == nil || bid.BlockHash != s.LatestBlockHash() {
		return nil
	}

	// Compute expected withdrawals from state
	expect, _ := state.ExpectedWithdrawals(s, state.Epoch(s))
	expectWithdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(s.BeaconConfig().MaxWithdrawalsPerPayload), 44)
	for i := range expect {
		expectWithdrawals.Append(expect[i])
	}
	return impl.ProcessWithdrawals(s, expectWithdrawals)
}

// ProcessExecutionPayloadBid implements process_execution_payload_header for Gloas/EIP-7732.
// Exported for use by spec tests.
func ProcessExecutionPayloadBid(s abstract.BeaconState, body cltypes.GenericBeaconBody, proposerIndex uint64, blockSlot uint64, blockParentRoot common.Hash) error {
	return processExecutionPayloadHeader(s, body, proposerIndex, blockSlot, blockParentRoot)
}

// processExecutionPayloadHeader implements process_execution_payload_bid for Gloas/EIP-7732.
func processExecutionPayloadHeader(s abstract.BeaconState, body cltypes.GenericBeaconBody, proposerIndex uint64, blockSlot uint64, blockParentRoot common.Hash) error {
	beaconBody, ok := body.(*cltypes.BeaconBody)
	if !ok {
		return errors.New("processExecutionPayloadHeader: expected *cltypes.BeaconBody")
	}
	signedBid := beaconBody.SignedExecutionPayloadBid
	if signedBid == nil {
		return errors.New("processExecutionPayloadHeader: missing signed_execution_payload_header")
	}
	bid := signedBid.Message
	if bid == nil {
		return errors.New("processExecutionPayloadHeader: missing execution_payload_header message")
	}

	beaconConfig := s.BeaconConfig()
	builderIndex := bid.BuilderIndex
	amount := bid.Value

	// Self-build: builder_index == proposer_index
	isSelfBuild := builderIndex == proposerIndex

	if isSelfBuild {
		// For self-builds, amount must be zero
		if amount != 0 {
			return errors.New("processExecutionPayloadHeader: self-build bid must have zero value")
		}
	} else {
		// Not a self-build: validate the builder
		if builderIndex >= uint64(s.ValidatorLength()) {
			return fmt.Errorf("processExecutionPayloadHeader: builder_index %d out of range", builderIndex)
		}
		builder, err := s.ValidatorForValidatorIndex(int(builderIndex))
		if err != nil {
			return fmt.Errorf("processExecutionPayloadHeader: failed to get builder validator: %w", err)
		}
		// Check withdrawal credentials have builder prefix (0x03)
		wc := builder.WithdrawalCredentials()
		if wc[0] != byte(beaconConfig.BuilderWithdrawalPrefix) {
			return errors.New("processExecutionPayloadHeader: validator is not a builder (wrong withdrawal credentials prefix)")
		}
		// Check builder is active and not slashed
		epoch := state.Epoch(s)
		if !builder.Active(epoch) {
			return errors.New("processExecutionPayloadHeader: builder is not active")
		}
		if builder.Slashed() {
			return errors.New("processExecutionPayloadHeader: builder is slashed")
		}
		// Check builder can cover the bid
		builderBalance, err := s.ValidatorBalance(int(builderIndex))
		if err != nil {
			return fmt.Errorf("processExecutionPayloadHeader: failed to get builder balance: %w", err)
		}
		pendingAmount := getPendingBalanceToWithdrawForBuilder(s, builderIndex)
		minBalance := beaconConfig.MaxEffectiveBalance + pendingAmount
		if builderBalance < minBalance || builderBalance-minBalance < amount {
			return errors.New("processExecutionPayloadHeader: builder cannot cover bid")
		}
		// TODO: Verify builder bid signature when builder registry is available.
		// Currently skipped because the builder pubkey comes from state.builders[]
		// (a separate registry not yet implemented), not from the validator set.
	}

	// Verify that the bid is for the current slot (spec: bid.slot == block.slot)
	if bid.Slot != blockSlot {
		return fmt.Errorf("processExecutionPayloadHeader: bid slot %d != block slot %d", bid.Slot, blockSlot)
	}
	// Verify that the bid is for the right parent block
	if bid.ParentBlockHash != s.LatestBlockHash() {
		return fmt.Errorf("processExecutionPayloadHeader: bid parent_block_hash mismatch")
	}
	if bid.ParentBlockRoot != blockParentRoot {
		return fmt.Errorf("processExecutionPayloadHeader: bid parent_block_root mismatch")
	}

	// Record the pending payment (always, even for zero-value bids)
	bpp := s.BuilderPendingPayments()
	paymentIdx := beaconConfig.SlotsPerEpoch + bid.Slot%beaconConfig.SlotsPerEpoch
	payment := bpp.Get(int(paymentIdx))
	payment.Weight = 0
	payment.Withdrawal.FeeRecipient = bid.FeeRecipient
	payment.Withdrawal.Amount = amount
	payment.Withdrawal.BuilderIndex = builderIndex
	s.SetBuilderPendingPayments(bpp)

	// Cache the execution payload header
	s.SetLatestExecutionPayloadBid(bid)

	return nil
}

// getPendingBalanceToWithdrawForBuilder computes the total pending balance for a builder.
func getPendingBalanceToWithdrawForBuilder(s abstract.BeaconState, builderIndex uint64) uint64 {
	var total uint64
	bpw := s.BuilderPendingWithdrawals()
	bpw.Range(func(_ int, w *cltypes.BuilderPendingWithdrawal, _ int) bool {
		if w.BuilderIndex == builderIndex {
			total += w.Amount
		}
		return true
	})
	bpp := s.BuilderPendingPayments()
	bpp.Range(func(_ int, p *cltypes.BuilderPendingPayment, _ int) bool {
		if p.Withdrawal != nil && p.Withdrawal.BuilderIndex == builderIndex {
			total += p.Withdrawal.Amount
		}
		return true
	})
	return total
}

// ProcessPayloadAttestation processes a single payload attestation for Gloas/EIP-7732.
// Exported for use by spec tests.
func ProcessPayloadAttestation(s abstract.BeaconState, pa *cltypes.PayloadAttestation) error {
	data := pa.Data
	latestBlockHeader := s.LatestBlockHeader()
	// Check that the attestation is for the parent beacon block
	if data.BeaconBlockRoot != latestBlockHeader.ParentRoot {
		return fmt.Errorf("processPayloadAttestation: beacon_block_root mismatch: got %x, expected %x",
			data.BeaconBlockRoot, latestBlockHeader.ParentRoot)
	}
	// Check that the attestation is for the previous slot
	if data.Slot+1 != s.Slot() {
		return fmt.Errorf("processPayloadAttestation: slot mismatch: data.slot=%d, state.slot=%d",
			data.Slot, s.Slot())
	}
	// Check that there is at least one attesting index (non-empty aggregation bits)
	hasAny := false
	for i := 0; i < pa.AggregationBits.BitLen(); i++ {
		if pa.AggregationBits.GetBitAt(i) {
			hasAny = true
			break
		}
	}
	if !hasAny {
		return errors.New("processPayloadAttestation: no attesting indices")
	}
	// Verify signature is a valid BLS point (full verification skipped without builder registry)
	if _, err := bls.NewSignatureFromBytes(pa.Signature[:]); err != nil {
		return fmt.Errorf("processPayloadAttestation: invalid signature: %w", err)
	}
	return nil
}

// processPayloadAttestations processes payload attestations for Gloas/EIP-7732.
// For now this is a minimal implementation that validates basic constraints.
func processPayloadAttestations(s abstract.BeaconState, body cltypes.GenericBeaconBody) error {
	beaconBody, ok := body.(*cltypes.BeaconBody)
	if !ok {
		return errors.New("processPayloadAttestations: expected *cltypes.BeaconBody")
	}
	if beaconBody.PayloadAttestations == nil {
		return nil
	}

	return solid.RangeErr(beaconBody.PayloadAttestations, func(_ int, pa *cltypes.PayloadAttestation, _ int) error {
		data := pa.Data
		latestBlockHeader := s.LatestBlockHeader()
		// Check that the attestation is for the parent beacon block
		if data.BeaconBlockRoot != latestBlockHeader.ParentRoot {
			return fmt.Errorf("processPayloadAttestation: beacon_block_root mismatch: got %x, expected %x",
				data.BeaconBlockRoot, latestBlockHeader.ParentRoot)
		}
		// Check that the attestation is for the previous slot
		if data.Slot+1 != s.Slot() {
			return fmt.Errorf("processPayloadAttestation: slot mismatch: data.slot=%d, state.slot=%d",
				data.Slot, s.Slot())
		}
		// Signature verification would go here for full validation
		// For spec tests, we skip BLS verification in non-full-validation mode
		return nil
	})
}

