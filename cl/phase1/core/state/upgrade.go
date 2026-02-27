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

package state

import (
	"sort"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func (b *CachingBeaconState) UpgradeToAltair() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().AltairForkVersion))
	b.SetFork(fork)
	// Process new fields
	b.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, b.ValidatorLength()))
	b.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, b.ValidatorLength()))
	b.SetInactivityScores(make([]uint64, b.ValidatorLength()))
	// Change version
	b.SetVersion(clparams.AltairVersion)
	// Fill in previous epoch participation from the pre state's pending attestations
	if err := solid.RangeErr[*solid.PendingAttestation](b.PreviousEpochAttestations(), func(i1 int, pa *solid.PendingAttestation, i2 int) error {
		attestationData := pa.Data
		flags, err := b.GetAttestationParticipationFlagIndicies(attestationData, pa.InclusionDelay, false)
		if err != nil {
			return err
		}
		attestation := &solid.Attestation{
			AggregationBits: pa.AggregationBits,
			Data:            attestationData,
			// don't care signature and committee_bits here
		}
		indices, err := b.GetAttestingIndicies(attestation, false)
		if err != nil {
			return err
		}
		for _, index := range indices {
			for _, flagIndex := range flags {
				b.AddPreviousEpochParticipationAt(int(index), flagIndex)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	b.ResetPreviousEpochAttestations()
	// Process sync committees
	var err error
	currentSyncCommittee, err := b.ComputeNextSyncCommittee()
	if err != nil {
		return err
	}
	b.SetCurrentSyncCommittee(currentSyncCommittee)
	nextSyncCommittee, err := b.ComputeNextSyncCommittee()
	if err != nil {
		return err
	}
	b.SetNextSyncCommittee(nextSyncCommittee)

	return nil
}

func (b *CachingBeaconState) UpgradeToBellatrix() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().BellatrixForkVersion))
	b.SetFork(fork)
	b.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(clparams.BellatrixVersion))
	// Update the state root cache
	b.SetVersion(clparams.BellatrixVersion)
	return nil
}

func (b *CachingBeaconState) UpgradeToCapella() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().CapellaForkVersion))
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.Capella()
	b.SetLatestExecutionPayloadHeader(header)
	// Set new fields
	b.SetNextWithdrawalIndex(0)
	b.SetNextWithdrawalValidatorIndex(0)
	b.ResetHistoricalSummaries()
	// Update the state root cache
	b.SetVersion(clparams.CapellaVersion)
	return nil
}

func (b *CachingBeaconState) UpgradeToDeneb() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().DenebForkVersion))
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.Deneb()
	b.SetLatestExecutionPayloadHeader(header)
	// Update the state root cache
	b.SetVersion(clparams.DenebVersion)
	return nil
}

func (b *CachingBeaconState) UpgradeToElectra() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().ElectraForkVersion))
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.SetVersion(clparams.ElectraVersion)
	b.SetLatestExecutionPayloadHeader(header)
	// Update the state root cache
	b.SetVersion(clparams.ElectraVersion)

	earliestExitEpoch := ComputeActivationExitEpoch(b.BeaconConfig(), epoch)
	b.ValidatorSet().Range(func(i int, v solid.Validator, _ int) bool {
		if v.ExitEpoch() != b.BeaconConfig().FarFutureEpoch {
			if v.ExitEpoch() > earliestExitEpoch {
				earliestExitEpoch = v.ExitEpoch()
			}
		}
		return true
	})
	earliestExitEpoch += 1
	// New in Electra:EIP6110
	b.SetDepositRequestsStartIndex(b.BeaconConfig().UnsetDepositRequestsStartIndex)
	// New in Electra:EIP7251
	b.SetDepositBalanceToConsume(0)
	b.SetExitBalanceToConsume(0)
	b.SetEarliestExitEpoch(earliestExitEpoch)
	b.SetConsolidationBalanceToConsume(0)
	b.SetEarlistConsolidationEpoch(ComputeActivationExitEpoch(b.BeaconConfig(), epoch))
	b.SetPendingDeposits(solid.NewPendingDepositList(b.BeaconConfig()))
	b.SetPendingPartialWithdrawals(solid.NewPendingWithdrawalList(b.BeaconConfig()))
	b.SetPendingConsolidations(solid.NewPendingConsolidationList(b.BeaconConfig()))
	// update
	newExitBalanceToConsume := GetActivationExitChurnLimit(b)
	newConsolidationBalanceToConsume := GetConsolidationChurnLimit(b)
	b.SetExitBalanceToConsume(newExitBalanceToConsume)
	b.SetConsolidationBalanceToConsume(newConsolidationBalanceToConsume)

	// add validators that are not yet active to pending balance deposits
	type tempValidator struct {
		validator solid.Validator
		index     uint64
	}
	validators := []tempValidator{}
	b.ValidatorSet().Range(func(i int, v solid.Validator, _ int) bool {
		if v.ActivationEpoch() == b.BeaconConfig().FarFutureEpoch {
			validators = append(validators, tempValidator{
				validator: v,
				index:     uint64(i),
			})
		}
		return true
	})
	// sort
	sort.Slice(validators, func(i, j int) bool {
		vi, vj := validators[i].validator, validators[j].validator
		if vi.ActivationEligibilityEpoch() == vj.ActivationEligibilityEpoch() {
			//  If eligibility epochs are equal, compare indices
			return validators[i].index < validators[j].index
		}
		// Otherwise, sort by activationEligibilityEpoch
		return vi.ActivationEligibilityEpoch() < vj.ActivationEligibilityEpoch()
	})

	for _, v := range validators {
		balance, err := b.ValidatorBalance(int(v.index))
		if err != nil {
			return err
		}
		if err := b.SetValidatorBalance(int(v.index), 0); err != nil {
			return err
		}
		curValidator := v.validator
		// Do NOT directly modify the validator in the validator set, because we need to mark validatorSet as dirty in BeaconState
		//curValidator.SetEffectiveBalance(0)
		//curValidator.SetActivationEligibilityEpoch(b.BeaconConfig().FarFutureEpoch)
		b.SetEffectiveBalanceForValidatorAtIndex(int(v.index), 0)
		b.SetActivationEligibilityEpochForValidatorAtIndex(int(v.index), b.BeaconConfig().FarFutureEpoch)
		// Use bls.G2_POINT_AT_INFINITY as a signature field placeholder
		// and GENESIS_SLOT to distinguish from a pending deposit request
		b.AppendPendingDeposit(&solid.PendingDeposit{
			PubKey:                curValidator.PublicKey(),
			WithdrawalCredentials: curValidator.WithdrawalCredentials(),
			Amount:                balance,
			Signature:             bls.InfiniteSignature,
			Slot:                  b.BeaconConfig().GenesisSlot,
		})
	}

	// Ensure early adopters of compounding credentials go through the activation churn
	b.ValidatorSet().Range(func(vindex int, v solid.Validator, _ int) bool {
		if HasCompoundingWithdrawalCredential(v, b.BeaconConfig()) {
			QueueExcessActiveBalance(b, uint64(vindex), &v)
		}
		return true
	})
	log.Info("Upgrade to Electra complete")
	return nil
}

func (b *CachingBeaconState) UpgradeToFulu() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	// update version
	fork := b.Fork()
	fork.Epoch = epoch
	fork.PreviousVersion = fork.CurrentVersion
	fork.CurrentVersion = utils.Uint32ToBytes4(uint32(b.BeaconConfig().FuluForkVersion))
	b.SetFork(fork)
	// Update the payload header.
	header := b.LatestExecutionPayloadHeader()
	header.SetVersion(clparams.FuluVersion)
	b.SetLatestExecutionPayloadHeader(header)
	// Update the state root cache
	b.SetVersion(clparams.FuluVersion)

	// initialize the proposer lookahead
	lookahead := solid.NewUint64VectorSSZ(int((b.BeaconConfig().MinSeedLookahead + 1) * b.BeaconConfig().SlotsPerEpoch))
	for i := 0; i < int(b.BeaconConfig().MinSeedLookahead+1); i++ {
		proposerIndices, err := b.GetBeaconProposerIndices(epoch + uint64(i))
		if err != nil {
			return err
		}
		for j := 0; j < len(proposerIndices); j++ {
			lookahead.Set(i*int(b.BeaconConfig().SlotsPerEpoch)+j, proposerIndices[j])
		}
	}
	b.SetProposerLookahead(lookahead)

	log.Info("Upgrade to Fulu complete")
	return nil
}

func (b *CachingBeaconState) UpgradeToGloas() error {
	b.previousStateRoot = common.Hash{}
	epoch := Epoch(b.BeaconState)
	cfg := b.BeaconConfig()

	// Update fork version
	forkData := b.Fork()
	forkData.Epoch = epoch
	forkData.PreviousVersion = forkData.CurrentVersion
	forkData.CurrentVersion = utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion))
	b.SetFork(forkData)

	// Get the latest block hash from the previous execution payload header
	latestBlockHash := b.LatestExecutionPayloadHeader().BlockHash

	// Replace latest_execution_payload_header with latest_execution_payload_bid
	// The bid contains only the block_hash from the previous header
	bid := &cltypes.ExecutionPayloadBid{
		BuilderIndex:       0,
		Slot:               0,
		PrevRandao:         common.Hash{},
		ParentBlockHash:    common.Hash{},
		ParentBlockRoot:    common.Hash{},
		BlockHash:          latestBlockHash,
		FeeRecipient:       common.Address{},
		GasLimit:           0,
		Value:              0,
		ExecutionPayment:   0,
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}
	b.SetLatestExecutionPayloadBid(bid)

	// Initialize builder-related fields
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	b.SetBuilders(builders)

	b.SetNextWithdrawalBuilderIndex(0)

	// Initialize execution_payload_availability to all 1s (all prior slots had payloads)
	for i := uint64(0); i < cfg.SlotsPerHistoricalRoot; i++ {
		b.SetExecutionPayloadAvailability(i, true)
	}

	// Initialize builder_pending_payments with empty payments
	builderPendingPayments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * cfg.SlotsPerEpoch))
	for i := 0; i < int(2*cfg.SlotsPerEpoch); i++ {
		builderPendingPayments.Set(i, &cltypes.BuilderPendingPayment{
			Withdrawal: &cltypes.BuilderPendingWithdrawal{},
		})
	}
	b.SetBuilderPendingPayments(builderPendingPayments)

	// Initialize empty builder_pending_withdrawals
	builderPendingWithdrawals := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](int(cfg.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
	b.SetBuilderPendingWithdrawals(builderPendingWithdrawals)

	// Set latest_block_hash
	b.SetLatestBlockHash(latestBlockHash)

	// Initialize empty payload_expected_withdrawals
	payloadExpectedWithdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), new(cltypes.Withdrawal).EncodingSizeSSZ())
	b.SetPayloadExpectedWithdrawals(payloadExpectedWithdrawals)

	// Update the state version
	b.SetVersion(clparams.GloasVersion)

	// Onboard builders from pending deposits
	if err := b.onboardBuildersFromPendingDeposits(); err != nil {
		return err
	}

	log.Info("Upgrade to Gloas complete")
	return nil
}

// onboardBuildersFromPendingDeposits processes pending deposits to onboard builders at the fork.
// Applies any pending deposit for builders, effectively onboarding builders at the fork.
// [New in Gloas:EIP7732]
func (b *CachingBeaconState) onboardBuildersFromPendingDeposits() error {
	cfg := b.BeaconConfig()

	// Build a set of validator pubkeys
	validatorPubkeys := make(map[common.Bytes48]struct{})
	b.ValidatorSet().Range(func(_ int, v solid.Validator, _ int) bool {
		validatorPubkeys[v.PublicKey()] = struct{}{}
		return true
	})

	// Process pending deposits
	pendingDeposits := b.GetPendingDeposits()
	newPendingDeposits := solid.NewPendingDepositList(cfg)

	for i := 0; i < pendingDeposits.Len(); i++ {
		deposit := pendingDeposits.Get(i)

		// Deposits for existing validators stay in pending queue
		if _, isValidator := validatorPubkeys[deposit.PubKey]; isValidator {
			newPendingDeposits.Append(deposit)
			continue
		}

		// Check if pubkey is associated with an existing builder or has builder credentials
		// Note: builders list may be mutated by apply_deposit_for_builder, so we check each iteration
		isExistingBuilder := IsBuilderPubkey(b, deposit.PubKey)
		hasBuilderCredentials := IsBuilderWithdrawalCredential(deposit.WithdrawalCredentials, cfg)

		if isExistingBuilder || hasBuilderCredentials {
			// Apply deposit for builder
			ApplyDepositForBuilder(b, deposit.PubKey, deposit.WithdrawalCredentials, deposit.Amount, deposit.Signature, deposit.Slot)
			continue
		}

		// For new validator deposits with valid signature, track pubkey and keep in pending
		// Deposits with invalid signatures are dropped
		valid, err := IsValidDepositSignature(cfg, deposit.PubKey, deposit.WithdrawalCredentials, deposit.Amount, deposit.Signature)
		if err != nil {
			log.Debug("Error validating deposit signature during upgrade", "err", err)
			continue
		}
		if valid {
			// Track this pubkey so subsequent builder deposits for same pubkey stay pending
			validatorPubkeys[deposit.PubKey] = struct{}{}
			newPendingDeposits.Append(deposit)
		}
		// Invalid signature deposits are dropped (they would fail in apply_pending_deposit anyway)
	}

	b.SetPendingDeposits(newPendingDeposits)
	return nil
}
