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

package eth2

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/monitor"

	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"

	"github.com/erigontech/erigon/cl/utils/bls"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
)

const (
	FullExitRequestAmount = 0
)

func (I *impl) FullValidate() bool {
	return I.FullValidation
}

func (I *impl) ProcessProposerSlashing(
	s abstract.BeaconState,
	propSlashing *cltypes.ProposerSlashing,
) error {
	h1 := propSlashing.Header1.Header
	h2 := propSlashing.Header2.Header

	if h1.Slot != h2.Slot {
		return fmt.Errorf("non-matching slots on proposer slashing: %d != %d", h1.Slot, h2.Slot)
	}

	if h1.ProposerIndex != h2.ProposerIndex {
		return fmt.Errorf(
			"non-matching proposer indices proposer slashing: %d != %d",
			h1.ProposerIndex,
			h2.ProposerIndex,
		)
	}

	if *h1 == *h2 {
		return errors.New("proposee slashing headers are the same")
	}

	proposer, err := s.ValidatorForValidatorIndex(int(h1.ProposerIndex))
	if err != nil {
		return err
	}
	if !proposer.IsSlashable(state.Epoch(s)) {
		return fmt.Errorf("proposer is not slashable: %v", proposer)
	}

	// Set whistleblower index to 0 so current proposer gets reward.
	pr, err := s.SlashValidator(h1.ProposerIndex, nil)
	if I.BlockRewardsCollector != nil {
		I.BlockRewardsCollector.ProposerSlashings += pr
	}
	return err
}

func (I *impl) ProcessAttesterSlashing(
	s abstract.BeaconState,
	attSlashing *cltypes.AttesterSlashing,
) error {
	att1 := attSlashing.Attestation_1
	att2 := attSlashing.Attestation_2

	if !cltypes.IsSlashableAttestationData(att1.Data, att2.Data) {
		return fmt.Errorf("attestation data not slashable: %+v; %+v", att1.Data, att2.Data)
	}

	valid, err := state.IsValidIndexedAttestation(s, att1)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 1 validity: %v", err)
	}
	if !valid {
		return errors.New("invalid indexed attestation 1")
	}

	valid, err = state.IsValidIndexedAttestation(s, att2)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 2 validity: %v", err)
	}
	if !valid {
		return errors.New("invalid indexed attestation 2")
	}

	slashedAny := false
	currentEpoch := state.GetEpochAtSlot(s.BeaconConfig(), s.Slot())
	for _, ind := range solid.IntersectionOfSortedSets(
		solid.IterableSSZ[uint64](att1.AttestingIndices),
		solid.IterableSSZ[uint64](att2.AttestingIndices)) {
		validator, err := s.ValidatorForValidatorIndex(int(ind))
		if err != nil {
			return err
		}
		if validator.IsSlashable(currentEpoch) {
			pr, err := s.SlashValidator(ind, nil)
			if err != nil {
				return fmt.Errorf("unable to slash validator: %d: %s", ind, err)
			}
			if I.BlockRewardsCollector != nil {
				I.BlockRewardsCollector.AttesterSlashings += pr
			}
			slashedAny = true
		}
	}

	if !slashedAny {
		return errors.New("no validators slashed")
	}
	return nil
}

func isValidDepositSignature(depositData *cltypes.DepositData, cfg *clparams.BeaconChainConfig) (bool, error) {
	// Agnostic domain.
	domain, err := fork.ComputeDomain(
		cfg.DomainDeposit[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	if err != nil {
		return false, err
	}
	depositMessageRoot, err := depositData.MessageHash()
	if err != nil {
		return false, err
	}
	signedRoot := utils.Sha256(depositMessageRoot[:], domain)
	// Perform BLS verification and if successful noice.
	valid, err := bls.Verify(depositData.Signature[:], signedRoot[:], depositData.PubKey[:])
	if err != nil || !valid {
		// ignore err here
		log.Debug("Validator BLS verification failed", "valid", valid, "err", err)
		return false, nil
	}
	return true, nil
}

func (I *impl) ProcessDeposit(s abstract.BeaconState, deposit *cltypes.Deposit) error {
	if deposit == nil {
		return nil
	}
	depositLeaf, err := deposit.Data.HashSSZ()
	if err != nil {
		return err
	}
	depositIndex := s.Eth1DepositIndex()
	eth1Data := s.Eth1Data()
	rawProof := []common.Hash{}
	deposit.Proof.Range(func(_ int, l common.Hash, _ int) bool {
		rawProof = append(rawProof, l)
		return true
	})
	// Validate merkle proof for deposit leaf.
	if I.FullValidation && !utils.IsValidMerkleBranch(
		depositLeaf,
		rawProof,
		s.BeaconConfig().DepositContractTreeDepth+1,
		depositIndex,
		eth1Data.Root,
	) {
		return errors.New("processDepositForAltair: Could not validate deposit root")
	}
	// Increment index
	s.SetEth1DepositIndex(depositIndex + 1)
	publicKey := deposit.Data.PubKey
	amount := deposit.Data.Amount
	// Check if pub key is in validator set
	validatorIndex, has := s.ValidatorIndexByPubkey(publicKey)
	if !has {
		// Check if the deposit is valid
		if valid, err := statechange.IsValidDepositSignature(deposit.Data, s.BeaconConfig()); err != nil {
			return err
		} else if !valid {
			return nil
		}
		// Append validator
		if s.Version() >= clparams.ElectraVersion {
			statechange.AddValidatorToRegistry(s, publicKey, deposit.Data.WithdrawalCredentials, 0)
		} else {
			// Append validator and done
			statechange.AddValidatorToRegistry(s, publicKey, deposit.Data.WithdrawalCredentials, amount)
			return nil
		}
	}
	if s.Version() >= clparams.ElectraVersion {
		s.AppendPendingDeposit(&solid.PendingDeposit{
			PubKey:                publicKey,
			WithdrawalCredentials: deposit.Data.WithdrawalCredentials,
			Amount:                amount,
			Signature:             deposit.Data.Signature,
			Slot:                  s.BeaconConfig().GenesisSlot, // Use GENESIS_SLOT to distinguish from a pending deposit request
		})
		return nil
	} else {
		// Deneb and before: Increase the balance if exists already
		return state.IncreaseBalance(s, validatorIndex, amount)
	}
}

func getPendingBalanceToWithdraw(s abstract.BeaconState, validatorIndex uint64) uint64 {
	ws := s.GetPendingPartialWithdrawals()
	balance := uint64(0)
	ws.Range(func(index int, withdrawal *solid.PendingPartialWithdrawal, length int) bool {
		if withdrawal.ValidatorIndex == validatorIndex {
			balance += withdrawal.Amount
		}
		return true
	})
	return balance
}

func IsVoluntaryExitApplicable(s abstract.BeaconState, voluntaryExit *cltypes.VoluntaryExit) error {
	currentEpoch := state.Epoch(s)
	validator, err := s.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		return err
	}
	// Verify the validator is active
	if !validator.Active(currentEpoch) {
		return errors.New("ProcessVoluntaryExit: validator is not active")
	}
	// Verify exit has not been initiated
	if validator.ExitEpoch() != s.BeaconConfig().FarFutureEpoch {
		return errors.New(
			"ProcessVoluntaryExit: another exit for the same validator is already getting processed",
		)
	}
	// Exits must specify an epoch when they become valid; they are not valid before then
	if currentEpoch < voluntaryExit.Epoch {
		return errors.New("ProcessVoluntaryExit: exit is happening in the future")
	}
	// Verify the validator has been active long enough
	if currentEpoch < validator.ActivationEpoch()+s.BeaconConfig().ShardCommitteePeriod {
		return errors.New("ProcessVoluntaryExit: exit is happening too fast")
	}
	if s.Version() >= clparams.ElectraVersion {
		// Only exit validator if it has no pending withdrawals in the queue
		if b := getPendingBalanceToWithdraw(s, voluntaryExit.ValidatorIndex); b > 0 {
			return fmt.Errorf("ProcessVoluntaryExit: validator has pending balance to withdraw: %d", b)
		}
	}
	return nil
}

// ProcessVoluntaryExit takes a voluntary exit and applies state transition.
func (I *impl) ProcessVoluntaryExit(
	s abstract.BeaconState,
	signedVoluntaryExit *cltypes.SignedVoluntaryExit,
) error {
	// Sanity checks so that we know it is good.
	voluntaryExit := signedVoluntaryExit.VoluntaryExit
	err := IsVoluntaryExitApplicable(s, voluntaryExit)
	if err != nil {
		return err
	}

	// Do the exit (same process in slashing).
	return s.InitiateValidatorExit(voluntaryExit.ValidatorIndex)
}

// ProcessWithdrawals processes withdrawals by decreasing the balance of each validator
// and updating the next withdrawal index and validator index.
func (I *impl) ProcessWithdrawals(
	s abstract.BeaconState,
	withdrawals *solid.ListSSZ[*cltypes.Withdrawal],
) error {
	// Get the list of withdrawals, the expected withdrawals (if performing full validation),
	// and the beacon configuration.
	beaconConfig := s.BeaconConfig()
	numValidators := uint64(s.ValidatorLength())

	// Check if full validation is required and verify expected withdrawals.
	expectedWithdrawals, partialWithdrawalsCount := state.ExpectedWithdrawals(s, state.Epoch(s))
	if I.FullValidation {
		if len(expectedWithdrawals) != withdrawals.Len() {
			return fmt.Errorf(
				"ProcessWithdrawals: expected %d withdrawals, but got %d",
				len(expectedWithdrawals),
				withdrawals.Len(),
			)
		}
		if err := solid.RangeErr[*cltypes.Withdrawal](withdrawals, func(i int, w *cltypes.Withdrawal, _ int) error {
			if *expectedWithdrawals[i] != *w {
				return fmt.Errorf("ProcessWithdrawals: withdrawal %d does not match expected withdrawal", i)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if s.Version() >= clparams.ElectraVersion {
		// Update pending partial withdrawals [New in Electra:EIP7251]
		pendingPartialWithdrawal := s.GetPendingPartialWithdrawals()
		pendingPartialWithdrawal.Cut(int(partialWithdrawalsCount))
		s.SetPendingPartialWithdrawals(pendingPartialWithdrawal)
	}

	if err := solid.RangeErr[*cltypes.Withdrawal](withdrawals, func(_ int, w *cltypes.Withdrawal, _ int) error {
		if err := state.DecreaseBalance(s, w.Validator, w.Amount); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Update next withdrawal index based on number of withdrawals.
	if withdrawals.Len() > 0 {
		lastWithdrawalIndex := withdrawals.Get(withdrawals.Len() - 1).Index
		s.SetNextWithdrawalIndex(lastWithdrawalIndex + 1)
	}

	// Update next withdrawal validator index based on number of withdrawals.
	if withdrawals.Len() == int(beaconConfig.MaxWithdrawalsPerPayload) {
		lastWithdrawalValidatorIndex := withdrawals.Get(withdrawals.Len()-1).Validator + 1
		s.SetNextWithdrawalValidatorIndex(lastWithdrawalValidatorIndex % numValidators)
	} else {
		nextIndex := s.NextWithdrawalValidatorIndex() + beaconConfig.MaxValidatorsPerWithdrawalsSweep
		s.SetNextWithdrawalValidatorIndex(nextIndex % numValidators)
	}

	return nil
}

// ProcessExecutionPayload sets the latest payload header accordinly.
func (I *impl) ProcessExecutionPayload(s abstract.BeaconState, body cltypes.GenericBeaconBody) error {
	payloadHeader, err := body.GetPayloadHeader()
	if err != nil {
		return err
	}
	parentHash := payloadHeader.ParentHash
	prevRandao := payloadHeader.PrevRandao
	time := payloadHeader.Time
	if state.IsMergeTransitionComplete(s) {
		// Verify consistency of the parent hash with respect to the previous execution payload header
		// assert payload.parent_hash == state.latest_execution_payload_header.block_hash
		if !bytes.Equal(parentHash[:], s.LatestExecutionPayloadHeader().BlockHash[:]) {
			return errors.New("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	random := s.GetRandaoMixes(state.Epoch(s))
	if !bytes.Equal(prevRandao[:], random[:]) {
		// Verify prev_randao
		// assert payload.prev_randao == get_randao_mix(state, get_current_epoch(state))
		return fmt.Errorf(
			"ProcessExecutionPayload: randao mix mismatches with mix digest, expected %x, got %x",
			random,
			prevRandao,
		)
	}
	if time != state.ComputeTimestampAtSlot(s, s.Slot()) {
		// Verify timestamp
		// assert payload.timestamp == compute_timestamp_at_slot(state, state.slot)
		return errors.New("ProcessExecutionPayload: invalid Eth1 timestamp")
	}

	// Verify commitments are under limit
	if s.Version() >= clparams.FuluVersion {
		// Fulu:EIP7892
		blobParameters := s.BeaconConfig().GetBlobParameters(state.Epoch(s))
		if body.GetBlobKzgCommitments().Len() > int(blobParameters.MaxBlobsPerBlock) {
			return errors.New("ProcessExecutionPayload: too many blob commitments")
		}
	} else {
		// assert len(body.blob_kzg_commitments) <= MAX_BLOBS_PER_BLOCK
		if body.GetBlobKzgCommitments().Len() > int(s.BeaconConfig().MaxBlobsPerBlockByVersion(s.Version())) {
			return errors.New("ProcessExecutionPayload: too many blob commitments")
		}
	}

	s.SetLatestExecutionPayloadHeader(payloadHeader)
	return nil
}

func (I *impl) ProcessSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) error {
	votedKeys, err := I.processSyncAggregate(s, sync)
	if err != nil {
		return err
	}
	if I.FullValidation {
		previousSlot := s.PreviousSlot()

		domain, err := fork.Domain(
			s.Fork(),
			state.GetEpochAtSlot(s.BeaconConfig(), previousSlot),
			s.BeaconConfig().DomainSyncCommittee,
			s.GenesisValidatorsRoot(),
		)
		if err != nil {
			return nil
		}
		blockRoot, err := s.GetBlockRootAtSlot(previousSlot)
		if err != nil {
			return err
		}
		msg := utils.Sha256(blockRoot[:], domain)
		isValid, err := bls.VerifyAggregate(sync.SyncCommiteeSignature[:], msg[:], votedKeys)
		if err != nil {
			return err
		}
		if !isValid {
			return errors.New("ProcessSyncAggregate: cannot validate sync committee signature")
		}
	}
	return nil
}

// processSyncAggregate applies all the logic in the spec function `process_sync_aggregate` except
// verifying the BLS signatures. It returns the modified beacons state and the list of validators'
// public keys that voted, for future signature verification.
func (I *impl) processSyncAggregate(
	s abstract.BeaconState,
	sync *cltypes.SyncAggregate,
) ([][]byte, error) {
	currentSyncCommittee := s.CurrentSyncCommittee()

	if currentSyncCommittee == nil {
		return nil, errors.New("nil current sync committee in s")
	}
	committeeKeys := currentSyncCommittee.GetCommittee()
	if len(sync.SyncCommiteeBits)*8 > len(committeeKeys) {
		return nil, errors.New("bits length exceeds committee length")
	}
	var votedKeys [][]byte

	proposerReward, participantReward, err := s.SyncRewards()
	if err != nil {
		return nil, err
	}

	proposerIndex, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}

	syncAggregateBits := sync.SyncCommiteeBits
	earnedProposerReward := uint64(0)
	currPubKeyIndex := 0
	for i := range syncAggregateBits {
		for bit := 1; bit <= 128; bit *= 2 {
			vIdx, exists := s.ValidatorIndexByPubkey(committeeKeys[currPubKeyIndex])
			// Impossible scenario.
			if !exists {
				return nil, fmt.Errorf(
					"validator public key does not exist in state: %x",
					committeeKeys[currPubKeyIndex],
				)
			}
			if syncAggregateBits[i]&byte(bit) > 0 {
				votedKeys = append(votedKeys, committeeKeys[currPubKeyIndex][:])
				if err := state.IncreaseBalance(s, vIdx, participantReward); err != nil {
					return nil, err
				}
				earnedProposerReward += proposerReward
			} else {
				if err := state.DecreaseBalance(s, vIdx, participantReward); err != nil {
					return nil, err
				}
			}
			currPubKeyIndex++
		}
	}

	if I.BlockRewardsCollector != nil {
		I.BlockRewardsCollector.SyncAggregate = earnedProposerReward
	}
	return votedKeys, state.IncreaseBalance(s, proposerIndex, earnedProposerReward)
}

// ProcessBlsToExecutionChange processes a BLSToExecutionChange message by updating a validator's withdrawal credentials.
func (I *impl) ProcessBlsToExecutionChange(
	s abstract.BeaconState,
	signedChange *cltypes.SignedBLSToExecutionChange,
) error {
	change := signedChange.Message
	beaconConfig := s.BeaconConfig()
	validator, err := s.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return err
	}
	credentials := validator.WithdrawalCredentials()
	// assert validator.withdrawal_credentials[:1] == BLS_WITHDRAWAL_PREFIX
	if credentials[0] != byte(beaconConfig.BLSWithdrawalPrefixByte) {
		return errors.New("ProcessBlsToExecutionChange: withdrawal credentials prefix mismatch")
	}
	// assert validator.withdrawal_credentials[1:] == hash(address_change.from_bls_pubkey)[1:]
	hashKey := utils.Sha256(change.From[:])
	if !bytes.Equal(credentials[1:], hashKey[1:]) {
		return errors.New("ProcessBlsToExecutionChange: withdrawal credentials mismatch")
	}

	// Fork-agnostic domain since address changes are valid across forks
	domain, err := fork.ComputeDomain(
		s.BeaconConfig().DomainBLSToExecutionChange[:],
		utils.Uint32ToBytes4(uint32(s.BeaconConfig().GenesisForkVersion)),
		s.GenesisValidatorsRoot())
	if err != nil {
		return err
	}
	signingRoot, err := fork.ComputeSigningRoot(change, domain)
	if err != nil {
		return err
	}
	// Verify the signature
	ok, err := bls.Verify(signedChange.Signature[:], signingRoot[:], change.From[:])
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("ProcessBlsToExecutionChange: invalid signature")
	}

	// Perform full validation if requested.
	// Reset the validator's withdrawal credentials.
	credentials[0] = byte(beaconConfig.ETH1AddressWithdrawalPrefixByte)
	copy(credentials[1:], make([]byte, 11))
	copy(credentials[12:], change.To[:])

	// Update the state with the modified validator.
	s.SetWithdrawalCredentialForValidatorAtIndex(int(change.ValidatorIndex), credentials)
	return nil
}

func (I *impl) ProcessAttestations(
	s abstract.BeaconState,
	attestations *solid.ListSSZ[*solid.Attestation],
) error {
	attestingIndiciesSet := make([][]uint64, attestations.Len())
	baseRewardPerIncrement := s.BaseRewardPerIncrement()

	var err error
	if err := solid.RangeErr[*solid.Attestation](attestations, func(i int, a *solid.Attestation, _ int) error {
		if attestingIndiciesSet[i], err = I.processAttestation(s, a, baseRewardPerIncrement); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	var valid bool
	if I.FullValidation {
		start := time.Now()
		valid, err = verifyAttestations(s, attestations, attestingIndiciesSet)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessAttestation: wrong bls data")
		}
		monitor.ObserveAttestationBlockProcessingTime(start)
	}

	return nil
}

func (I *impl) processAttestationPostAltair(
	s abstract.BeaconState,
	attestation *solid.Attestation,
	baseRewardPerIncrement uint64,
) ([]uint64, error) {
	data := attestation.Data
	currentEpoch := state.Epoch(s)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()

	if s.Version() >= clparams.ElectraVersion {
		// assert index == 0
		if data.CommitteeIndex != 0 {
			return nil, errors.New("processAttestationPostAltair: committee index must be 0")
		}
		// check committee
		committeeIndices := attestation.CommitteeBits.GetOnIndices()
		committeeOffset := 0
		for _, committeeIndex := range committeeIndices {
			// assert committee_index < get_committee_count_per_slot(state, data.target.epoch)
			if uint64(committeeIndex) >= s.CommitteeCount(currentEpoch) {
				return nil, errors.New("processAttestationPostAltair: committee index out of bounds")
			}
			committee, err := s.GetBeaconCommitee(data.Slot, uint64(committeeIndex))
			if err != nil {
				return nil, err
			}
			attesters := []uint64{}
			for i, attester := range committee {
				if attestation.AggregationBits.GetBitAt(committeeOffset + i) {
					attesters = append(attesters, attester)
				}
			}
			// assert len(committee_attesters) > 0
			if len(attesters) == 0 {
				return nil, errors.New("processAttestationPostAltair: no attesters in committee")
			}
			committeeOffset += len(committee)
		}
		// Bitfield length matches total number of participants
		// assert len(attestation.aggregation_bits) == committee_offset
		if attestation.AggregationBits.Bits() != committeeOffset {
			return nil, errors.New("processAttestationPostAltair: aggregation bits length mismatch")
		}
	}

	participationFlagsIndicies, err := s.GetAttestationParticipationFlagIndicies(
		data,
		stateSlot-data.Slot,
		false,
	)
	if err != nil {
		return nil, err
	}

	attestingIndicies, err := s.GetAttestingIndicies(attestation, true)
	if err != nil {
		return nil, err
	}

	var proposerRewardNumerator uint64

	isCurrentEpoch := data.Target.Epoch == currentEpoch

	for _, attesterIndex := range attestingIndicies {
		val, err := s.ValidatorEffectiveBalance(int(attesterIndex))
		if err != nil {
			return nil, err
		}

		baseReward := (val / beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range beaconConfig.ParticipationWeights() {
			flagParticipation := s.EpochParticipationForValidatorIndex(
				isCurrentEpoch,
				int(attesterIndex),
			)
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) ||
				flagParticipation.HasFlag(flagIndex) {
				continue
			}
			s.SetEpochParticipationForValidatorIndex(
				isCurrentEpoch,
				int(attesterIndex),
				flagParticipation.Add(flagIndex),
			)
			proposerRewardNumerator += baseReward * weight
		}
	}
	// Reward proposer
	proposer, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	proposerRewardDenominator := (beaconConfig.WeightDenominator - beaconConfig.ProposerWeight) * beaconConfig.WeightDenominator / beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	if I.BlockRewardsCollector != nil {
		I.BlockRewardsCollector.Attestations += reward
	}
	return attestingIndicies, state.IncreaseBalance(s, proposer, reward)
}

// processAttestationPhase0 implements the rules for phase0 processing.
func (I *impl) processAttestationPhase0(
	s abstract.BeaconState,
	attestation *solid.Attestation,
) ([]uint64, error) {
	data := attestation.Data
	// NOTE: this function is only called in phase0, so don't need to change committee index field by electra fork.
	committee, err := s.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
	if err != nil {
		return nil, err
	}

	if len(committee) != utils.GetBitlistLength(attestation.AggregationBits.Bytes()) {
		return nil, errors.New("processAttestationPhase0: mismatching aggregation bits size")
	}
	// Cached so it is performant.
	proposerIndex, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	// Create the attestation to add to pending attestations
	pendingAttestation := &solid.PendingAttestation{
		AggregationBits: attestation.AggregationBits,
		Data:            data,
		InclusionDelay:  s.Slot() - data.Slot,
		ProposerIndex:   proposerIndex,
	}

	isCurrentAttestation := data.Target.Epoch == state.Epoch(s)
	// Depending of what slot we are on we put in either the current justified or previous justified.
	if isCurrentAttestation {
		if !data.Source.Equal(s.CurrentJustifiedCheckpoint()) {
			return nil, errors.New("processAttestationPhase0: mismatching sources")
		}
		s.AddCurrentEpochAtteastation(pendingAttestation)
	} else {
		if !data.Source.Equal(s.PreviousJustifiedCheckpoint()) {
			return nil, errors.New("processAttestationPhase0: mismatching sources")
		}
		s.AddPreviousEpochAttestation(pendingAttestation)
	}
	// Not required by specs but needed if we want performant epoch transition.
	indicies, err := s.GetAttestingIndicies(
		attestation,
		true,
	)
	if err != nil {
		return nil, err
	}
	epochRoot, err := state.GetBlockRoot(s, attestation.Data.Target.Epoch)
	if err != nil {
		return nil, err
	}
	slotRoot, err := s.GetBlockRootAtSlot(attestation.Data.Slot)
	if err != nil {
		return nil, err
	}
	// Basically we flag all validators we are currently attesting. will be important for rewards/finalization processing.
	for _, index := range indicies {
		minCurrentInclusionDelayAttestation, err := s.ValidatorMinCurrentInclusionDelayAttestation(
			int(index),
		)
		if err != nil {
			return nil, err
		}

		minPreviousInclusionDelayAttestation, err := s.ValidatorMinPreviousInclusionDelayAttestation(
			int(index),
		)
		if err != nil {
			return nil, err
		}
		// NOTE: does not affect state root.
		// We need to set it to currents or previouses depending on which attestation we process.
		if isCurrentAttestation {
			if minCurrentInclusionDelayAttestation == nil ||
				minCurrentInclusionDelayAttestation.InclusionDelay > pendingAttestation.InclusionDelay {
				if err := s.SetValidatorMinCurrentInclusionDelayAttestation(int(index), pendingAttestation); err != nil {
					return nil, err
				}
			}
			if err := s.SetValidatorIsCurrentMatchingSourceAttester(int(index), true); err != nil {
				return nil, err
			}
			if attestation.Data.Target.Root == epochRoot {
				if err := s.SetValidatorIsCurrentMatchingTargetAttester(int(index), true); err != nil {
					return nil, err
				}
			} else {
				continue
			}
			if attestation.Data.BeaconBlockRoot == slotRoot {
				if err := s.SetValidatorIsCurrentMatchingHeadAttester(int(index), true); err != nil {
					return nil, err
				}
			}
		} else {
			if minPreviousInclusionDelayAttestation == nil ||
				minPreviousInclusionDelayAttestation.InclusionDelay > pendingAttestation.InclusionDelay {
				if err := s.SetValidatorMinPreviousInclusionDelayAttestation(int(index), pendingAttestation); err != nil {
					return nil, err
				}
			}
			if err := s.SetValidatorIsPreviousMatchingSourceAttester(int(index), true); err != nil {
				return nil, err
			}
			if attestation.Data.Target.Root != epochRoot {
				continue
			}
			if err := s.SetValidatorIsPreviousMatchingTargetAttester(int(index), true); err != nil {
				return nil, err
			}
			if attestation.Data.BeaconBlockRoot == slotRoot {
				if err := s.SetValidatorIsPreviousMatchingHeadAttester(int(index), true); err != nil {
					return nil, err
				}
			}
		}
	}
	return indicies, nil
}

func IsAttestationApplicable(s abstract.BeaconState, attestation *solid.Attestation) error {
	data := attestation.Data
	currentEpoch := state.Epoch(s)
	previousEpoch := state.PreviousEpoch(s)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()
	// Prelimary checks.
	if (data.Target.Epoch != currentEpoch && data.Target.Epoch != previousEpoch) ||
		data.Target.Epoch != state.GetEpochAtSlot(s.BeaconConfig(), data.Slot) {
		return errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if s.Version() < clparams.DenebVersion &&
		((data.Slot+beaconConfig.MinAttestationInclusionDelay > stateSlot) || (stateSlot > data.Slot+beaconConfig.SlotsPerEpoch)) {
		return errors.New("ProcessAttestation: attestation slot not in range")
	}
	if s.Version() >= clparams.DenebVersion &&
		data.Slot+beaconConfig.MinAttestationInclusionDelay > stateSlot {
		return errors.New("ProcessAttestation: attestation slot not in range")
	}
	cIndex := data.CommitteeIndex
	if s.Version().AfterOrEqual(clparams.ElectraVersion) {
		index, err := attestation.GetCommitteeIndexFromBits()
		if err != nil {
			return err
		}
		cIndex = index
	}
	if cIndex >= s.CommitteeCount(data.Target.Epoch) {
		return errors.New("ProcessAttestation: attester index out of range")
	}
	return nil
}

// ProcessAttestation takes an attestation and process it.
func (I *impl) processAttestation(
	s abstract.BeaconState,
	attestation *solid.Attestation,
	baseRewardPerIncrement uint64,
) ([]uint64, error) {
	// Prelimary checks.
	if err := IsAttestationApplicable(s, attestation); err != nil {
		return nil, err
	}
	// check if we need to use rules for phase0 or post-altair.
	if s.Version() == clparams.Phase0Version {
		return I.processAttestationPhase0(s, attestation)
	}
	return I.processAttestationPostAltair(s, attestation, baseRewardPerIncrement)
}

func verifyAttestations(
	s abstract.BeaconState,
	attestations *solid.ListSSZ[*solid.Attestation],
	attestingIndicies [][]uint64,
) (bool, error) {
	indexedAttestations := make([]*cltypes.IndexedAttestation, 0, attestations.Len())
	attestations.Range(func(idx int, a *solid.Attestation, _ int) bool {
		idxAttestations := state.GetIndexedAttestation(a, attestingIndicies[idx])
		indexedAttestations = append(indexedAttestations, idxAttestations)
		return true
	})

	return batchVerifyAttestations(s, indexedAttestations)
}

type indexedAttestationVerificationResult struct {
	valid bool
	err   error
}

// Concurrent verification of BLS.
func batchVerifyAttestations(
	s abstract.BeaconState,
	indexedAttestations []*cltypes.IndexedAttestation,
) (valid bool, err error) {
	c := make(chan indexedAttestationVerificationResult, len(indexedAttestations))

	for idx := range indexedAttestations {
		go func(idx int) {
			valid, err := state.IsValidIndexedAttestation(s, indexedAttestations[idx])
			c <- indexedAttestationVerificationResult{
				valid: valid,
				err:   err,
			}
		}(idx)
	}
	for i := 0; i < len(indexedAttestations); i++ {
		result := <-c
		if result.err != nil {
			return false, result.err
		}
		if !result.valid {
			return false, nil
		}
	}
	return true, nil
}

func (I *impl) ProcessBlockHeader(s abstract.BeaconState, slot, proposerIndex uint64, parentRoot common.Hash, bodyRoot [32]byte) error {
	if slot != s.Slot() {
		return fmt.Errorf("state slot: %d, not equal to block slot: %d", s.Slot(), slot)
	}
	if slot <= s.LatestBlockHeader().Slot {
		return fmt.Errorf(
			"slock slot: %d, not greater than latest block slot: %d",
			slot,
			s.LatestBlockHeader().Slot,
		)
	}
	propInd, err := s.GetBeaconProposerIndex()
	if err != nil {
		return fmt.Errorf("error in GetBeaconProposerIndex: %v", err)
	}
	if proposerIndex != propInd {
		return fmt.Errorf(
			"block proposer index: %d, does not match beacon proposer index: %d",
			proposerIndex,
			propInd,
		)
	}
	blockHeader := s.LatestBlockHeader()
	latestRoot, err := (&blockHeader).HashSSZ()
	if err != nil {
		return fmt.Errorf("unable to hash tree root of latest block header: %v", err)
	}
	if parentRoot != latestRoot {
		return fmt.Errorf(
			"block parent root: %x, does not match latest block root: %x",
			parentRoot,
			latestRoot,
		)
	}

	s.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          slot,
		ProposerIndex: proposerIndex,
		ParentRoot:    parentRoot,
		BodyRoot:      bodyRoot,
	})

	proposer, err := s.ValidatorForValidatorIndex(int(proposerIndex))
	if err != nil {
		return err
	}
	if proposer.Slashed() {
		return fmt.Errorf("proposer: %d is slashed", proposerIndex)
	}
	return nil
}

func (I *impl) ProcessRandao(s abstract.BeaconState, randao [96]byte, proposerIndex uint64) error {
	epoch := state.Epoch(s)
	randaoMixes := s.GetRandaoMixes(epoch)
	randaoHash := utils.Sha256(randao[:])
	mix := [32]byte{}
	for i := range mix {
		mix[i] = randaoMixes[i] ^ randaoHash[i]
	}
	s.SetRandaoMixAt(int(epoch%s.BeaconConfig().EpochsPerHistoricalVector), mix)
	return nil
}

func (I *impl) ProcessEth1Data(state abstract.BeaconState, eth1Data *cltypes.Eth1Data) error {
	state.AddEth1DataVote(eth1Data)
	newVotes := state.Eth1DataVotes()

	// Count how many times body.Eth1Data appears in the votes.
	numVotes := 0
	newVotes.Range(func(index int, value *cltypes.Eth1Data, length int) bool {
		if eth1Data.Equal(value) {
			numVotes += 1
		}
		return true
	})

	if uint64(numVotes*2) > state.BeaconConfig().EpochsPerEth1VotingPeriod*state.BeaconConfig().SlotsPerEpoch {
		state.SetEth1Data(eth1Data)
	}
	return nil
}

func (I *impl) ProcessSlots(s abstract.BeaconState, slot uint64) error {
	beaconConfig := s.BeaconConfig()
	sSlot := s.Slot()
	if slot <= sSlot {
		return fmt.Errorf("new slot: %d not greater than current slot: %d", slot, sSlot)
	}
	// Process each slot.
	for i := sSlot; i < slot; i++ {
		err := transitionSlot(s)
		if err != nil {
			return fmt.Errorf("unable to process slot transition: %v", err)
		}

		if (sSlot+1)%beaconConfig.SlotsPerEpoch == 0 {
			start := time.Now()
			if err := statechange.ProcessEpoch(s); err != nil {
				return err
			}
			log.Debug(
				"Processed new epoch successfully",
				"epoch",
				state.Epoch(s),
				"process_epoch_elpsed",
				time.Since(start),
			)
		}

		sSlot += 1
		s.SetSlot(sSlot)
		if sSlot%beaconConfig.SlotsPerEpoch != 0 {
			continue
		}
		if state.Epoch(s) == beaconConfig.AltairForkEpoch {
			if err := s.UpgradeToAltair(); err != nil {
				return err
			}
		}
		if state.Epoch(s) == beaconConfig.BellatrixForkEpoch {
			if err := s.UpgradeToBellatrix(); err != nil {
				return err
			}
		}
		if state.Epoch(s) == beaconConfig.CapellaForkEpoch {
			if err := s.UpgradeToCapella(); err != nil {
				return err
			}
		}
		if state.Epoch(s) == beaconConfig.DenebForkEpoch {
			if err := s.UpgradeToDeneb(); err != nil {
				return err
			}
		}

		if state.Epoch(s) == beaconConfig.ElectraForkEpoch {
			if err := s.UpgradeToElectra(); err != nil {
				return err
			}
		}

		if state.Epoch(s) == beaconConfig.FuluForkEpoch {
			if err := s.UpgradeToFulu(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (I *impl) ProcessDepositRequest(s abstract.BeaconState, depositRequest *solid.DepositRequest) error {
	if s.GetDepositRequestsStartIndex() == s.BeaconConfig().UnsetDepositRequestsStartIndex {
		s.SetDepositRequestsStartIndex(depositRequest.Index)
	}

	// Create pending deposit
	s.AppendPendingDeposit(&solid.PendingDeposit{
		PubKey:                depositRequest.PubKey,
		WithdrawalCredentials: depositRequest.WithdrawalCredentials,
		Amount:                depositRequest.Amount,
		Signature:             depositRequest.Signature,
		Slot:                  s.Slot(),
	})
	return nil
}

func (I *impl) ProcessWithdrawalRequest(s abstract.BeaconState, req *solid.WithdrawalRequest) error {
	var (
		amount            = req.Amount
		isFullExitRequest = req.Amount == FullExitRequestAmount
		reqPubkey         = req.ValidatorPubKey
	)
	// If partial withdrawal queue is full, only full exits are processed
	if uint64(s.GetPendingPartialWithdrawals().Len()) >= s.BeaconConfig().PendingPartialWithdrawalsLimit && !isFullExitRequest {
		return nil
	}
	// Verify pubkey exists
	vindex, exist := s.ValidatorIndexByPubkey(reqPubkey)
	if !exist {
		log.Warn("ProcessWithdrawalRequest: validator index not found", "pubkey", common.Bytes2Hex(reqPubkey[:]))
		return nil
	}
	validator, err := s.ValidatorForValidatorIndex(int(vindex))
	if err != nil {
		return fmt.Errorf("ProcessWithdrawalRequest: validator not found for index %d", vindex)
	}
	// Verify withdrawal credentials
	hasCorrectCredential := state.HasExecutionWithdrawalCredential(validator, s.BeaconConfig())
	wc := validator.WithdrawalCredentials()
	isCorrectSourceAddress := bytes.Equal(req.SourceAddress[:], wc[12:])
	if !(isCorrectSourceAddress && hasCorrectCredential) {
		return nil
	}
	// check validator is active
	if !validator.Active(state.Epoch(s)) {
		return nil
	}
	// Verify exit has not been initiated
	if validator.ExitEpoch() != s.BeaconConfig().FarFutureEpoch {
		return nil
	}
	// Verify the validator has been active long enough
	if state.Epoch(s) < validator.ActivationEpoch()+s.BeaconConfig().ShardCommitteePeriod {
		return nil
	}
	pendingBalanceToWithdraw := getPendingBalanceToWithdraw(s, vindex)
	if isFullExitRequest {
		// Only exit validator if it has no pending withdrawals in the queue
		if pendingBalanceToWithdraw == 0 {
			return s.InitiateValidatorExit(vindex)
		}
		return nil
	}

	vbalance, err := s.ValidatorBalance(int(vindex))
	if err != nil {
		return err
	}
	hasSufficientEffectiveBalance := validator.EffectiveBalance() >= s.BeaconConfig().MinActivationBalance
	hasExcessBalance := vbalance > s.BeaconConfig().MinActivationBalance+pendingBalanceToWithdraw
	// Only allow partial withdrawals with compounding withdrawal credentials
	if state.HasCompoundingWithdrawalCredential(validator, s.BeaconConfig()) && hasSufficientEffectiveBalance && hasExcessBalance {
		toWithdraw := min(
			vbalance-s.BeaconConfig().MinActivationBalance-pendingBalanceToWithdraw,
			amount,
		)
		exitQueueEpoch := s.ComputeExitEpochAndUpdateChurn(toWithdraw)
		withdrawableEpoch := exitQueueEpoch + s.BeaconConfig().MinValidatorWithdrawabilityDelay
		s.AppendPendingPartialWithdrawal(&solid.PendingPartialWithdrawal{
			ValidatorIndex:    vindex,
			Amount:            toWithdraw,
			WithdrawableEpoch: withdrawableEpoch,
		})
	}
	return nil
}

func (I *impl) ProcessConsolidationRequest(s abstract.BeaconState, consolidationRequest *solid.ConsolidationRequest) error {
	if isValidSwitchToCompoundingRequest(s, consolidationRequest) {
		// source index
		sourceIndex, exist := s.ValidatorIndexByPubkey(consolidationRequest.SourcePubKey)
		if !exist {
			log.Warn("Validator index not found for source pubkey", "pubkey", consolidationRequest.SourcePubKey)
			return nil
		}
		if err := switchToCompoundingValidator(s, sourceIndex); err != nil {
			return err
		}
		return nil
	}
	// Verify that source != target, so a consolidation cannot be used as an exit.
	if bytes.Equal(consolidationRequest.SourcePubKey[:], consolidationRequest.TargetPubKey[:]) {
		return nil
	}
	// If the pending consolidations queue is full, consolidation requests are ignored
	if s.GetPendingConsolidations().Len() == int(s.BeaconConfig().PendingConsolidationsLimit) {
		return nil
	}
	// If there is too little available consolidation churn limit, consolidation requests are ignored
	if state.GetConsolidationChurnLimit(s) <= s.BeaconConfig().MinActivationBalance {
		return nil
	}
	// source/target index and validator
	sourceIndex, exist := s.ValidatorIndexByPubkey(consolidationRequest.SourcePubKey)
	if !exist {
		log.Warn("Validator index not found for source pubkey", "pubkey", consolidationRequest.SourcePubKey)
		return nil
	}
	targetIndex, exist := s.ValidatorIndexByPubkey(consolidationRequest.TargetPubKey)
	if !exist {
		log.Warn("Validator index not found for target pubkey", "pubkey", consolidationRequest.TargetPubKey)
		return nil
	}
	sourceValidator, err := s.ValidatorForValidatorIndex(int(sourceIndex))
	if err != nil {
		return err
	}
	targetValidator, err := s.ValidatorForValidatorIndex(int(targetIndex))
	if err != nil {
		return err
	}

	// Verify source withdrawal credentials
	hasCorrectCredential := state.HasExecutionWithdrawalCredential(sourceValidator, s.BeaconConfig())
	sourceWc := sourceValidator.WithdrawalCredentials()
	isCorrectSourceAddress := bytes.Equal(consolidationRequest.SourceAddress[:], sourceWc[12:])
	if !(isCorrectSourceAddress && hasCorrectCredential) {
		return nil
	}
	// Verify that target has compounding withdrawal credentials
	if !state.HasCompoundingWithdrawalCredential(targetValidator, s.BeaconConfig()) {
		return nil
	}
	// Verify the source and the target are active
	curEpoch := state.Epoch(s)
	if !sourceValidator.Active(curEpoch) || !targetValidator.Active(curEpoch) {
		return nil
	}
	// Verify exits for source and target have not been initiated
	if sourceValidator.ExitEpoch() != s.BeaconConfig().FarFutureEpoch ||
		targetValidator.ExitEpoch() != s.BeaconConfig().FarFutureEpoch {
		return nil
	}
	// Verify the source has been active long enough
	if curEpoch < sourceValidator.ActivationEpoch()+s.BeaconConfig().ShardCommitteePeriod {
		log.Info("[Consolidation] Source has not been active long enough, ignoring consolidation request", "slot", s.Slot(), "curEpoch", curEpoch, "activationEpoch", sourceValidator.ActivationEpoch())
		return nil
	}
	// Verify the source has no pending withdrawals in the queue
	if getPendingBalanceToWithdraw(s, sourceIndex) > 0 {
		log.Info("[Consolidation] Source has pending withdrawals, ignoring consolidation request", "slot", s.Slot())
		return nil
	}

	// Initiate source validator exit and append pending consolidation
	s.SetExitEpochForValidatorAtIndex(int(sourceIndex), computeConsolidationEpochAndUpdateChurn(s, sourceValidator.EffectiveBalance()))
	s.SetWithdrawableEpochForValidatorAtIndex(int(sourceIndex), sourceValidator.ExitEpoch()+s.BeaconConfig().MinValidatorWithdrawabilityDelay)

	s.AppendPendingConsolidation(&solid.PendingConsolidation{
		SourceIndex: sourceIndex,
		TargetIndex: targetIndex,
	})
	return nil
}

func isValidSwitchToCompoundingRequest(s abstract.BeaconState, request *solid.ConsolidationRequest) bool {
	// Switch to compounding requires source and target be equal
	if !bytes.Equal(request.SourcePubKey[:], request.TargetPubKey[:]) {
		return false
	}
	// Verify pubkey exists
	vindex, exist := s.ValidatorIndexByPubkey(request.SourcePubKey)
	if !exist {
		return false
	}
	sourceValidator, err := s.ValidatorForValidatorIndex(int(vindex))
	if err != nil {
		log.Warn("Error getting validator for source pubkey", "error", err)
		return false
	}
	// Verify request has been authorized
	wc := sourceValidator.WithdrawalCredentials()
	if !bytes.Equal(wc[12:], request.SourceAddress[:]) {
		return false
	}
	// Verify source withdrawal credentials
	if !state.HasEth1WithdrawalCredential(sourceValidator, s.BeaconConfig()) {
		return false
	}
	// Verify the source is active
	curEpoch := state.Epoch(s)
	if !sourceValidator.Active(curEpoch) {
		return false
	}
	// Verify exit for source has not been initiated
	if sourceValidator.ExitEpoch() != s.BeaconConfig().FarFutureEpoch {
		return false
	}
	return true
}

func switchToCompoundingValidator(s abstract.BeaconState, vindex uint64) error {
	validator, err := s.ValidatorForValidatorIndex(int(vindex))
	if err != nil {
		return err
	}
	// copy the withdrawal credentials
	wc := validator.WithdrawalCredentials()
	newWc := common.Hash{}
	copy(newWc[:], wc[:])
	newWc[0] = byte(s.BeaconConfig().CompoundingWithdrawalPrefix)
	s.SetWithdrawalCredentialForValidatorAtIndex(int(vindex), newWc)
	return state.QueueExcessActiveBalance(s, vindex, &validator)
}

// compute_consolidation_epoch_and_update_churn
func computeConsolidationEpochAndUpdateChurn(s abstract.BeaconState, consolidationBalance uint64) uint64 {
	earlistConsolidationEpoch := max(
		s.GetEarlistConsolidationEpoch(),
		state.ComputeActivationExitEpoch(s.BeaconConfig(), state.Epoch(s)),
	)
	perEpochConsolidationChurn := state.GetConsolidationChurnLimit(s)
	// New epoch for consolidations.
	var consolidationBalanceToConsume uint64
	if s.GetEarlistConsolidationEpoch() < earlistConsolidationEpoch {
		consolidationBalanceToConsume = perEpochConsolidationChurn
	} else {
		consolidationBalanceToConsume = s.GetConsolidationBalanceToConsume()
	}
	// Consolidation doesn't fit in the current earliest epoch.
	if consolidationBalance > consolidationBalanceToConsume {
		balanceToProcess := consolidationBalance - consolidationBalanceToConsume
		additionalEpochs := (balanceToProcess-1)/perEpochConsolidationChurn + 1
		earlistConsolidationEpoch += additionalEpochs
		consolidationBalanceToConsume += additionalEpochs * perEpochConsolidationChurn
	}
	// Consume the balance and update state variables.
	s.SetConsolidationBalanceToConsume(consolidationBalanceToConsume - consolidationBalance)
	s.SetEarlistConsolidationEpoch(earlistConsolidationEpoch)
	return earlistConsolidationEpoch
}
