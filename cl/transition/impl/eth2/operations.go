package eth2

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/metrics"

	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2/statechange"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
)

func (I *impl) ProcessProposerSlashing(s abstract.BeaconState, propSlashing *cltypes.ProposerSlashing) error {
	h1 := propSlashing.Header1.Header
	h2 := propSlashing.Header2.Header

	if h1.Slot != h2.Slot {
		return fmt.Errorf("non-matching slots on proposer slashing: %d != %d", h1.Slot, h2.Slot)
	}

	if h1.ProposerIndex != h2.ProposerIndex {
		return fmt.Errorf("non-matching proposer indices proposer slashing: %d != %d", h1.ProposerIndex, h2.ProposerIndex)
	}

	if *h1 == *h2 {
		return fmt.Errorf("proposee slashing headers are the same")
	}

	proposer, err := s.ValidatorForValidatorIndex(int(h1.ProposerIndex))
	if err != nil {
		return err
	}
	if !proposer.IsSlashable(state.Epoch(s)) {
		return fmt.Errorf("proposer is not slashable: %v", proposer)
	}

	for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{propSlashing.Header1, propSlashing.Header2} {
		domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconProposer, state.GetEpochAtSlot(s.BeaconConfig(), signedHeader.Header.Slot))
		if err != nil {
			return fmt.Errorf("unable to get domain: %v", err)
		}
		signingRoot, err := fork.ComputeSigningRoot(signedHeader.Header, domain)
		if err != nil {
			return fmt.Errorf("unable to compute signing root: %v", err)
		}
		pk := proposer.PublicKey()
		valid, err := bls.Verify(signedHeader.Signature[:], signingRoot[:], pk[:])
		if err != nil {
			return fmt.Errorf("unable to verify signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("invalid signature: signature %v, root %v, pubkey %v", signedHeader.Signature[:], signingRoot[:], pk)
		}
	}

	// Set whistleblower index to 0 so current proposer gets reward.
	s.SlashValidator(h1.ProposerIndex, nil)
	return nil
}

func (I *impl) ProcessAttesterSlashing(s abstract.BeaconState, attSlashing *cltypes.AttesterSlashing) error {
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
		return fmt.Errorf("invalid indexed attestation 1")
	}

	valid, err = state.IsValidIndexedAttestation(s, att2)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 2 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 2")
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
			err := s.SlashValidator(ind, nil)
			if err != nil {
				return fmt.Errorf("unable to slash validator: %d", ind)
			}
			slashedAny = true
		}
	}

	if !slashedAny {
		return fmt.Errorf("no validators slashed")
	}
	return nil
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
		return fmt.Errorf("processDepositForAltair: Could not validate deposit root")
	}

	// Increment index
	s.SetEth1DepositIndex(depositIndex + 1)
	publicKey := deposit.Data.PubKey
	amount := deposit.Data.Amount
	// Check if pub key is in validator set
	validatorIndex, has := s.ValidatorIndexByPubkey(publicKey)
	if !has {
		// Agnostic domain.
		domain, err := fork.ComputeDomain(s.BeaconConfig().DomainDeposit[:], utils.Uint32ToBytes4(s.BeaconConfig().GenesisForkVersion), [32]byte{})
		if err != nil {
			return err
		}
		depositMessageRoot, err := deposit.Data.MessageHash()
		if err != nil {
			return err
		}
		signedRoot := utils.Keccak256(depositMessageRoot[:], domain)
		// Perform BLS verification and if successful noice.
		valid, err := bls.Verify(deposit.Data.Signature[:], signedRoot[:], publicKey[:])
		// Literally you can input it trash.
		if !valid || err != nil {
			log.Debug("Validator BLS verification failed", "valid", valid, "err", err)
			return nil
		}
		// Append validator
		s.AddValidator(state.ValidatorFromDeposit(s.BeaconConfig(), deposit), amount)
		// Altair forward
		if s.Version() >= clparams.AltairVersion {
			s.AddCurrentEpochParticipationFlags(cltypes.ParticipationFlags(0))
			s.AddPreviousEpochParticipationFlags(cltypes.ParticipationFlags(0))
			s.AddInactivityScore(0)
		}
		return nil
	}
	// Increase the balance if exists already
	return state.IncreaseBalance(s, validatorIndex, amount)
}

// ProcessVoluntaryExit takes a voluntary exit and applies state transition.
func (I *impl) ProcessVoluntaryExit(s abstract.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error {
	// Sanity checks so that we know it is good.
	voluntaryExit := signedVoluntaryExit.VoluntaryExit
	currentEpoch := state.Epoch(s)
	validator, err := s.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		return err
	}
	if !validator.Active(currentEpoch) {
		return errors.New("ProcessVoluntaryExit: validator is not active")
	}
	if validator.ExitEpoch() != s.BeaconConfig().FarFutureEpoch {
		return errors.New("ProcessVoluntaryExit: another exit for the same validator is already getting processed")
	}
	if currentEpoch < voluntaryExit.Epoch {
		return errors.New("ProcessVoluntaryExit: exit is happening in the future")
	}
	if currentEpoch < validator.ActivationEpoch()+s.BeaconConfig().ShardCommitteePeriod {
		return errors.New("ProcessVoluntaryExit: exit is happening too fast")
	}

	// We can skip it in some instances if we want to optimistically sync up.
	if I.FullValidation {
		domain, err := s.GetDomain(s.BeaconConfig().DomainVoluntaryExit, voluntaryExit.Epoch)
		if err != nil {
			return err
		}
		signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
		if err != nil {
			return err
		}
		pk := validator.PublicKey()
		valid, err := bls.Verify(signedVoluntaryExit.Signature[:], signingRoot[:], pk[:])
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessVoluntaryExit: BLS verification failed")
		}
	}
	// Do the exit (same process in slashing).
	return s.InitiateValidatorExit(voluntaryExit.ValidatorIndex)
}

// ProcessWithdrawals processes withdrawals by decreasing the balance of each validator
// and updating the next withdrawal index and validator index.
func (I *impl) ProcessWithdrawals(s abstract.BeaconState, withdrawals *solid.ListSSZ[*types.Withdrawal]) error {
	// Get the list of withdrawals, the expected withdrawals (if performing full validation),
	// and the beacon configuration.
	beaconConfig := s.BeaconConfig()
	numValidators := uint64(s.ValidatorLength())

	// Check if full validation is required and verify expected withdrawals.
	if I.FullValidation {
		expectedWithdrawals := state.ExpectedWithdrawals(s)
		if len(expectedWithdrawals) != withdrawals.Len() {
			return fmt.Errorf("ProcessWithdrawals: expected %d withdrawals, but got %d", len(expectedWithdrawals), withdrawals.Len())
		}
		if err := solid.RangeErr[*types.Withdrawal](withdrawals, func(i int, w *types.Withdrawal, _ int) error {
			if !expectedWithdrawals[i].Equal(w) {
				return fmt.Errorf("ProcessWithdrawals: withdrawal %d does not match expected withdrawal", i)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if err := solid.RangeErr[*types.Withdrawal](withdrawals, func(_ int, w *types.Withdrawal, _ int) error {
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
func (I *impl) ProcessExecutionPayload(s abstract.BeaconState, payload *cltypes.Eth1Block) error {
	if state.IsMergeTransitionComplete(s) {
		if payload.ParentHash != s.LatestExecutionPayloadHeader().BlockHash {
			return fmt.Errorf("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	if payload.PrevRandao != s.GetRandaoMixes(state.Epoch(s)) {
		return fmt.Errorf("ProcessExecutionPayload: randao mix mismatches with mix digest")
	}
	if payload.Time != state.ComputeTimestampAtSlot(s, s.Slot()) {
		return fmt.Errorf("ProcessExecutionPayload: invalid Eth1 timestamp")
	}
	payloadHeader, err := payload.PayloadHeader()
	if err != nil {
		return err
	}
	s.SetLatestExecutionPayloadHeader(payloadHeader)
	return nil
}

func (I *impl) ProcessSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) error {
	votedKeys, err := processSyncAggregate(s, sync)
	if err != nil {
		return err
	}
	if I.FullValidation {
		previousSlot := s.PreviousSlot()

		domain, err := fork.Domain(s.Fork(), state.GetEpochAtSlot(s.BeaconConfig(), previousSlot), s.BeaconConfig().DomainSyncCommittee, s.GenesisValidatorsRoot())
		if err != nil {
			return nil
		}
		blockRoot, err := s.GetBlockRootAtSlot(previousSlot)
		if err != nil {
			return err
		}
		msg := utils.Keccak256(blockRoot[:], domain)
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
func processSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) ([][]byte, error) {
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
				return nil, errors.New("validator public key does not exist in state")
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

	return votedKeys, state.IncreaseBalance(s, proposerIndex, earnedProposerReward)
}

// ProcessBlsToExecutionChange processes a BLSToExecutionChange message by updating a validator's withdrawal credentials.
func (I *impl) ProcessBlsToExecutionChange(s abstract.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error {
	change := signedChange.Message

	beaconConfig := s.BeaconConfig()
	validator, err := s.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return err
	}

	// Perform full validation if requested.
	wc := validator.WithdrawalCredentials()
	if I.FullValidation {
		// Check the validator's withdrawal credentials prefix.
		if wc[0] != beaconConfig.BLSWithdrawalPrefixByte {
			return fmt.Errorf("invalid withdrawal credentials prefix")
		}

		// Check the validator's withdrawal credentials against the provided message.
		hashedFrom := utils.Keccak256(change.From[:])
		if !bytes.Equal(hashedFrom[1:], wc[1:]) {
			return fmt.Errorf("invalid withdrawal credentials")
		}

		// Compute the signing domain and verify the message signature.
		domain, err := fork.ComputeDomain(beaconConfig.DomainBLSToExecutionChange[:], utils.Uint32ToBytes4(beaconConfig.GenesisForkVersion), s.GenesisValidatorsRoot())
		if err != nil {
			return err
		}
		signedRoot, err := fork.ComputeSigningRoot(change, domain)
		if err != nil {
			return err
		}
		valid, err := bls.Verify(signedChange.Signature[:], signedRoot[:], change.From[:])
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("invalid signature")
		}
	}
	credentials := wc
	// Reset the validator's withdrawal credentials.
	credentials[0] = beaconConfig.ETH1AddressWithdrawalPrefixByte
	copy(credentials[1:], make([]byte, 11))
	copy(credentials[12:], change.To[:])

	// Update the state with the modified validator.
	s.SetWithdrawalCredentialForValidatorAtIndex(int(change.ValidatorIndex), credentials)
	return nil
}

func (I *impl) VerifyKzgCommitmentsAgainstTransactions(transactions *solid.TransactionsSSZ, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) (bool, error) {
	if I.FullValidation {
		return true, nil
	}
	allVersionedHashes := []common.Hash{}
	transactions.ForEach(func(tx []byte, idx, total int) bool {
		if tx[0] != types.BlobTxType {
			return true
		}

		allVersionedHashes = append(allVersionedHashes, txPeekBlobVersionedHashes(tx)...)
		return true
	})

	commitmentVersionedHash := []common.Hash{}
	var err error
	var versionedHash common.Hash
	kzgCommitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
		versionedHash, err = kzgCommitmentToVersionedHash(value)
		if err != nil {
			return false
		}

		commitmentVersionedHash = append(commitmentVersionedHash, versionedHash)
		return true
	})
	if err != nil {
		return false, err
	}

	return reflect.DeepEqual(allVersionedHashes, commitmentVersionedHash), nil
}

func (I *impl) ProcessAttestations(s abstract.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error {
	attestingIndiciesSet := make([][]uint64, attestations.Len())
	h := metrics.NewHistTimer("beacon_process_attestations")
	baseRewardPerIncrement := s.BaseRewardPerIncrement()

	c := h.Tag("attestation_step", "process")
	var err error
	if err := solid.RangeErr[*solid.Attestation](attestations, func(i int, a *solid.Attestation, _ int) error {
		if attestingIndiciesSet[i], err = processAttestation(s, a, baseRewardPerIncrement); err != nil {
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
	c.PutSince()
	if I.FullValidation {
		c = h.Tag("attestation_step", "validate")
		valid, err = verifyAttestations(s, attestations, attestingIndiciesSet)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessAttestation: wrong bls data")
		}
		c.PutSince()
	}

	return nil
}

func processAttestationPostAltair(s abstract.BeaconState, attestation *solid.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.AttestantionData()
	currentEpoch := state.Epoch(s)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()

	h := metrics.NewHistTimer("beacon_process_attestation_post_altair")

	c := h.Tag("step", "get_participation_flag")
	participationFlagsIndicies, err := s.GetAttestationParticipationFlagIndicies(attestation.AttestantionData(), stateSlot-data.Slot())
	if err != nil {
		return nil, err
	}
	c.PutSince()

	c = h.Tag("step", "get_attesting_indices")

	attestingIndicies, err := s.GetAttestingIndicies(attestation.AttestantionData(), attestation.AggregationBits(), true)
	if err != nil {
		return nil, err
	}

	c.PutSince()

	var proposerRewardNumerator uint64

	isCurrentEpoch := data.Target().Epoch() == currentEpoch

	c = h.Tag("step", "update_attestation")
	for _, attesterIndex := range attestingIndicies {
		val, err := s.ValidatorEffectiveBalance(int(attesterIndex))
		if err != nil {
			return nil, err
		}

		baseReward := (val / beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range beaconConfig.ParticipationWeights() {
			flagParticipation := s.EpochParticipationForValidatorIndex(isCurrentEpoch, int(attesterIndex))
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) || flagParticipation.HasFlag(flagIndex) {
				continue
			}
			s.SetEpochParticipationForValidatorIndex(isCurrentEpoch, int(attesterIndex), flagParticipation.Add(flagIndex))
			proposerRewardNumerator += baseReward * weight
		}
	}
	c.PutSince()
	// Reward proposer
	c = h.Tag("step", "get_proposer_index")
	proposer, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	c.PutSince()
	proposerRewardDenominator := (beaconConfig.WeightDenominator - beaconConfig.ProposerWeight) * beaconConfig.WeightDenominator / beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return attestingIndicies, state.IncreaseBalance(s, proposer, reward)
}

// processAttestationsPhase0 implements the rules for phase0 processing.
func processAttestationPhase0(s abstract.BeaconState, attestation *solid.Attestation) ([]uint64, error) {
	data := attestation.AttestantionData()
	committee, err := s.GetBeaconCommitee(data.Slot(), data.ValidatorIndex())
	if err != nil {
		return nil, err
	}

	if len(committee) != utils.GetBitlistLength(attestation.AggregationBits()) {
		return nil, fmt.Errorf("processAttestationPhase0: mismatching aggregation bits size")
	}
	// Cached so it is performant.
	proposerIndex, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	// Create the attestation to add to pending attestations
	pendingAttestation := solid.NewPendingAttestionFromParameters(
		attestation.AggregationBits(),
		data,
		s.Slot()-data.Slot(),
		proposerIndex,
	)

	isCurrentAttestation := data.Target().Epoch() == state.Epoch(s)
	// Depending of what slot we are on we put in either the current justified or previous justified.
	if isCurrentAttestation {
		if !data.Source().Equal(s.CurrentJustifiedCheckpoint()) {
			return nil, fmt.Errorf("processAttestationPhase0: mismatching sources")
		}
		s.AddCurrentEpochAtteastation(pendingAttestation)
	} else {
		if !data.Source().Equal(s.PreviousJustifiedCheckpoint()) {
			return nil, fmt.Errorf("processAttestationPhase0: mismatching sources")
		}
		s.AddPreviousEpochAttestation(pendingAttestation)
	}
	// Not required by specs but needed if we want performant epoch transition.
	indicies, err := s.GetAttestingIndicies(attestation.AttestantionData(), attestation.AggregationBits(), true)
	if err != nil {
		return nil, err
	}
	epochRoot, err := state.GetBlockRoot(s, attestation.AttestantionData().Target().Epoch())
	if err != nil {
		return nil, err
	}
	slotRoot, err := s.GetBlockRootAtSlot(attestation.AttestantionData().Slot())
	if err != nil {
		return nil, err
	}
	// Basically we flag all validators we are currently attesting. will be important for rewards/finalization processing.
	for _, index := range indicies {
		minCurrentInclusionDelayAttestation, err := s.ValidatorMinCurrentInclusionDelayAttestation(int(index))
		if err != nil {
			return nil, err
		}

		minPreviousInclusionDelayAttestation, err := s.ValidatorMinPreviousInclusionDelayAttestation(int(index))
		if err != nil {
			return nil, err
		}
		// NOTE: does not affect state root.
		// We need to set it to currents or previouses depending on which attestation we process.
		if isCurrentAttestation {
			if minCurrentInclusionDelayAttestation == nil ||
				minCurrentInclusionDelayAttestation.InclusionDelay() > pendingAttestation.InclusionDelay() {
				if err := s.SetValidatorMinCurrentInclusionDelayAttestation(int(index), pendingAttestation); err != nil {
					return nil, err
				}
			}
			if err := s.SetValidatorIsCurrentMatchingSourceAttester(int(index), true); err != nil {
				return nil, err
			}
			if attestation.AttestantionData().Target().BlockRoot() == epochRoot {
				if err := s.SetValidatorIsCurrentMatchingTargetAttester(int(index), true); err != nil {
					return nil, err
				}
			} else {
				continue
			}
			if attestation.AttestantionData().BeaconBlockRoot() == slotRoot {
				if err := s.SetValidatorIsCurrentMatchingHeadAttester(int(index), true); err != nil {
					return nil, err
				}
			}
		} else {
			if minPreviousInclusionDelayAttestation == nil ||
				minPreviousInclusionDelayAttestation.InclusionDelay() > pendingAttestation.InclusionDelay() {
				if err := s.SetValidatorMinPreviousInclusionDelayAttestation(int(index), pendingAttestation); err != nil {
					return nil, err
				}
			}
			if err := s.SetValidatorIsPreviousMatchingSourceAttester(int(index), true); err != nil {
				return nil, err
			}
			if attestation.AttestantionData().Target().BlockRoot() != epochRoot {
				continue
			}
			if err := s.SetValidatorIsPreviousMatchingTargetAttester(int(index), true); err != nil {
				return nil, err
			}
			if attestation.AttestantionData().BeaconBlockRoot() == slotRoot {
				if err := s.SetValidatorIsPreviousMatchingHeadAttester(int(index), true); err != nil {
					return nil, err
				}
			}
		}
	}
	return indicies, nil
}

// ProcessAttestation takes an attestation and process it.
func processAttestation(s abstract.BeaconState, attestation *solid.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.AttestantionData()
	currentEpoch := state.Epoch(s)
	previousEpoch := state.PreviousEpoch(s)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()
	// Prelimary checks.
	if (data.Target().Epoch() != currentEpoch && data.Target().Epoch() != previousEpoch) || data.Target().Epoch() != state.GetEpochAtSlot(s.BeaconConfig(), data.Slot()) {
		return nil, errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if data.Slot()+beaconConfig.MinAttestationInclusionDelay > stateSlot || stateSlot > data.Slot()+beaconConfig.SlotsPerEpoch {
		return nil, errors.New("ProcessAttestation: attestation slot not in range")
	}
	if data.ValidatorIndex() >= s.CommitteeCount(data.Target().Epoch()) {
		return nil, errors.New("ProcessAttestation: attester index out of range")
	}
	// check if we need to use rules for phase0 or post-altair.
	if s.Version() == clparams.Phase0Version {
		return processAttestationPhase0(s, attestation)
	}
	return processAttestationPostAltair(s, attestation, baseRewardPerIncrement)
}

func verifyAttestations(s abstract.BeaconState, attestations *solid.ListSSZ[*solid.Attestation], attestingIndicies [][]uint64) (bool, error) {
	indexedAttestations := make([]*cltypes.IndexedAttestation, 0, attestations.Len())
	attestations.Range(func(idx int, a *solid.Attestation, _ int) bool {
		indexedAttestations = append(indexedAttestations, state.GetIndexedAttestation(a, attestingIndicies[idx]))
		return true
	})

	return batchVerifyAttestations(s, indexedAttestations)
}

type indexedAttestationVerificationResult struct {
	valid bool
	err   error
}

// Concurrent verification of BLS.
func batchVerifyAttestations(s abstract.BeaconState, indexedAttestations []*cltypes.IndexedAttestation) (valid bool, err error) {
	c := make(chan indexedAttestationVerificationResult, 1)

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
			return false, err
		}
		if !result.valid {
			return false, nil
		}
	}
	return true, nil
}

func (I *impl) ProcessBlockHeader(s abstract.BeaconState, block *cltypes.BeaconBlock) error {
	if I.FullValidation {
		if block.Slot != s.Slot() {
			return fmt.Errorf("state slot: %d, not equal to block slot: %d", s.Slot(), block.Slot)
		}
		if block.Slot <= s.LatestBlockHeader().Slot {
			return fmt.Errorf("slock slot: %d, not greater than latest block slot: %d", block.Slot, s.LatestBlockHeader().Slot)
		}
		propInd, err := s.GetBeaconProposerIndex()
		if err != nil {
			return fmt.Errorf("error in GetBeaconProposerIndex: %v", err)
		}
		if block.ProposerIndex != propInd {
			return fmt.Errorf("block proposer index: %d, does not match beacon proposer index: %d", block.ProposerIndex, propInd)
		}
		blockHeader := s.LatestBlockHeader()
		latestRoot, err := (&blockHeader).HashSSZ()
		if err != nil {
			return fmt.Errorf("unable to hash tree root of latest block header: %v", err)
		}
		if block.ParentRoot != latestRoot {
			return fmt.Errorf("block parent root: %x, does not match latest block root: %x", block.ParentRoot, latestRoot)
		}
	}

	bodyRoot, err := block.Body.HashSSZ()
	if err != nil {
		return fmt.Errorf("unable to hash tree root of block body: %v", err)
	}
	s.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		BodyRoot:      bodyRoot,
	})

	proposer, err := s.ValidatorForValidatorIndex(int(block.ProposerIndex))
	if err != nil {
		return err
	}
	if proposer.Slashed() {
		return fmt.Errorf("proposer: %d is slashed", block.ProposerIndex)
	}
	return nil
}

func (I *impl) ProcessRandao(s abstract.BeaconState, randao [96]byte, proposerIndex uint64) error {
	epoch := state.Epoch(s)
	proposer, err := s.ValidatorForValidatorIndex(int(proposerIndex))
	if err != nil {
		return err
	}
	if I.FullValidation {
		domain, err := s.GetDomain(s.BeaconConfig().DomainRandao, epoch)
		if err != nil {
			return fmt.Errorf("ProcessRandao: unable to get domain: %v", err)
		}
		signingRoot, err := computeSigningRootEpoch(epoch, domain)
		if err != nil {
			return fmt.Errorf("ProcessRandao: unable to compute signing root: %v", err)
		}
		pk := proposer.PublicKey()
		valid, err := bls.Verify(randao[:], signingRoot[:], pk[:])
		if err != nil {
			return fmt.Errorf("ProcessRandao: unable to verify public key: %x, with signing root: %x, and signature: %x, %v", pk[:], signingRoot[:], randao[:], err)
		}
		if !valid {
			return fmt.Errorf("ProcessRandao: invalid signature: public key: %x, signing root: %x, signature: %x", pk[:], signingRoot[:], randao[:])
		}
	}

	randaoMixes := s.GetRandaoMixes(epoch)
	randaoHash := utils.Keccak256(randao[:])
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
		return fmt.Errorf("new slot: %d not greater than s slot: %d", slot, sSlot)
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
			log.Debug("Processed new epoch successfully", "epoch", state.Epoch(s), "process_epoch_elpsed", time.Since(start))
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
	}
	return nil
}
