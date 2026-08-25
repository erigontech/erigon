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

package raw

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func (b *BeaconState) SetVersion(version clparams.StateVersion) {
	if (b.version < clparams.GloasVersion) != (version < clparams.GloasVersion) {
		b.validators.SetProgressiveHashing(version >= clparams.GloasVersion)
		b.markLeaf(
			ValidatorsLeafIndex,
			BalancesLeafIndex,
			PreviousEpochParticipationLeafIndex,
			CurrentEpochParticipationLeafIndex,
			InactivityScoresLeafIndex,
			LatestBlockHashLeafIndex,
			PendingDepositsLeafIndex,
			PendingPartialWithdrawalsLeafIndex,
			PendingConsolidationsLeafIndex,
		)
	}
	b.version = version
}

func (b *BeaconState) SetSlot(slot uint64) error {
	oldSlot := b.slot
	b.slot = slot
	if b.events.OnEpochBoundary != nil && b.slot%b.beaconConfig.SlotsPerEpoch == 0 {
		if err := b.events.OnEpochBoundary(b.slot / b.beaconConfig.SlotsPerEpoch); err != nil {
			b.slot = oldSlot
			return err
		}
	}

	b.markLeaf(SlotLeafIndex)
	return nil
}

func (b *BeaconState) SetFork(fork *cltypes.Fork) {
	b.fork = fork
	b.markLeaf(ForkLeafIndex)
}

func (b *BeaconState) SetLatestBlockHeader(header *cltypes.BeaconBlockHeader) {
	b.latestBlockHeader = header
	b.markLeaf(LatestBlockHeaderLeafIndex)
}

func (b *BeaconState) SetBlockRootAt(index int, root common.Hash) error {
	if b.events.OnNewBlockRoot != nil {
		if err := b.events.OnNewBlockRoot(index, root); err != nil {
			return err
		}
	}
	b.markLeaf(BlockRootsLeafIndex)
	b.blockRoots.Set(index, root)
	return nil
}

func (b *BeaconState) SetStateRootAt(index int, root common.Hash) error {
	if b.events.OnNewStateRoot != nil {
		if err := b.events.OnNewStateRoot(index, root); err != nil {
			return err
		}
	}
	b.markLeaf(StateRootsLeafIndex)
	b.stateRoots.Set(index, root)
	return nil
}

func (b *BeaconState) SetWithdrawalCredentialForValidatorAtIndex(index int, creds common.Hash) error {
	if b.events.OnNewValidatorWithdrawalCredentials != nil {
		if err := b.events.OnNewValidatorWithdrawalCredentials(index, creds[:]); err != nil {
			return err
		}
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetWithdrawalCredentialForValidatorAtIndex(index, creds)
	return nil
}

func (b *BeaconState) SetExitEpochForValidatorAtIndex(index int, epoch uint64) error {
	if b.events.OnNewValidatorExitEpoch != nil {
		if err := b.events.OnNewValidatorExitEpoch(index, epoch); err != nil {
			return err
		}
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetExitEpochForValidatorAtIndex(index, epoch)
	return nil
}

func (b *BeaconState) SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	if b.events.OnNewValidatorWithdrawableEpoch != nil {
		if err := b.events.OnNewValidatorWithdrawableEpoch(index, epoch); err != nil {
			return err
		}
	}

	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetWithdrawableEpochForValidatorAtIndex(index, epoch)
	return nil
}

func (b *BeaconState) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) error {
	if b.events.OnNewValidatorEffectiveBalance != nil {
		if err := b.events.OnNewValidatorEffectiveBalance(index, balance); err != nil {
			return err
		}
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetEffectiveBalanceForValidatorAtIndex(index, balance)
	return nil
}

func (b *BeaconState) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) error {
	if b.events.OnNewValidatorActivationEpoch != nil {
		if err := b.events.OnNewValidatorActivationEpoch(index, epoch); err != nil {
			return err
		}
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetActivationEpochForValidatorAtIndex(index, epoch)
	return nil
}

func (b *BeaconState) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) error {
	if b.events.OnNewValidatorActivationEligibilityEpoch != nil {
		if err := b.events.OnNewValidatorActivationEligibilityEpoch(index, epoch); err != nil {
			return err
		}
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetActivationEligibilityEpochForValidatorAtIndex(index, epoch)
	return nil
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.markLeaf(Eth1DataLeafIndex)
	b.eth1Data = eth1Data
}

func (b *BeaconState) SetEth1DataVotes(votes *solid.ListSSZ[*cltypes.Eth1Data]) {
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes = votes
}

func (b *BeaconState) AddEth1DataVote(vote *cltypes.Eth1Data) error {
	if b.events.OnAppendEth1Data != nil {
		if err := b.events.OnAppendEth1Data(vote); err != nil {
			return err
		}
	}
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes.Append(vote)
	return nil
}

func (b *BeaconState) ResetEth1DataVotes() {
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes.Clear()
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.markLeaf(Eth1DepositIndexLeafIndex)
	b.eth1DepositIndex = eth1DepositIndex
}

func (b *BeaconState) SetValidators(validators *solid.ValidatorSet) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators = validators
}

func (b *BeaconState) SetRandaoMixes(mixes solid.HashVectorSSZ) {
	b.markLeaf(RandaoMixesLeafIndex)
	b.randaoMixes = mixes
}

func (b *BeaconState) SetHistoricalRoots(hRoots solid.HashListSSZ) {
	b.markLeaf(HistoricalRootsLeafIndex)
	b.historicalRoots = hRoots
}

func (b *BeaconState) SetValidatorSlashed(index int, slashed bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	if b.events.OnNewValidatorSlashed != nil {
		if err := b.events.OnNewValidatorSlashed(index, slashed); err != nil {
			return err
		}
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetValidatorSlashed(index, slashed)
	return nil
}

func (b *BeaconState) SetValidatorMinCurrentInclusionDelayAttestation(index int, value *solid.PendingAttestation) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetMinCurrentInclusionDelayAttestation(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsCurrentMatchingSourceAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsCurrentMatchingSourceAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsCurrentMatchingTargetAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsCurrentMatchingTargetAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsCurrentMatchingHeadAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsCurrentMatchingHeadAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorMinPreviousInclusionDelayAttestation(index int, value *solid.PendingAttestation) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetMinPreviousInclusionDelayAttestation(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsPreviousMatchingSourceAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsPreviousMatchingSourceAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsPreviousMatchingTargetAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetIsPreviousMatchingTargetAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsPreviousMatchingHeadAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)

	b.validators.SetIsPreviousMatchingHeadAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorBalance(index int, balance uint64) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	if b.events.OnNewValidatorBalance != nil {
		if err := b.events.OnNewValidatorBalance(index, balance); err != nil {
			return err
		}
	}
	b.markLeaf(BalancesLeafIndex)
	b.balances.Set(index, balance)
	return nil
}

func (b *BeaconState) AddValidator(validator solid.Validator, balance uint64) error {
	if b.events.OnNewValidator != nil {
		if err := b.events.OnNewValidator(b.validators.Length(), validator, balance); err != nil {
			return err
		}
	}
	b.validators.Append(validator)
	b.balances.Append(balance)

	b.markLeaf(ValidatorsLeafIndex)
	b.markLeaf(BalancesLeafIndex)
	return nil
}

func (b *BeaconState) SetRandaoMixAt(index int, mix common.Hash) error {
	if b.events.OnRandaoMixChange != nil {
		if err := b.events.OnRandaoMixChange(index, mix); err != nil {
			return err
		}
	}
	b.markLeaf(RandaoMixesLeafIndex)
	b.randaoMixes.Set(index, mix)
	return nil
}

func (b *BeaconState) SetSlashingSegmentAt(index int, segment uint64) error {
	if b.events.OnNewSlashingSegment != nil {
		if err := b.events.OnNewSlashingSegment(index, segment); err != nil {
			return err
		}
	}
	b.markLeaf(SlashingsLeafIndex)
	b.slashings.Set(index, segment)
	return nil
}

func (b *BeaconState) SetEpochParticipationForValidatorIndex(isCurrentEpoch bool, index int, flags cltypes.ParticipationFlags) {
	if isCurrentEpoch {
		b.markLeaf(CurrentEpochParticipationLeafIndex)
		b.currentEpochParticipation.Set(index, byte(flags))
		return
	}
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation.Set(index, byte(flags))
}

func (b *BeaconState) SetValidatorAtIndex(index int, validator solid.Validator) {
	b.validators.Set(index, validator)
	b.markLeaf(ValidatorsLeafIndex)
}

func (b *BeaconState) ResetEpochParticipation() error {
	if b.events.OnResetParticipation != nil {
		if err := b.events.OnResetParticipation(b.currentEpochParticipation); err != nil {
			return err
		}
	}
	b.previousEpochParticipation = b.currentEpochParticipation
	b.currentEpochParticipation = solid.NewParticipationBitList(b.validators.Length(), int(b.beaconConfig.ValidatorRegistryLimit))
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	return nil
}

func (b *BeaconState) SetJustificationBits(justificationBits cltypes.JustificationBits) {
	b.justificationBits = justificationBits
	b.markLeaf(JustificationBitsLeafIndex)
}

func (b *BeaconState) SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint solid.Checkpoint) {
	b.previousJustifiedCheckpoint = previousJustifiedCheckpoint
	b.markLeaf(PreviousJustifiedCheckpointLeafIndex)
}

func (b *BeaconState) SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint solid.Checkpoint) {
	b.currentJustifiedCheckpoint = currentJustifiedCheckpoint
	b.markLeaf(CurrentJustifiedCheckpointLeafIndex)
}

func (b *BeaconState) SetFinalizedCheckpoint(finalizedCheckpoint solid.Checkpoint) {
	b.finalizedCheckpoint = finalizedCheckpoint
	b.markLeaf(FinalizedCheckpointLeafIndex)
}

func (b *BeaconState) SetCurrentSyncCommittee(currentSyncCommittee *solid.SyncCommittee) error {
	if b.events.OnNewCurrentSyncCommittee != nil {
		if err := b.events.OnNewCurrentSyncCommittee(currentSyncCommittee); err != nil {
			return err
		}
	}
	b.currentSyncCommittee = currentSyncCommittee
	b.markLeaf(CurrentSyncCommitteeLeafIndex)
	return nil
}

func (b *BeaconState) SetNextSyncCommittee(nextSyncCommittee *solid.SyncCommittee) error {
	if b.events.OnNewNextSyncCommittee != nil {
		if err := b.events.OnNewNextSyncCommittee(nextSyncCommittee); err != nil {
			return err
		}
	}
	b.nextSyncCommittee = nextSyncCommittee
	b.markLeaf(NextSyncCommitteeLeafIndex)
	return nil
}

func (b *BeaconState) SetLatestExecutionPayloadHeader(header *cltypes.Eth1Header) {
	if b.version >= clparams.GloasVersion {
		return
	}
	b.latestExecutionPayloadHeader = header
	b.markLeaf(LatestExecutionPayloadHeaderLeafIndex)
}

func (b *BeaconState) SetLatestExecutionPayloadBid(bid *cltypes.ExecutionPayloadBid) {
	b.latestExecutionPayloadBid = bid
	b.markLeaf(LatestExecutionPayloadBidLeafIndex)
}

func (b *BeaconState) SetNextWithdrawalIndex(index uint64) {
	b.nextWithdrawalIndex = index
	b.markLeaf(NextWithdrawalIndexLeafIndex)
}

func (b *BeaconState) SetNextWithdrawalValidatorIndex(index uint64) {
	b.nextWithdrawalValidatorIndex = index
	b.markLeaf(NextWithdrawalValidatorIndexLeafIndex)
}

func (b *BeaconState) ResetHistoricalSummaries() {
	b.historicalSummaries.Clear()
	b.markLeaf(HistoricalSummariesLeafIndex)
}

func (b *BeaconState) SetHistoricalSummaries(l *solid.ListSSZ[*cltypes.HistoricalSummary]) {
	b.historicalSummaries = l
	b.markLeaf(HistoricalSummariesLeafIndex)
}

func (b *BeaconState) AddHistoricalSummary(summary *cltypes.HistoricalSummary) {
	b.historicalSummaries.Append(summary)
	b.markLeaf(HistoricalSummariesLeafIndex)
}

func (b *BeaconState) AddHistoricalRoot(root common.Hash) {
	b.historicalRoots.Append(root)
	b.markLeaf(HistoricalRootsLeafIndex)
}

func (b *BeaconState) SetInactivityScores(scores []uint64) {
	b.inactivityScores.Clear()
	for _, v := range scores {
		b.inactivityScores.Append(v)
	}
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) SetInactivityScoresRaw(scores solid.Uint64VectorSSZ) {
	b.inactivityScores = scores
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) AddInactivityScore(score uint64) {
	b.inactivityScores.Append(score)
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) SetValidatorInactivityScore(index int, score uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if index >= b.inactivityScores.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(InactivityScoresLeafIndex)
	b.inactivityScores.Set(index, score)
	return nil
}

func (b *BeaconState) SetCurrentEpochParticipationFlags(flags []cltypes.ParticipationFlags) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation.Clear()
	for _, v := range flags {
		b.currentEpochParticipation.Append(byte(v))
	}
}

func (b *BeaconState) SetPreviousEpochParticipationFlags(flags []cltypes.ParticipationFlags) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation.Clear()
	for _, v := range flags {
		b.previousEpochParticipation.Append(byte(v))
	}
}

func (b *BeaconState) AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation.Append(byte(flags))
}

func (b *BeaconState) AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation.Append(byte(flags))
}

func (b *BeaconState) AddPreviousEpochParticipationAt(index int, delta byte) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	tmp := cltypes.ParticipationFlags(b.previousEpochParticipation.Get(index)).Add(int(delta))
	b.previousEpochParticipation.Set(index, byte(tmp))
}

// phase0 fields
func (b *BeaconState) AddCurrentEpochAtteastation(attestation *solid.PendingAttestation) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations.Append(attestation)
}

func (b *BeaconState) AddPreviousEpochAttestation(attestation *solid.PendingAttestation) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations.Append(attestation)
}

func (b *BeaconState) ResetCurrentEpochAttestations() {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.beaconConfig.PreviousEpochAttestationsLength()))
}

func (b *BeaconState) SetCurrentEpochAttestations(attestations *solid.ListSSZ[*solid.PendingAttestation]) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations = attestations
}

func (b *BeaconState) SetPreviousEpochAttestations(attestations *solid.ListSSZ[*solid.PendingAttestation]) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = attestations
}

func (b *BeaconState) SetCurrentEpochParticipation(participation *solid.ParticipationBitList) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation = participation
}

func (b *BeaconState) SetPreviousEpochParticipation(participation *solid.ParticipationBitList) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation = participation
}

func (b *BeaconState) ResetPreviousEpochAttestations() {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.beaconConfig.PreviousEpochAttestationsLength()))
}

// SetGenesisTime sets the genesis time of the BeaconState.
func (b *BeaconState) SetGenesisTime(time uint64) {
	b.markLeaf(GenesisTimeLeafIndex)
	b.genesisTime = time
}

// SetGenesisValidatorsRoot sets the genesis validators root of the BeaconState.
func (b *BeaconState) SetGenesisValidatorsRoot(root common.Hash) {
	b.markLeaf(GenesisValidatorsRootLeafIndex)
	b.genesisValidatorsRoot = root
}

// SetBlockRoots sets the block roots of the BeaconState.
func (b *BeaconState) SetBlockRoots(roots solid.HashVectorSSZ) {
	b.markLeaf(BlockRootsLeafIndex)
	b.blockRoots = roots
}

// SetStateRoots sets the state roots of the BeaconState.
func (b *BeaconState) SetStateRoots(roots solid.HashVectorSSZ) {
	b.markLeaf(StateRootsLeafIndex)
	b.stateRoots = roots
}

// SetBlockRoots sets the block roots of the BeaconState.
func (b *BeaconState) SetBalances(balances solid.Uint64VectorSSZ) {
	b.markLeaf(BalancesLeafIndex)
	b.balances = balances
}

func (b *BeaconState) SetSlashings(slashings solid.Uint64VectorSSZ) {
	b.markLeaf(SlashingsLeafIndex)
	b.slashings = slashings
}

func (b *BeaconState) SetEarliestExitEpoch(epoch uint64) {
	b.earliestExitEpoch = epoch
	b.markLeaf(EarliestExitEpochLeafIndex)
}

func (b *BeaconState) SetExitBalanceToConsume(balance uint64) {
	b.exitBalanceToConsume = balance
	b.markLeaf(ExitBalanceToConsumeLeafIndex)
}

func (b *BeaconState) SetPendingPartialWithdrawals(pendingWithdrawals *solid.ListSSZ[*solid.PendingPartialWithdrawal]) {
	b.pendingPartialWithdrawals = pendingWithdrawals
	b.markLeaf(PendingPartialWithdrawalsLeafIndex)
}

func (b *BeaconState) AppendPendingDeposit(deposit *solid.PendingDeposit) {
	b.pendingDeposits.Append(deposit)
	b.markLeaf(PendingDepositsLeafIndex)
}

func (b *BeaconState) AppendPendingPartialWithdrawal(withdrawal *solid.PendingPartialWithdrawal) {
	b.pendingPartialWithdrawals.Append(withdrawal)
	b.markLeaf(PendingPartialWithdrawalsLeafIndex)
}

func (b *BeaconState) AppendPendingConsolidation(consolidation *solid.PendingConsolidation) {
	b.pendingConsolidations.Append(consolidation)
	b.markLeaf(PendingConsolidationsLeafIndex)
}

func (b *BeaconState) SetPendingDeposits(deposits *solid.ListSSZ[*solid.PendingDeposit]) {
	b.pendingDeposits = deposits
	b.markLeaf(PendingDepositsLeafIndex)
}

func (b *BeaconState) SetPendingConsolidations(consolidations *solid.ListSSZ[*solid.PendingConsolidation]) {
	b.pendingConsolidations = consolidations
	b.markLeaf(PendingConsolidationsLeafIndex)
}

func (b *BeaconState) SetDepositBalanceToConsume(balance uint64) {
	b.depositBalanceToConsume = balance
	b.markLeaf(DepositBalanceToConsumeLeafIndex)
}

func (b *BeaconState) SetDepositRequestsStartIndex(index uint64) {
	b.depositRequestsStartIndex = index
	b.markLeaf(DepositRequestsStartIndexLeafIndex)
}

func (b *BeaconState) SetConsolidationBalanceToConsume(balance uint64) {
	b.consolidationBalanceToConsume = balance
	b.markLeaf(ConsolidationBalanceToConsumeLeafIndex)
}

func (b *BeaconState) SetEarlistConsolidationEpoch(epoch uint64) {
	b.earliestConsolidationEpoch = epoch
	b.markLeaf(EarliestConsolidationEpochLeafIndex)
}

func (b *BeaconState) SetProposerLookahead(proposerLookahead solid.Uint64VectorSSZ) {
	b.proposerLookahead = proposerLookahead
	b.markLeaf(ProposerLookaheadLeafIndex)
}

func (b *BeaconState) SetExecutionPayloadAvailability(slot uint64, available bool) {
	// slot % SlotsPerHistoricalRoot is always within the bitvector's cap
	// (it was allocated with that same size), so SetBitAt cannot error here.
	_ = b.executionPayloadAvailability.SetBitAt(int(slot%b.beaconConfig.SlotsPerHistoricalRoot), available)
	b.markLeaf(ExecutionPayloadAvailabilityLeafIndex)
}

// SetExecutionPayloadAvailabilityRaw replaces the entire bitvector (used by the historical state reader).
func (b *BeaconState) SetExecutionPayloadAvailabilityRaw(bv *solid.BitVector) {
	b.executionPayloadAvailability = bv
	b.markLeaf(ExecutionPayloadAvailabilityLeafIndex)
}

func (b *BeaconState) SetBuilderPendingPayments(payments *solid.VectorSSZ[*cltypes.BuilderPendingPayment]) {
	b.builderPendingPayments = payments
	b.markLeaf(BuilderPendingPaymentsLeafIndex)
}

func (b *BeaconState) SetBuilderPendingWithdrawals(withdrawals *solid.ListSSZ[*cltypes.BuilderPendingWithdrawal]) {
	b.builderPendingWithdrawals = withdrawals
	b.markLeaf(BuilderPendingWithdrawalsLeafIndex)
}

func (b *BeaconState) SetLatestBlockHash(hash common.Hash) {
	b.latestBlockHash = hash
	b.markLeaf(LatestBlockHashLeafIndex)
}

func (b *BeaconState) SetPayloadExpectedWithdrawals(withdrawals *solid.ListSSZ[*cltypes.Withdrawal]) {
	b.payloadExpectedWithdrawals = withdrawals
	b.markLeaf(PayloadExpectedWithdrawalsLeafIndex)
}

func (b *BeaconState) SetPtcWindow(ptcWindow *solid.VectorSSZ[solid.Uint64VectorSSZ]) {
	b.ptcWindow = ptcWindow
	b.markLeaf(PtcWindowLeafIndex)
}

func (b *BeaconState) SetNextWithdrawalBuilderIndex(index uint64) {
	b.nextWithdrawalBuilderIndex = index
	b.markLeaf(NextWithdrawalBuilderIndexLeafIndex)
}

func (b *BeaconState) SetBuilders(builders *solid.ListSSZ[*cltypes.Builder]) {
	b.builders = builders
	b.markLeaf(BuildersLeafIndex)
}
