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

package abstract

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

//go:generate mockgen -typed=true -destination=./mock_services/beacon_state_mock.go -package=mock_services . BeaconState
type BeaconState interface {
	BeaconStateBasic
	BeaconStateExtension
	BeaconStateUpgradable
}

type BeaconStateUpgradable interface {
	UpgradeToAltair() error
	UpgradeToBellatrix() error
	UpgradeToCapella() error
	UpgradeToDeneb() error
	UpgradeToElectra() error
	UpgradeToFulu() error
}

type BeaconStateExtension interface {
	SlashValidator(slashedInd uint64, whistleblowerInd *uint64) (uint64, error)
	InitiateValidatorExit(index uint64) error
	GetActiveValidatorsIndices(epoch uint64) (indicies []uint64)
	GetTotalActiveBalance() uint64
	ComputeCommittee(indicies []uint64, slot uint64, index, count uint64) ([]uint64, error)
	GetBeaconProposerIndex() (uint64, error)
	GetBeaconProposerIndices(epoch uint64) ([]uint64, error)
	BaseRewardPerIncrement() uint64
	BaseReward(index uint64) (uint64, error)
	SyncRewards() (proposerReward, participantReward uint64, err error)
	CommitteeCount(epoch uint64) uint64
	GetAttestationParticipationFlagIndicies(data *solid.AttestationData, inclusionDelay uint64, skipAssert bool) ([]uint8, error)
	GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error)
	ComputeNextSyncCommittee() (*solid.SyncCommittee, error)
	GetAttestingIndicies(attestation *solid.Attestation, checkBitsLength bool) ([]uint64, error)
	GetValidatorChurnLimit() uint64
	ValidatorIndexByPubkey(key [48]byte) (uint64, bool)
	PreviousStateRoot() common.Hash
	SetPreviousStateRoot(root common.Hash)
	GetValidatorActivationChurnLimit() uint64
	GetPendingPartialWithdrawals() *solid.ListSSZ[*solid.PendingPartialWithdrawal]
	GetDepositBalanceToConsume() uint64
	GetPendingDeposits() *solid.ListSSZ[*solid.PendingDeposit]
	GetDepositRequestsStartIndex() uint64
	GetPendingConsolidations() *solid.ListSSZ[*solid.PendingConsolidation]
	GetEarlistConsolidationEpoch() uint64
	ComputeExitEpochAndUpdateChurn(exitBalance uint64) uint64
	GetConsolidationBalanceToConsume() uint64
	GetProposerLookahead() solid.Uint64VectorSSZ
}

type BeaconStateBasic interface {
	BeaconStateMinimal
	BeaconStateExtra
	BeaconStateMutator
	BeaconStateSSZ

	Clone() clonable.Clonable
	DebugPrint(prefix string)
}

type BeaconStateSSZ interface {
	BlockRoot() ([32]byte, error)
	EncodeSSZ(buf []byte) ([]byte, error)
	DecodeSSZ(buf []byte, version int) error
	EncodingSizeSSZ() (size int)
	HashSSZ() (out [32]byte, err error)
}

//go:generate mockgen -typed=true -destination=./mock_services/beacon_state_mutator_mock.go -package=mock_services . BeaconStateMutator
type BeaconStateMutator interface {
	SetVersion(version clparams.StateVersion)
	SetSlot(slot uint64)
	SetFork(fork *cltypes.Fork)
	SetLatestBlockHeader(header *cltypes.BeaconBlockHeader)
	SetBlockRootAt(index int, root common.Hash)
	SetStateRootAt(index int, root common.Hash)
	SetWithdrawalCredentialForValidatorAtIndex(index int, creds common.Hash)
	SetExitEpochForValidatorAtIndex(index int, epoch uint64)
	SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) error
	SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64)
	SetActivationEpochForValidatorAtIndex(index int, epoch uint64)
	SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64)
	SetEth1Data(eth1Data *cltypes.Eth1Data)
	SetEth1DepositIndex(eth1DepositIndex uint64)
	SetValidatorSlashed(index int, slashed bool) error
	SetValidatorMinCurrentInclusionDelayAttestation(index int, value *solid.PendingAttestation) error
	SetValidatorIsCurrentMatchingSourceAttester(index int, value bool) error
	SetValidatorIsCurrentMatchingTargetAttester(index int, value bool) error
	SetValidatorIsCurrentMatchingHeadAttester(index int, value bool) error
	SetValidatorMinPreviousInclusionDelayAttestation(index int, value *solid.PendingAttestation) error
	SetValidatorIsPreviousMatchingSourceAttester(index int, value bool) error
	SetValidatorIsPreviousMatchingTargetAttester(index int, value bool) error
	SetValidatorIsPreviousMatchingHeadAttester(index int, value bool) error
	SetValidatorBalance(index int, balance uint64) error
	SetRandaoMixAt(index int, mix common.Hash)
	SetSlashingSegmentAt(index int, segment uint64)
	SetEpochParticipationForValidatorIndex(isCurrentEpoch bool, index int, flags cltypes.ParticipationFlags)
	SetValidatorAtIndex(index int, validator solid.Validator)

	SetJustificationBits(justificationBits cltypes.JustificationBits)
	SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint solid.Checkpoint)
	SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint solid.Checkpoint)
	SetFinalizedCheckpoint(finalizedCheckpoint solid.Checkpoint)
	SetCurrentSyncCommittee(currentSyncCommittee *solid.SyncCommittee)
	SetNextSyncCommittee(nextSyncCommittee *solid.SyncCommittee)
	SetLatestExecutionPayloadHeader(header *cltypes.Eth1Header)
	SetNextWithdrawalIndex(index uint64)
	SetNextWithdrawalValidatorIndex(index uint64)
	SetInactivityScores(scores []uint64)
	SetValidatorInactivityScore(index int, score uint64) error
	SetCurrentEpochParticipationFlags(flags []cltypes.ParticipationFlags)
	SetPreviousEpochParticipationFlags(flags []cltypes.ParticipationFlags)
	SetPreviousEpochAttestations(attestations *solid.ListSSZ[*solid.PendingAttestation])
	SetPendingPartialWithdrawals(*solid.ListSSZ[*solid.PendingPartialWithdrawal])
	SetPendingDeposits(*solid.ListSSZ[*solid.PendingDeposit])
	SetDepositBalanceToConsume(uint64)
	SetPendingConsolidations(consolidations *solid.ListSSZ[*solid.PendingConsolidation])
	SetDepositRequestsStartIndex(uint64)
	SetConsolidationBalanceToConsume(uint64)
	SetEarlistConsolidationEpoch(uint64)
	SetProposerLookahead(proposerLookahead solid.Uint64VectorSSZ)

	AddEth1DataVote(vote *cltypes.Eth1Data)
	AddValidator(validator solid.Validator, balance uint64)
	AddHistoricalSummary(summary *cltypes.HistoricalSummary)
	AddHistoricalRoot(root common.Hash)
	AddInactivityScore(score uint64)
	AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags)
	AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags)
	AddPreviousEpochParticipationAt(index int, delta byte)
	AddCurrentEpochAtteastation(attestation *solid.PendingAttestation)
	AddPreviousEpochAttestation(attestation *solid.PendingAttestation)

	AppendValidator(in solid.Validator)
	AppendPendingDeposit(deposit *solid.PendingDeposit)
	AppendPendingPartialWithdrawal(withdrawal *solid.PendingPartialWithdrawal)
	AppendPendingConsolidation(consolidation *solid.PendingConsolidation)

	ResetEth1DataVotes()
	ResetEpochParticipation()
	ResetHistoricalSummaries()
	ResetCurrentEpochAttestations()
	ResetPreviousEpochAttestations()
}

type BeaconStateExtra interface {
	ValidatorLength() int
	ValidatorBalance(index int) (uint64, error)
	RandaoMixes() solid.HashVectorSSZ
	ForEachBalance(fn func(v uint64, idx int, total int) bool)
	ValidatorExitEpoch(index int) (uint64, error)
	ValidatorWithdrawableEpoch(index int) (uint64, error)
	ValidatorEffectiveBalance(index int) (uint64, error)
	ValidatorMinCurrentInclusionDelayAttestation(index int) (*solid.PendingAttestation, error)
	ValidatorMinPreviousInclusionDelayAttestation(index int) (*solid.PendingAttestation, error)
	ValidatorIsCurrentMatchingSourceAttester(idx int) (bool, error)
	ValidatorIsCurrentMatchingTargetAttester(idx int) (bool, error)
	ValidatorIsCurrentMatchingHeadAttester(idx int) (bool, error)
	ValidatorIsPreviousMatchingSourceAttester(idx int) (bool, error)
	ValidatorIsPreviousMatchingTargetAttester(idx int) (bool, error)
	ValidatorIsPreviousMatchingHeadAttester(idx int) (bool, error)
	GetRandaoMixes(epoch uint64) [32]byte
	GetRandaoMix(index int) [32]byte
	EpochParticipationForValidatorIndex(isCurrentEpoch bool, index int) cltypes.ParticipationFlags
	GetBlockRootAtSlot(slot uint64) (common.Hash, error)
	GetDomain(domainType [4]byte, epoch uint64) ([]byte, error)
}

type BeaconStateMinimal interface {
	BeaconConfig() *clparams.BeaconChainConfig
	Version() clparams.StateVersion
	GenesisTime() uint64
	GenesisValidatorsRoot() common.Hash
	Slot() uint64
	PreviousSlot() uint64
	Fork() *cltypes.Fork
	LatestBlockHeader() cltypes.BeaconBlockHeader
	BlockRoots() solid.HashVectorSSZ
	StateRoots() solid.HashVectorSSZ
	Eth1Data() *cltypes.Eth1Data
	Eth1DataVotes() *solid.ListSSZ[*cltypes.Eth1Data]
	Eth1DepositIndex() uint64
	ValidatorSet() *solid.ValidatorSet
	PreviousEpochParticipation() *solid.ParticipationBitList

	ForEachValidator(fn func(v solid.Validator, idx int, total int) bool)
	ValidatorForValidatorIndex(index int) (solid.Validator, error)

	ForEachSlashingSegment(fn func(idx int, v uint64, total int) bool)
	SlashingSegmentAt(pos int) uint64

	EpochParticipation(currentEpoch bool) *solid.ParticipationBitList
	JustificationBits() cltypes.JustificationBits

	PreviousJustifiedCheckpoint() solid.Checkpoint
	CurrentJustifiedCheckpoint() solid.Checkpoint
	FinalizedCheckpoint() solid.Checkpoint
	ValidatorInactivityScore(index int) (uint64, error)
	CurrentSyncCommittee() *solid.SyncCommittee
	NextSyncCommittee() *solid.SyncCommittee
	LatestExecutionPayloadHeader() *cltypes.Eth1Header
	NextWithdrawalIndex() uint64
	NextWithdrawalValidatorIndex() uint64
	// HistoricalSummary has no accessor yet.

	CurrentEpochAttestations() *solid.ListSSZ[*solid.PendingAttestation]
	CurrentEpochAttestationsLength() int
	PreviousEpochAttestations() *solid.ListSSZ[*solid.PendingAttestation]
	PreviousEpochAttestationsLength() int
}

// BeaconStateReader is an interface for reading the beacon state.
//
//go:generate mockgen -typed=true -destination=./mock_services/beacon_state_reader_mock.go -package=mock_services . BeaconStateReader
type BeaconStateReader interface {
	ValidatorPublicKey(index int) (common.Bytes48, error)
	GetDomain(domainType [4]byte, epoch uint64) ([]byte, error)
	CommitteeCount(epoch uint64) uint64
	ValidatorForValidatorIndex(index int) (solid.Validator, error)
	Version() clparams.StateVersion
	GenesisValidatorsRoot() common.Hash
	GetBeaconProposerIndexForSlot(slot uint64) (uint64, error)
}
