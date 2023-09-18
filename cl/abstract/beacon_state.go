package abstract

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

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
}

type BeaconStateExtension interface {
	SlashValidator(slashedInd uint64, whistleblowerInd *uint64) error
	InitiateValidatorExit(index uint64) error
	GetActiveValidatorsIndices(epoch uint64) (indicies []uint64)
	GetTotalActiveBalance() uint64
	ComputeCommittee(indicies []uint64, slot uint64, index, count uint64) ([]uint64, error)
	GetBeaconProposerIndex() (uint64, error)
	BaseRewardPerIncrement() uint64
	BaseReward(index uint64) (uint64, error)
	SyncRewards() (proposerReward, participantReward uint64, err error)
	CommitteeCount(epoch uint64) uint64
	GetAttestationParticipationFlagIndicies(data solid.AttestationData, inclusionDelay uint64) ([]uint8, error)
	GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error)
	ComputeNextSyncCommittee() (*solid.SyncCommittee, error)
	GetAttestingIndicies(attestation solid.AttestationData, aggregationBits []byte, checkBitsLength bool) ([]uint64, error)
	GetValidatorChurnLimit() uint64
	ValidatorIndexByPubkey(key [48]byte) (uint64, bool)
	PreviousStateRoot() common.Hash
	SetPreviousStateRoot(root common.Hash)
}

type BeaconStateBasic interface {
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
	AddEth1DataVote(vote *cltypes.Eth1Data)
	ResetEth1DataVotes()
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
	AddValidator(validator solid.Validator, balance uint64)
	SetRandaoMixAt(index int, mix common.Hash)
	SetSlashingSegmentAt(index int, segment uint64)
	IncrementSlashingSegmentAt(index int, delta uint64)
	SetEpochParticipationForValidatorIndex(isCurrentEpoch bool, index int, flags cltypes.ParticipationFlags)
	SetValidatorAtIndex(index int, validator solid.Validator)
	ResetEpochParticipation()
	SetJustificationBits(justificationBits cltypes.JustificationBits)
	SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint solid.Checkpoint)
	SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint solid.Checkpoint)
	SetFinalizedCheckpoint(finalizedCheckpoint solid.Checkpoint)
	SetCurrentSyncCommittee(currentSyncCommittee *solid.SyncCommittee)
	SetNextSyncCommittee(nextSyncCommittee *solid.SyncCommittee)
	SetLatestExecutionPayloadHeader(header *cltypes.Eth1Header)
	SetNextWithdrawalIndex(index uint64)
	SetNextWithdrawalValidatorIndex(index uint64)
	ResetHistoricalSummaries()
	AddHistoricalSummary(summary *cltypes.HistoricalSummary)
	AddHistoricalRoot(root common.Hash)
	SetInactivityScores(scores []uint64)
	AddInactivityScore(score uint64)
	SetValidatorInactivityScore(index int, score uint64) error
	SetCurrentEpochParticipationFlags(flags []cltypes.ParticipationFlags)
	SetPreviousEpochParticipationFlags(flags []cltypes.ParticipationFlags)
	AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags)
	AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags)
	AddPreviousEpochParticipationAt(index int, delta byte)
	AddCurrentEpochAtteastation(attestation *solid.PendingAttestation)
	AddPreviousEpochAttestation(attestation *solid.PendingAttestation)
	ResetCurrentEpochAttestations()
	SetPreviousEpochAttestations(attestations *solid.ListSSZ[*solid.PendingAttestation])
	ResetPreviousEpochAttestations()
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
	ValidatorLength() int
	AppendValidator(in solid.Validator)
	ForEachValidator(fn func(v solid.Validator, idx int, total int) bool)
	ValidatorForValidatorIndex(index int) (solid.Validator, error)
	ForEachBalance(fn func(v uint64, idx int, total int) bool)
	ValidatorBalance(index int) (uint64, error)
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
	RandaoMixes() solid.HashVectorSSZ
	GetRandaoMixes(epoch uint64) [32]byte
	GetRandaoMix(index int) [32]byte
	ForEachSlashingSegment(fn func(idx int, v uint64, total int) bool)
	SlashingSegmentAt(pos int) uint64
	EpochParticipation(currentEpoch bool) *solid.BitList
	JustificationBits() cltypes.JustificationBits
	EpochParticipationForValidatorIndex(isCurrentEpoch bool, index int) cltypes.ParticipationFlags
	PreviousJustifiedCheckpoint() solid.Checkpoint
	CurrentJustifiedCheckpoint() solid.Checkpoint
	ValidatorInactivityScore(index int) (uint64, error)
	FinalizedCheckpoint() solid.Checkpoint
	CurrentSyncCommittee() *solid.SyncCommittee
	NextSyncCommittee() *solid.SyncCommittee
	LatestExecutionPayloadHeader() *cltypes.Eth1Header
	NextWithdrawalIndex() uint64
	CurrentEpochAttestations() *solid.ListSSZ[*solid.PendingAttestation]
	CurrentEpochAttestationsLength() int
	PreviousEpochAttestations() *solid.ListSSZ[*solid.PendingAttestation]
	PreviousEpochAttestationsLength() int
	NextWithdrawalValidatorIndex() uint64
	GetBlockRootAtSlot(slot uint64) (common.Hash, error)
	GetDomain(domainType [4]byte, epoch uint64) ([]byte, error)
	DebugPrint(prefix string)
	BlockRoot() ([32]byte, error)
	EncodeSSZ(buf []byte) ([]byte, error)
	DecodeSSZ(buf []byte, version int) error
	EncodingSizeSSZ() (size int)
	Clone() clonable.Clonable
	HashSSZ() (out [32]byte, err error)
}

// TODO figure this out
type BeaconStateCopying interface {
	//CopyInto(dst *raw.BeaconState) error
	//Copy() (*raw.BeaconState, error)
}
