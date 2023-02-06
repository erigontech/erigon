package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
)

const maxEth1Votes = 2048

// Below are setters. Note that they also dirty the state.

func (b *BeaconState) SetGenesisTime(genesisTime uint64) {
	b.touchedLeaves[GenesisTimeLeafIndex] = true
	b.genesisTime = genesisTime
}

func (b *BeaconState) SetGenesisValidatorsRoot(genesisValidatorRoot libcommon.Hash) {
	b.touchedLeaves[GenesisValidatorsRootLeafIndex] = true
	b.genesisValidatorsRoot = genesisValidatorRoot
}

func (b *BeaconState) SetSlot(slot uint64) {
	b.touchedLeaves[SlotLeafIndex] = true
	b.slot = slot
	// If there is a new slot update the active balance cache.
	b._refreshActiveBalances()
}

func (b *BeaconState) SetFork(fork *cltypes.Fork) {
	b.touchedLeaves[ForkLeafIndex] = true
	b.fork = fork
}

func (b *BeaconState) SetLatestBlockHeader(header *cltypes.BeaconBlockHeader) {
	b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	b.latestBlockHeader = header
}

func (b *BeaconState) SetHistoricalRoots(historicalRoots []libcommon.Hash) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	b.historicalRoots = historicalRoots
}

func (b *BeaconState) SetBlockRootAt(index int, root libcommon.Hash) {
	b.touchedLeaves[BlockRootsLeafIndex] = true
	b.blockRoots[index] = root
}

func (b *BeaconState) SetStateRootAt(index int, root libcommon.Hash) {
	b.touchedLeaves[StateRootsLeafIndex] = true
	b.stateRoots[index] = root
}

func (b *BeaconState) SetHistoricalRootAt(index int, root [32]byte) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	b.historicalRoots[index] = root
}

func (b *BeaconState) SetValidatorAt(index int, validator *cltypes.Validator) error {
	if index >= len(b.validators) {
		return InvalidValidatorIndex
	}
	b.validators[index] = validator
	// change in validator set means cache purging
	b.activeValidatorsCache.Purge()
	b._refreshActiveBalances()
	return nil
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataLeafIndex] = true
	b.eth1Data = eth1Data
}

func (b *BeaconState) AddEth1DataVote(vote *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	b.eth1DataVotes = append(b.eth1DataVotes, vote)
}

func (b *BeaconState) ResetEth1DataVotes() {
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	b.eth1DataVotes = b.eth1DataVotes[:0]
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.touchedLeaves[Eth1DepositIndexLeafIndex] = true
	b.eth1DepositIndex = eth1DepositIndex
}

// Should not be called if not for testing
func (b *BeaconState) SetValidators(validators []*cltypes.Validator) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	b.validators = validators
	b.initBeaconState()
}

func (b *BeaconState) AddValidator(validator *cltypes.Validator, balance uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	b.validators = append(b.validators, validator)
	b.balances = append(b.balances, balance)
	if validator.Active(b.Epoch()) {
		b.totalActiveBalanceCache += validator.EffectiveBalance
	}
	b.publicKeyIndicies[validator.PublicKey] = uint64(len(b.validators)) - 1
	// change in validator set means cache purging
	b.activeValidatorsCache.Purge()

}

func (b *BeaconState) SetBalances(balances []uint64) {
	b.touchedLeaves[BalancesLeafIndex] = true
	b.balances = balances
	b._refreshActiveBalances()
}

func (b *BeaconState) SetValidatorBalance(index int, balance uint64) error {
	if index >= len(b.balances) {
		return InvalidValidatorIndex
	}

	b.touchedLeaves[BalancesLeafIndex] = true
	b.balances[index] = balance
	return nil
}

func (b *BeaconState) SetRandaoMixAt(index int, mix libcommon.Hash) {
	b.touchedLeaves[RandaoMixesLeafIndex] = true
	b.randaoMixes[index] = mix
}

func (b *BeaconState) SetSlashingSegmentAt(index int, segment uint64) {
	b.touchedLeaves[SlashingsLeafIndex] = true
	b.slashings[index] = segment
}

func (b *BeaconState) SetPreviousEpochParticipation(previousEpochParticipation []cltypes.ParticipationFlags) {
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochParticipation = previousEpochParticipation
}

func (b *BeaconState) SetCurrentEpochParticipation(currentEpochParticipation []cltypes.ParticipationFlags) {
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochParticipation = currentEpochParticipation
}

func (b *BeaconState) SetJustificationBits(justificationBits cltypes.JustificationBits) {
	b.touchedLeaves[JustificationBitsLeafIndex] = true
	b.justificationBits = justificationBits
}

func (b *BeaconState) SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[PreviousJustifiedCheckpointLeafIndex] = true
	b.previousJustifiedCheckpoint = previousJustifiedCheckpoint
}

func (b *BeaconState) SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[CurrentJustifiedCheckpointLeafIndex] = true
	b.currentJustifiedCheckpoint = currentJustifiedCheckpoint
}

func (b *BeaconState) SetFinalizedCheckpoint(finalizedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[FinalizedCheckpointLeafIndex] = true
	b.finalizedCheckpoint = finalizedCheckpoint
}

func (b *BeaconState) SetCurrentSyncCommittee(currentSyncCommittee *cltypes.SyncCommittee) {
	b.touchedLeaves[CurrentSyncCommitteeLeafIndex] = true
	b.currentSyncCommittee = currentSyncCommittee
}

func (b *BeaconState) SetNextSyncCommittee(nextSyncCommittee *cltypes.SyncCommittee) {
	b.touchedLeaves[NextSyncCommitteeLeafIndex] = true
	b.nextSyncCommittee = nextSyncCommittee
}

func (b *BeaconState) SetLatestExecutionPayloadHeader(header *types.Header) {
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	b.latestExecutionPayloadHeader = header
}

func (b *BeaconState) SetNextWithdrawalIndex(index uint64) {
	b.nextWithdrawalIndex = index
}

func (b *BeaconState) SetNextWithdrawalValidatorIndex(index uint64) {
	b.nextWithdrawalValidatorIndex = index
}

func (b *BeaconState) AddHistoricalSummary(summary *cltypes.HistoricalSummary) {
	b.historicalSummaries = append(b.historicalSummaries, summary)
}

func (b *BeaconState) AddInactivityScore(score uint64) {
	b.inactivityScores = append(b.inactivityScores, score)
}

func (b *BeaconState) AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.currentEpochParticipation = append(b.currentEpochParticipation, flags)
}

func (b *BeaconState) AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.previousEpochParticipation = append(b.previousEpochParticipation, flags)
}
