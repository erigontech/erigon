package state

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
)

// Below are setters. Note that they also dirty the state.

func (b *BeaconState) SetGenesisTime(genesisTime uint64) {
	b.touchedLeaves[GenesisTimeLeafIndex] = true
	b.genesisTime = genesisTime
}

func (b *BeaconState) SetGenesisValidatorsRoot(genesisValidatorRoot common.Hash) {
	b.touchedLeaves[GenesisValidatorsRootLeafIndex] = true
	b.genesisValidatorsRoot = genesisValidatorRoot
}

func (b *BeaconState) SetSlot(slot uint64) {
	b.touchedLeaves[SlotLeafIndex] = true
	b.slot = slot
}

func (b *BeaconState) SetFork(fork *cltypes.Fork) {
	b.touchedLeaves[ForkLeafIndex] = true
	b.fork = fork
}

func (b *BeaconState) SetLatestBlockHeader(header *cltypes.BeaconBlockHeader) {
	b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	b.latestBlockHeader = header
}

func (b *BeaconState) SetBlockRoots(blockRoots [][32]byte) {
	b.touchedLeaves[BlockRootsLeafIndex] = true
	b.blockRoots = blockRoots
}

func (b *BeaconState) SetStateRoots(stateRoots [][32]byte) {
	b.touchedLeaves[StateRootsLeafIndex] = true
	b.stateRoots = stateRoots
}

func (b *BeaconState) SetHistoricalRoots(historicalRoots [][32]byte) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	b.historicalRoots = historicalRoots
}

func (b *BeaconState) SetBlockRootAt(index int, root [32]byte) {
	b.touchedLeaves[BlockRootsLeafIndex] = true
	b.blockRoots[index] = root
}

func (b *BeaconState) SetStateRootAt(index int, root [32]byte) {
	b.touchedLeaves[StateRootsLeafIndex] = true
	b.stateRoots[index] = root
}

func (b *BeaconState) SetHistoricalRootAt(index int, root [32]byte) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	b.historicalRoots[index] = root
}

func (b *BeaconState) SetValidatorAt(index int, validator *cltypes.Validator) {
	b.validators[index] = validator
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataLeafIndex] = true
	b.eth1Data = eth1Data
}

func (b *BeaconState) SetEth1DataVotes(eth1DataVotes []*cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	b.eth1DataVotes = eth1DataVotes
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.touchedLeaves[Eth1DepositIndexLeafIndex] = true
	b.eth1DepositIndex = eth1DepositIndex
}

func (b *BeaconState) SetValidators(validators []*cltypes.Validator) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	b.validators = validators
}

func (b *BeaconState) SetBalances(balances []uint64) {
	b.touchedLeaves[BalancesLeafIndex] = true
	b.balances = balances
}

func (b *BeaconState) SetRandaoMixes(randaoMixes [][32]byte) {
	b.touchedLeaves[RandaoMixesLeafIndex] = true
	b.randaoMixes = randaoMixes
}

func (b *BeaconState) SetSlashings(slashings []uint64) {
	b.touchedLeaves[SlashingsLeafIndex] = true
	b.slashings = slashings
}

func (b *BeaconState) SetPreviousEpochParticipation(previousEpochParticipation []byte) {
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochParticipation = previousEpochParticipation
}

func (b *BeaconState) SetCurrentEpochParticipation(currentEpochParticipation []byte) {
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochParticipation = currentEpochParticipation
}

func (b *BeaconState) SetJustificationBits(justificationBits []byte) {
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

func (b *BeaconState) SetLatestExecutionPayloadHeader(header *cltypes.ExecutionHeader) {
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	b.latestExecutionPayloadHeader = header
}
