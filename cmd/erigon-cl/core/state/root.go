package state

import (
	"github.com/ledgerwatch/erigon/common"
)

func (b *BeaconState) HashTreeRoot() ([32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return [32]byte{}, err
	}
	return merkleRootFromLeaves(b.leaves, BellatrixLeavesSize)
}

func (b *BeaconState) computeDirtyLeaves() error {
	// Update all dirty leafs
	// ----

	// Field(0): GenesisTime
	if b.isLeafDirty(GenesisTimeLeafIndex) {
		b.updateLeaf(GenesisTimeLeafIndex, Uint64Root(b.genesisTime))
	}

	// Field(1): GenesisValidatorsRoot
	if b.isLeafDirty(GenesisValidatorsRootLeafIndex) {
		b.updateLeaf(GenesisValidatorsRootLeafIndex, b.genesisValidatorsRoot)
	}

	// Field(2): Slot
	if b.isLeafDirty(SlotLeafIndex) {
		b.updateLeaf(SlotLeafIndex, Uint64Root(b.slot))
	}

	// Field(3): Fork
	if b.isLeafDirty(LatestBlockHeaderLeafIndex) {
		headerRoot, err := b.fork.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(LatestBlockHeaderLeafIndex, headerRoot)
	}

	// Field(4): LatestBlockHeader
	if b.isLeafDirty(LatestBlockHeaderLeafIndex) {
		headerRoot, err := b.latestBlockHeader.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(LatestBlockHeaderLeafIndex, headerRoot)
	}

	// Field(5): BlockRoots
	if b.isLeafDirty(BlockRootsLeafIndex) {
		blockRootsRoot, err := ArraysRoot(b.blockRoots, BlockRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(BlockRootsLeafIndex, blockRootsRoot)
	}

	// Field(6): StateRoots
	if b.isLeafDirty(StateRootsLeafIndex) {
		stateRootsRoot, err := ArraysRoot(b.stateRoots, StateRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(StateRootsLeafIndex, stateRootsRoot)
	}

	// Field(7): HistoricalRoots
	if b.isLeafDirty(HistoricalRootsLeafIndex) {
		historicalRootsRoot, err := ArraysRootWithLength(b.historicalRoots, HistoricalRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(HistoricalRootsLeafIndex, historicalRootsRoot)
	}

	// Field(8): Eth1Data
	if b.isLeafDirty(Eth1DataLeafIndex) {
		dataRoot, err := b.eth1Data.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(Eth1DataLeafIndex, dataRoot)
	}

	// Field(9): Eth1DataVotes
	if b.isLeafDirty(Eth1DataVotesLeafIndex) {
		votesRoot, err := Eth1DataVectorRoot(b.eth1DataVotes, Eth1DataVotesRootsLimit)
		if err != nil {
			return err
		}
		b.updateLeaf(Eth1DataLeafIndex, votesRoot)
	}

	// Field(10): Eth1DepositIndex
	if b.isLeafDirty(Eth1DepositIndexLeafIndex) {
		b.updateLeaf(Eth1DepositIndexLeafIndex, Uint64Root(b.eth1DepositIndex))
	}

	// Field(11): Validators
	if b.isLeafDirty(ValidatorsLeafIndex) {
		vRoot, err := ValidatorsVectorRoot(b.validators, ValidatorRegistryLimit)
		if err != nil {
			return err
		}
		b.updateLeaf(ValidatorsLeafIndex, vRoot)
	}

	// Field(12): Balances
	if b.isLeafDirty(BalancesLeafIndex) {
		balancesRoot, err := Uint64ListRootWithLength(b.balances, ValidatorLimitForBalancesChunks())
		if err != nil {
			return err
		}
		b.updateLeaf(BalancesLeafIndex, balancesRoot)
	}

	// Field(13): RandaoMixes
	if b.isLeafDirty(RandaoMixesLeafIndex) {
		randaoRootsRoot, err := ArraysRoot(b.randaoMixes, RandaoMixesLength)
		if err != nil {
			return err
		}
		b.updateLeaf(RandaoMixesLeafIndex, randaoRootsRoot)
	}

	// Field(14): Slashings
	if b.isLeafDirty(SlashingsLeafIndex) {
		slashingsRoot, err := SlashingsRoot(b.slashings)
		if err != nil {
			return err
		}
		b.updateLeaf(SlashingsLeafIndex, slashingsRoot)
	}
	// Field(15): PreviousEpochParticipation
	if b.isLeafDirty(PreviousEpochParticipationLeafIndex) {
		participationRoot, err := ParticipationBitsRoot(b.previousEpochParticipation)
		if err != nil {
			return err
		}
		b.updateLeaf(PreviousEpochParticipationLeafIndex, participationRoot)
	}

	// Field(16): CurrentEpochParticipation
	if b.isLeafDirty(CurrentEpochParticipationLeafIndex) {
		participationRoot, err := ParticipationBitsRoot(b.currentEpochParticipation)
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentEpochParticipationLeafIndex, participationRoot)
	}

	// Field(17): JustificationBits
	if b.isLeafDirty(JustificationBitsLeafIndex) {
		var root [32]byte
		copy(root[:], b.justificationBits)
		b.updateLeaf(JustificationBitsLeafIndex, root)
	}

	// Field(18): PreviousJustifiedCheckpoint
	if b.isLeafDirty(PreviousJustifiedCheckpointLeafIndex) {
		checkpointRoot, err := b.previousJustifiedCheckpoint.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(PreviousJustifiedCheckpointLeafIndex, checkpointRoot)
	}

	// Field(19): CurrentJustifiedCheckpoint
	if b.isLeafDirty(CurrentJustifiedCheckpointLeafIndex) {
		checkpointRoot, err := b.currentJustifiedCheckpoint.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentJustifiedCheckpointLeafIndex, checkpointRoot)
	}

	// Field(20): FinalizedCheckpoint
	if b.isLeafDirty(FinalizedCheckpointLeafIndex) {
		checkpointRoot, err := b.finalizedCheckpoint.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(FinalizedCheckpointLeafIndex, checkpointRoot)
	}

	// Field(21): Inactivity Scores
	if b.isLeafDirty(InactivityScoresLeafIndex) {
		scoresRoot, err := Uint64ListRootWithLength(b.inactivityScores, ValidatorLimitForBalancesChunks())
		if err != nil {
			return err
		}
		b.updateLeaf(InactivityScoresLeafIndex, scoresRoot)
	}

	// Field(22): CurrentSyncCommitte
	if b.isLeafDirty(CurrentSyncCommitteeLeafIndex) {
		committeeRoot, err := b.currentSyncCommittee.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentSyncCommitteeLeafIndex, committeeRoot)
	}

	// Field(23): NextSyncCommitte
	if b.isLeafDirty(NextSyncCommitteeLeafIndex) {
		committeeRoot, err := b.nextSyncCommittee.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(NextSyncCommitteeLeafIndex, committeeRoot)
	}

	// Field(24): LatestExecutionPayloadHeader
	if b.isLeafDirty(LatestBlockHeaderLeafIndex) {
		headerRoot, err := b.latestBlockHeader.HashTreeRoot()
		if err != nil {
			return err
		}
		b.updateLeaf(LatestBlockHeaderLeafIndex, headerRoot)
	}
	return nil
}

func (b *BeaconState) updateLeaf(idx StateLeafIndex, leaf common.Hash) {
	// Update leaf with new value.
	b.leaves[idx] = leaf
	// Now leaf is clean :).
	b.touchedLeaves[idx] = false
}

func (b *BeaconState) isLeafDirty(idx StateLeafIndex) bool {
	// If leaf is non-initialized or if it was touched then we change it.
	touched, isInitialized := b.touchedLeaves[idx]
	return !isInitialized || touched // change only if the leaf was touched or root is non-initialized.
}
