package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
)

func (b *BeaconState) HashSSZ() ([32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return [32]byte{}, err
	}
	// Pad to 32 of length
	return merkle_tree.MerkleRootFromLeaves(b.leaves[:])
}

func (b *BeaconState) computeDirtyLeaves() error {
	// Update all dirty leafs
	// ----

	// Field(0): GenesisTime
	if b.isLeafDirty(GenesisTimeLeafIndex) {
		b.updateLeaf(GenesisTimeLeafIndex, merkle_tree.Uint64Root(b.genesisTime))
	}

	// Field(1): GenesisValidatorsRoot
	if b.isLeafDirty(GenesisValidatorsRootLeafIndex) {
		b.updateLeaf(GenesisValidatorsRootLeafIndex, b.genesisValidatorsRoot)
	}

	// Field(2): Slot
	if b.isLeafDirty(SlotLeafIndex) {
		b.updateLeaf(SlotLeafIndex, merkle_tree.Uint64Root(b.slot))
	}

	// Field(3): Fork
	if b.isLeafDirty(ForkLeafIndex) {
		forkRoot, err := b.fork.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(ForkLeafIndex, forkRoot)
	}

	// Field(4): LatestBlockHeader
	if b.isLeafDirty(LatestBlockHeaderLeafIndex) {
		headerRoot, err := b.latestBlockHeader.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(LatestBlockHeaderLeafIndex, headerRoot)
	}

	// Field(5): BlockRoots
	if b.isLeafDirty(BlockRootsLeafIndex) {
		blockRootsRoot, err := merkle_tree.ArraysRoot(preparateRootsForHashing(b.blockRoots[:]), state_encoding.BlockRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(BlockRootsLeafIndex, blockRootsRoot)
	}

	// Field(6): StateRoots
	if b.isLeafDirty(StateRootsLeafIndex) {
		stateRootsRoot, err := merkle_tree.ArraysRoot(preparateRootsForHashing(b.stateRoots[:]), state_encoding.StateRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(StateRootsLeafIndex, stateRootsRoot)
	}

	// Field(7): HistoricalRoots
	if b.isLeafDirty(HistoricalRootsLeafIndex) {
		historicalRootsRoot, err := merkle_tree.ArraysRootWithLimit(preparateRootsForHashing(b.historicalRoots), state_encoding.HistoricalRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(HistoricalRootsLeafIndex, historicalRootsRoot)
	}

	// Field(8): Eth1Data
	if b.isLeafDirty(Eth1DataLeafIndex) {
		dataRoot, err := b.eth1Data.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(Eth1DataLeafIndex, dataRoot)
	}

	// Field(9): Eth1DataVotes
	if b.isLeafDirty(Eth1DataVotesLeafIndex) {
		votesRoot, err := state_encoding.Eth1DataVectorRoot(b.eth1DataVotes)
		if err != nil {
			return err
		}
		b.updateLeaf(Eth1DataVotesLeafIndex, votesRoot)
	}

	// Field(10): Eth1DepositIndex
	if b.isLeafDirty(Eth1DepositIndexLeafIndex) {
		b.updateLeaf(Eth1DepositIndexLeafIndex, merkle_tree.Uint64Root(b.eth1DepositIndex))
	}

	// Field(11): Validators
	if b.isLeafDirty(ValidatorsLeafIndex) {
		vRoot, err := state_encoding.ValidatorsVectorRoot(b.validators)
		if err != nil {
			return err
		}
		b.updateLeaf(ValidatorsLeafIndex, vRoot)
	}

	// Field(12): Balances
	if b.isLeafDirty(BalancesLeafIndex) {
		balancesRoot, err := merkle_tree.Uint64ListRootWithLimit(b.balances, state_encoding.ValidatorLimitForBalancesChunks())
		if err != nil {
			return err
		}
		b.updateLeaf(BalancesLeafIndex, balancesRoot)
	}

	// Field(13): RandaoMixes
	if b.isLeafDirty(RandaoMixesLeafIndex) {
		randaoRootsRoot, err := merkle_tree.ArraysRoot(preparateRootsForHashing(b.randaoMixes[:]), state_encoding.RandaoMixesLength)
		if err != nil {
			return err
		}
		b.updateLeaf(RandaoMixesLeafIndex, randaoRootsRoot)
	}

	// Field(14): Slashings
	if b.isLeafDirty(SlashingsLeafIndex) {
		slashingsRoot, err := state_encoding.SlashingsRoot(b.slashings[:])
		if err != nil {
			return err
		}
		b.updateLeaf(SlashingsLeafIndex, slashingsRoot)
	}
	// Field(15): PreviousEpochParticipation
	if b.isLeafDirty(PreviousEpochParticipationLeafIndex) {
		participationRoot, err := merkle_tree.BitlistRootWithLimitForState(b.previousEpochParticipation, state_encoding.ValidatorRegistryLimit)
		if err != nil {
			return err
		}
		b.updateLeaf(PreviousEpochParticipationLeafIndex, participationRoot)
	}

	// Field(16): CurrentEpochParticipation
	if b.isLeafDirty(CurrentEpochParticipationLeafIndex) {
		participationRoot, err := merkle_tree.BitlistRootWithLimitForState(b.currentEpochParticipation, state_encoding.ValidatorRegistryLimit)
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentEpochParticipationLeafIndex, participationRoot)
	}

	// Field(17): JustificationBits
	if b.isLeafDirty(JustificationBitsLeafIndex) {
		var root [32]byte
		root[0] = b.justificationBits
		b.updateLeaf(JustificationBitsLeafIndex, root)
	}

	// Field(18): PreviousJustifiedCheckpoint
	if b.isLeafDirty(PreviousJustifiedCheckpointLeafIndex) {
		checkpointRoot, err := b.previousJustifiedCheckpoint.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(PreviousJustifiedCheckpointLeafIndex, checkpointRoot)
	}

	// Field(19): CurrentJustifiedCheckpoint
	if b.isLeafDirty(CurrentJustifiedCheckpointLeafIndex) {
		checkpointRoot, err := b.currentJustifiedCheckpoint.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentJustifiedCheckpointLeafIndex, checkpointRoot)
	}

	// Field(20): FinalizedCheckpoint
	if b.isLeafDirty(FinalizedCheckpointLeafIndex) {
		checkpointRoot, err := b.finalizedCheckpoint.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(FinalizedCheckpointLeafIndex, checkpointRoot)
	}

	// Field(21): Inactivity Scores
	if b.isLeafDirty(InactivityScoresLeafIndex) {
		scoresRoot, err := merkle_tree.Uint64ListRootWithLimit(b.inactivityScores, state_encoding.ValidatorLimitForBalancesChunks())
		if err != nil {
			return err
		}
		b.updateLeaf(InactivityScoresLeafIndex, scoresRoot)
	}

	// Field(22): CurrentSyncCommitte
	if b.isLeafDirty(CurrentSyncCommitteeLeafIndex) {
		committeeRoot, err := b.currentSyncCommittee.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentSyncCommitteeLeafIndex, committeeRoot)
	}

	// Field(23): NextSyncCommitte
	if b.isLeafDirty(NextSyncCommitteeLeafIndex) {
		committeeRoot, err := b.nextSyncCommittee.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(NextSyncCommitteeLeafIndex, committeeRoot)
	}

	if b.version < clparams.BellatrixVersion {
		return nil
	}
	// Field(24): LatestExecutionPayloadHeader
	if b.isLeafDirty(LatestExecutionPayloadHeaderLeafIndex) {
		headerRoot, err := b.latestExecutionPayloadHeader.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(LatestExecutionPayloadHeaderLeafIndex, headerRoot)
	}

	if b.version < clparams.CapellaVersion {
		return nil
	}

	// Field(25): NextWithdrawalIndex
	if b.isLeafDirty(NextWithdrawalIndexLeafIndex) {
		b.updateLeaf(NextWithdrawalIndexLeafIndex, merkle_tree.Uint64Root(b.nextWithdrawalIndex))
	}

	// Field(26): NextWithdrawalValidatorIndex
	if b.isLeafDirty(NextWithdrawalValidatorIndexLeafIndex) {
		b.updateLeaf(NextWithdrawalValidatorIndexLeafIndex, merkle_tree.Uint64Root(b.nextWithdrawalValidatorIndex))
	}

	// Field(27): HistoricalSummaries
	if b.isLeafDirty(HistoricalSummariesLeafIndex) {
		root, err := merkle_tree.ListObjectSSZRoot(b.historicalSummaries, state_encoding.HistoricalRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(HistoricalSummariesLeafIndex, root)
	}
	return nil
}

func (b *BeaconState) updateLeaf(idx StateLeafIndex, leaf libcommon.Hash) {
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
