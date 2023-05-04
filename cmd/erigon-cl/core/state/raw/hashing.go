package raw

import (
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
)

const maxEth1Votes = 2048

func (b *BeaconState) HashSSZ() ([32]byte, error) {
	var err error
	if err = b.computeDirtyLeaves(); err != nil {
		return [32]byte{}, err
	}
	/*fmt.Println(b.slot)
	for i, val := range b.leaves {
		fmt.Println(i, libcommon.Hash(val))
	}*/

	// Pad to 32 of length
	return merkle_tree.MerkleRootFromLeaves(b.leaves[:])
}

func preparateRootsForHashing(roots []common.Hash) [][32]byte {
	ret := make([][32]byte, len(roots))
	for i := range roots {
		copy(ret[i][:], roots[i][:])
	}
	return ret
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
		root, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(b.blockRoots[:]), state_encoding.BlockRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(BlockRootsLeafIndex, root)
	}

	// Field(6): StateRoots
	if b.isLeafDirty(StateRootsLeafIndex) {
		root, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(b.stateRoots[:]), state_encoding.StateRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(StateRootsLeafIndex, root)
	}

	// Field(7): HistoricalRoots
	if b.isLeafDirty(HistoricalRootsLeafIndex) {
		root, err := merkle_tree.ArraysRootWithLimit(utils.PreparateRootsForHashing(b.historicalRoots), state_encoding.HistoricalRootsLength)
		if err != nil {
			return err
		}
		b.updateLeaf(HistoricalRootsLeafIndex, root)
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
		root, err := merkle_tree.ListObjectSSZRoot(b.eth1DataVotes, maxEth1Votes)
		if err != nil {
			return err
		}
		b.updateLeaf(Eth1DataVotesLeafIndex, root)
	}

	// Field(10): Eth1DepositIndex
	if b.isLeafDirty(Eth1DepositIndexLeafIndex) {
		b.updateLeaf(Eth1DepositIndexLeafIndex, merkle_tree.Uint64Root(b.eth1DepositIndex))
	}

	// Field(11): Validators
	if b.isLeafDirty(ValidatorsLeafIndex) {
		root, err := merkle_tree.ListObjectSSZRoot(b.validators, state_encoding.ValidatorRegistryLimit)
		if err != nil {
			return err
		}
		b.updateLeaf(ValidatorsLeafIndex, root)
	}

	// Field(12): Balances
	if b.isLeafDirty(BalancesLeafIndex) {
		root, err := merkle_tree.Uint64ListRootWithLimit(b.balances, state_encoding.ValidatorLimitForBalancesChunks())
		if err != nil {
			return err
		}
		b.updateLeaf(BalancesLeafIndex, root)
	}

	// Field(13): RandaoMixes
	if b.isLeafDirty(RandaoMixesLeafIndex) {
		root, err := merkle_tree.ArraysRoot(preparateRootsForHashing(b.randaoMixes[:]), state_encoding.RandaoMixesLength)
		if err != nil {
			return err
		}
		b.updateLeaf(RandaoMixesLeafIndex, root)
	}

	// Field(14): Slashings
	if b.isLeafDirty(SlashingsLeafIndex) {
		root, err := state_encoding.SlashingsRoot(b.slashings[:])
		if err != nil {
			return err
		}
		b.updateLeaf(SlashingsLeafIndex, root)
	}

	// Field(15) and Field(16) are special due to the fact that they have different format in Phase0.

	// Field(15): PreviousEpochParticipation
	if b.isLeafDirty(PreviousEpochParticipationLeafIndex) {
		var root libcommon.Hash
		var err error
		if b.version == clparams.Phase0Version {
			root, err = merkle_tree.ListObjectSSZRoot(b.previousEpochAttestations, b.beaconConfig.SlotsPerEpoch*b.beaconConfig.MaxAttestations)
		} else {
			root, err = merkle_tree.BitlistRootWithLimitForState(b.previousEpochParticipation.Bytes(), state_encoding.ValidatorRegistryLimit)
		}
		if err != nil {
			return err
		}

		b.updateLeaf(PreviousEpochParticipationLeafIndex, root)
	}

	// Field(16): CurrentEpochParticipation
	if b.isLeafDirty(CurrentEpochParticipationLeafIndex) {
		var root libcommon.Hash
		var err error
		if b.version == clparams.Phase0Version {
			root, err = merkle_tree.ListObjectSSZRoot(b.currentEpochAttestations, b.beaconConfig.SlotsPerEpoch*b.beaconConfig.MaxAttestations)
		} else {
			root, err = merkle_tree.BitlistRootWithLimitForState(b.currentEpochParticipation.Bytes(), state_encoding.ValidatorRegistryLimit)
		}
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentEpochParticipationLeafIndex, root)
	}

	// Field(17): JustificationBits
	if b.isLeafDirty(JustificationBitsLeafIndex) {
		var root [32]byte
		root[0] = b.justificationBits.Byte()
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
	if b.version == clparams.Phase0Version {
		return nil
	}
	// Field(21): Inactivity Scores
	if b.isLeafDirty(InactivityScoresLeafIndex) {
		root, err := merkle_tree.Uint64ListRootWithLimit(b.inactivityScores, state_encoding.ValidatorLimitForBalancesChunks())
		if err != nil {
			return err
		}
		b.updateLeaf(InactivityScoresLeafIndex, root)
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

func (b *BeaconState) markLeaf(idxs ...StateLeafIndex) {
	for _, idx := range idxs {
		b.touchedLeaves[idx] = true
	}
}
