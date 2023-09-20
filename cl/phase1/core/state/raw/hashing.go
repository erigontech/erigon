package raw

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/log/v3"
)

func (b *BeaconState) HashSSZ() (out [32]byte, err error) {
	if err = b.computeDirtyLeaves(); err != nil {
		return [32]byte{}, err
	}

	// for i := 0; i < len(b.leaves); i += 32 {
	// 	fmt.Println(i/32, libcommon.BytesToHash(b.leaves[i:i+32]))
	// }
	// Pad to 32 of length
	err = merkle_tree.MerkleRootFromFlatLeaves(b.leaves, out[:])
	return
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
		root, err := b.blockRoots.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(BlockRootsLeafIndex, root)
	}

	// Field(6): StateRoots
	if b.isLeafDirty(StateRootsLeafIndex) {
		root, err := b.stateRoots.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(StateRootsLeafIndex, root)
	}

	begin := time.Now()

	// Field(7): HistoricalRoots
	if b.isLeafDirty(HistoricalRootsLeafIndex) {
		root, err := b.historicalRoots.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(HistoricalRootsLeafIndex, root)
	}
	log.Trace("HistoricalRoots hashing", "elapsed", time.Since(begin))

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
		root, err := b.eth1DataVotes.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(Eth1DataVotesLeafIndex, root)
	}

	// Field(10): Eth1DepositIndex
	if b.isLeafDirty(Eth1DepositIndexLeafIndex) {
		b.updateLeaf(Eth1DepositIndexLeafIndex, merkle_tree.Uint64Root(b.eth1DepositIndex))
	}

	begin = time.Now()

	// Field(11): Validators
	if b.isLeafDirty(ValidatorsLeafIndex) {
		root, err := b.validators.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(ValidatorsLeafIndex, root)

	}
	log.Trace("ValidatorSet hashing", "elapsed", time.Since(begin))

	begin = time.Now()
	// Field(12): Balances
	if b.isLeafDirty(BalancesLeafIndex) {
		root, err := b.balances.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(BalancesLeafIndex, root)
	}
	log.Trace("Balances hashing", "elapsed", time.Since(begin))

	begin = time.Now()
	// Field(13): RandaoMixes
	if b.isLeafDirty(RandaoMixesLeafIndex) {
		root, err := b.randaoMixes.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(RandaoMixesLeafIndex, root)
	}
	log.Trace("RandaoMixes hashing", "elapsed", time.Since(begin))

	begin = time.Now()
	// Field(14): Slashings
	if b.isLeafDirty(SlashingsLeafIndex) {
		root, err := b.slashings.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(SlashingsLeafIndex, root)
	}
	log.Trace("Slashings hashing", "elapsed", time.Since(begin))
	// Field(15) and Field(16) are special due to the fact that they have different format in Phase0.

	begin = time.Now()
	// Field(15): PreviousEpochParticipation
	if b.isLeafDirty(PreviousEpochParticipationLeafIndex) {
		var root libcommon.Hash
		var err error
		if b.version == clparams.Phase0Version {
			root, err = b.previousEpochAttestations.HashSSZ()
		} else {
			root, err = b.previousEpochParticipation.HashSSZ()
		}
		if err != nil {
			return err
		}

		b.updateLeaf(PreviousEpochParticipationLeafIndex, root)
	}
	log.Trace("PreviousEpochParticipation hashing", "elapsed", time.Since(begin))

	begin = time.Now()

	// Field(16): CurrentEpochParticipation
	if b.isLeafDirty(CurrentEpochParticipationLeafIndex) {
		var root libcommon.Hash
		var err error
		if b.version == clparams.Phase0Version {
			root, err = b.currentEpochAttestations.HashSSZ()
		} else {
			root, err = b.currentEpochParticipation.HashSSZ()
		}
		if err != nil {
			return err
		}
		b.updateLeaf(CurrentEpochParticipationLeafIndex, root)
	}
	log.Trace("CurrentEpochParticipation hashing", "elapsed", time.Since(begin))

	// Field(17): JustificationBits
	if b.isLeafDirty(JustificationBitsLeafIndex) {
		root, _ := b.justificationBits.HashSSZ()
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
	begin = time.Now()
	// Field(21): Inactivity Scores
	if b.isLeafDirty(InactivityScoresLeafIndex) {
		root, err := b.inactivityScores.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(InactivityScoresLeafIndex, root)
	}
	log.Trace("InactivityScores hashing", "elapsed", time.Since(begin))

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

	begin = time.Now()
	// Field(27): HistoricalSummaries
	if b.isLeafDirty(HistoricalSummariesLeafIndex) {
		root, err := b.historicalSummaries.HashSSZ()
		if err != nil {
			return err
		}
		b.updateLeaf(HistoricalSummariesLeafIndex, root)
	}
	log.Trace("HistoricalSummaries hashing", "elapsed", time.Since(begin))

	return nil
}

func (b *BeaconState) updateLeaf(idx StateLeafIndex, leaf libcommon.Hash) {
	// Update leaf with new value.
	copy(b.leaves[idx*32:], leaf[:])
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
