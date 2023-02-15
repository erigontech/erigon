package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
)

func (b *BeaconState) HashSSZ() ([32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return [32]byte{}, err
	}
	// Pad to 32 of length
	return merkle_tree.MerkleRootFromLeaves(b.leaves[:])
}

// An hash component is a payload for a specific state leaves given base inner leaves.
type hashComponent struct {
	hashF func() ([32]byte, error)
	index int
}

// An hash component is a payload for a specific state leaves given base inner leaves.
type hashResult struct {
	root  libcommon.Hash
	index int
	err   error
}

// computeRandaoMixesHash computes the randao mix hash in hopefully less time than required.
func (b *BeaconState) computeRandaoMixesHash() ([32]byte, error) {
	mixes := utils.PreparateRootsForHashing(b.randaoMixes[:])
	// Divide computation in 8 threads
	hashComponents := []*hashComponent{}
	numThreads := 16
	branchSize := len(b.randaoMixes) / numThreads // should be 8192
	for i := 0; i < numThreads; i++ {
		leaves := make([][32]byte, branchSize)
		copy(leaves, mixes[i*branchSize:])
		hashComponents = append(hashComponents, &hashComponent{
			hashF: func() ([32]byte, error) {
				root, err := merkle_tree.ArraysRoot(leaves, uint64(branchSize))
				if err != nil {
					return [32]byte{}, err
				}
				return root, nil
			},
			index: i,
		})
	}
	merkleLayer := make([][32]byte, numThreads)
	resultCh := make(chan hashResult)
	for _, component := range hashComponents {
		go hashComponentWorker(component, resultCh)
	}
	for range hashComponents {
		result := <-resultCh
		if result.err != nil {
			return [32]byte{}, result.err
		}
		merkleLayer[result.index] = result.root
	}
	return merkle_tree.ArraysRoot(merkleLayer, uint64(numThreads))
}

func (b *BeaconState) computeDirtyLeaves() error {
	hashComponents := []*hashComponent{}
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
		// Make the hash component.
		hashComponents = append(hashComponents, &hashComponent{
			hashF: func() ([32]byte, error) {
				root, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(b.blockRoots[:]), state_encoding.BlockRootsLength)
				if err != nil {
					return [32]byte{}, err
				}
				return root, nil
			},
			index: int(BlockRootsLeafIndex),
		})
	}

	// Field(6): StateRoots
	if b.isLeafDirty(StateRootsLeafIndex) {
		// Make the hash component.
		hashComponents = append(hashComponents, &hashComponent{
			hashF: func() ([32]byte, error) {
				root, err := merkle_tree.ArraysRoot(utils.PreparateRootsForHashing(b.stateRoots[:]), state_encoding.StateRootsLength)
				if err != nil {
					return [32]byte{}, err
				}
				return root, nil
			},
			index: int(StateRootsLeafIndex),
		})
	}

	// Field(7): HistoricalRoots
	if b.isLeafDirty(HistoricalRootsLeafIndex) {
		hashComponents = append(hashComponents, &hashComponent{
			hashF: func() ([32]byte, error) {
				root, err := merkle_tree.ArraysRootWithLimit(utils.PreparateRootsForHashing(b.historicalRoots), state_encoding.HistoricalRootsLength)
				if err != nil {
					return [32]byte{}, err
				}
				return root, nil
			},
			index: int(HistoricalRootsLeafIndex),
		})
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
		hashComponents = append(hashComponents, &hashComponent{
			hashF: func() ([32]byte, error) {
				root, err := state_encoding.Eth1DataVectorRoot(b.eth1DataVotes)
				if err != nil {
					return [32]byte{}, err
				}
				return root, nil
			},
			index: int(Eth1DataVotesLeafIndex),
		})
	}

	// Field(10): Eth1DepositIndex
	if b.isLeafDirty(Eth1DepositIndexLeafIndex) {
		b.updateLeaf(Eth1DepositIndexLeafIndex, merkle_tree.Uint64Root(b.eth1DepositIndex))
	}

	// Field(11): Validators
	if b.isLeafDirty(ValidatorsLeafIndex) {
		root, err := state_encoding.ValidatorsVectorRoot(b.validators)
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
		root, err := b.computeRandaoMixesHash()
		if err != nil {
			return err
		}
		b.updateLeaf(RandaoMixesLeafIndex, root)
	}

	// Field(14): Slashings
	if b.isLeafDirty(SlashingsLeafIndex) {
		hashComponents = append(hashComponents, &hashComponent{
			hashF: func() ([32]byte, error) {
				root, err := state_encoding.SlashingsRoot(b.slashings[:])
				if err != nil {
					return [32]byte{}, err
				}
				return root, nil
			},
			index: int(SlashingsLeafIndex),
		})
	}
	// Field(15): PreviousEpochParticipation
	if b.isLeafDirty(PreviousEpochParticipationLeafIndex) {
		root, err := merkle_tree.BitlistRootWithLimitForState(b.previousEpochParticipation.Bytes(), state_encoding.ValidatorRegistryLimit)
		if err != nil {
			return err
		}
		b.updateLeaf(PreviousEpochParticipationLeafIndex, root)

	}

	// Field(16): CurrentEpochParticipation
	if b.isLeafDirty(CurrentEpochParticipationLeafIndex) {
		root, err := merkle_tree.BitlistRootWithLimitForState(b.currentEpochParticipation.Bytes(), state_encoding.ValidatorRegistryLimit)
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

	if b.version >= clparams.CapellaVersion {

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
	}
	// Execute hash components in parallel
	resultCh := make(chan hashResult)
	for _, component := range hashComponents {
		go hashComponentWorker(component, resultCh)
	}
	for range hashComponents {
		result := <-resultCh
		if result.err != nil {
			return result.err
		}
		b.updateLeaf(StateLeafIndex(result.index), result.root)
	}
	return nil
}

func hashComponentWorker(component *hashComponent, resultCh chan hashResult) {
	root, err := component.hashF()
	if err != nil {
		resultCh <- hashResult{err: err}
		return
	}
	resultCh <- hashResult{
		root:  root,
		index: component.index,
	}
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
