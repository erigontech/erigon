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
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type parallelBeaconStateHasher struct {
	jobs    map[StateLeafIndex]ssz.HashableSSZ
	results sync.Map
}

func (p *parallelBeaconStateHasher) run(b *BeaconState) {
	wg := sync.WaitGroup{}
	if p.jobs == nil {
		p.jobs = make(map[StateLeafIndex]ssz.HashableSSZ)
	}

	for idx, job := range p.jobs {
		wg.Add(1)
		go func(idx StateLeafIndex, job ssz.HashableSSZ) {
			defer wg.Done()
			root, err := job.HashSSZ()
			if err != nil {
				panic(err)
			}
			p.results.Store(idx, root)
		}(idx, job)
	}
	wg.Wait()
	p.results.Range(func(key, value any) bool {
		idx := key.(StateLeafIndex)
		root := value.([32]byte)
		b.updateLeaf(idx, root)
		return true
	})
}

func (b *parallelBeaconStateHasher) add(idx StateLeafIndex, job ssz.HashableSSZ) {
	if b.jobs == nil {
		b.jobs = make(map[StateLeafIndex]ssz.HashableSSZ)
	}
	b.jobs[idx] = job
}

func (b *BeaconState) HashSSZ() (out [32]byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

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

func (b *BeaconState) CurrentSyncCommitteeBranch() ([][32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return nil, err
	}
	schema := []interface{}{}
	for i := 0; i < len(b.leaves); i += 32 {
		schema = append(schema, b.leaves[i:i+32])
	}
	return merkle_tree.MerkleProof(5, 22, schema...)
}

func (b *BeaconState) NextSyncCommitteeBranch() ([][32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return nil, err
	}
	schema := []interface{}{}
	for i := 0; i < len(b.leaves); i += 32 {
		schema = append(schema, b.leaves[i:i+32])
	}
	return merkle_tree.MerkleProof(5, 23, schema...)
}

func (b *BeaconState) FinalityRootBranch() ([][32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return nil, err
	}
	schema := []interface{}{}
	for i := 0; i < len(b.leaves); i += 32 {
		schema = append(schema, b.leaves[i:i+32])
	}
	proof, err := merkle_tree.MerkleProof(5, 20, schema...)
	if err != nil {
		return nil, err
	}

	proof = append([][32]byte{merkle_tree.Uint64Root(b.finalizedCheckpoint.Epoch())}, proof...)
	return proof, nil
}

func preparateRootsForHashing(roots []common.Hash) [][32]byte {
	ret := make([][32]byte, len(roots))
	for i := range roots {
		copy(ret[i][:], roots[i][:])
	}
	return ret
}

func (b *BeaconState) computeDirtyLeaves() error {

	parallelHasher := parallelBeaconStateHasher{}
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
		parallelHasher.add(BlockRootsLeafIndex, b.blockRoots)
	}

	// Field(6): StateRoots
	if b.isLeafDirty(StateRootsLeafIndex) {
		parallelHasher.add(StateRootsLeafIndex, b.stateRoots)
	}

	// Field(7): HistoricalRoots
	if b.isLeafDirty(HistoricalRootsLeafIndex) {
		parallelHasher.add(HistoricalRootsLeafIndex, b.historicalRoots)
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

	// Field(11): Validators
	if b.isLeafDirty(ValidatorsLeafIndex) {
		parallelHasher.add(ValidatorsLeafIndex, b.validators)
	}

	// Field(12): Balances
	if b.isLeafDirty(BalancesLeafIndex) {
		parallelHasher.add(BalancesLeafIndex, b.balances)
	}

	// Field(13): RandaoMixes
	if b.isLeafDirty(RandaoMixesLeafIndex) {
		parallelHasher.add(RandaoMixesLeafIndex, b.randaoMixes)
	}

	// Field(14): Slashings
	if b.isLeafDirty(SlashingsLeafIndex) {
		parallelHasher.add(SlashingsLeafIndex, b.slashings)
	}
	// Field(15) and Field(16) are special due to the fact that they have different format in Phase0.

	// Field(15): PreviousEpochParticipation
	if b.isLeafDirty(PreviousEpochParticipationLeafIndex) {
		if b.version == clparams.Phase0Version {
			parallelHasher.add(PreviousEpochParticipationLeafIndex, b.previousEpochAttestations)
		} else {
			parallelHasher.add(PreviousEpochParticipationLeafIndex, b.previousEpochParticipation)
		}
	}

	// Field(16): CurrentEpochParticipation
	if b.isLeafDirty(CurrentEpochParticipationLeafIndex) {
		if b.version == clparams.Phase0Version {
			parallelHasher.add(CurrentEpochParticipationLeafIndex, b.currentEpochAttestations)
		} else {
			parallelHasher.add(CurrentEpochParticipationLeafIndex, b.currentEpochParticipation)
		}
	}

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
		parallelHasher.run(b)
		return nil
	}

	// Field(21): Inactivity Scores
	if b.isLeafDirty(InactivityScoresLeafIndex) {
		parallelHasher.add(InactivityScoresLeafIndex, b.inactivityScores)
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
		parallelHasher.run(b)
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
		parallelHasher.run(b)
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
		parallelHasher.add(HistoricalSummariesLeafIndex, b.historicalSummaries)
	}
	parallelHasher.run(b)

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
