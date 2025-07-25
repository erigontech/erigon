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
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

func (b *BeaconState) HashSSZ() (out [32]byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err = b.computeDirtyLeaves(); err != nil {
		return [32]byte{}, err
	}
	// for i := 0; i < len(b.leaves); i += 32 {
	// 	fmt.Println(i/32, common.BytesToHash(b.leaves[i:i+32]))
	// }
	// Pad to 32 of length
	endIndex := StateLeafSizeDeneb * 32
	if b.Version() >= clparams.ElectraVersion {
		endIndex = StateLeafSizeElectra * 32
	}
	if b.Version() >= clparams.FuluVersion {
		endIndex = StateLeafSizeFulu * 32
	}
	err = merkle_tree.MerkleRootFromFlatLeaves(b.leaves[:endIndex], out[:])
	return
}

func (b *BeaconState) PrintLeaves() {
	fmt.Println("TRACE: BeaconState leaves:")
	for i := 0; i < len(b.leaves); i += 32 {
		fmt.Println(i/32, common.BytesToHash(b.leaves[i:i+32]))
	}
}

func (b *BeaconState) CurrentSyncCommitteeBranch() ([][32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return nil, err
	}
	depth := 5
	leafSize := StateLeafSizeDeneb
	if b.Version() >= clparams.ElectraVersion {
		depth = 6
		leafSize = StateLeafSizeElectra
	}
	if b.Version() >= clparams.FuluVersion {
		depth = 6
		leafSize = StateLeafSizeFulu
	}

	schema := []interface{}{}
	for i := 0; i < leafSize*32; i += 32 {
		schema = append(schema, b.leaves[i:i+32])
	}

	return merkle_tree.MerkleProof(depth, 22, schema...)
}

func (b *BeaconState) NextSyncCommitteeBranch() ([][32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return nil, err
	}
	depth := 5
	leafSize := StateLeafSizeDeneb
	if b.Version() >= clparams.ElectraVersion {
		depth = 6
		leafSize = StateLeafSizeElectra
	}
	if b.Version() >= clparams.FuluVersion {
		depth = 6
		leafSize = StateLeafSizeFulu
	}

	schema := []interface{}{}
	for i := 0; i < leafSize*32; i += 32 {
		schema = append(schema, b.leaves[i:i+32])
	}
	return merkle_tree.MerkleProof(depth, 23, schema...)
}

func (b *BeaconState) FinalityRootBranch() ([][32]byte, error) {
	if err := b.computeDirtyLeaves(); err != nil {
		return nil, err
	}
	depth := 5
	leafSize := StateLeafSizeDeneb
	if b.Version() >= clparams.ElectraVersion {
		depth = 6
		leafSize = StateLeafSizeElectra
	}
	if b.Version() >= clparams.FuluVersion {
		depth = 6
		leafSize = StateLeafSizeFulu
	}

	schema := []interface{}{}
	for i := 0; i < leafSize*32; i += 32 {
		schema = append(schema, b.leaves[i:i+32])
	}
	proof, err := merkle_tree.MerkleProof(depth, 20, schema...)
	if err != nil {
		return nil, err
	}

	proof = append([][32]byte{merkle_tree.Uint64Root(b.finalizedCheckpoint.Epoch)}, proof...)
	return proof, nil
}

type beaconStateHasher struct {
	b    *BeaconState
	jobs map[StateLeafIndex]any
}

func (p *beaconStateHasher) run() {
	wg := sync.WaitGroup{}
	if p.jobs == nil {
		p.jobs = make(map[StateLeafIndex]any)
	}

	for idx, job := range p.jobs {
		wg.Add(1)
		go func(idx StateLeafIndex, job any) {
			defer wg.Done()
			switch obj := job.(type) {
			case ssz.HashableSSZ:
				root, err := obj.HashSSZ()
				if err != nil {
					panic(err)
				}
				p.b.updateLeaf(idx, root)
			case uint64:
				p.b.updateLeaf(idx, merkle_tree.Uint64Root(obj))
			case common.Hash:
				p.b.updateLeaf(idx, obj)
			}

		}(idx, job)
	}
	wg.Wait()
}

func (p *beaconStateHasher) add(idx StateLeafIndex, job any) {
	if !p.b.isLeafDirty(idx) {
		return
	}

	if p.jobs == nil {
		p.jobs = make(map[StateLeafIndex]any)
	}
	p.jobs[idx] = job
}

func (b *BeaconState) computeDirtyLeaves() error {
	beaconStateHasher := &beaconStateHasher{b: b}
	// Update all dirty leafs.
	beaconStateHasher.add(GenesisTimeLeafIndex, b.genesisTime)
	beaconStateHasher.add(GenesisValidatorsRootLeafIndex, b.genesisValidatorsRoot)
	beaconStateHasher.add(SlotLeafIndex, b.slot)
	beaconStateHasher.add(ForkLeafIndex, b.fork)
	beaconStateHasher.add(LatestBlockHeaderLeafIndex, b.latestBlockHeader)
	beaconStateHasher.add(BlockRootsLeafIndex, b.blockRoots)
	beaconStateHasher.add(StateRootsLeafIndex, b.stateRoots)
	beaconStateHasher.add(HistoricalRootsLeafIndex, b.historicalRoots)
	beaconStateHasher.add(Eth1DataLeafIndex, b.eth1Data)
	beaconStateHasher.add(Eth1DataVotesLeafIndex, b.eth1DataVotes)
	beaconStateHasher.add(Eth1DepositIndexLeafIndex, b.eth1DepositIndex)
	beaconStateHasher.add(ValidatorsLeafIndex, b.validators)
	beaconStateHasher.add(BalancesLeafIndex, b.balances)
	beaconStateHasher.add(RandaoMixesLeafIndex, b.randaoMixes)
	beaconStateHasher.add(SlashingsLeafIndex, b.slashings)
	// Special case for Participation, if phase0 use attestation format, otherwise use bitlist format.
	if b.version == clparams.Phase0Version {
		beaconStateHasher.add(PreviousEpochParticipationLeafIndex, b.previousEpochAttestations)
		beaconStateHasher.add(CurrentEpochParticipationLeafIndex, b.currentEpochAttestations)
	} else {
		beaconStateHasher.add(PreviousEpochParticipationLeafIndex, b.previousEpochParticipation)
		beaconStateHasher.add(CurrentEpochParticipationLeafIndex, b.currentEpochParticipation)
	}

	// Field(17): JustificationBits
	root, _ := b.justificationBits.HashSSZ()
	b.updateLeaf(JustificationBitsLeafIndex, root)

	beaconStateHasher.add(PreviousJustifiedCheckpointLeafIndex, &b.previousJustifiedCheckpoint)
	beaconStateHasher.add(CurrentJustifiedCheckpointLeafIndex, &b.currentJustifiedCheckpoint)
	beaconStateHasher.add(FinalizedCheckpointLeafIndex, &b.finalizedCheckpoint)

	if b.version >= clparams.AltairVersion {
		// Altair fields
		beaconStateHasher.add(InactivityScoresLeafIndex, b.inactivityScores)
		beaconStateHasher.add(CurrentSyncCommitteeLeafIndex, b.currentSyncCommittee)
		beaconStateHasher.add(NextSyncCommitteeLeafIndex, b.nextSyncCommittee)
	}

	if b.version >= clparams.BellatrixVersion {
		// Bellatrix fields
		beaconStateHasher.add(LatestExecutionPayloadHeaderLeafIndex, b.latestExecutionPayloadHeader)
	}

	if b.version >= clparams.CapellaVersion {
		// Capella fields
		beaconStateHasher.add(NextWithdrawalIndexLeafIndex, b.nextWithdrawalIndex)
		beaconStateHasher.add(NextWithdrawalValidatorIndexLeafIndex, b.nextWithdrawalValidatorIndex)
		beaconStateHasher.add(HistoricalSummariesLeafIndex, b.historicalSummaries)
	}

	if b.version >= clparams.ElectraVersion {
		// Electra fields
		beaconStateHasher.add(DepositRequestsStartIndexLeafIndex, b.depositRequestsStartIndex)
		beaconStateHasher.add(DepositBalanceToConsumeLeafIndex, b.depositBalanceToConsume)
		beaconStateHasher.add(ExitBalanceToConsumeLeafIndex, b.exitBalanceToConsume)
		beaconStateHasher.add(EarliestExitEpochLeafIndex, b.earliestExitEpoch)
		beaconStateHasher.add(ConsolidationBalanceToConsumeLeafIndex, b.consolidationBalanceToConsume)
		beaconStateHasher.add(EarliestConsolidationEpochLeafIndex, b.earliestConsolidationEpoch)
		beaconStateHasher.add(PendingDepositsLeafIndex, b.pendingDeposits)
		beaconStateHasher.add(PendingPartialWithdrawalsLeafIndex, b.pendingPartialWithdrawals)
		beaconStateHasher.add(PendingConsolidationsLeafIndex, b.pendingConsolidations)
	}

	if b.version >= clparams.FuluVersion {
		beaconStateHasher.add(ProposerLookaheadLeafIndex, b.proposerLookahead)
	}

	beaconStateHasher.run()

	return nil
}

// updateLeaf updates the leaf with the new value and marks it as clean. It's safe to call this function concurrently.
func (b *BeaconState) updateLeaf(idx StateLeafIndex, leaf common.Hash) {
	// Update leaf with new value.
	copy(b.leaves[idx*32:], leaf[:])
	// Now leaf is clean :).
	b.touchedLeaves[idx].Store(LeafCleanValue)
}

func (b *BeaconState) isLeafDirty(idx StateLeafIndex) bool {
	// If leaf is non-initialized or if it was touched then we change it.
	v := b.touchedLeaves[idx].Load()
	return v == LeafInitValue || v == LeafDirtyValue
}

func (b *BeaconState) markLeaf(idxs ...StateLeafIndex) {
	for _, idx := range idxs {
		b.touchedLeaves[idx].Store(LeafDirtyValue)
	}
}
