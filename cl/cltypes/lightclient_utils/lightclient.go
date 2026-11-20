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

package lightclient_utils

import (
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// def create_light_client_update(state: BeaconState,
// 	block: SignedBeaconBlock,
// 	attested_state: BeaconState,
// 	attested_block: SignedBeaconBlock,
// 	finalized_block: Optional[SignedBeaconBlock]) -> LightClientUpdate:
// assert compute_epoch_at_slot(attested_state.slot) >= ALTAIR_FORK_EPOCH
// assert sum(block.message.body.sync_aggregate.sync_committee_bits) >= MIN_SYNC_COMMITTEE_PARTICIPANTS

// assert state.slot == state.latest_block_header.slot
// header = state.latest_block_header.copy()
// header.state_root = hash_tree_root(state)
// assert hash_tree_root(header) == hash_tree_root(block.message)
// update_signature_period = compute_sync_committee_period_at_slot(block.message.slot)
// assert attested_state.slot == attested_state.latest_block_header.slot
// attested_header = attested_state.latest_block_header.copy()
// attested_header.state_root = hash_tree_root(attested_state)
// assert hash_tree_root(attested_header) == hash_tree_root(attested_block.message) == block.message.parent_root
// update_attested_period = compute_sync_committee_period_at_slot(attested_block.message.slot)
// update = LightClientUpdate()
// update.attested_header = block_to_light_client_header(attested_block)
// # `next_sync_committee` is only useful if the message is signed by the current sync committee
// if update_attested_period == update_signature_period:
// update.next_sync_committee = attested_state.next_sync_committee
// update.next_sync_committee_branch = NextSyncCommitteeBranch(
// compute_merkle_proof(attested_state, NEXT_SYNC_COMMITTEE_GINDEX))
// # Indicate finality whenever possible
// if finalized_block is not None:
// if finalized_block.message.slot != GENESIS_SLOT:
// update.finalized_header = block_to_light_client_header(finalized_block)
// assert hash_tree_root(update.finalized_header.beacon) == attested_state.finalized_checkpoint.root
// else:
// assert attested_state.finalized_checkpoint.root == Bytes32()
// update.finality_branch = FinalityBranch(
// compute_merkle_proof(attested_state, FINALIZED_ROOT_GINDEX))

// update.sync_aggregate = block.message.body.sync_aggregate
// update.signature_slot = block.message.slot

// return update
// CreateLightClientUpdate implements the specs to initialize the light client update
func CreateLightClientUpdate(cfg *clparams.BeaconChainConfig, block *cltypes.SignedBeaconBlock, finalizedBlock *cltypes.SignedBeaconBlock,
	attestedBlock *cltypes.SignedBeaconBlock, attestedSlot uint64,
	attestedNextSyncCommittee *solid.SyncCommittee, attestedFinalizedCheckpoint solid.Checkpoint,
	attestedNextSyncCommitteeBranch, attestedFinalityBranch solid.HashVectorSSZ,
) (*cltypes.LightClientUpdate, error) {
	var err error
	if attestedBlock.Version() < clparams.AltairVersion {
		return nil, fmt.Errorf("attested slot %d is before altair fork epoch %d", attestedSlot, cfg.AltairForkEpoch)
	}
	if block.Block.Body.SyncAggregate.Sum() < int(cfg.MinSyncCommitteeParticipants) {
		return nil, fmt.Errorf("sync committee participants %d is less than minimum %d", block.Block.Body.SyncAggregate.Sum(), cfg.MinSyncCommitteeParticipants)
	}

	updateSignaturePeriod := cfg.SyncCommitteePeriod(block.Block.Slot)

	attestedHeader := attestedBlock.SignedBeaconBlockHeader()
	if attestedSlot != attestedHeader.Header.Slot {
		return nil, fmt.Errorf("attested slot %d is not equal to attested latest block header slot %d", attestedSlot, attestedHeader.Header.Slot)
	}
	updateAttestedPeriod := cfg.SyncCommitteePeriod(attestedBlock.Block.Slot)

	update := cltypes.NewLightClientUpdate(attestedBlock.Version(), cfg)
	update.AttestedHeader, err = BlockToLightClientHeader(attestedBlock)
	if err != nil {
		return nil, err
	}
	if updateAttestedPeriod == updateSignaturePeriod {
		update.NextSyncCommittee = attestedNextSyncCommittee
		update.NextSyncCommitteeBranch = attestedNextSyncCommitteeBranch
	}
	if finalizedBlock != nil {
		if finalizedBlock.Block.Slot != cfg.GenesisSlot {
			update.FinalizedHeader, err = blockToLightClientHeaderAt(finalizedBlock, attestedBlock.Version())
			if err != nil {
				return nil, err
			}
			finalizedBeaconRoot, err := update.FinalizedHeader.Beacon.HashSSZ()
			if err != nil {
				return nil, err
			}
			if finalizedBeaconRoot != attestedFinalizedCheckpoint.Root {
				return nil, fmt.Errorf("finalized beacon root %x is not equal to attested finalized checkpoint root %x", finalizedBeaconRoot, attestedFinalizedCheckpoint.Root)
			}
		} else if attestedFinalizedCheckpoint.Root != (common.Hash{}) {
			return nil, fmt.Errorf("attested finalized checkpoint root %x is not equal to zero hash", attestedFinalizedCheckpoint.Root)
		}
		update.FinalityBranch = attestedFinalityBranch
	}
	update.SyncAggregate = block.Block.Body.SyncAggregate
	update.SignatureSlot = block.Block.Slot
	return update, nil
}

// def block_to_light_client_header(block: SignedBeaconBlock) -> LightClientHeader:
//
//	return LightClientHeader(
//	    beacon=BeaconBlockHeader(
//	        slot=block.message.slot,
//	        proposer_index=block.message.proposer_index,
//	        parent_root=block.message.parent_root,
//	        state_root=block.message.state_root,
//	        body_root=hash_tree_root(block.message.body),
//	    ),
//	)
func BlockToLightClientHeader(block *cltypes.SignedBeaconBlock) (*cltypes.LightClientHeader, error) {
	return blockToLightClientHeaderAt(block, block.Version())
}

// blockToLightClientHeaderAt is block_to_light_client_header of the given pre-Gloas fork, which also takes a block of
// an earlier fork: before Capella the header carries no execution data, and payload fields of later forks stay zero.
// A Gloas header gets only the beacon block header.
func blockToLightClientHeaderAt(block *cltypes.SignedBeaconBlock, version clparams.StateVersion) (*cltypes.LightClientHeader, error) {
	h := cltypes.NewLightClientHeader(version)
	h.Beacon = block.SignedBeaconBlockHeader().Header
	if version < clparams.CapellaVersion || block.Version() < clparams.CapellaVersion {
		return h, nil
	}
	// [Modified in Gloas:EIP7732] ExecutionPayload not in BeaconBody for GLOAS blocks
	if version >= clparams.GloasVersion || block.Block.Body.ExecutionPayload == nil {
		return h, nil
	}
	var err error
	h.ExecutionPayloadHeader, err = block.Block.Body.ExecutionPayload.PayloadHeader()
	if err != nil {
		return nil, err
	}
	h.ExecutionPayloadHeader.SetVersion(version)
	payloadMerkleProof, err := block.Block.Body.ExecutionPayloadMerkleProof()
	if err != nil {
		return nil, err
	}
	payloadMerkleProofHashVector := solid.NewHashVector(len(payloadMerkleProof))
	for i := range payloadMerkleProof {
		payloadMerkleProofHashVector.Set(i, payloadMerkleProof[i])
	}
	h.ExecutionBranch = payloadMerkleProofHashVector
	return h, nil
}

// def create_light_client_bootstrap(state: BeaconState,
// 	block: SignedBeaconBlock) -> LightClientBootstrap:
// 	assert compute_epoch_at_slot(state.slot) >= ALTAIR_FORK_EPOCH

// 	assert state.slot == state.latest_block_header.slot
// 	header = state.latest_block_header.copy()
// 	header.state_root = hash_tree_root(state)
// 	assert hash_tree_root(header) == hash_tree_root(block.message)

// return LightClientBootstrap(
//
//	header=block_to_light_client_header(block),
//	current_sync_committee=state.current_sync_committee,
//	current_sync_committee_branch=CurrentSyncCommitteeBranch(
//	compute_merkle_proof(state, CURRENT_SYNC_COMMITTEE_GINDEX)),
//
// )
func CreateLightClientBootstrap(state *state.CachingBeaconState, block *cltypes.SignedBeaconBlock) (*cltypes.LightClientBootstrap, error) {
	cfg := state.BeaconConfig()
	if state.Version() < clparams.AltairVersion {
		return nil, fmt.Errorf("state slot %d is before altair fork epoch %d", state.Slot(), cfg.AltairForkEpoch)
	}

	if state.Slot() != state.LatestBlockHeader().Slot {
		return nil, fmt.Errorf("state slot %d is not equal to state latest block header slot %d", state.Slot(), state.LatestBlockHeader().Slot)
	}

	currentSyncCommitteeBranch, err := state.CurrentSyncCommitteeBranch()
	if err != nil {
		return nil, err
	}

	hashVector := solid.NewHashVector(len(currentSyncCommitteeBranch))
	for i := range currentSyncCommitteeBranch {
		hashVector.Set(i, currentSyncCommitteeBranch[i])
	}
	lcHeader, err := BlockToLightClientHeader(block)
	if err != nil {
		return nil, err
	}

	return &cltypes.LightClientBootstrap{
		Header:                     lcHeader,
		CurrentSyncCommittee:       state.CurrentSyncCommittee(),
		CurrentSyncCommitteeBranch: hashVector,
	}, nil
}

// IsBetterUpdate implements is_better_update: whether newUpdate is a better light client update for its sync
// committee period than oldUpdate.
func IsBetterUpdate(cfg *clparams.BeaconChainConfig, newUpdate, oldUpdate *cltypes.LightClientUpdate) bool {
	maxActiveParticipants := int(cfg.SyncCommitteeSize)
	newActiveParticipants := newUpdate.SyncAggregate.Sum()
	oldActiveParticipants := oldUpdate.SyncAggregate.Sum()
	newHasSupermajority := newActiveParticipants*3 >= maxActiveParticipants*2
	oldHasSupermajority := oldActiveParticipants*3 >= maxActiveParticipants*2
	if newHasSupermajority != oldHasSupermajority {
		return newHasSupermajority
	}
	if !newHasSupermajority && newActiveParticipants != oldActiveParticipants {
		return newActiveParticipants > oldActiveParticipants
	}

	newHasRelevantSyncCommittee := hasRelevantSyncCommittee(cfg, newUpdate)
	if oldHasRelevantSyncCommittee := hasRelevantSyncCommittee(cfg, oldUpdate); newHasRelevantSyncCommittee != oldHasRelevantSyncCommittee {
		return newHasRelevantSyncCommittee
	}

	newHasFinality := IsFinalityUpdate(newUpdate)
	if oldHasFinality := IsFinalityUpdate(oldUpdate); newHasFinality != oldHasFinality {
		return newHasFinality
	}

	if newHasFinality {
		newHasSyncCommitteeFinality := cfg.SyncCommitteePeriod(newUpdate.FinalizedHeader.Beacon.Slot) == cfg.SyncCommitteePeriod(newUpdate.AttestedHeader.Beacon.Slot)
		oldHasSyncCommitteeFinality := cfg.SyncCommitteePeriod(oldUpdate.FinalizedHeader.Beacon.Slot) == cfg.SyncCommitteePeriod(oldUpdate.AttestedHeader.Beacon.Slot)
		if newHasSyncCommitteeFinality != oldHasSyncCommitteeFinality {
			return newHasSyncCommitteeFinality
		}
	}

	if newActiveParticipants != oldActiveParticipants {
		return newActiveParticipants > oldActiveParticipants
	}
	if newUpdate.AttestedHeader.Beacon.Slot != oldUpdate.AttestedHeader.Beacon.Slot {
		return newUpdate.AttestedHeader.Beacon.Slot < oldUpdate.AttestedHeader.Beacon.Slot
	}
	return newUpdate.SignatureSlot < oldUpdate.SignatureSlot
}

// IsFinalityUpdate implements is_finality_update: whether the update proves a finalized header.
func IsFinalityUpdate(update *cltypes.LightClientUpdate) bool {
	return !isZeroBranch(update.FinalityBranch)
}

func hasRelevantSyncCommittee(cfg *clparams.BeaconChainConfig, update *cltypes.LightClientUpdate) bool {
	return !isZeroBranch(update.NextSyncCommitteeBranch) &&
		cfg.SyncCommitteePeriod(update.AttestedHeader.Beacon.Slot) == cfg.SyncCommitteePeriod(update.SignatureSlot)
}

func isZeroBranch(branch solid.HashVectorSSZ) bool {
	return len(common.TrimLeftZeroes(branch.Bytes())) == 0
}
