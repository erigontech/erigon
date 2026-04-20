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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/ssz"
)

// BlockRoot computes hash_tree_root(latest_block_header) — the block root for the current state.
//
// LatestBlockHeader.Root (the state_root field) follows a deferred-fill pattern from the spec:
//   - process_block_header sets it to Bytes32() (zero) because the state is still being modified.
//   - It is backfilled later by either:
//     (a) process_slot at the start of the next slot, or
//     (b) process_execution_payload (GLOAS/EIP-7732) before envelope processing in the same slot.
//
// In GLOAS the envelope further mutates the state after the block, but the block root must
// reflect the pre-envelope state. Since (b) locks Root before those mutations, we must use
// the already-stored value when present rather than recomputing HashSSZ() on a post-envelope state.
func (b *BeaconState) BlockRoot() ([32]byte, error) {
	var root [32]byte
	if b.version >= clparams.GloasVersion {
		// GLOAS: use the stored state_root if already backfilled by process_slot or
		// ProcessExecutionPayloadEnvelope; fall back to HashSSZ() only when Root is
		// still zero (i.e. right after process_block_header, before envelope processing).
		root = b.latestBlockHeader.Root
		if root == [32]byte{} {
			var err error
			root, err = b.HashSSZ()
			if err != nil {
				return [32]byte{}, err
			}
		}
	} else {
		// Pre-GLOAS: always compute the current state root (original behaviour).
		var err error
		root, err = b.HashSSZ()
		if err != nil {
			return [32]byte{}, err
		}
	}
	// Reconstruct a complete header (with the resolved state_root) and hash it.
	return (&cltypes.BeaconBlockHeader{
		Slot:          b.latestBlockHeader.Slot,
		ProposerIndex: b.latestBlockHeader.ProposerIndex,
		BodyRoot:      b.latestBlockHeader.BodyRoot,
		ParentRoot:    b.latestBlockHeader.ParentRoot,
		Root:          root,
	}).HashSSZ()
}

// baseOffsetSSZ computes the fixed portion of the SSZ encoding (all fixed-size
// fields plus 4-byte offsets for each variable-length field). This is derived
// from beaconConfig so it works with any preset (mainnet, minimal, custom).
func (b *BeaconState) baseOffsetSSZ() uint32 {
	cfg := b.beaconConfig
	size := uint32(0)

	// Phase0 base fields (all versions):
	size += 8                                          // genesis_time
	size += 32                                         // genesis_validators_root
	size += 8                                          // slot
	size += 16                                         // fork (prev_version + cur_version + epoch)
	size += 112                                        // latest_block_header
	size += uint32(cfg.SlotsPerHistoricalRoot) * 32    // block_roots (Vector[Hash, SLOTS_PER_HISTORICAL_ROOT])
	size += uint32(cfg.SlotsPerHistoricalRoot) * 32    // state_roots (Vector[Hash, SLOTS_PER_HISTORICAL_ROOT])
	size += 4                                          // historical_roots offset (List, variable)
	size += 72                                         // eth1_data
	size += 4                                          // eth1_data_votes offset (List, variable)
	size += 8                                          // eth1_deposit_index
	size += 4                                          // validators offset (List, variable)
	size += 4                                          // balances offset (List, variable)
	size += uint32(cfg.EpochsPerHistoricalVector) * 32 // randao_mixes (Vector[Hash, EPOCHS_PER_HISTORICAL_VECTOR])
	size += uint32(cfg.EpochsPerSlashingsVector) * 8   // slashings (Vector[uint64, EPOCHS_PER_SLASHINGS_VECTOR])

	if b.version == clparams.Phase0Version {
		size += 4 // previous_epoch_attestations offset
		size += 4 // current_epoch_attestations offset
	} else {
		// Altair+
		size += 4 // previous_epoch_participation offset
		size += 4 // current_epoch_participation offset
	}

	size += 1  // justification_bits
	size += 40 // previous_justified_checkpoint (epoch + root)
	size += 40 // current_justified_checkpoint
	size += 40 // finalized_checkpoint

	if b.version >= clparams.AltairVersion {
		size += 4                                     // inactivity_scores offset (List, variable)
		size += uint32(cfg.SyncCommitteeSize)*48 + 48 // current_sync_committee
		size += uint32(cfg.SyncCommitteeSize)*48 + 48 // next_sync_committee
	}

	if b.version >= clparams.BellatrixVersion {
		size += 4 // latest_execution_payload_header offset (variable)
	}

	if b.version >= clparams.CapellaVersion {
		size += 8 // next_withdrawal_index
		size += 8 // next_withdrawal_validator_index
		size += 4 // historical_summaries offset (List, variable)
	}

	if b.version >= clparams.ElectraVersion {
		size += 8 // deposit_requests_start_index
		size += 8 // deposit_balance_to_consume
		size += 8 // exit_balance_to_consume
		size += 8 // earliest_exit_epoch
		size += 8 // consolidation_balance_to_consume
		size += 8 // earliest_consolidation_epoch
		size += 4 // pending_deposits offset
		size += 4 // pending_partial_withdrawals offset
		size += 4 // pending_consolidations offset
	}

	if b.version >= clparams.FuluVersion {
		size += uint32((cfg.MinSeedLookahead+1)*cfg.SlotsPerEpoch) * 8 // proposer_lookahead (Vector)
	}

	return size
}

func (b *BeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

// getSchema gives the schema for the current beacon state version according to ETH 2.0 specs.
func (b *BeaconState) getSchema() []any {
	s := []any{&b.genesisTime, b.genesisValidatorsRoot[:], &b.slot, b.fork, b.latestBlockHeader, b.blockRoots, b.stateRoots, b.historicalRoots,
		b.eth1Data, b.eth1DataVotes, &b.eth1DepositIndex, b.validators, b.balances, b.randaoMixes, b.slashings}
	if b.version == clparams.Phase0Version {
		return append(s, b.previousEpochAttestations, b.currentEpochAttestations, &b.justificationBits, &b.previousJustifiedCheckpoint, &b.currentJustifiedCheckpoint,
			&b.finalizedCheckpoint)
	}
	s = append(s, b.previousEpochParticipation, b.currentEpochParticipation, &b.justificationBits, &b.previousJustifiedCheckpoint, &b.currentJustifiedCheckpoint,
		&b.finalizedCheckpoint, b.inactivityScores, b.currentSyncCommittee, b.nextSyncCommittee)
	if b.version >= clparams.BellatrixVersion {
		// Position 24: pre-Gloas holds latestExecutionPayloadHeader; Gloas replaces it with latestBlockHash (consensus-specs #5113)
		if b.version >= clparams.GloasVersion {
			s = append(s, b.latestBlockHash[:])
		} else {
			s = append(s, b.latestExecutionPayloadHeader)
		}
	}
	if b.version >= clparams.CapellaVersion {
		s = append(s, &b.nextWithdrawalIndex, &b.nextWithdrawalValidatorIndex, b.historicalSummaries)
	}
	if b.version >= clparams.ElectraVersion {
		// Electra fields
		s = append(s, &b.depositRequestsStartIndex, &b.depositBalanceToConsume, &b.exitBalanceToConsume, &b.earliestExitEpoch, &b.consolidationBalanceToConsume,
			&b.earliestConsolidationEpoch, b.pendingDeposits, b.pendingPartialWithdrawals, b.pendingConsolidations)
	}
	if b.version >= clparams.FuluVersion {
		s = append(s, b.proposerLookahead)
	}
	if b.version >= clparams.GloasVersion {
		s = append(s, b.builders, &b.nextWithdrawalBuilderIndex, b.executionPayloadAvailability, b.builderPendingPayments, b.builderPendingWithdrawals, b.latestExecutionPayloadBid, b.payloadExpectedWithdrawals, b.ptcWindow)
	}
	return s
}

func (b *BeaconState) DecodeSSZ(buf []byte, version int) error {
	b.version = clparams.StateVersion(version)
	if len(buf) < int(b.baseOffsetSSZ()) {
		return fmt.Errorf("[BeaconState] err: %s", ssz.ErrLowBufferSize)
	}
	if version >= int(clparams.BellatrixVersion) {
		b.latestExecutionPayloadHeader = cltypes.NewEth1Header(clparams.StateVersion(version))
	}
	if version >= int(clparams.ElectraVersion) {
		b.pendingDeposits = solid.NewPendingDepositList(b.beaconConfig)
		b.pendingPartialWithdrawals = solid.NewPendingWithdrawalList(b.beaconConfig)
		b.pendingConsolidations = solid.NewPendingConsolidationList(b.beaconConfig)
	}
	if version >= int(clparams.FuluVersion) {
		b.proposerLookahead = solid.NewUint64VectorSSZ(int((b.beaconConfig.MinSeedLookahead + 1) * b.beaconConfig.SlotsPerEpoch))
	}
	if version >= int(clparams.GloasVersion) {
		b.latestExecutionPayloadBid = &cltypes.ExecutionPayloadBid{}
		b.builders = solid.NewStaticListSSZ[*cltypes.Builder](int(b.beaconConfig.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
		b.nextWithdrawalBuilderIndex = 0
		b.executionPayloadAvailability = solid.NewBitVector(int(b.beaconConfig.SlotsPerHistoricalRoot))
		b.builderPendingPayments = solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * b.beaconConfig.SlotsPerEpoch))
		b.builderPendingWithdrawals = solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](int(b.beaconConfig.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
		b.latestBlockHash = common.Hash{}
		b.payloadExpectedWithdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(b.beaconConfig.MaxWithdrawalsPerPayload), new(cltypes.Withdrawal).EncodingSizeSSZ())
		b.ptcWindow = solid.NewUint64VectorOfVectors(int((2+b.beaconConfig.MinSeedLookahead)*b.beaconConfig.SlotsPerEpoch), int(clparams.PtcSize))
	}
	if err := ssz2.UnmarshalSSZ(buf, version, b.getSchema()...); err != nil {
		return err
	}
	// Capella
	return b.init()
}

// SSZ size of the Beacon State
func (b *BeaconState) EncodingSizeSSZ() (size int) {
	// Start with the fixed portion (includes offset pointers for variable-size fields).
	size = int(b.baseOffsetSSZ())
	// Add the variable-size field data (only fields where Static() == false).
	size += b.historicalRoots.EncodingSizeSSZ()
	size += b.eth1DataVotes.EncodingSizeSSZ()
	size += b.validators.EncodingSizeSSZ()
	size += b.balances.Length() * 8
	if b.version == clparams.Phase0Version {
		size += b.previousEpochAttestations.EncodingSizeSSZ()
		size += b.currentEpochAttestations.EncodingSizeSSZ()
	} else {
		size += b.previousEpochParticipation.Length()
		size += b.currentEpochParticipation.Length()
		size += b.inactivityScores.Length() * 8
	}
	if b.version >= clparams.BellatrixVersion && b.version < clparams.GloasVersion {
		size += b.latestExecutionPayloadHeader.EncodingSizeSSZ()
	}
	if b.version >= clparams.CapellaVersion {
		size += b.historicalSummaries.EncodingSizeSSZ()
	}
	if b.version >= clparams.ElectraVersion {
		size += b.pendingDeposits.EncodingSizeSSZ()
		size += b.pendingPartialWithdrawals.EncodingSizeSSZ()
		size += b.pendingConsolidations.EncodingSizeSSZ()
	}
	if b.version >= clparams.GloasVersion {
		size += b.builders.EncodingSizeSSZ()
		size += b.builderPendingWithdrawals.EncodingSizeSSZ()
		size += b.latestExecutionPayloadBid.EncodingSizeSSZ()
		size += b.payloadExpectedWithdrawals.EncodingSizeSSZ()
	}
	return
}

func (b *BeaconState) Clone() clonable.Clonable {
	return &BeaconState{beaconConfig: b.beaconConfig}
}
