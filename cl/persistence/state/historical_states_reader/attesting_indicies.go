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

package historical_states_reader

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/kv"
)

func (r *HistoricalStatesReader) attestingIndicies(attestation *solid.Attestation, checkBitsLength bool, mix common.Hash, idxs []uint64) ([]uint64, error) {
	slot := attestation.Data.Slot
	epoch := slot / r.cfg.SlotsPerEpoch
	clversion := r.cfg.GetCurrentStateVersion(epoch)

	if clversion.BeforeOrEqual(clparams.DenebVersion) {
		// Deneb and earlier
		aggregationBits := attestation.AggregationBits.Bytes()
		committeesPerSlot := committeeCount(r.cfg, slot/r.cfg.SlotsPerEpoch, idxs)
		committeeIndex := attestation.Data.CommitteeIndex
		index := (slot%r.cfg.SlotsPerEpoch)*committeesPerSlot + committeeIndex
		count := committeesPerSlot * r.cfg.SlotsPerEpoch

		committee, err := r.ComputeCommittee(mix, idxs, slot, count, index)
		if err != nil {
			return nil, err
		}
		aggregationBitsLen := utils.GetBitlistLength(aggregationBits)
		if checkBitsLength && utils.GetBitlistLength(aggregationBits) != len(committee) {
			return nil, fmt.Errorf("GetAttestingIndicies: invalid aggregation bits. agg bits size: %d, expect: %d", aggregationBitsLen, len(committee))
		}

		attestingIndices := []uint64{}
		for i, member := range committee {
			bitIndex := i % 8
			sliceIndex := i / 8
			if sliceIndex >= len(aggregationBits) {
				return nil, errors.New("GetAttestingIndicies: committee is too big")
			}
			if (aggregationBits[sliceIndex] & (1 << bitIndex)) > 0 {
				attestingIndices = append(attestingIndices, member)
			}
		}
		return attestingIndices, nil
	}

	// Electra and later
	var (
		committeeBits   = attestation.CommitteeBits
		aggregationBits = attestation.AggregationBits
		aggrBitsLen     = aggregationBits.Bits()
		attesters       = []uint64{}
	)
	committeeOffset := 0
	for _, committeeIndex := range committeeBits.GetOnIndices() {
		committeesPerSlot := committeeCount(r.cfg, slot/r.cfg.SlotsPerEpoch, idxs)
		index := (slot%r.cfg.SlotsPerEpoch)*committeesPerSlot + uint64(committeeIndex)
		count := committeesPerSlot * r.cfg.SlotsPerEpoch
		committee, err := r.ComputeCommittee(mix, idxs, slot, count, index)
		if err != nil {
			return nil, err
		}
		for i, member := range committee {
			if i >= aggrBitsLen {
				return nil, fmt.Errorf("attestingIndicies: committee is too big, committeeOffset: %d, aggrBitsLen: %d, committeeSize: %d",
					committeeOffset, aggrBitsLen, len(committee))
			}
			if aggregationBits.GetBitAt(committeeOffset + i) {
				attesters = append(attesters, member)
			}
		}
		committeeOffset += len(committee)
	}
	return attesters, nil
}

// computeCommittee uses cache to compute compittee
func (r *HistoricalStatesReader) ComputeCommittee(mix common.Hash, indicies []uint64, slot uint64, count, index uint64) ([]uint64, error) {
	cfg := r.cfg
	lenIndicies := uint64(len(indicies))

	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	var shuffledIndicies []uint64

	shuffledIndicies, ok := r.shuffledIndiciesCache.Get(slot / cfg.SlotsPerEpoch)
	if !ok {
		shuffledIndicies = make([]uint64, lenIndicies)
		shuffledIndicies = shuffling.ComputeShuffledIndicies(cfg, mix, shuffledIndicies, indicies, slot)
		r.shuffledIndiciesCache.Add(slot/cfg.SlotsPerEpoch, shuffledIndicies)
	}

	return shuffledIndicies[start:end], nil
}

func committeeCount(cfg *clparams.BeaconChainConfig, epoch uint64, idxs []uint64) uint64 {
	committeCount := uint64(len(idxs)) / cfg.SlotsPerEpoch / cfg.TargetCommitteeSize
	if cfg.MaxCommitteesPerSlot < committeCount {
		committeCount = cfg.MaxCommitteesPerSlot
	}
	if committeCount < 1 {
		committeCount = 1
	}
	return committeCount
}

func (r *HistoricalStatesReader) readHistoricalBlockRoot(kvGetter state_accessors.GetValFn, slot, index uint64) (common.Hash, error) {
	slotSubIndex := slot % r.cfg.SlotsPerHistoricalRoot
	needFromGenesis := true

	var slotLookup uint64
	if index <= slotSubIndex {
		if slot > (slotSubIndex - index) {
			slotLookup = slot - (slotSubIndex - index)
			needFromGenesis = false
		}
	} else {
		if slot > (slotSubIndex + (r.cfg.SlotsPerHistoricalRoot - index)) {
			slotLookup = slot - (slotSubIndex + (r.cfg.SlotsPerHistoricalRoot - index))
			needFromGenesis = false
		}
	}

	if needFromGenesis {
		return r.genesisState.GetBlockRootAtSlot(slot)
	}
	br, err := kvGetter(kv.BlockRoot, base_encoding.Encode64ToBytes4(slotLookup))
	if err != nil {
		return common.Hash{}, err
	}
	if len(br) != 32 {
		return common.Hash{}, fmt.Errorf("invalid block root length %d", len(br))
	}
	return common.BytesToHash(br), nil

}

func (r *HistoricalStatesReader) getAttestationParticipationFlagIndicies(tx kv.Tx, getter state_accessors.GetValFn, version clparams.StateVersion, stateSlot uint64, data solid.AttestationData, inclusionDelay uint64, skipAssert bool) ([]uint8, error) {

	currentCheckpoint, previousCheckpoint, _, ok, err := state_accessors.ReadCheckpoints(getter, r.cfg.RoundSlotToEpoch(stateSlot), r.cfg)
	if err != nil {
		return nil, err
	}

	if !ok {
		currentCheckpoint = r.genesisState.CurrentJustifiedCheckpoint()
		previousCheckpoint = r.genesisState.PreviousJustifiedCheckpoint()
	}

	var justifiedCheckpoint solid.Checkpoint
	// get checkpoint from epoch
	if data.Target.Epoch == stateSlot/r.cfg.SlotsPerEpoch {
		justifiedCheckpoint = currentCheckpoint
	} else {
		justifiedCheckpoint = previousCheckpoint
	}
	// Matching roots
	if !data.Source.Equal(justifiedCheckpoint) && !skipAssert {
		return nil, errors.New("GetAttestationParticipationFlagIndicies: source does not match.")
	}
	i := (data.Target.Epoch * r.cfg.SlotsPerEpoch) % r.cfg.SlotsPerHistoricalRoot
	targetRoot, err := r.readHistoricalBlockRoot(getter, stateSlot, i)
	if err != nil {
		return nil, err
	}

	i = data.Slot % r.cfg.SlotsPerHistoricalRoot
	headRoot, err := r.readHistoricalBlockRoot(getter, stateSlot, i)
	if err != nil {
		return nil, err
	}
	matchingTarget := data.Target.Root == targetRoot
	matchingHead := matchingTarget && data.BeaconBlockRoot == headRoot
	participationFlagIndicies := []uint8{}
	if inclusionDelay <= utils.IntegerSquareRoot(r.cfg.SlotsPerEpoch) {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelySourceFlagIndex)
	}

	if matchingHead && inclusionDelay == r.cfg.MinAttestationInclusionDelay {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelyHeadFlagIndex)
	}
	if version < clparams.DenebVersion && matchingTarget && inclusionDelay <= r.cfg.SlotsPerEpoch {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelyTargetFlagIndex)
	}
	if version >= clparams.DenebVersion && matchingTarget {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelyTargetFlagIndex)
	}
	return participationFlagIndicies, nil
}
