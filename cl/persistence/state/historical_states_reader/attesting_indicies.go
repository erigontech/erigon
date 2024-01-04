package historical_states_reader

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (r *HistoricalStatesReader) attestingIndicies(attestation solid.AttestationData, aggregationBits []byte, checkBitsLength bool, mix libcommon.Hash, idxs []uint64) ([]uint64, error) {
	slot := attestation.Slot()
	committeesPerSlot := committeeCount(r.cfg, slot/r.cfg.SlotsPerEpoch, idxs)
	committeeIndex := attestation.ValidatorIndex()
	index := (slot%r.cfg.SlotsPerEpoch)*committeesPerSlot + committeeIndex
	count := committeesPerSlot * r.cfg.SlotsPerEpoch

	committee, err := r.ComputeCommittee(mix, idxs, attestation.Slot(), count, index)
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
			return nil, fmt.Errorf("GetAttestingIndicies: committee is too big")
		}
		if (aggregationBits[sliceIndex] & (1 << bitIndex)) > 0 {
			attestingIndices = append(attestingIndices, member)
		}
	}
	return attestingIndices, nil
}

// computeCommittee uses cache to compute compittee
func (r *HistoricalStatesReader) ComputeCommittee(mix libcommon.Hash, indicies []uint64, slot uint64, count, index uint64) ([]uint64, error) {
	cfg := r.cfg
	lenIndicies := uint64(len(indicies))

	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	var shuffledIndicies []uint64
	epoch := slot / cfg.SlotsPerEpoch
	/*
	   mixPosition := (epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector
	*/
	if shuffledIndicesInterface, ok := r.shuffledSetsCache.Get(epoch); ok {
		shuffledIndicies = shuffledIndicesInterface
	} else {
		shuffledIndicies = make([]uint64, lenIndicies)
		shuffledIndicies = shuffling.ComputeShuffledIndicies(cfg, mix, shuffledIndicies, indicies, slot)
		r.shuffledSetsCache.Add(epoch, shuffledIndicies)
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

func (r *HistoricalStatesReader) readHistoricalBlockRoot(tx kv.Tx, slot, index uint64) (libcommon.Hash, error) {
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
	br, err := tx.GetOne(kv.BlockRoot, base_encoding.Encode64ToBytes4(slotLookup))
	if err != nil {
		return libcommon.Hash{}, err
	}
	if len(br) != 32 {
		return libcommon.Hash{}, fmt.Errorf("invalid block root length %d", len(br))
	}
	return libcommon.BytesToHash(br), nil

}

func (r *HistoricalStatesReader) getAttestationParticipationFlagIndicies(tx kv.Tx, stateSlot uint64, data solid.AttestationData, inclusionDelay uint64, skipAssert bool) ([]uint8, error) {
	currentCheckpoint, previousCheckpoint, _, err := state_accessors.ReadCheckpoints(tx, r.cfg.RoundSlotToEpoch(stateSlot))
	if err != nil {
		return nil, err
	}
	if currentCheckpoint == nil {
		currentCheckpoint = r.genesisState.CurrentJustifiedCheckpoint()
	}
	if previousCheckpoint == nil {
		previousCheckpoint = r.genesisState.PreviousJustifiedCheckpoint()
	}

	var justifiedCheckpoint solid.Checkpoint
	// get checkpoint from epoch
	if data.Target().Epoch() == stateSlot/r.cfg.SlotsPerEpoch {
		justifiedCheckpoint = currentCheckpoint
	} else {
		justifiedCheckpoint = previousCheckpoint
	}
	// Matching roots
	if !data.Source().Equal(justifiedCheckpoint) && !skipAssert {
		// jsonify the data.Source and justifiedCheckpoint
		jsonSource, err := data.Source().MarshalJSON()
		if err != nil {
			return nil, err
		}
		jsonJustifiedCheckpoint, err := justifiedCheckpoint.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("GetAttestationParticipationFlagIndicies: source does not match. source: %s, justifiedCheckpoint: %s", jsonSource, jsonJustifiedCheckpoint)
	}
	i := (data.Target().Epoch() * r.cfg.SlotsPerEpoch) % r.cfg.SlotsPerHistoricalRoot
	targetRoot, err := r.readHistoricalBlockRoot(tx, stateSlot, i)
	if err != nil {
		return nil, err
	}

	i = data.Slot() % r.cfg.SlotsPerHistoricalRoot
	headRoot, err := r.readHistoricalBlockRoot(tx, stateSlot, i)
	if err != nil {
		return nil, err
	}
	matchingTarget := data.Target().BlockRoot() == targetRoot
	matchingHead := matchingTarget && data.BeaconBlockRoot() == headRoot
	participationFlagIndicies := []uint8{}
	if inclusionDelay <= utils.IntegerSquareRoot(r.cfg.SlotsPerEpoch) {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelySourceFlagIndex)
	}
	if matchingTarget && inclusionDelay <= r.cfg.SlotsPerEpoch {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelyTargetFlagIndex)
	}
	if matchingHead && inclusionDelay == r.cfg.MinAttestationInclusionDelay {
		participationFlagIndicies = append(participationFlagIndicies, r.cfg.TimelyHeadFlagIndex)
	}
	return participationFlagIndicies, nil
}
