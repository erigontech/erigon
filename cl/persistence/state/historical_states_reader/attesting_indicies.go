package historical_states_reader

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (r *HistoricalStatesReader) attestingIndicies(attestation solid.AttestationData, aggregationBits []byte, checkBitsLength bool, randaoMixes solid.HashVectorSSZ, idxs []uint64) ([]uint64, error) {
	slot := attestation.Slot()
	committeesPerSlot := committeeCount(r.cfg, slot/r.cfg.SlotsPerEpoch, idxs)
	committeeIndex := attestation.ValidatorIndex()
	index := (slot%r.cfg.SlotsPerEpoch)*committeesPerSlot + committeeIndex
	count := committeesPerSlot * r.cfg.SlotsPerEpoch

	committee, err := r.computeCommittee(randaoMixes, idxs, attestation.Slot(), count, index)
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
func (r *HistoricalStatesReader) computeCommittee(randaoMixes solid.HashVectorSSZ, indicies []uint64, slot uint64, count, index uint64) ([]uint64, error) {
	cfg := r.cfg
	lenIndicies := uint64(len(indicies))

	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	var shuffledIndicies []uint64
	epoch := slot / cfg.SlotsPerEpoch

	mixPosition := (epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) %
		cfg.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := randaoMixes.Get(int(mixPosition))

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
