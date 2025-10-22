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

package shuffling

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"

	"github.com/erigontech/erigon/cl/utils"
)

func ComputeProposerIndex(b *raw.BeaconState, indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, nil
	}
	if b.Version() >= clparams.ElectraVersion {
		return computeProposerIndexElectra(b, indices, seed)
	}

	// before electra case
	maxRandomByte := uint64(1<<8 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	input := make([]byte, 40)
	preInputs := ComputeShuffledIndexPreInputs(b.BeaconConfig(), seed)
	for {
		shuffled, err := ComputeShuffledIndex(b.BeaconConfig(), i%total, total, seed, preInputs, utils.Sha256)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		if candidateIndex >= uint64(b.ValidatorLength()) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, b.ValidatorLength())
		}
		copy(input, seed[:])
		binary.LittleEndian.PutUint64(input[32:], i/32)
		randomByte := uint64(utils.Sha256(input)[i%32])
		validator, err := b.ValidatorForValidatorIndex(int(candidateIndex))
		if err != nil {
			return 0, err
		}
		if validator.EffectiveBalance()*maxRandomByte >= b.BeaconConfig().MaxEffectiveBalanceForVersion(b.Version())*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}

func computeProposerIndexElectra(b *raw.BeaconState, indices []uint64, seed [32]byte) (uint64, error) {
	maxRandomValue := uint64(1<<16 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	input := make([]byte, 40)
	preInputs := ComputeShuffledIndexPreInputs(b.BeaconConfig(), seed)
	for {
		shuffled, err := ComputeShuffledIndex(b.BeaconConfig(), i%total, total, seed, preInputs, utils.Sha256)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		// [Modified in Electra]
		// random_bytes = hash(seed + uint_to_bytes(i // 16))
		// offset = i % 16 * 2
		// random_value = bytes_to_uint64(random_bytes[offset:offset + 2])
		copy(input, seed[:])
		binary.LittleEndian.PutUint64(input[32:], i/16)
		randomBytes := utils.Sha256(input)
		offset := (i % 16) * 2
		randomValue := binary.LittleEndian.Uint16(randomBytes[offset : offset+2])

		validator, err := b.ValidatorForValidatorIndex(int(candidateIndex))
		if err != nil {
			return 0, err
		}
		if validator.EffectiveBalance()*maxRandomValue >= b.BeaconConfig().MaxEffectiveBalanceForVersion(b.Version())*uint64(randomValue) {
			return candidateIndex, nil
		}
		i += 1
	}
}

func ComputeProposerIndices(b *raw.BeaconState, epoch uint64, seed [32]byte, indices []uint64) ([]uint64, error) {
	startSlot := epoch * b.BeaconConfig().SlotsPerEpoch
	proposerIndices := make([]uint64, b.BeaconConfig().SlotsPerEpoch)

	// Generate seed for each slot
	input := make([]byte, 40)
	copy(input, seed[:])
	for i := uint64(0); i < b.BeaconConfig().SlotsPerEpoch; i++ {
		// Hash seed + slot to get per-slot seed
		binary.LittleEndian.PutUint64(input[32:], startSlot+i)
		slotSeed := utils.Sha256(input)

		// Compute proposer index for this slot
		proposerIndex, err := ComputeProposerIndex(b, indices, slotSeed)
		if err != nil {
			return nil, err
		}
		proposerIndices[i] = proposerIndex
	}

	return proposerIndices, nil
}
