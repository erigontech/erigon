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

	"github.com/erigontech/erigon/cl/phase1/core/state/raw"

	"github.com/erigontech/erigon/cl/utils"
)

func ComputeProposerIndex(b *raw.BeaconState, indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, nil
	}
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
		if validator.EffectiveBalance()*maxRandomByte >= b.BeaconConfig().MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}
