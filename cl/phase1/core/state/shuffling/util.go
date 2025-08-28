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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth2shuffle"
)

func ComputeShuffledIndex(conf *clparams.BeaconChainConfig, ind, ind_count uint64, seed [32]byte, preInputs [][32]byte, hashFunc utils.HashFunc) (uint64, error) {
	if ind >= ind_count {
		return 0, fmt.Errorf("index=%d must be less than the index count=%d", ind, ind_count)
	}
	if len(preInputs) == 0 {
		preInputs = ComputeShuffledIndexPreInputs(conf, seed)
	}
	input2 := make([]byte, 32+1+4)
	for i := uint64(0); i < conf.ShuffleRoundCount; i++ {
		// Read hash value.
		hashValue := binary.LittleEndian.Uint64(preInputs[i][:8])

		// Calculate pivot and flip.
		pivot := hashValue % ind_count
		flip := (pivot + ind_count - ind) % ind_count

		// No uint64 max function in go standard library.
		position := ind
		if flip > ind {
			position = flip
		}
		// Construct the second hash input.
		copy(input2, seed[:])
		input2[32] = byte(i)
		binary.LittleEndian.PutUint32(input2[33:], uint32(position>>8))
		hashedInput2 := hashFunc(input2)
		// Read hash value.
		byteVal := hashedInput2[(position%256)/8]
		bitVal := (byteVal >> (position % 8)) % 2
		if bitVal == 1 {
			ind = flip
		}
	}
	return ind, nil
}

func ComputeShuffledIndexPreInputs(conf *clparams.BeaconChainConfig, seed [32]byte) [][32]byte {
	ret := make([][32]byte, conf.ShuffleRoundCount)
	for i := range ret {
		ret[i] = utils.Sha256(append(seed[:], byte(i)))
	}
	return ret
}

func GetSeed(beaconConfig *clparams.BeaconChainConfig, mix common.Hash, epoch uint64, domain [4]byte) common.Hash {
	epochByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochByteArray, epoch)
	input := append(domain[:], epochByteArray...)
	input = append(input, mix[:]...)
	return utils.Sha256(input)
}

func ComputeShuffledIndicies(beaconConfig *clparams.BeaconChainConfig, mix common.Hash, out, indicies []uint64, slot uint64) []uint64 {
	copy(out, indicies)
	hashFunc := utils.OptimizedSha256NotThreadSafe()
	epoch := slot / beaconConfig.SlotsPerEpoch
	seed := GetSeed(beaconConfig, mix, epoch, beaconConfig.DomainBeaconAttester)
	eth2ShuffleHashFunc := func(data []byte) []byte {
		hashed := hashFunc(data)
		return hashed[:]
	}
	eth2shuffle.UnshuffleList(eth2ShuffleHashFunc, out, uint8(beaconConfig.ShuffleRoundCount), seed)
	return out
}
