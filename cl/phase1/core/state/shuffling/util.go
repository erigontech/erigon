package shuffling

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/eth2shuffle"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
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

		// Caclulate pivot and flip.
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
		ret[i] = utils.Keccak256(append(seed[:], byte(i)))
	}
	return ret
}

func GetSeed(beaconConfig *clparams.BeaconChainConfig, mix common.Hash, epoch uint64, domain [4]byte) common.Hash {
	epochByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochByteArray, epoch)
	input := append(domain[:], epochByteArray...)
	input = append(input, mix[:]...)
	return utils.Keccak256(input)
}

func ComputeShuffledIndicies(beaconConfig *clparams.BeaconChainConfig, mix common.Hash, indicies []uint64, slot uint64) []uint64 {
	shuffledIndicies := make([]uint64, len(indicies))
	copy(shuffledIndicies, indicies)
	hashFunc := utils.OptimizedKeccak256NotThreadSafe()
	epoch := slot / beaconConfig.SlotsPerEpoch
	seed := GetSeed(beaconConfig, mix, epoch, beaconConfig.DomainBeaconAttester)
	eth2ShuffleHashFunc := func(data []byte) []byte {
		hashed := hashFunc(data)
		return hashed[:]
	}
	eth2shuffle.UnshuffleList(eth2ShuffleHashFunc, shuffledIndicies, uint8(beaconConfig.ShuffleRoundCount), seed)
	return shuffledIndicies
}
