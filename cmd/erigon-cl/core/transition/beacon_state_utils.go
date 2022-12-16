package transition

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

const SHUFFLE_ROUND_COUNT = uint8(90)

func ComputeShuffledIndex(ind, ind_count uint64, seed [32]byte) (uint64, error) {
	if ind >= ind_count {
		return 0, fmt.Errorf("index=%d must be less than the index count=%d", ind, ind_count)
	}

	for i := uint8(0); i < SHUFFLE_ROUND_COUNT; i++ {
		// Construct first hash input.
		input := append(seed[:], i)
		hash := sha256.New()
		hash.Write(input)

		// Read hash value.
		hashValue := binary.LittleEndian.Uint64(hash.Sum(nil)[:8])

		// Caclulate pivot and flip.
		pivot := hashValue % ind_count
		flip := (pivot + ind_count - ind) % ind_count

		// No uint64 max function in go standard library.
		position := ind
		if flip > ind {
			position = flip
		}

		// Construct the second hash input.
		positionByteArray := make([]byte, 4)
		binary.LittleEndian.PutUint32(positionByteArray, uint32(position>>8))
		input2 := append(seed[:], i)
		input2 = append(input2, positionByteArray...)

		hash.Reset()
		hash.Write(input2)
		// Read hash value.
		source := hash.Sum(nil)
		byteVal := source[(position%256)/8]
		bitVal := (byteVal >> (position % 8)) % 2
		if bitVal == 1 {
			ind = flip
		}
	}
	return ind, nil
}

func ComputePropserIndex(state *cltypes.BeaconStateBellatrix, indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, fmt.Errorf("must have >0 indices")
	}
	maxRandomByte := uint64(1<<8 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	hash := sha256.New()
	buf := make([]byte, 8)
	for {
		shuffled, err := ComputeShuffledIndex(i%total, total, seed)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		if candidateIndex >= uint64(len(state.Validators)) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, len(state.Validators))
		}
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		hash.Reset()
		hash.Write(input)
		randomByte := uint64(hash.Sum(nil)[i%32])
		effectiveBalance := state.Validators[candidateIndex].EffectiveBalance
		if effectiveBalance*maxRandomByte >= clparams.MainnetBeaconConfig.MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}
