package transition

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
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
