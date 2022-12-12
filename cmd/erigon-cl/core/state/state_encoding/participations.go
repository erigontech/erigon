package state_encoding

import "github.com/ledgerwatch/erigon/cl/utils"

// ParticipationBitsRoot computes the HashTreeRoot merkleization of
// participation roots.
func ParticipationBitsRoot(bits []byte) ([32]byte, error) {
	roots, err := packParticipationBits(bits)
	if err != nil {
		return [32]byte{}, err
	}

	base, err := MerkleizeVector(roots, uint64(ValidatorRegistryLimit+31)/32)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(bits)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

// packParticipationBits into chunks. It'll pad the last chunk with zero bytes if
// it does not have length bytes per chunk.
func packParticipationBits(bytes []byte) ([][32]byte, error) {
	numItems := len(bytes)
	chunks := make([][32]byte, 0, numItems/32)
	for i := 0; i < numItems; i += 32 {
		j := i + 32
		// We create our upper bound index of the chunk, if it is greater than numItems,
		// we set it as numItems itself.
		if j > numItems {
			j = numItems
		}
		// We create chunks from the list of items based on the
		// indices determined above.
		chunk := [32]byte{}
		copy(chunk[:], bytes[i:j])
		chunks = append(chunks, chunk)
	}

	if len(chunks) == 0 {
		return chunks, nil
	}

	return chunks, nil
}
