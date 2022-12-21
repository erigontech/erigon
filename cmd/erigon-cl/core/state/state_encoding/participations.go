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

func packParticipationBits(bytes []byte) ([][32]byte, error) {
	var chunks [][32]byte
	for i := 0; i < len(bytes); i += 32 {
		var chunk [32]byte
		copy(chunk[:], bytes[i:])
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}
