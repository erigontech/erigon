package merkle_tree

import (
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/prysmaticlabs/gohashtree"
)

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeVector(elements [][32]byte, length uint64) ([32]byte, error) {
	depth := getDepth(length)
	// Return zerohash at depth
	if len(elements) == 0 {
		return ZeroHashes[depth], nil
	}
	for i := uint8(0); i < depth; i++ {
		layerLen := len(elements)
		oddNodeLength := layerLen%2 == 1
		if oddNodeLength {
			zerohash := ZeroHashes[i]
			elements = append(elements, zerohash)
		}
		outputLen := len(elements) / 2
		if err := gohashtree.Hash(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}
	return elements[0], nil
}

// ArraysRootWithLimit calculates the root hash of an array of hashes by first vectorizing the input array using the MerkleizeVector function, then calculating the root hash of the vectorized array using the Keccak256 function and the root hash of the length of the input array.
func ArraysRootWithLimit(input [][32]byte, limit uint64) ([32]byte, error) {
	base, err := MerkleizeVector(input, limit)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(input)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

// ArraysRoot calculates the root hash of an array of hashes by first making a copy of the input array, then calculating the Merkle root of the copy using the MerkleRootFromLeaves function.
func ArraysRoot(input [][32]byte, length uint64) ([32]byte, error) {
	leaves := make([][32]byte, length)
	copy(leaves, input)

	res, err := MerkleRootFromLeaves(leaves)
	if err != nil {
		return [32]byte{}, err
	}

	return res, nil
}
