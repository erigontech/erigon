package solid

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/prysmaticlabs/gohashtree"
)

func TreeHashFlatSlice(input []byte, res []byte) (err error) {
	err = merkle_tree.MerkleRootFromFlatLeaves(input, res)
	if err != nil {
		return err
	}
	return nil
}

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeVector(elements [][32]byte, length uint64) ([32]byte, error) {
	depth := getDepth(length)
	// Return zerohash at depth
	if len(elements) == 0 {
		return merkle_tree.ZeroHashes[depth], nil
	}
	for i := uint8(0); i < depth; i++ {
		// Sequential
		layerLen := len(elements)
		if layerLen%2 == 1 {
			elements = append(elements, merkle_tree.ZeroHashes[i])
		}
		outputLen := len(elements) / 2
		if err := gohashtree.Hash(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}
	return elements[0], nil
}

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeFlatLeaves(elements []byte) ([]byte, error) {
	// merkleizeTrieLeaves returns intermediate roots of given leaves.
	layer := make([]byte, len(elements)/2)
	for len(elements) > 32 {
		if !utils.IsPowerOf2(uint64(len(elements))) {
			return nil, fmt.Errorf("hash layer is a non power of 2: %d", len(elements))
		}
		if err := gohashtree.HashByteSlice(layer, elements); err != nil {
			return nil, err
		}
		elements = layer[:len(elements)/2]
	}
	return elements[:32], nil
}

func getDepth(v uint64) uint8 {
	// If there are 0 or 1 nodes, the depth is 0.
	if v <= 1 {
		return 0
	}

	// Initialize the depth to 0.
	depth := uint8(0)

	// Divide the number of nodes by 2 until it is less than or equal to 1.
	// The number of iterations is the depth of the tree.
	for v > 1 {
		v >>= 1
		depth++
	}

	return depth
}
