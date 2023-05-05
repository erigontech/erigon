package merkle_tree

import (
	"errors"
)

func MerkleRootFromLeaves(leaves [][32]byte) ([32]byte, error) {
	if len(leaves) == 0 {
		return [32]byte{}, errors.New("zero leaves provided")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	hashLayer := leaves
	return globalHasher.merkleizeTrieLeaves(hashLayer)
}

// getDepth returns the depth of a merkle tree with a given number of nodes.
// The depth is defined as the number of levels in the tree, with the root
// node at level 0 and each child node at a level one greater than its parent.
// If the number of nodes is less than or equal to 1, the depth is 0.
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
