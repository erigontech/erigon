package merkle_tree

func nextPowerOf2(n int) int {
	if n == 0 {
		return 1
	}
	if n&(n-1) == 0 {
		return n
	}
	for n&(n-1) > 0 {
		n &= (n - 1)
	}
	return n << 1
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
