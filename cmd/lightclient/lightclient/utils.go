package lightclient

import "github.com/ledgerwatch/erigon/cl/utils"

// PowerOf2 returns an integer that is the provided
// exponent of 2. Can only return powers of 2 till 63,
// after that it overflows
func powerOf2(n uint64) uint64 {
	if n >= 64 {
		panic("integer overflow")
	}
	return 1 << n
}

// Check if leaf at index verifies against the Merkle root and branch
func isValidMerkleBranch(leaf [32]byte, branch [][]byte, depth uint64, index uint64, root [32]byte) bool {
	value := leaf
	for i := uint64(0); i < depth; i++ {
		if (index / powerOf2(i) % 2) == 1 {
			value = utils.Keccak256(append(branch[i], value[:]...))
		} else {
			value = utils.Keccak256(append(value[:], branch[i]...))
		}
	}
	return value == root
}
