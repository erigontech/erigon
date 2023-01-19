package utils

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// Check if leaf at index verifies against the Merkle root and branch
func IsValidMerkleBranch(leaf libcommon.Hash, branch []libcommon.Hash, depth uint64, index uint64, root [32]byte) bool {
	value := leaf
	for i := uint64(0); i < depth; i++ {
		if (index / PowerOf2(i) % 2) == 1 {
			value = Keccak256(append(branch[i][:], value[:]...))
		} else {
			value = Keccak256(append(value[:], branch[i][:]...))
		}
	}
	return value == root
}
