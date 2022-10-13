package lightclient

import "github.com/ledgerwatch/erigon/cmd/lightclient/utils"

func power(x uint64, e uint64) uint64 {
	if e == 0 {
		return 1
	}
	if x == 0 {
		return 0
	}
	ret := x
	for i := 0; i < int(e-1); i++ {
		ret *= x
	}
	return ret
}

// Check if leaf at index verifies against the Merkle ``root`` and ``branch``
func isValidMerkleBranch(leaf [32]byte, branch [][]byte, depth uint64, index uint64, root [32]byte) bool {
	value := leaf
	for i := uint64(0); i < depth; i++ {
		if (index / power(2, i) % 2) == 1 {
			value = utils.Keccak256(append(branch[i], value[:]...))
		} else {
			value = utils.Keccak256(append(value[:], branch[i]...))
		}
	}
	return value == root
}
