// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package utils

import "github.com/erigontech/erigon-lib/common"

// Check if leaf at index verifies against the Merkle root and branch
func IsValidMerkleBranch(leaf common.Hash, branch []common.Hash, depth uint64, index uint64, root [32]byte) bool {
	value := leaf
	for i := uint64(0); i < depth; i++ {
		if (index / PowerOf2(i) % 2) == 1 {
			value = Sha256(append(branch[i][:], value[:]...))
		} else {
			value = Sha256(append(value[:], branch[i][:]...))
		}
	}
	return value == root
}

func PreparateRootsForHashing(roots []common.Hash) [][32]byte {
	ret := make([][32]byte, len(roots))
	for i := range roots {
		copy(ret[i][:], roots[i][:])
	}
	return ret
}
