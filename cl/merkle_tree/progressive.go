// Copyright 2026 The Erigon Authors
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

package merkle_tree

import "github.com/erigontech/erigon/cl/utils"

// MerkleizeProgressive computes the progressive Merkle tree root specified by
// EIP-7916. Successive right-hand subtrees have capacities 1, 4, 16, 64, ...
// chunks, and the sequence is terminated by a zero chunk.
func MerkleizeProgressive(chunks [][32]byte) ([32]byte, error) {
	return merkleizeProgressive(chunks, 1)
}

func merkleizeProgressive(chunks [][32]byte, numLeaves uint64) ([32]byte, error) {
	if len(chunks) == 0 {
		return [32]byte{}, nil
	}

	subtreeLen := len(chunks)
	if uint64(subtreeLen) > numLeaves {
		subtreeLen = int(numLeaves)
	}

	// MerkleizeVector hashes in place, so copy the current subtree to preserve
	// the caller's chunks and the unconsumed suffix.
	subtree := append([][32]byte(nil), chunks[:subtreeLen]...)
	left, err := MerkleizeVector(subtree, numLeaves)
	if err != nil {
		return [32]byte{}, err
	}

	right, err := merkleizeProgressive(chunks[subtreeLen:], numLeaves*4)
	if err != nil {
		return [32]byte{}, err
	}

	return utils.Sha256(left[:], right[:]), nil
}
