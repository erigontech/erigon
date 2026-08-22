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

import (
	"errors"
	"math"

	"github.com/erigontech/erigon/common/crypto"
)

// MerkleizeProgressive computes the progressive Merkle tree root specified by
// EIP-7916. Successive right-hand subtrees have capacities 1, 4, 16, 64, ...
// chunks, and the sequence is terminated by a zero chunk.
func MerkleizeProgressive(chunks [][32]byte) ([32]byte, error) {
	return merkleizeProgressive(chunks, 1)
}

// ProgressiveListRoot computes the EIP-7916 root from already packed basic
// values or composite element roots. logicalLength counts elements, not chunks.
func ProgressiveListRoot(chunks [][32]byte, logicalLength uint64) ([32]byte, error) {
	progressiveRoot, err := MerkleizeProgressive(chunks)
	if err != nil {
		return [32]byte{}, err
	}
	lengthRoot := Uint64Root(logicalLength)
	return crypto.Sha256(progressiveRoot[:], lengthRoot[:]), nil
}

// MixInActiveFields computes the EIP-7495 active-fields mix-in. Bit i is
// packed into bit i%8 of byte i/8 in a zero-padded 32-byte chunk.
func MixInActiveFields(root [32]byte, activeFields []bool) ([32]byte, error) {
	if err := validateActiveFields(activeFields); err != nil {
		return [32]byte{}, err
	}

	var packed [32]byte
	for i, active := range activeFields {
		if active {
			packed[i/8] |= 1 << (uint(i) % 8)
		}
	}

	return crypto.Sha256(root[:], packed[:]), nil
}

// ProgressiveContainerRoot computes the EIP-7495 root from field roots ordered
// by the active entries in activeFields.
func ProgressiveContainerRoot(fieldRoots [][32]byte, activeFields []bool) ([32]byte, error) {
	if len(fieldRoots) == 0 {
		return [32]byte{}, errors.New("progressive container has no fields")
	}
	if err := validateActiveFields(activeFields); err != nil {
		return [32]byte{}, err
	}

	var stackRoots [maxStackLeaves][32]byte
	var expandedRoots [][32]byte
	if len(activeFields) <= maxStackLeaves {
		expandedRoots = stackRoots[:len(activeFields)]
	} else {
		expandedRoots = make([][32]byte, len(activeFields))
	}

	fieldIndex := 0
	for i, active := range activeFields {
		if active {
			if fieldIndex >= len(fieldRoots) {
				return [32]byte{}, errors.New("active field count does not match field roots")
			}
			expandedRoots[i] = fieldRoots[fieldIndex]
			fieldIndex++
		}
	}
	if fieldIndex != len(fieldRoots) {
		return [32]byte{}, errors.New("active field count does not match field roots")
	}

	progressiveRoot, err := MerkleizeProgressive(expandedRoots)
	if err != nil {
		return [32]byte{}, err
	}
	return MixInActiveFields(progressiveRoot, activeFields)
}

func validateActiveFields(activeFields []bool) error {
	if len(activeFields) == 0 {
		return errors.New("active fields cannot be empty")
	}
	if len(activeFields) > 256 {
		return errors.New("active fields exceed 256 bits")
	}
	if !activeFields[len(activeFields)-1] {
		return errors.New("active fields must end with an active field")
	}
	return nil
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

	if numLeaves > math.MaxUint64/4 {
		return [32]byte{}, errors.New("progressive tree capacity overflow")
	}

	right, err := merkleizeProgressive(chunks[subtreeLen:], numLeaves*4)
	if err != nil {
		return [32]byte{}, err
	}

	return crypto.Sha256(left[:], right[:]), nil
}
