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

package merkle_tree

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

// Uint64Root retrieves the root hash of a uint64 value by converting it to a byte array and returning it as a hash.
func Uint64Root(val uint64) common.Hash {
	var root common.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}

func BoolRoot(b bool) (root common.Hash) {
	if b {
		root[0] = 1
	}
	return
}

func BytesRoot(b []byte) (out [32]byte, err error) {
	leafCount := NextPowerOfTwo(uint64((len(b) + 31) / length.Hash))
	leaves := make([]byte, leafCount*length.Hash)
	copy(leaves, b)
	if err = MerkleRootFromFlatLeaves(leaves, leaves); err != nil {
		return [32]byte{}, err
	}
	copy(out[:], leaves)
	return
}

func InPlaceRoot(key []byte) error {
	err := MerkleRootFromFlatLeaves(key, key)
	if err != nil {
		return err
	}
	return nil
}
