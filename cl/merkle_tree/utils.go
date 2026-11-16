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

import "math/bits"

// GetDepth returns the depth of a merkle tree with room for v leaves, that is
// log2(next_pow_of_two(v)).
func GetDepth(v uint64) uint8 {
	if v <= 1 {
		return 0
	}
	return uint8(bits.Len64(v - 1))
}
