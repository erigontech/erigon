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

func IntersectionOfSortedSets(v1, v2 []uint64) []uint64 {
	intersection := []uint64{}
	// keep track of v1 and v2 element iteration
	var i, j int
	// Note that v1 and v2 are both sorted.
	for i < len(v1) && j < len(v2) {
		if v1[i] == v2[j] {
			intersection = append(intersection, v1[i])
			// Change both iterators
			i++
			j++
			continue
		}
		// increase i and j accordingly
		if v1[i] > v2[j] {
			j++
		} else {
			i++
		}
	}
	return intersection
}
