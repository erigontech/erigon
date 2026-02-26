// Copyright 2022 The Erigon Authors
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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveDuplicatesFromSorted(t *testing.T) {
	tests := []struct {
		in  []int
		out []int
	}{
		{[]int{}, []int{}},
		{[]int{1, 2, 5}, []int{1, 2, 5}},
		{[]int{1, 1, 2, 5, 5, 5, 7}, []int{1, 2, 5, 7}},
		{[]int{8, 8, 5, 4, 4, 4, 2}, []int{8, 5, 4, 2}},
		{[]int{8, 8, 8, 8, 8, 4}, []int{8, 4}},
		{[]int{1, 8, 8, 8, 8, 8}, []int{1, 8}},
		{[]int{8, 8, 8, 8, 8}, []int{8}},
		{[]int{-3, -3}, []int{-3}},
		{[]int{-3}, []int{-3}},
	}

	for _, test := range tests {
		x := RemoveDuplicatesFromSorted(test.in)
		assert.Equal(t, test.out, x)
	}
}
