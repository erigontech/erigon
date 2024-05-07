/*
   Copyright 2022 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
