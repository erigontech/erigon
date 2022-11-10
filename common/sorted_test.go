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
