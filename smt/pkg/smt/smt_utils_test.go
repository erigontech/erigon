package smt

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_DoesNodeExist(t *testing.T) {
	tests := []struct {
		name           string
		insertPaths    [][]int
		searchPath     []int
		expectedResult bool
		expectedError  error
	}{
		{
			name:           "empty tree",
			insertPaths:    [][]int{},
			searchPath:     []int{1},
			expectedResult: false,
			expectedError:  nil,
		},
		{
			name:           "Search for empty path",
			insertPaths:    [][]int{{1}},
			searchPath:     []int{},
			expectedResult: false,
			expectedError:  ErrEmptySearchPath,
		},
		{
			name:           "Insert 1 node and search for it",
			insertPaths:    [][]int{{1}},
			searchPath:     []int{1},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name:           "Insert 1 node and search for the one next to it",
			insertPaths:    [][]int{{1}},
			searchPath:     []int{0},
			expectedResult: false,
			expectedError:  nil,
		},
		{
			name:           "Insert 2 nodes and search for the first one",
			insertPaths:    [][]int{{1}, {1, 1}},
			searchPath:     []int{1},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name:           "Insert 2 nodes and search for the second one",
			insertPaths:    [][]int{{1}, {1, 1}},
			searchPath:     []int{1, 1},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name:           "Search for node with longer path than the depth",
			insertPaths:    [][]int{{1}},
			searchPath:     []int{1, 1},
			expectedResult: false,
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSMT(nil, false)
			for _, insertPath := range tt.insertPaths {
				fullPath := make([]int, 256)
				copy(fullPath, insertPath)
				nodeKey, err := utils.NodeKeyFromPath(fullPath)
				assert.NoError(t, err, tt.name+": Failed to create node key from path ")
				_, err = s.InsertKA(nodeKey, new(big.Int).SetUint64(1) /*arbitrary, not used in test*/)
				assert.NoError(t, err, tt.name+": Failed to insert node")
			}

			result, err := s.GetNodeAtPath(tt.searchPath)
			if tt.expectedError != nil {
				assert.Error(t, err, tt.name)
				assert.Equal(t, tt.expectedError, err, tt.name)
			} else {
				assert.NoError(t, err, tt.name)
			}
			assert.Equal(t, tt.expectedResult, result != nil, tt.name)
		})
	}
}
