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

package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// A block after the reference header has no hash yet, like the block being executed. Block
// overrides in a trace can ask for one.
func TestGetHashFnAnswersZeroForALaterBlock(t *testing.T) {
	for _, test := range []struct {
		name      string
		refNumber uint64
		n         uint64
	}{
		{"the reference block", 10, 10},
		{"a block after it", 10, 11},
		{"the genesis reference block", 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := &types.Header{Number: *uint256.NewInt(test.refNumber)}
			getHash := GetHashFn(header, func(common.Hash, uint64) (*types.Header, error) {
				t.Fatal("a later block needs no header lookup")
				return nil, nil
			})
			hash, err := getHash(test.n)
			require.NoError(t, err)
			require.Equal(t, common.Hash{}, hash)
		})
	}
}

// A lookup that finds no ancestor header has failed; any hash it answered would be made up.
func TestGetHashFnFailsOnAMissingHeader(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(10)}
	getHash := GetHashFn(header, func(common.Hash, uint64) (*types.Header, error) { return nil, nil })
	_, err := getHash(5)
	require.ErrorContains(t, err, "not found")
}

// The walk moves one block down per lookup, so a header for another number fails the lookup.
func TestGetHashFnFailsOnAHeaderOfAnotherNumber(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(10)}
	lookups := 0
	getHash := GetHashFn(header, func(common.Hash, uint64) (*types.Header, error) {
		lookups++
		if lookups > 1 {
			t.Fatal("the lookup went on after a header of another number")
		}
		return &types.Header{}, nil
	})
	_, err := getHash(5)
	require.ErrorContains(t, err, "has number 0, want 9")
}
