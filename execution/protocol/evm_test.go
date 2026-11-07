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
