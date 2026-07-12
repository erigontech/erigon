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

package jsonrpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func hashN(n byte) common.Hash {
	var h common.Hash
	h[0] = n
	return h
}

func mkResult() *ExecutionWitnessResult {
	return &ExecutionWitnessResult{State: []hexutil.Bytes{{0x01}}}
}

// TestNewWitnessResultCacheClampsBlocks pins the only behaviour the constructor
// owns on top of the hashicorp LRU: clamp above witnessCacheMaxBlocks, honor
// requested sizes below it.
func TestNewWitnessResultCacheClampsBlocks(t *testing.T) {
	c := newWitnessResultCache(witnessCacheMaxBlocks + 100)
	for n := 0; n < int(witnessCacheMaxBlocks)+100; n++ {
		var h common.Hash
		h[0], h[1] = byte(n), byte(n>>8)
		c.Add(h, mkResult())
	}
	require.Equal(t, int(witnessCacheMaxBlocks), c.Len(),
		"entry count is clamped to witnessCacheMaxBlocks")

	c1 := newWitnessResultCache(1)
	c1.Add(hashN(1), mkResult())
	c1.Add(hashN(2), mkResult())
	require.Equal(t, 1, c1.Len(), "a sub-cap size is honored, not clamped up")
}
