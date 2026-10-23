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

package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTierEdgeBlocks(t *testing.T) {
	blockOf := func(txNum uint64) (uint64, error) { return txNum / 100, nil }

	hddEnd, ssdStart, err := tierEdgeBlocks(1000, 1000, blockOf)
	require.NoError(t, err)
	require.Equal(t, uint64(9), hddEnd, "the last cold tx 999 is in block 9")
	require.Equal(t, uint64(10), ssdStart, "the first hot tx 1000 is in block 10")

	hddEnd, ssdStart, err = tierEdgeBlocks(0, 0, blockOf)
	require.NoError(t, err)
	require.Equal(t, uint64(0), hddEnd, "nothing cold")
	require.Equal(t, uint64(0), ssdStart)
}
