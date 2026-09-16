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

package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestSideBlocksToRetain(t *testing.T) {
	hash := func(i int) common.Hash { return common.Hash{byte(i)} }
	pending := make(map[common.Hash]pendingBlock, retainedBlockLimit+4)
	for i := range retainedBlockLimit + 4 {
		pending[hash(i)] = pendingBlock{number: uint64(i)}
	}

	side := sideBlocksToRetain(pending, []common.Hash{hash(retainedBlockLimit + 3)})

	require.Len(t, side, retainedBlockLimit)
	for i := 3; i < retainedBlockLimit+3; i++ {
		require.Contains(t, side, hash(i))
	}
}
