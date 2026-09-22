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

package evmtypes

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

func TestEIP8038RevisedFollowsBinaryTrieSchedule(t *testing.T) {
	t.Parallel()

	amsterdam := uint64(100)
	binaryTrie := uint64(200)
	withBinaryTrie := &chain.Config{AmsterdamTime: &amsterdam, BinaryTrieTime: &binaryTrie}
	withoutBinaryTrie := &chain.Config{AmsterdamTime: &amsterdam}

	for _, time := range []uint64{150, 250} {
		bc := &BlockContext{Time: time}
		require.True(t, bc.Rules(withBinaryTrie).EIP8038Revised)
		require.False(t, bc.Rules(withoutBinaryTrie).EIP8038Revised)
	}

	withExplicitSchedule := &chain.Config{AmsterdamTime: &amsterdam, EIP8038Revised: true}
	require.True(t, (&BlockContext{Time: 150}).Rules(withExplicitSchedule).EIP8038Revised)
}
