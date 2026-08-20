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

// The devnet genesis carries binaryTrieTime and no eip8038Revised, and geth charges the
// revised schedule from Amsterdam unconditionally, so the tree has to imply it.
func TestBinaryTrieImpliesEIP8038Revised(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	bc := &BlockContext{Time: 1}

	c := &chain.Config{AmsterdamTime: &zero, BinaryTrieTime: &zero}
	require.True(t, bc.Rules(c).EIP8038Revised)

	c = &chain.Config{AmsterdamTime: &zero}
	require.False(t, bc.Rules(c).EIP8038Revised, "Amsterdam alone keeps the pinned-corpus schedule")

	c = &chain.Config{AmsterdamTime: &zero, EIP8038Revised: true}
	require.True(t, bc.Rules(c).EIP8038Revised, "the explicit key still works on its own")
}
