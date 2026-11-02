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

package vm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestJumpDestCacheComparesWholeHash(t *testing.T) {
	t.Parallel()
	c := newJumpDestCache()
	var h, sameSlot common.Hash
	h[0], h[31] = 1, 1
	sameSlot[0], sameSlot[31] = 1, 2

	c.Add(h, make(bitvec, 3))
	got, ok := c.Get(h)
	require.True(t, ok)
	require.Len(t, got, 3)
	_, ok = c.Get(sameSlot)
	require.False(t, ok, "a hash sharing the first 8 bytes must miss")
}
