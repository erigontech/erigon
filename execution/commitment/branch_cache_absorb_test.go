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

package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Commitment files built from the process's own writes cover txNums at or
// below the put watermark and must not churn the cache; files from a snapshot
// download exceed it and must clear everything — a stale branch restores the
// trie to the wrong state.
func TestBranchCacheAbsorbFilesExtension(t *testing.T) {
	t.Parallel()

	c := NewBranchCache(64)
	t.Cleanup(c.Close)
	prefix := []byte{0x01}
	c.Put(prefix, []byte{0xbb}, 0, 100)

	c.AbsorbFilesExtension(101)
	_, _, ok := c.Get(prefix)
	require.True(t, ok, "files covering the process's own writes must not clear the cache")

	c.AbsorbFilesExtension(150)
	_, _, ok = c.Get(prefix)
	require.False(t, ok, "files beyond the put watermark carry foreign state — clear")

	c.Put(prefix, []byte{0xcc}, 0, 200)
	c.AbsorbFilesExtension(150)
	_, _, ok = c.Get(prefix)
	require.True(t, ok, "an already-absorbed extension must not clear again")
}
