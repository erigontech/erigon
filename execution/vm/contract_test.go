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
)

func TestJumpDestCacheSizeBytes(t *testing.T) {
	t.Parallel()
	c := newJumpDestCache()
	t.Cleanup(c.Close)
	key := []byte("code hash")

	c.Put(key, make(bitvec, 1), 0)
	small := c.SizeBytes()
	c.Put(key, make(bitvec, 512), 0)
	require.Equal(t, int64(511*8), c.SizeBytes()-small, "replacement")
	c.Delete(key)
	require.Zero(t, c.SizeBytes(), "removal")
}
