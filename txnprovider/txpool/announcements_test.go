// Copyright 2025 The Erigon Authors
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

package txpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDedupHashes(t *testing.T) {
	h := toHashes(2, 6, 2, 5, 2, 4)
	c := h.DedupCopy()
	assert.Equal(t, 6, h.Len())
	assert.Equal(t, 4, c.Len())
	assert.Equal(t, toHashes(2, 2, 2, 4, 5, 6), h)
	assert.Equal(t, toHashes(2, 4, 5, 6), c)

	h = toHashes(2, 2)
	c = h.DedupCopy()
	assert.Equal(t, toHashes(2, 2), h)
	assert.Equal(t, toHashes(2), c)

	h = toHashes(1)
	c = h.DedupCopy()
	assert.Equal(t, 1, h.Len())
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, toHashes(1), h)
	assert.Equal(t, toHashes(1), c)

	h = toHashes()
	c = h.DedupCopy()
	assert.Zero(t, h.Len())
	assert.Zero(t, c.Len())
	assert.Zero(t, len(h))
	assert.Zero(t, len(c))

	h = toHashes(1, 2, 3, 4)
	c = h.DedupCopy()
	assert.Equal(t, toHashes(1, 2, 3, 4), h)
	assert.Equal(t, toHashes(1, 2, 3, 4), c)

	h = toHashes(4, 2, 1, 3)
	c = h.DedupCopy()
	assert.Equal(t, toHashes(1, 2, 3, 4), h)
	assert.Equal(t, toHashes(1, 2, 3, 4), c)

}

func toHashes(h ...byte) (out Hashes) {
	for i := range h {
		hash := [32]byte{h[i]}
		out = append(out, hash[:]...)
	}
	return out
}
