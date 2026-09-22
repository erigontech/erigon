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

type touchEntry struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
}

type touchChunk struct {
	entries []touchEntry
}

func (c *touchChunk) collect(hashedKey, plainKey []byte, update *Update) {
	c.entries = append(c.entries, touchEntry{
		hashedKey: hashedKey,
		plainKey:  plainKey,
		update:    update,
	})
}

func (c *touchChunk) count() int { return len(c.entries) }

func (c *touchChunk) reset() {
	clear(c.entries)
	c.entries = c.entries[:0]
}
