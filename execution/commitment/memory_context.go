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
	"bytes"

	"github.com/erigontech/erigon/db/kv"
)

// MemoryContext is an in-memory PatriciaContext: branch nodes live in a map with no
// persistent backing. Leaf values are supplied directly through the updates buffer,
// so Account and Storage are never consulted — it drives a fresh trie (e.g. the
// genesis root) with no database.
type MemoryContext struct {
	branches map[string][]byte
}

func NewMemoryContext() *MemoryContext {
	return &MemoryContext{branches: make(map[string][]byte)}
}

func (m *MemoryContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	data := m.branches[string(prefix)]
	if data == nil {
		return nil, 0, nil
	}
	return bytes.Clone(data), 0, nil
}

func (m *MemoryContext) PutBranch(prefix, data, _ []byte) error {
	m.branches[string(prefix)] = bytes.Clone(data)
	return nil
}

func (m *MemoryContext) Account([]byte) (*Update, error) { return &Update{Flags: DeleteUpdate}, nil }

func (m *MemoryContext) Storage([]byte) (*Update, error) { return &Update{Flags: DeleteUpdate}, nil }
