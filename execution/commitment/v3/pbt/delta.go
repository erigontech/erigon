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

package pbt

import (
	"bytes"

	"github.com/erigontech/erigon/execution/commitment"
)

func (t *Trie) addDelta(key, data, prev []byte) {
	if bytes.Equal(data, prev) {
		return
	}
	t.deltas = append(t.deltas, commitment.BranchDelta{
		Key: bytes.Clone(key), Data: data, Prev: prev,
	})
}

func (t *Trie) TakeDeltas() []commitment.BranchDelta {
	deltas := make([]commitment.BranchDelta, len(t.deltas))
	for i := range t.deltas {
		deltas[i] = commitment.BranchDelta{
			Key: bytes.Clone(t.deltas[i].Key), Data: bytes.Clone(t.deltas[i].Data), Prev: bytes.Clone(t.deltas[i].Prev),
		}
	}
	t.deltas = nil
	return deltas
}
