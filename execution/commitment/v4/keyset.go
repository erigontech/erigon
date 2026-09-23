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

package v4

import (
	"bytes"
	"slices"
)

type keySpan struct{ start, end int32 }

type keySet struct {
	arena []byte
	spans []keySpan
}

func (s *keySet) len() int { return len(s.spans) }

func (s *keySet) at(i int) []byte { return s.arena[s.spans[i].start:s.spans[i].end] }

func (s *keySet) add(key []byte) {
	start := int32(len(s.arena))
	s.arena = append(s.arena, key...)
	s.spans = append(s.spans, keySpan{start, int32(len(s.arena))})
}

func (s *keySet) addNodeKey(g graph, path []byte) {
	start := int32(len(s.arena))
	s.arena = g.nodeKey(path, s.arena)
	s.spans = append(s.spans, keySpan{start, int32(len(s.arena))})
}

func (s *keySet) sortDedup() {
	slices.SortFunc(s.spans, func(a, b keySpan) int {
		return bytes.Compare(s.arena[a.start:a.end], s.arena[b.start:b.end])
	})
	s.spans = slices.CompactFunc(s.spans, func(a, b keySpan) bool {
		return bytes.Equal(s.arena[a.start:a.end], s.arena[b.start:b.end])
	})
}

func (s *keySet) forEachMissing(other *keySet, fn func(key []byte) error) error {
	s.sortDedup()
	other.sortDedup()
	for i, j := 0, 0; i < len(s.spans); {
		if j == len(other.spans) {
			if err := fn(s.at(i)); err != nil {
				return err
			}
			i++
			continue
		}
		switch bytes.Compare(s.at(i), other.at(j)) {
		case 0:
			i++
			j++
		case -1:
			if err := fn(s.at(i)); err != nil {
				return err
			}
			i++
		default:
			j++
		}
	}
	return nil
}
