// Copyright 2024 The Erigon Authors
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
	"fmt"
)

const witnessNodeChunk = 8 * 1024

type witnessNodeSet struct {
	byHash map[string][]byte
	buf    []byte
}

func newWitnessNodeSet() *witnessNodeSet { return &witnessNodeSet{byHash: make(map[string][]byte)} }

func (s *witnessNodeSet) onNode(rlp, hash []byte) {
	if _, ok := s.byHash[string(hash)]; ok {
		return
	}
	if cap(s.buf)-len(s.buf) < len(rlp) {
		s.buf = make([]byte, 0, max(len(rlp), witnessNodeChunk))
	}
	start := len(s.buf)
	s.buf = append(s.buf, rlp...)
	s.byHash[string(hash)] = s.buf[start:len(s.buf):len(s.buf)]
}

func (s *witnessNodeSet) nodes(root []byte) ([][]byte, error) {
	if len(s.byHash) == 0 {
		return nil, nil
	}
	rootKey := string(root)
	r, ok := s.byHash[rootKey]
	if !ok {
		return nil, fmt.Errorf("witness root %x absent from captured node set", root)
	}
	out := make([][]byte, 0, len(s.byHash))
	out = append(out, r) // RLPDecode requires index 0 to be the root
	for k, v := range s.byHash {
		if k != rootKey {
			out = append(out, v)
		}
	}
	return out, nil
}
