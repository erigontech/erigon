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

	"github.com/erigontech/erigon/common"
)

// witnessNodeSet collects consensus trie nodes emitted during a witness fold,
// deduplicated by node hash.
type witnessNodeSet struct {
	byHash map[string][]byte
}

func newWitnessNodeSet() *witnessNodeSet { return &witnessNodeSet{byHash: make(map[string][]byte)} }

func (s *witnessNodeSet) onNode(rlp, hash []byte) {
	k := string(hash)
	if _, ok := s.byHash[k]; ok {
		return
	}
	s.byHash[k] = common.Copy(rlp)
}

// nodes returns the captured nodes root first, per the RLPDecode contract that
// treats index 0 as the trie root. A non-empty set missing its root is an error
// rather than a silently mis-rooted trie.
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
	out = append(out, r)
	for k, v := range s.byHash {
		if k != rootKey {
			out = append(out, v)
		}
	}
	return out, nil
}
