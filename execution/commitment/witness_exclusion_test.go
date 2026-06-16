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
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// witnessResolvesAbsence walks the witness trie following key the way a strict stateless
// verifier does: every node on the path, including the child of a divergent extension, must
// be materialized. Unlike trie.Get it does not accept a bare HashNode behind a divergent
// extension as proof of absence.
func witnessResolvesAbsence(n trie.Node, key []byte, pos int) bool {
	switch x := n.(type) {
	case nil:
		return true
	case trie.ValueNode:
		return true
	case *trie.AccountNode:
		return witnessResolvesAbsence(x.Storage, key, pos)
	case *trie.ShortNode:
		matchlen := nibbles.CommonPrefixLen(key[pos:], x.Key)
		if matchlen == len(x.Key) || x.Key[matchlen] == 16 {
			return witnessResolvesAbsence(x.Val, key, pos+matchlen)
		}
		_, isHash := x.Val.(*trie.HashNode)
		return !isHash
	case *trie.FullNode:
		child := x.Children[key[pos]]
		if child == nil {
			return true
		}
		return witnessResolvesAbsence(child, key, pos+1)
	case *trie.HashNode:
		return false
	default:
		return false
	}
}
