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
		// reaching a value means the key is present; only a value short of the full key
		// length (a divergent leaf) proves absence
		return pos < len(key)
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

// witnessNodeAtPath returns the witness node reached after consuming the whole
// hashed path, descending account→storage and through extension/branch nodes
// (terminator-aware). It is used to assert what a strict verifier finds at a
// collapse sibling's prefix — a materialized branch rather than a bare HashNode.
func witnessNodeAtPath(n trie.Node, key []byte, pos int) trie.Node {
	if pos == len(key) {
		return n
	}
	switch x := n.(type) {
	case *trie.AccountNode:
		return witnessNodeAtPath(x.Storage, key, pos)
	case *trie.ShortNode:
		k := x.Key
		if len(k) > 0 && k[len(k)-1] == 16 {
			k = k[:len(k)-1]
		}
		if len(key)-pos < len(k) || nibbles.CommonPrefixLen(key[pos:], k) < len(k) {
			return nil
		}
		return witnessNodeAtPath(x.Val, key, pos+len(k))
	case *trie.FullNode:
		return witnessNodeAtPath(x.Children[key[pos]], key, pos+1)
	default:
		return n
	}
}

// witnessMaterializesNodeAt reports whether the witness holds a materialized
// (present, non-blinded) node at the end of the hashed path. A strict verifier
// descending to a collapse sibling's prefix must find a real branch/leaf there,
// not a bare HashNode it cannot re-form the collapsing branch from.
func witnessMaterializesNodeAt(root trie.Node, key []byte) bool {
	n := witnessNodeAtPath(root, key, 0)
	if n == nil {
		return false
	}
	_, blinded := n.(*trie.HashNode)
	return !blinded
}
