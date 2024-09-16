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

package trie

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types/accounts"
)

type keyTransformFunc func([]byte) []byte

func transformSubTrie(nd Node, hex []byte, newTrie *Trie, transformFunc keyTransformFunc) {
	switch n := nd.(type) {
	case nil:
		return
	case ValueNode:
		nCopy := make(ValueNode, len(n))
		copy(nCopy, n)
		_, newTrie.root = newTrie.insert(newTrie.root, transformFunc(hex), nCopy)
		return
	case *AccountNode:
		accountCopy := accounts.NewAccount()
		accountCopy.Copy(&n.Account)
		var code []byte = nil
		if n.Code != nil {
			code = make([]byte, len(n.Code))
			copy(code, n.Code)
		}
		_, newTrie.root = newTrie.insert(newTrie.root, transformFunc(hex), &AccountNode{accountCopy, nil, true, code, n.CodeSize})
		aHex := hex
		if aHex[len(aHex)-1] == 16 {
			aHex = aHex[:len(aHex)-1]
		}
		transformSubTrie(n.Storage, aHex, newTrie, transformFunc)
	case HashNode:
		_, newTrie.root = newTrie.insert(newTrie.root, transformFunc(hex), HashNode{hash: common.CopyBytes(n.hash)})
		return
	case *ShortNode:
		var hexVal []byte
		hexVal = concat(hex, n.Key...)
		transformSubTrie(n.Val, hexVal, newTrie, transformFunc)
	case *DuoNode:
		i1, i2 := n.childrenIdx()
		hex1 := make([]byte, len(hex)+1)
		copy(hex1, hex)
		hex1[len(hex)] = i1
		hex2 := make([]byte, len(hex)+1)
		copy(hex2, hex)
		hex2[len(hex)] = i2
		transformSubTrie(n.child1, hex1, newTrie, transformFunc)
		transformSubTrie(n.child2, hex2, newTrie, transformFunc)
	case *FullNode:
		for i, child := range n.Children {
			if child != nil {
				transformSubTrie(child, concat(hex, byte(i)), newTrie, transformFunc)
			}
		}
	default:
		panic("")
	}
}
