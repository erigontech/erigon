package trie

import (
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

type keyTransformFunc func([]byte) []byte

func transformSubTrie(nd node, hex []byte, newTrie *Trie, transformFunc keyTransformFunc) {
	switch n := nd.(type) {
	case nil:
		return
	case valueNode:
		nCopy := make(valueNode, len(n))
		copy(nCopy, n)
		_, newTrie.root = newTrie.insert(newTrie.root, transformFunc(hex), 0, nCopy)
		return
	case *accountNode:
		accountCopy := accounts.NewAccount()
		accountCopy.Copy(&n.Account)
		var code []byte = nil
		if n.code != nil {
			code = make([]byte, len(n.code))
			copy(code, n.code)
		}
		_, newTrie.root = newTrie.insert(newTrie.root, transformFunc(hex), 0, &accountNode{accountCopy, nil, true, codeNode(code)})
		aHex := hex
		if aHex[len(aHex)-1] == 16 {
			aHex = aHex[:len(aHex)-1]
		}
		transformSubTrie(n.storage, aHex, newTrie, transformFunc)
	case hashNode:
		nCopy := make(hashNode, len(n))
		copy(nCopy, n)
		_, newTrie.root = newTrie.insert(newTrie.root, transformFunc(hex), 0, nCopy)
		return
	case *shortNode:
		var hexVal []byte
		hexVal = concat(hex, n.Key...)
		transformSubTrie(n.Val, hexVal, newTrie, transformFunc)
	case *duoNode:
		i1, i2 := n.childrenIdx()
		hex1 := make([]byte, len(hex)+1)
		copy(hex1, hex)
		hex1[len(hex)] = i1
		hex2 := make([]byte, len(hex)+1)
		copy(hex2, hex)
		hex2[len(hex)] = i2
		transformSubTrie(n.child1, hex1, newTrie, transformFunc)
		transformSubTrie(n.child2, hex2, newTrie, transformFunc)
	case *fullNode:
		for i, child := range n.Children {
			if child != nil {
				transformSubTrie(child, concat(hex, byte(i)), newTrie, transformFunc)
			}
		}
	default:
		panic("")
	}
}
