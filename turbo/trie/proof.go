package trie

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
)

// Prove constructs a merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root node), ending
// with the node that proves the absence of the key.
func (t *Trie) Prove(key []byte, fromLevel int, storage bool) ([][]byte, error) {
	var proof [][]byte
	hasher := newHasher(false)
	defer returnHasherToPool(hasher)
	// Collect all nodes on the path to key.
	key = keybytesToHex(key)
	key = key[:len(key)-1] // Remove terminator
	tn := t.root
	for len(key) > 0 && tn != nil {
		switch n := tn.(type) {
		case *shortNode:
			if fromLevel == 0 {
				if rlp, err := hasher.hashChildren(n, 0); err == nil {
					proof = append(proof, common.CopyBytes(rlp))
				} else {
					return nil, err
				}
			}
			nKey := n.Key
			if nKey[len(nKey)-1] == 16 {
				nKey = nKey[:len(nKey)-1]
			}
			if len(key) < len(nKey) || !bytes.Equal(nKey, key[:len(nKey)]) {
				// The trie doesn't contain the key.
				tn = nil
			} else {
				tn = n.Val
				key = key[len(nKey):]
			}
			if fromLevel > 0 {
				fromLevel -= len(nKey)
			}
		case *duoNode:
			if fromLevel == 0 {
				if rlp, err := hasher.hashChildren(n, 0); err == nil {
					proof = append(proof, common.CopyBytes(rlp))
				} else {
					return nil, err
				}
			}
			i1, i2 := n.childrenIdx()
			switch key[0] {
			case i1:
				tn = n.child1
				key = key[1:]
			case i2:
				tn = n.child2
				key = key[1:]
			default:
				tn = nil
			}
			if fromLevel > 0 {
				fromLevel--
			}
		case *fullNode:
			if fromLevel == 0 {
				if rlp, err := hasher.hashChildren(n, 0); err == nil {
					proof = append(proof, common.CopyBytes(rlp))
				} else {
					return nil, err
				}
			}
			tn = n.Children[key[0]]
			key = key[1:]
			if fromLevel > 0 {
				fromLevel--
			}
		case *accountNode:
			if storage {
				tn = n.storage
			} else {
				tn = nil
			}
		case hashNode:
			return nil, fmt.Errorf("encountered hashNode unexpectedly, key %x, fromLevel %d", key, fromLevel)
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}
	return proof, nil
}
