package trie

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
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
			if len(key) < len(n.Key) || !bytes.Equal(n.Key, key[:len(n.Key)]) {
				// The trie doesn't contain the key.
				tn = nil
			} else {
				tn = n.Val
				key = key[len(n.Key):]
			}
			if n.Key[len(n.Key)-1] == 16 {
				fromLevel -= (len(n.Key) - 1)
			} else {
				fromLevel -= len(n.Key)
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
		case *accountNode:
			if storage {
				tn = n.storage
			} else {
				tn = nil
			}
		case hashNode:
			return nil, fmt.Errorf("encountered hashNode unexpectedly, key %x", key)
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}
	return proof, nil
}
