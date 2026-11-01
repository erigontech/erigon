// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
// Deprecated
// use package turbo/trie
type Trie struct {
	RootNode             Node
	valueNodesRLPEncoded bool

	newHasherFunc func() *hasher
}

// New creates a trie with an existing root node from db.
// Deprecated
// use package turbo/trie
func New(root common.Hash) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ false) },
	}
	if (root != common.Hash{}) && root != empty.RootHash {
		trie.RootNode = &HashNode{hash: root[:]}
	}
	return trie
}

func NewInMemoryTrie(root Node) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ false) },
		RootNode:      root,
	}
	return trie
}

func NewInMemoryTrieRLPEncoded(root Node) *Trie {
	trie := &Trie{
		newHasherFunc:        func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ true) },
		RootNode:             root,
		valueNodesRLPEncoded: true,
	}
	return trie
}

// NewTestRLPTrie treats all the data provided to `Update` function as rlp-encoded.
// it is usually used for testing purposes.
func NewTestRLPTrie(root common.Hash) *Trie {
	trie := &Trie{
		valueNodesRLPEncoded: true,
		newHasherFunc:        func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ true) },
	}
	if (root != common.Hash{}) && root != empty.RootHash {
		trie.RootNode = &HashNode{hash: root[:]}
	}
	return trie
}

// Get returns the value for key stored in the trie.
func (t *Trie) Get(key []byte) (value []byte, gotValue bool) {
	if t.RootNode == nil {
		return nil, true
	}

	hex := nibbles.KeybytesToHex(key)
	return t.get(t.RootNode, hex, 0)
}

func (t *Trie) GetAccount(key []byte) (value *accounts.Account, gotValue bool) {
	if t.RootNode == nil {
		return nil, true
	}

	hex := nibbles.KeybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode != nil {
		var value accounts.Account
		value.Copy(&accNode.Account)
		return &value, gotValue
	}
	return nil, gotValue
}

func (t *Trie) GetAccountCode(key []byte) (value []byte, gotValue bool) {
	if t.RootNode == nil {
		return nil, false
	}

	hex := nibbles.KeybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode != nil {
		if accNode.Account.CodeHash == accounts.EmptyCodeHash {
			return nil, gotValue
		}

		if accNode.Code == nil {
			return nil, false
		}

		return accNode.Code, gotValue
	}
	return nil, gotValue
}

func (t *Trie) GetAccountCodeSize(key []byte) (value int, gotValue bool) {
	if t.RootNode == nil {
		return 0, false
	}

	hex := nibbles.KeybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode != nil {
		if accNode.Account.CodeHash == accounts.EmptyCodeHash {
			return 0, gotValue
		}

		if accNode.CodeSize == codeSizeUncached {
			return 0, false
		}

		return accNode.CodeSize, gotValue
	}
	return 0, gotValue
}

func (t *Trie) getAccount(origNode Node, key []byte, pos int) (value *AccountNode, gotValue bool) {
	switch n := origNode.(type) {
	case nil:
		return nil, true
	case *ShortNode:
		matchlen := nibbles.CommonPrefixLen(key[pos:], n.Key)
		if matchlen == len(n.Key) {
			if v, ok := n.Val.(*AccountNode); ok {
				return v, true
			} else {
				return t.getAccount(n.Val, key, pos+matchlen)
			}
		} else {
			return nil, true
		}
	case *DuoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			return t.getAccount(n.child1, key, pos+1)
		case i2:
			return t.getAccount(n.child2, key, pos+1)
		default:
			return nil, true
		}
	case *FullNode:
		child := n.Children[key[pos]]
		return t.getAccount(child, key, pos+1)
	case *HashNode:
		return nil, false

	case *AccountNode:
		return n, true
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

func (t *Trie) get(origNode Node, key []byte, pos int) (value []byte, gotValue bool) {
	switch n := origNode.(type) {
	case nil:
		return nil, true
	case ValueNode:
		return n, true
	case *AccountNode:
		return t.get(n.Storage, key, pos)
	case *ShortNode:
		matchlen := nibbles.CommonPrefixLen(key[pos:], n.Key)
		if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
			value, gotValue = t.get(n.Val, key, pos+matchlen)
		} else {
			value, gotValue = nil, true
		}
		return
	case *DuoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			value, gotValue = t.get(n.child1, key, pos+1)
		case i2:
			value, gotValue = t.get(n.child2, key, pos+1)
		default:
			value, gotValue = nil, true
		}
		return
	case *FullNode:
		child := n.Children[key[pos]]
		if child == nil {
			return nil, true
		}
		return t.get(child, key, pos+1)
	case *HashNode:
		return n.hash, false

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
// DESCRIBED: docs/programmers_guide/guide.md#root
func (t *Trie) Update(key, value []byte) {
	hex := nibbles.KeybytesToHex(key)

	if len(value) == 0 {
		_, t.RootNode = t.delete(t.RootNode, hex, false)
		return
	}

	newnode := ValueNode(value)

	if t.RootNode == nil {
		t.RootNode = NewShortNode(hex, newnode)
	} else {
		_, t.RootNode = t.insert(t.RootNode, hex, newnode)
	}
}

func (t *Trie) UpdateAccount(key []byte, acc *accounts.Account) {
	// make account copy. There are some pointer into big.Int
	value := new(accounts.Account)
	value.Copy(acc)

	hex := nibbles.KeybytesToHex(key)

	var newnode *AccountNode
	if value.Root == empty.RootHash || value.Root == (common.Hash{}) {
		newnode = &AccountNode{*value, nil, true, nil, codeSizeUncached}
	} else {
		newnode = &AccountNode{*value, &HashNode{hash: value.Root[:]}, true, nil, codeSizeUncached}
	}

	if t.RootNode == nil {
		t.RootNode = NewShortNode(hex, newnode)
	} else {
		_, t.RootNode = t.insert(t.RootNode, hex, newnode)
	}
}

// UpdateAccountCode attaches the code node to an account at specified key
func (t *Trie) UpdateAccountCode(key []byte, code CodeNode) error {
	if t.RootNode == nil {
		return nil
	}

	hex := nibbles.KeybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode == nil || !gotValue {
		return fmt.Errorf("account not found with key: %x", key)
	}

	actualCodeHash := crypto.Keccak256Hash(code)
	if accNode.CodeHash.Value() != actualCodeHash {
		return fmt.Errorf("inserted code mismatch account hash (acc.CodeHash=%x codeHash=%x)", accNode.CodeHash, actualCodeHash)
	}

	accNode.Code = code
	accNode.CodeSize = len(code)

	// t.insert will call the observer methods itself
	_, t.RootNode = t.insert(t.RootNode, hex, accNode)
	return nil
}

// can pass incarnation=0 if start from root, method internally will
// put incarnation from accountNode when pass it by traverse
func (t *Trie) insert(origNode Node, key []byte, value Node) (updated bool, newNode Node) {
	return t.insertRecursive(origNode, key, 0, value)
}

func (t *Trie) insertRecursive(origNode Node, key []byte, pos int, value Node) (updated bool, newNode Node) {
	if len(key) == pos {
		origN, origNok := origNode.(ValueNode)
		vn, vnok := value.(ValueNode)
		if origNok && vnok {
			updated = !bytes.Equal(origN, vn)
			if updated {
				newNode = value
			} else {
				newNode = origN
			}
			return
		}
		origAccN, origNok := origNode.(*AccountNode)
		vAccN, vnok := value.(*AccountNode)
		if origNok && vnok {
			updated = !origAccN.Equals(&vAccN.Account)
			if updated {
				if origAccN.CodeHash != vAccN.CodeHash {
					origAccN.Code = nil
				} else if vAccN.Code != nil {
					origAccN.Code = vAccN.Code
				}
				origAccN.Account.Copy(&vAccN.Account)
				origAccN.CodeSize = vAccN.CodeSize
				origAccN.RootCorrect = false
			}
			newNode = origAccN
			return
		}

		// replacing nodes except accounts
		if !origNok {
			return true, value
		}
	}

	var nn Node
	switch n := origNode.(type) {
	case nil:
		return true, NewShortNode(bytes.Clone(key[pos:]), value)
	case *AccountNode:
		updated, nn = t.insertRecursive(n.Storage, key, pos, value)
		if updated {
			n.Storage = nn
			n.RootCorrect = false
		}
		return updated, n
	case *ShortNode:
		matchlen := nibbles.CommonPrefixLen(key[pos:], n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
			updated, nn = t.insertRecursive(n.Val, key, pos+matchlen, value)
			if updated {
				n.Val = nn
				n.ref.len = 0
			}
			newNode = n
		} else {
			// Otherwise branch out at the index where they differ.
			var c1 Node
			if len(n.Key) == matchlen+1 {
				c1 = n.Val
			} else {
				c1 = NewShortNode(bytes.Clone(n.Key[matchlen+1:]), n.Val)
			}
			var c2 Node
			if len(key) == pos+matchlen+1 {
				c2 = value
			} else {
				c2 = NewShortNode(bytes.Clone(key[pos+matchlen+1:]), value)
			}
			branch := &DuoNode{}
			if n.Key[matchlen] < key[pos+matchlen] {
				branch.child1 = c1
				branch.child2 = c2
			} else {
				branch.child1 = c2
				branch.child2 = c1
			}
			branch.mask = (1 << n.Key[matchlen]) | (1 << key[pos+matchlen])

			// Replace this shortNode with the branch if it occurs at index 0.
			if matchlen == 0 {
				newNode = branch // current node leaves the generation, but new node branch joins it
			} else {
				// Otherwise, replace it with a short node leading up to the branch.
				n.Key = bytes.Clone(key[pos : pos+matchlen])
				n.Val = branch
				n.ref.len = 0
				newNode = n
			}
			updated = true
		}
		return

	case *DuoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			updated, nn = t.insertRecursive(n.child1, key, pos+1, value)
			if updated {
				n.child1 = nn
				n.ref.len = 0
			}
			newNode = n
		case i2:
			updated, nn = t.insertRecursive(n.child2, key, pos+1, value)
			if updated {
				n.child2 = nn
				n.ref.len = 0
			}
			newNode = n
		default:
			var child Node
			if len(key) == pos+1 {
				child = value
			} else {
				child = NewShortNode(bytes.Clone(key[pos+1:]), value)
			}
			newnode := &FullNode{}
			newnode.Children[i1] = n.child1
			newnode.Children[i2] = n.child2
			newnode.Children[key[pos]] = child
			updated = true
			// current node leaves the generation but newnode joins it
			newNode = newnode
		}
		return

	case *FullNode:
		child := n.Children[key[pos]]
		if child == nil {
			if len(key) == pos+1 {
				n.Children[key[pos]] = value
			} else {
				n.Children[key[pos]] = NewShortNode(bytes.Clone(key[pos+1:]), value)
			}
			updated = true
			n.ref.len = 0
		} else {
			updated, nn = t.insertRecursive(child, key, pos+1, value)
			if updated {
				n.Children[key[pos]] = nn
				n.ref.len = 0
			}
		}
		newNode = n
		return
	default:
		panic(fmt.Sprintf("%T: invalid node: %v. Searched by: key=%x, pos=%d", n, n, key, pos))
	}
}

// non-recursive version of get and returns: node and parent node
func (t *Trie) getNode(hex []byte) (Node, Node, bool, uint64) {
	nd := t.RootNode
	var parent Node
	pos := 0
	var account bool
	var incarnation uint64
	for pos < len(hex) || account {
		switch n := nd.(type) {
		case nil:
			return nil, nil, false, incarnation
		case *ShortNode:
			matchlen := nibbles.CommonPrefixLen(hex[pos:], n.Key)
			if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
				parent = n
				nd = n.Val
				pos += matchlen
				if _, ok := nd.(*AccountNode); ok {
					account = true
				}
			} else {
				return nil, nil, false, incarnation
			}
		case *DuoNode:
			i1, i2 := n.childrenIdx()
			switch hex[pos] {
			case i1:
				parent = n
				nd = n.child1
				pos++
			case i2:
				parent = n
				nd = n.child2
				pos++
			default:
				return nil, nil, false, incarnation
			}
		case *FullNode:
			child := n.Children[hex[pos]]
			if child == nil {
				return nil, nil, false, incarnation
			}
			parent = n
			nd = child
			pos++
		case *AccountNode:
			parent = n
			nd = n.Storage
			incarnation = n.Incarnation
			account = false
		case ValueNode:
			return nd, parent, true, incarnation
		case HashNode:
			return nd, parent, true, incarnation
		default:
			panic(fmt.Sprintf("Unknown node: %T", n))
		}
	}
	return nd, parent, true, incarnation
}

func (t *Trie) touchAll(n Node, hex []byte, del bool, incarnation uint64) {
	switch n := n.(type) {
	case *ShortNode:
		if _, ok := n.Val.(ValueNode); !ok {
			// Don't need to compute prefix for a leaf
			h := n.Key
			// Remove terminator
			if h[len(h)-1] == 16 {
				h = h[:len(h)-1]
			}
			hexVal := concat(hex, h...)
			t.touchAll(n.Val, hexVal, del, incarnation)
		}
	case *DuoNode:
		i1, i2 := n.childrenIdx()
		hex1 := make([]byte, len(hex)+1)
		copy(hex1, hex)
		hex1[len(hex)] = i1
		hex2 := make([]byte, len(hex)+1)
		copy(hex2, hex)
		hex2[len(hex)] = i2
		t.touchAll(n.child1, hex1, del, incarnation)
		t.touchAll(n.child2, hex2, del, incarnation)
	case *FullNode:
		for i, child := range n.Children {
			if child != nil {
				t.touchAll(child, concat(hex, byte(i)), del, incarnation)
			}
		}
	case *AccountNode:
		if n.Storage != nil {
			t.touchAll(n.Storage, hex, del, n.Incarnation)
		}
	}
}

// Delete removes any existing value for key from the trie.
// DESCRIBED: docs/programmers_guide/guide.md#root
func (t *Trie) Delete(key []byte) {
	hex := nibbles.KeybytesToHex(key)
	_, t.RootNode = t.delete(t.RootNode, hex, false)
}

func (t *Trie) convertToShortNode(child Node, pos uint) Node {
	if pos != 16 {
		// If the remaining entry is a short node, it replaces
		// n and its key gets the missing nibble tacked to the
		// front. This avoids creating an invalid
		// shortNode{..., shortNode{...}}.  Since the entry
		// might not be loaded yet, resolve it just for this
		// check.
		if short, ok := child.(*ShortNode); ok {
			k := make([]byte, len(short.Key)+1)
			k[0] = byte(pos)
			copy(k[1:], short.Key)
			return NewShortNode(k, short.Val)
		}
	}
	// Otherwise, n is replaced by a one-nibble short node
	// containing the child.
	return NewShortNode([]byte{byte(pos)}, child)
}

func (t *Trie) delete(origNode Node, key []byte, preserveAccountNode bool) (updated bool, newNode Node) {
	return t.deleteRecursive(origNode, key, 0, preserveAccountNode, 0)
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
//
// can pass incarnation=0 if start from root, method internally will
// put incarnation from accountNode when pass it by traverse
func (t *Trie) deleteRecursive(origNode Node, key []byte, keyStart int, preserveAccountNode bool, incarnation uint64) (updated bool, newNode Node) {
	var nn Node
	switch n := origNode.(type) {
	case *ShortNode:
		matchlen := nibbles.CommonPrefixLen(key[keyStart:], n.Key)
		if matchlen == min(len(n.Key), len(key[keyStart:])) || n.Key[matchlen] == 16 || key[keyStart+matchlen] == 16 {
			fullMatch := matchlen == len(key)-keyStart
			removeNodeEntirely := fullMatch
			if preserveAccountNode {
				removeNodeEntirely = len(key) == keyStart || matchlen == len(key[keyStart:])-1
			}

			if removeNodeEntirely {
				updated = true
				touchKey := key[:keyStart+matchlen]
				if touchKey[len(touchKey)-1] == 16 {
					touchKey = touchKey[:len(touchKey)-1]
				}
				t.touchAll(n.Val, touchKey, true, incarnation)
				newNode = nil
			} else {
				// The key is longer than n.Key. Remove the remaining suffix
				// from the subtrie. Child can never be nil here since the
				// subtrie must contain at least two other values with keys
				// longer than n.Key.
				updated, nn = t.deleteRecursive(n.Val, key, keyStart+matchlen, preserveAccountNode, incarnation)
				if !updated {
					newNode = n
				} else {
					if nn == nil {
						newNode = nil
					} else {
						if shortChild, ok := nn.(*ShortNode); ok {
							// Deleting from the subtrie reduced it to another
							// short node. Merge the nodes to avoid creating a
							// shortNode{..., shortNode{...}}. Use concat (which
							// always creates a new slice) instead of append to
							// avoid modifying n.Key since it might be shared with
							// other nodes.
							newNode = NewShortNode(concat(n.Key, shortChild.Key...), shortChild.Val)
						} else {
							n.Val = nn
							newNode = n
							n.ref.len = 0
						}
					}
				}
			}
		} else {
			updated = false
			newNode = n // don't replace n on mismatch
		}
		return

	case *DuoNode:
		i1, i2 := n.childrenIdx()
		switch key[keyStart] {
		case i1:
			updated, nn = t.deleteRecursive(n.child1, key, keyStart+1, preserveAccountNode, incarnation)
			if !updated {
				newNode = n
			} else {
				if nn == nil {
					newNode = t.convertToShortNode(n.child2, uint(i2))
				} else {
					n.child1 = nn
					n.ref.len = 0
					newNode = n
				}
			}
		case i2:
			updated, nn = t.deleteRecursive(n.child2, key, keyStart+1, preserveAccountNode, incarnation)
			if !updated {
				newNode = n
			} else {
				if nn == nil {
					newNode = t.convertToShortNode(n.child1, uint(i1))
				} else {
					n.child2 = nn
					n.ref.len = 0
					newNode = n
				}
			}
		default:
			updated = false
			newNode = n
		}
		return

	case *FullNode:
		child := n.Children[key[keyStart]]
		updated, nn = t.deleteRecursive(child, key, keyStart+1, preserveAccountNode, incarnation)
		if !updated {
			newNode = n
		} else {
			n.Children[key[keyStart]] = nn
			// Check how many non-nil entries are left after deleting and
			// reduce the full node to a short node if only one entry is
			// left. Since n must've contained at least two children
			// before deletion (otherwise it would not be a full node) n
			// can never be reduced to nil.
			//
			// When the loop is done, pos contains the index of the single
			// value that is left in n or -2 if n contains at least two
			// values.
			var pos1, pos2 int
			count := 0
			for i, cld := range n.Children {
				if cld != nil {
					if count == 0 {
						pos1 = i
					}
					if count == 1 {
						pos2 = i
					}
					count++
					if count > 2 {
						break
					}
				}
			}
			switch {
			case count == 1:
				newNode = t.convertToShortNode(n.Children[pos1], uint(pos1))
			case count == 2:
				duo := &DuoNode{}
				if pos1 == int(key[keyStart]) {
					duo.child1 = nn
				} else {
					duo.child1 = n.Children[pos1]
				}
				if pos2 == int(key[keyStart]) {
					duo.child2 = nn
				} else {
					duo.child2 = n.Children[pos2]
				}
				duo.mask = (1 << uint(pos1)) | (uint32(1) << uint(pos2))
				newNode = duo
			case count > 2:
				// n still contains at least three values and cannot be reduced.
				n.ref.len = 0
				newNode = n
			}
		}
		return

	case ValueNode:
		updated = true
		newNode = nil
		return

	case *AccountNode:
		if keyStart >= len(key) || key[keyStart] == 16 {
			// Key terminates here
			h := key[:keyStart]
			if h[len(h)-1] == 16 {
				h = h[:len(h)-1]
			}
			if n.Storage != nil {
				// Mark all the storage nodes as deleted
				t.touchAll(n.Storage, h, true, n.Incarnation)
			}
			if preserveAccountNode {
				n.Storage = nil
				n.Code = nil
				n.Root = empty.RootHash
				n.RootCorrect = true
				return true, n
			}

			return true, nil
		}
		updated, nn = t.deleteRecursive(n.Storage, key, keyStart, preserveAccountNode, n.Incarnation)
		if updated {
			n.Storage = nn
			n.RootCorrect = false
		}
		newNode = n
		return

	case nil:
		updated = false
		newNode = nil
		return

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key[:keyStart]))
	}
}

// DeleteSubtree removes any existing value for key from the trie.
// The only difference between Delete and DeleteSubtree is that Delete would delete accountNode too,
// wherewas DeleteSubtree will keep the accountNode, but will make the storage sub-trie empty
func (t *Trie) DeleteSubtree(keyPrefix []byte) {
	hexPrefix := nibbles.KeybytesToHex(keyPrefix)

	_, t.RootNode = t.delete(t.RootNode, hexPrefix, true)
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

// Root returns the root hash of the trie.
//
// Deprecated: use Hash instead.
func (t *Trie) Root() []byte {
	h := t.Hash()
	return h[:]
}

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
// DESCRIBED: docs/programmers_guide/guide.md#root
func (t *Trie) Hash() common.Hash {
	if t == nil || t.RootNode == nil {
		return empty.RootHash
	}

	h := t.getHasher()
	defer returnHasherToPool(h)

	var result common.Hash
	_, _ = h.hash(t.RootNode, true, result[:])

	return result
}

func (t *Trie) Reset() {
	resetRefs(t.RootNode)
}

func (t *Trie) getHasher() *hasher {
	return t.newHasherFunc()
}

// DeepHash returns internal hash of a node reachable by the specified key prefix.
// Note that if the prefix points into the middle of a key for a leaf node or of an extension
// node, it will return the hash of a modified leaf node or extension node, where the
// key prefix is removed from the key.
// First returned value is `true` if the node with the specified prefix is found.
func (t *Trie) DeepHash(keyPrefix []byte) (bool, common.Hash, error) {
	hexPrefix := nibbles.KeybytesToHex(keyPrefix)
	accNode, gotValue := t.getAccount(t.RootNode, hexPrefix, 0)
	if !gotValue {
		return false, common.Hash{}, nil
	}
	if accNode.RootCorrect {
		return true, accNode.Root, nil
	}
	if accNode.Storage == nil {
		accNode.Root = empty.RootHash
		accNode.RootCorrect = true
	} else {
		h := t.getHasher()
		defer returnHasherToPool(h)
		if _, err := h.hash(accNode.Storage, true, accNode.Root[:]); err != nil {
			return false, common.Hash{}, err
		}
	}
	return true, accNode.Root, nil
}

// RLPEncode traverses the trie from root to leaves and collects
// all unique RLP-encoded nodes.
func (t *Trie) RLPEncode() ([][]byte, error) {
	if t == nil || t.RootNode == nil {
		return nil, nil
	}

	var nodes [][]byte
	seen := make(map[common.Hash]struct{})
	h := newHasher(t.valueNodesRLPEncoded)
	defer returnHasherToPool(h)

	var collect func(node Node) error
	collect = func(node Node) error {
		if node == nil {
			return nil
		}

		switch n := node.(type) {
		case *ShortNode:
			nodeRLP, err := h.hashChildren(n, 0)
			if err != nil {
				return err
			}
			hash := crypto.Keccak256Hash(nodeRLP)
			if _, ok := seen[hash]; !ok {
				seen[hash] = struct{}{}
				nodes = append(nodes, bytes.Clone(nodeRLP))
			}
			return collect(n.Val)

		case *DuoNode:
			nodeRLP, err := h.hashChildren(n, 0)
			if err != nil {
				return err
			}
			hash := crypto.Keccak256Hash(nodeRLP)
			if _, ok := seen[hash]; !ok {
				seen[hash] = struct{}{}
				nodes = append(nodes, bytes.Clone(nodeRLP))
			}
			if err := collect(n.child1); err != nil {
				return err
			}
			return collect(n.child2)

		case *FullNode:
			nodeRLP, err := h.hashChildren(n, 0)
			if err != nil {
				return err
			}
			hash := crypto.Keccak256Hash(nodeRLP)
			if _, ok := seen[hash]; !ok {
				seen[hash] = struct{}{}
				nodes = append(nodes, bytes.Clone(nodeRLP))
			}
			for i := range 17 {
				if n.Children[i] != nil {
					if err := collect(n.Children[i]); err != nil {
						return err
					}
				}
			}
			return nil

		case *AccountNode:
			// AccountNode may have a storage trie
			if n.Storage != nil {
				return collect(n.Storage)
			}
			return nil

		case ValueNode:
			// Leaf value, nothing to collect
			return nil

		case *HashNode:
			// HashNode means this subtrie wasn't expanded
			return nil

		default:
			return nil
		}
	}

	if err := collect(t.RootNode); err != nil {
		return nil, err
	}

	return nodes, nil
}

// RLPDecode reconstructs a trie from RLP-encoded nodes produced by RLPEncode.
// The first element must be the root node.
func RLPDecode(encodedNodes [][]byte) (*Trie, error) {
	if len(encodedNodes) == 0 {
		return New(common.Hash{}), nil
	}

	// Build a map from hash -> decoded node
	nodeMap := make(map[common.Hash]Node)
	for _, encoded := range encodedNodes {
		// The legacy witness carries the empty storage-trie preimage (RLP empty
		// string, keccak256 == EmptyRoot); it is not a trie node, so skip it.
		if len(encoded) == 1 && encoded[0] == 0x80 {
			continue
		}
		hash := crypto.Keccak256Hash(encoded)
		node, err := decodeTrieNode(encoded)
		if err != nil {
			return nil, fmt.Errorf("failed to decode node: %w", err)
		}
		nodeMap[hash] = node
	}

	// Decode the root node (first in the list)
	rootHash := crypto.Keccak256Hash(encodedNodes[0])
	rootNode, ok := nodeMap[rootHash]
	if !ok {
		return nil, errors.New("root node not found in map")
	}

	// Resolve all HashNodes by looking them up in the map
	resolved, err := resolveHashNodes(rootNode, nodeMap /* insideStorageTree */, false)
	if err != nil {
		return nil, err
	}

	return NewInMemoryTrie(resolved), nil
}

// decodeTrieNode decodes an RLP-encoded trie node for trie reconstruction.
// Unlike decodeNode (used for proof verification), this fully decodes leaf values.
func decodeTrieNode(encoded []byte) (Node, error) {
	if len(encoded) == 0 {
		return nil, errors.New("nodes must not be zero length")
	}
	elems, _, err := rlp.SplitList(encoded)
	if err != nil {
		return nil, err
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		return decodeTrieShort(elems)
	case 17:
		return decodeTrieFull(elems)
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

// decodeTrieShort decodes a short node (extension or leaf) for trie reconstruction.
func decodeTrieShort(elems []byte) (*ShortNode, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	kb := CompactToKeybytes(kbuf)
	if kb.Terminating {
		// For leaf nodes, the value is double-RLP encoded
		// First rlp.SplitString gets the outer RLP string
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, err
		}
		// Decode the inner RLP string to get the raw value
		rawVal, _, err := rlp.SplitString(val)
		if err != nil {
			// If inner decode fails, value might be empty or already raw
			return &ShortNode{
				Key: kb.ToHex(),
				Val: ValueNode(val),
			}, nil
		}
		return &ShortNode{
			Key: kb.ToHex(),
			Val: ValueNode(rawVal),
		}, nil
	}

	val, _, err := decodeTrieRef(rest)
	if err != nil {
		return nil, err
	}
	return &ShortNode{
		Key: kb.ToHex(),
		Val: val,
	}, nil
}

// decodeTrieFull decodes a full node (branch) for trie reconstruction.
func decodeTrieFull(elems []byte) (*FullNode, error) {
	n := &FullNode{}
	for i := range 16 {
		var err error
		n.Children[i], elems, err = decodeTrieRef(elems)
		if err != nil {
			return nil, err
		}
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	if len(val) > 0 {
		// Decode inner RLP for the value
		rawVal, _, err := rlp.SplitString(val)
		if err != nil {
			n.Children[16] = ValueNode(val)
		} else {
			n.Children[16] = ValueNode(rawVal)
		}
	}
	return n, nil
}

// decodeTrieRef decodes a node reference for trie reconstruction.
func decodeTrieRef(buf []byte) (Node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, nil, err
	}
	switch {
	case kind == rlp.List:
		if len(buf)-len(rest) >= 32 {
			return nil, nil, errors.New("embedded nodes must be less than hash size")
		}
		n, err := decodeTrieNode(buf)
		if err != nil {
			return nil, nil, err
		}
		return n, rest, nil
	case kind == rlp.String && len(val) == 0:
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return &HashNode{hash: val}, rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 through 32)", len(val))
	}
}

// decodeAccountNode attempts to decode a ValueNode as an AccountNode.
// Returns the AccountNode if successful
func decodeAccountNode(val ValueNode, nodeMap map[common.Hash]Node) (*AccountNode, error) {
	if len(val) == 0 {
		return nil, nil
	}

	// Try to decode as account RLP: [nonce, balance, storageRoot, codeHash]
	acc := new(accounts.Account)
	if err := acc.DecodeForHashing(val); err != nil {
		return nil, err
	}

	// Successfully decoded as account
	an := &AccountNode{
		Account:     *acc,
		RootCorrect: true,
	}
	// -1 marks a code-bearing proof node whose code isn't in the witness; an
	// empty-code account must stay 0 so serialization emits no bogus code size.
	if !acc.IsEmptyCodeHash() {
		an.CodeSize = codeSizeUncached
	}

	// If account has non-empty storage root, try to find it in nodeMap
	if acc.Root != empty.RootHash && acc.Root != (common.Hash{}) {
		if storageNode, ok := nodeMap[acc.Root]; ok {
			an.Storage = storageNode
		} else {
			// Storage root exists but we don't have the nodes - use HashNode
			an.Storage = &HashNode{hash: acc.Root[:]}
		}
	}

	return an, nil
}

// resolveHashNodes recursively replaces HashNodes with their actual nodes from the map
// and converts ValueNodes containing account data back into AccountNodes.
func resolveHashNodes(node Node, nodeMap map[common.Hash]Node, insideStorageTree bool) (Node, error) {
	if node == nil {
		return nil, nil
	}

	switch n := node.(type) {
	case *ShortNode:
		resolved, err := resolveHashNodes(n.Val, nodeMap, insideStorageTree)
		if err != nil {
			return nil, err
		}

		// Check if this is a leaf node (terminating key) with a ValueNode
		// that might be account data
		// resolve value node only if we're not inside the storage tree
		if vn, ok := resolved.(ValueNode); ok && len(n.Key) > 0 && n.Key[len(n.Key)-1] == 16 && !insideStorageTree {
			// Key ends with terminator (16), this is a leaf
			an, err := decodeAccountNode(vn, nodeMap)
			if err != nil {
				return nil, fmt.Errorf("failed to decode AccountNode : %w", err)
			}
			if an == nil {
				return nil, fmt.Errorf("AccountNode decoded into nil")
			}
			// Resolve storage if present
			if an.Storage != nil {
				resolvedStorage, err := resolveHashNodes(an.Storage, nodeMap /* insideStorageTree */, true)
				if err != nil {
					return nil, err
				}
				an.Storage = resolvedStorage
			}
			return &ShortNode{
				Key: n.Key,
				Val: an,
			}, nil
		}

		return &ShortNode{
			Key: n.Key,
			Val: resolved,
		}, nil

	case *FullNode:
		newNode := &FullNode{}
		for i := range 17 {
			if n.Children[i] != nil {
				resolved, err := resolveHashNodes(n.Children[i], nodeMap, insideStorageTree)
				if err != nil {
					return nil, err
				}
				newNode.Children[i] = resolved
			}
		}
		return newNode, nil

	case HashNode:
		hash := common.BytesToHash(n.hash)
		if resolved, ok := nodeMap[hash]; ok {
			// Recursively resolve the looked-up node
			return resolveHashNodes(resolved, nodeMap, insideStorageTree)
		}
		// HashNode not in map, keep as is (partial trie)
		return n, nil

	case *HashNode:
		hash := common.BytesToHash(n.hash)
		if resolved, ok := nodeMap[hash]; ok {
			return resolveHashNodes(resolved, nodeMap, insideStorageTree)
		}
		return n, nil

	case ValueNode:
		return n, nil

	case *AccountNode:
		if n.Storage != nil {
			resolved, err := resolveHashNodes(n.Storage, nodeMap /* inside storage tree */, true)
			if err != nil {
				return nil, err
			}
			newNode := *n
			newNode.Storage = resolved
			return &newNode, nil
		}
		return n, nil

	default:
		return n, nil
	}
}

// GetNode returns the trie node found at the given hex-nibble path,
// or nil if the path does not lead to a node.
func (t *Trie) GetNode(path []byte) Node {
	return getNode(t.RootNode, path, 0)
}

func getNode(n Node, path []byte, pos int) Node {
	if n == nil {
		return nil
	}
	if pos >= len(path) {
		return n
	}
	switch nd := n.(type) {
	case *ShortNode:
		key := nd.Key
		// Strip terminator if present
		if len(key) > 0 && key[len(key)-1] == 16 {
			key = key[:len(key)-1]
		}
		remaining := path[pos:]
		if len(remaining) < len(key) {
			// Path is a prefix of the short node key — we're inside this node
			if hasPrefix(key, remaining) {
				return nd
			}
			return nil
		}
		if !hasPrefix(remaining, key) {
			return nil
		}
		return getNode(nd.Val, path, pos+len(key))
	case *FullNode:
		child := nd.Children[path[pos]]
		return getNode(child, path, pos+1)
	case *DuoNode:
		i1, i2 := nd.childrenIdx()
		nibble := path[pos]
		if nibble == i1 {
			return getNode(nd.child1, path, pos+1)
		}
		if nibble == i2 {
			return getNode(nd.child2, path, pos+1)
		}
		return nil
	case *AccountNode:
		// If we've reached an account node and there's more path,
		// descend into the account's storage trie
		return getNode(nd.Storage, path, pos)
	default:
		// HashNode, ValueNode — terminal, no further traversal
		return n
	}
}

func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i, b := range prefix {
		if s[i] != b {
			return false
		}
	}
	return true
}
