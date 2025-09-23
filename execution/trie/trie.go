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
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	// EmptyRoot is the known root hash of an empty trie.
	// DESCRIBED: docs/programmers_guide/guide.md#root
	EmptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)
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
	strictHash    bool // if true, the trie will panic on a hash access
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
// Deprecated
// use package turbo/trie
func New(root common.Hash) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ false) },
	}
	if (root != common.Hash{}) && root != EmptyRoot {
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

// this will merge node2 into node1, returns a boolean mergeNecessary, if it was necessary to replace a child.
// If not, then the two full nodes are the same so no replacement was necessary
// This function also performs certain sanity checks which can result in an error if they fail
func merge2FullNodes(node1, node2 *FullNode) (bool, error) {
	furtherMergingNeeded := false
	for i := 0; i < len(node1.Children); i++ {
		// either both children are hashnodes, or only one of them is, or none of them is.
		// if both of the two children (of trie1 and trie2) at a certain index are not hashnodes
		// they must be the same type (e.g. both a FullNode, or both a ShortNode, or both nil) . If this is true for all children then no merge takes place at this level.
		child1 := node1.Children[i]
		child2 := node2.Children[i]
		if hashNode1, ok1 := child1.(*HashNode); ok1 { // child1 is a hashnode
			if hashNode2, ok2 := child2.(*HashNode); ok2 { //child2 is a hashnode
				// both are hashnodes
				if !bytes.Equal(hashNode1.hash, hashNode2.hash) { // sanity check
					return false, fmt.Errorf("children hashnodes have different hashes: hash1(%x)!=hash2(%x)", hashNode1.hash, hashNode2.hash)
				}
			} else if child2 == nil {
				return false, fmt.Errorf("child of tr2 should not be nil, because child of tr1 is a hashnode")
			} else { // child2 is not a hashnode, in this case replace the hashnode in tree 1 by child2 which has the expanded node type
				node1.Children[i] = child2
			}
		} else if child1 == nil {
			if child2 != nil {
				// sanity check
				return false, fmt.Errorf("child of first node is nil , but corresponding child of second node is non-nil")
			}
		} else { // child1 is not nil and not a hashnode
			if _, ok2 := child2.(*HashNode); !ok2 { // if child2 is not hashnode, now they are expected to have the same type , if child2 is a hashnode then no changes are necessary to node1
				if reflect.TypeOf(child1) != reflect.TypeOf(child2) { // sanity check
					return false, fmt.Errorf("children have different types: %T != %T", child1, child2)
				} else { // further merging will be needed at the next level
					furtherMergingNeeded = true
				}
			}
		}
	}
	return furtherMergingNeeded, nil
}

func merge2ShortNodes(node1, node2 *ShortNode) (bool, error) {
	furtherMergingNeeded := false
	if !bytes.Equal(node1.Key, node2.Key) { // sanity check
		return false, fmt.Errorf("mismatch in the short node keys node1.Key(%x)!=node2.Key(%x)", node1.Key, node2.Key)
	}
	if hashNode1, ok1 := node1.Val.(*HashNode); ok1 { // node1.Val is a HashNode
		if hashNode2, ok2 := node2.Val.(*HashNode); ok2 { // node2.Val is a HashNode
			// both are hashnodes
			if !bytes.Equal(hashNode1.hash, hashNode2.hash) { // sanity check
				return false, fmt.Errorf("hashnodes have different hashes: hash1(%x) != hash2(%x)", hashNode1.hash, hashNode2.hash)
			}
		} else if node2.Val == nil {
			return false, fmt.Errorf("node2.Val should not be nil, because node1.Val is a hashnode")
		} else { // in this case node2.Val is not a HashNode, while node1.Val is a hash node, so replace node1.Val by node2.Val, and the merging is complete
			node1.Val = node2.Val
		}
	} else { // node1.Val is not a hashnode
		// if node2.Val is not  a hashnode, node2.Val is expected to have the same type as node1.Val, otherwise if it is a hashnode no action is necessary (just ignore the hashnode)
		if _, ok2 := node2.Val.(*HashNode); !ok2 {
			if reflect.TypeOf(node1.Val) != reflect.TypeOf(node2.Val) { // sanity check
				return false, fmt.Errorf("node1.Val and node2.Val have different types: %T != %T ", node1.Val, node2.Val)
			} else {
				furtherMergingNeeded = true
			}
		}
	}
	return furtherMergingNeeded, nil
}

func merge2AccountNodes(node1, node2 *AccountNode) (furtherMergingNeeded bool) {
	storage1 := node1.Storage
	storage2 := node2.Storage
	if storage1 == nil || storage2 == nil { // in this case do nothing, we can use the storage tree of node 1
		return false
	}
	_, isHashNode1 := storage1.(*HashNode) // check if storage1 is a hashnode
	_, isHashNode2 := storage2.(*HashNode) // check if storage2 is a hashnode
	if isHashNode1 && !isHashNode2 {       // node2 has the expanded storage trie, so use that instead of the hashnode
		node1.Storage = storage2
		return false
	}

	if !isHashNode1 && !isHashNode2 { // the 2 storage tries need to be merged
		return true
	}
	return false
}

func merge2Tries(tr1 *Trie, tr2 *Trie) (*Trie, error) {
	// starting from the roots merge each level
	rootNode1 := tr1.RootNode
	rootNode2 := tr2.RootNode
	mergeComplete := false

	for !mergeComplete {
		switch node1 := (rootNode1).(type) {
		case nil:
			// sanity checks might be good later on
			return nil, nil
		case *ShortNode:
			node2, ok := rootNode2.(*ShortNode)
			if !ok {
				return nil, fmt.Errorf("expected *trie.ShortNode in trie 2, but got %T", rootNode2)
			}
			furtherMergingNeeded, err := merge2ShortNodes(node1, node2)
			if err != nil {
				return nil, err
			}
			if furtherMergingNeeded {
				rootNode1 = node1.Val
				rootNode2 = node2.Val
			} else {
				mergeComplete = true
			}
		case *FullNode:
			node2, ok := rootNode2.(*FullNode)
			if !ok {
				return nil, fmt.Errorf("expected *trie.FullNode in trie 2, but got %T", rootNode2)
			}
			furthedMergingNeeded, err := merge2FullNodes(node1, node2)
			if err != nil {
				return nil, err
			}
			if furthedMergingNeeded { // find the next nodes to merge
				nextRootsFound := false
				for i := 0; i < len(node1.Children); i++ { // it is guaranteed that we will find a non-nil, non-hashnode
					childNode1 := node1.Children[i]
					childNode2 := node2.Children[i]
					if _, isHashNode := childNode2.(*HashNode); childNode2 != nil && !isHashNode {
						// update rootNode1, and rootNode2 to merge at the next level at the next iteration
						rootNode1 = childNode1
						rootNode2 = childNode2
						nextRootsFound = true
						break
					}
				}
				if !nextRootsFound {
					return nil, errors.New("could not find next node pair to merge")
				}
			} else {
				mergeComplete = true
			}
		case *HashNode:
			return tr2, nil
		case ValueNode:
			return tr1, nil
		case *AccountNode:
			node2, ok := rootNode2.(*AccountNode)
			if !ok {
				return nil, fmt.Errorf("expected *trie.AccountNode in trie 2, but got %T", rootNode2)
			}
			furthedMergingNeeded := merge2AccountNodes(node1, node2)
			if !furthedMergingNeeded {
				return tr1, nil
			} else {
				// need to merge storage trees
				rootNode1 = node1.Storage
				rootNode2 = node2.Storage
			}

		}
	}
	return tr1, nil
}
func MergeTries(tries []*Trie) (*Trie, error) {
	if len(tries) == 0 {
		return nil, nil
	}

	if len(tries) == 1 {
		return tries[0], nil
	}

	resultingTrie := tries[0]
	for i := 1; i < len(tries); i++ {
		resultingTrie, err := merge2Tries(resultingTrie, tries[i])
		if err != nil {
			return resultingTrie, err
		}
	}
	return resultingTrie, nil
}

// NewTestRLPTrie treats all the data provided to `Update` function as rlp-encoded.
// it is usually used for testing purposes.
func NewTestRLPTrie(root common.Hash) *Trie {
	trie := &Trie{
		valueNodesRLPEncoded: true,
		newHasherFunc:        func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ true) },
	}
	if (root != common.Hash{}) && root != EmptyRoot {
		trie.RootNode = &HashNode{hash: root[:]}
	}
	return trie
}

func (t *Trie) SetStrictHash(strict bool) {
	t.strictHash = strict
}

// Get returns the value for key stored in the trie.
func (t *Trie) Get(key []byte) (value []byte, gotValue bool) {
	if t.RootNode == nil {
		return nil, true
	}

	hex := keybytesToHex(key)
	return t.get(t.RootNode, hex, 0)
}

func (t *Trie) FindPath(key []byte) (value []byte, parents [][]byte, gotValue bool) {
	if t.RootNode == nil {
		return nil, nil, true
	}

	hex := keybytesToHex(key)
	return t.getPath(t.RootNode, nil, hex, 0)
}

func (t *Trie) GetAccount(key []byte) (value *accounts.Account, gotValue bool) {
	if t.RootNode == nil {
		return nil, true
	}

	hex := keybytesToHex(key)

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

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode != nil {
		if bytes.Equal(accNode.Account.CodeHash[:], emptyCodeHash[:]) {
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

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode != nil {
		if bytes.Equal(accNode.Account.CodeHash[:], emptyCodeHash[:]) {
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
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case *ShortNode:
		matchlen := prefixLen(key[pos:], n.Key)
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
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case ValueNode:
		return n, true
	case *AccountNode:
		return t.get(n.Storage, key, pos)
	case *ShortNode:
		matchlen := prefixLen(key[pos:], n.Key)
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

func (t *Trie) getPath(origNode Node, parents [][]byte, key []byte, pos int) ([]byte, [][]byte, bool) {
	switch n := (origNode).(type) {
	case nil:
		return nil, parents, true
	case ValueNode:
		return n, parents, true
	case *AccountNode:
		return t.getPath(n.Storage, append(parents, n.reference()), key, pos)
	case *ShortNode:
		matchlen := prefixLen(key[pos:], n.Key)
		if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
			return t.getPath(n.Val, append(parents, n.reference()), key, pos+matchlen)
		} else {
			return nil, parents, true
		}

	case *DuoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			return t.getPath(n.child1, append(parents, n.reference()), key, pos+1)
		case i2:
			return t.getPath(n.child2, append(parents, n.reference()), key, pos+1)
		default:
			return nil, parents, true
		}
	case *FullNode:
		child := n.Children[key[pos]]
		if child == nil {
			return nil, parents, true
		}
		return t.getPath(child, append(parents, n.reference()), key, pos+1)
	case *HashNode:
		return n.hash, parents, false

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
	hex := keybytesToHex(key)

	newnode := ValueNode(value)

	if t.RootNode == nil {
		t.RootNode = NewShortNode(hex, newnode)
	} else {
		_, t.RootNode = t.insert(t.RootNode, hex, ValueNode(value))
	}
}

func (t *Trie) UpdateAccount(key []byte, acc *accounts.Account) {
	//make account copy. There are some pointer into big.Int
	value := new(accounts.Account)
	value.Copy(acc)

	hex := keybytesToHex(key)

	var newnode *AccountNode
	if value.Root == EmptyRoot || value.Root == (common.Hash{}) {
		newnode = &AccountNode{*value, nil, true, nil, codeSizeUncached}
	} else {
		newnode = &AccountNode{*value, HashNode{hash: value.Root[:]}, true, nil, codeSizeUncached}
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

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode == nil || !gotValue {
		return fmt.Errorf("account not found with key: %x", key)
	}

	actualCodeHash := crypto.Keccak256(code)
	if !bytes.Equal(accNode.CodeHash[:], actualCodeHash) {
		return fmt.Errorf("inserted code mismatch account hash (acc.CodeHash=%x codeHash=%x)", accNode.CodeHash[:], actualCodeHash)
	}

	accNode.Code = code
	accNode.CodeSize = len(code)

	// t.insert will call the observer methods itself
	_, t.RootNode = t.insert(t.RootNode, hex, accNode)
	return nil
}

// UpdateAccountCodeSize attaches the code size to the account
func (t *Trie) UpdateAccountCodeSize(key []byte, codeSize int) error {
	if t.RootNode == nil {
		return nil
	}

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.RootNode, hex, 0)
	if accNode == nil || !gotValue {
		return fmt.Errorf("account not found with key: %x", key)
	}

	accNode.CodeSize = codeSize

	// t.insert will call the observer methods itself
	_, t.RootNode = t.insert(t.RootNode, hex, accNode)
	return nil
}

// LoadRequestForCode Code expresses the need to fetch code from the DB (by its hash) and attach
// to a specific account leaf in the trie.
type LoadRequestForCode struct {
	t        *Trie
	addrHash common.Hash // contract address hash
	codeHash common.Hash
	bytecode bool // include the bytecode too
}

func (lrc *LoadRequestForCode) String() string {
	return fmt.Sprintf("rr_code{addrHash:%x,codeHash:%x,bytecode:%v}", lrc.addrHash, lrc.codeHash, lrc.bytecode)
}

func (t *Trie) NewLoadRequestForCode(addrHash common.Hash, codeHash common.Hash, bytecode bool) *LoadRequestForCode {
	return &LoadRequestForCode{t, addrHash, codeHash, bytecode}
}

func (t *Trie) NeedLoadCode(addrHash common.Hash, codeHash common.Hash, bytecode bool) (bool, *LoadRequestForCode) {
	if bytes.Equal(codeHash[:], emptyCodeHash[:]) {
		return false, nil
	}

	var ok bool
	if bytecode {
		_, ok = t.GetAccountCode(addrHash[:])
	} else {
		_, ok = t.GetAccountCodeSize(addrHash[:])
	}
	if !ok {
		return true, t.NewLoadRequestForCode(addrHash, codeHash, bytecode)
	}

	return false, nil
}

// FindSubTriesToLoad walks over the trie and creates the list of DB prefixes and
// corresponding list of valid bits in the prefix (for the cases when prefix contains an
// odd number of nibbles) that would allow loading the missing information from the database
// It also create list of `hooks`, the paths in the trie (in nibbles) where the loaded
// sub-tries need to be inserted.
func (t *Trie) FindSubTriesToLoad(rl RetainDecider) (prefixes [][]byte, fixedbits []int, hooks [][]byte) {
	return findSubTriesToLoad(t.RootNode, nil, nil, rl, nil, 0, nil, nil, nil)
}

var bytes8 [8]byte
var bytes16 [16]byte

func findSubTriesToLoad(nd Node, nibblePath []byte, hook []byte, rl RetainDecider, dbPrefix []byte, bits int, prefixes [][]byte, fixedbits []int, hooks [][]byte) (newPrefixes [][]byte, newFixedBits []int, newHooks [][]byte) {
	switch n := nd.(type) {
	case *ShortNode:
		nKey := n.Key
		if nKey[len(nKey)-1] == 16 {
			nKey = nKey[:len(nKey)-1]
		}
		nibblePath = append(nibblePath, nKey...)
		hook = append(hook, nKey...)
		if !rl.Retain(nibblePath) {
			return prefixes, fixedbits, hooks
		}
		for _, b := range nKey {
			if bits%8 == 0 {
				dbPrefix = append(dbPrefix, b<<4)
			} else {
				dbPrefix[len(dbPrefix)-1] &= 0xf0
				dbPrefix[len(dbPrefix)-1] |= b & 0xf
			}
			bits += 4
		}
		return findSubTriesToLoad(n.Val, nibblePath, hook, rl, dbPrefix, bits, prefixes, fixedbits, hooks)
	case *DuoNode:
		i1, i2 := n.childrenIdx()
		newPrefixes = prefixes
		newFixedBits = fixedbits
		newHooks = hooks
		newNibblePath := append(nibblePath, i1)
		newHook := append(hook, i1)
		if rl.Retain(newNibblePath) {
			var newDbPrefix []byte
			if bits%8 == 0 {
				newDbPrefix = append(dbPrefix, i1<<4)
			} else {
				newDbPrefix = dbPrefix
				newDbPrefix[len(newDbPrefix)-1] &= 0xf0
				newDbPrefix[len(newDbPrefix)-1] |= i1 & 0xf
			}
			newPrefixes, newFixedBits, newHooks = findSubTriesToLoad(n.child1, newNibblePath, newHook, rl, newDbPrefix, bits+4, newPrefixes, newFixedBits, newHooks)
		}
		newNibblePath = append(nibblePath, i2)
		newHook = append(hook, i2)
		if rl.Retain(newNibblePath) {
			var newDbPrefix []byte
			if bits%8 == 0 {
				newDbPrefix = append(dbPrefix, i2<<4)
			} else {
				newDbPrefix = dbPrefix
				newDbPrefix[len(newDbPrefix)-1] &= 0xf0
				newDbPrefix[len(newDbPrefix)-1] |= i2 & 0xf
			}
			newPrefixes, newFixedBits, newHooks = findSubTriesToLoad(n.child2, newNibblePath, newHook, rl, newDbPrefix, bits+4, newPrefixes, newFixedBits, newHooks)
		}
		return newPrefixes, newFixedBits, newHooks
	case *FullNode:
		newPrefixes = prefixes
		newFixedBits = fixedbits
		newHooks = hooks
		var newNibblePath []byte
		var newHook []byte
		for i, child := range n.Children {
			if child != nil {
				newNibblePath = append(nibblePath, byte(i))
				newHook = append(hook, byte(i))
				if rl.Retain(newNibblePath) {
					var newDbPrefix []byte
					if bits%8 == 0 {
						newDbPrefix = append(dbPrefix, byte(i)<<4)
					} else {
						newDbPrefix = dbPrefix
						newDbPrefix[len(newDbPrefix)-1] &= 0xf0
						newDbPrefix[len(newDbPrefix)-1] |= byte(i) & 0xf
					}
					newPrefixes, newFixedBits, newHooks = findSubTriesToLoad(child, newNibblePath, newHook, rl, newDbPrefix, bits+4, newPrefixes, newFixedBits, newHooks)
				}
			}
		}
		return newPrefixes, newFixedBits, newHooks
	case *AccountNode:
		if n.Storage == nil {
			return prefixes, fixedbits, hooks
		}
		binary.BigEndian.PutUint64(bytes8[:], n.Incarnation)
		dbPrefix = append(dbPrefix, bytes8[:]...)
		// Add decompressed incarnation to the nibblePath
		for i, b := range bytes8[:] {
			bytes16[i*2] = b / 16
			bytes16[i*2+1] = b % 16
		}
		nibblePath = append(nibblePath, bytes16[:]...)
		newPrefixes = prefixes
		newFixedBits = fixedbits
		newHooks = hooks
		if rl.Retain(nibblePath) {
			newPrefixes, newFixedBits, newHooks = findSubTriesToLoad(n.Storage, nibblePath, hook, rl, dbPrefix, bits+64, prefixes, fixedbits, hooks)
		}
		return newPrefixes, newFixedBits, newHooks
	case *HashNode:
		newPrefixes = append(prefixes, common.Copy(dbPrefix))
		newFixedBits = append(fixedbits, bits)
		newHooks = append(hooks, common.Copy(hook))
		return newPrefixes, newFixedBits, newHooks
	}
	return prefixes, fixedbits, hooks
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
				if !bytes.Equal(origAccN.CodeHash[:], vAccN.CodeHash[:]) {
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
		return true, NewShortNode(common.Copy(key[pos:]), value)
	case *AccountNode:
		updated, nn = t.insertRecursive(n.Storage, key, pos, value)
		if updated {
			n.Storage = nn
			n.RootCorrect = false
		}
		return updated, n
	case *ShortNode:
		matchlen := prefixLen(key[pos:], n.Key)
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
				c1 = NewShortNode(common.Copy(n.Key[matchlen+1:]), n.Val)
			}
			var c2 Node
			if len(key) == pos+matchlen+1 {
				c2 = value
			} else {
				c2 = NewShortNode(common.Copy(key[pos+matchlen+1:]), value)
			}
			branch := &DuoNode{}
			if n.Key[matchlen] < key[pos+matchlen] {
				branch.child1 = c1
				branch.child2 = c2
			} else {
				branch.child1 = c2
				branch.child2 = c1
			}
			branch.mask = (1 << (n.Key[matchlen])) | (1 << (key[pos+matchlen]))

			// Replace this shortNode with the branch if it occurs at index 0.
			if matchlen == 0 {
				newNode = branch // current node leaves the generation, but new node branch joins it
			} else {
				// Otherwise, replace it with a short node leading up to the branch.
				n.Key = common.Copy(key[pos : pos+matchlen])
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
				child = NewShortNode(common.Copy(key[pos+1:]), value)
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
				n.Children[key[pos]] = NewShortNode(common.Copy(key[pos+1:]), value)
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
func (t *Trie) getNode(hex []byte, doTouch bool) (Node, Node, bool, uint64) {
	var nd = t.RootNode
	var parent Node
	pos := 0
	var account bool
	var incarnation uint64
	for pos < len(hex) || account {
		switch n := nd.(type) {
		case nil:
			return nil, nil, false, incarnation
		case *ShortNode:
			matchlen := prefixLen(hex[pos:], n.Key)
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

func (t *Trie) HookSubTries(subTries SubTries, hooks [][]byte) error {
	for i, hookNibbles := range hooks {
		root := subTries.roots[i]
		hash := subTries.Hashes[i]
		if root == nil {
			return fmt.Errorf("root==nil for hook %x", hookNibbles)
		}
		if err := t.hook(hookNibbles, root, hash[:]); err != nil {
			return fmt.Errorf("hook %x: %w", hookNibbles, err)
		}
	}
	return nil
}

func (t *Trie) hook(hex []byte, n Node, hash []byte) error {
	nd, parent, ok, incarnation := t.getNode(hex, true)
	if !ok {
		return nil
	}
	if _, ok := nd.(ValueNode); ok {
		return nil
	}
	if hn, ok := nd.(HashNode); ok {
		if !bytes.Equal(hn.hash, hash) {
			return fmt.Errorf("wrong hash when hooking, expected %s, sub-tree hash %x", hn, hash)
		}
	} else if nd != nil {
		return fmt.Errorf("expected hash node at %x, got %T", hex, nd)
	}

	t.touchAll(n, hex, false, incarnation)
	switch p := parent.(type) {
	case nil:
		t.RootNode = n
	case *ShortNode:
		p.Val = n
	case *DuoNode:
		i1, i2 := p.childrenIdx()
		switch hex[len(hex)-1] {
		case i1:
			p.child1 = n
		case i2:
			p.child2 = n
		}
	case *FullNode:
		idx := hex[len(hex)-1]
		p.Children[idx] = n
	case *AccountNode:
		p.Storage = n
	}
	return nil
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
	hex := keybytesToHex(key)
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
		matchlen := prefixLen(key[keyStart:], n.Key)
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
			if count == 1 {
				newNode = t.convertToShortNode(n.Children[pos1], uint(pos1))
			} else if count == 2 {
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
			} else if count > 2 {
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
				n.Root = EmptyRoot
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
	hexPrefix := keybytesToHex(keyPrefix)

	_, t.RootNode = t.delete(t.RootNode, hexPrefix, true)

}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

// Root returns the root hash of the trie.
// Deprecated: use Hash instead.
func (t *Trie) Root() []byte { return t.Hash().Bytes() }

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
// DESCRIBED: docs/programmers_guide/guide.md#root
func (t *Trie) Hash() common.Hash {
	if t == nil || t.RootNode == nil {
		return EmptyRoot
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
func (t *Trie) DeepHash(keyPrefix []byte) (bool, common.Hash) {
	hexPrefix := keybytesToHex(keyPrefix)
	accNode, gotValue := t.getAccount(t.RootNode, hexPrefix, 0)
	if !gotValue {
		return false, common.Hash{}
	}
	if accNode.RootCorrect {
		return true, accNode.Root
	}
	if accNode.Storage == nil {
		accNode.Root = EmptyRoot
		accNode.RootCorrect = true
	} else {
		h := t.getHasher()
		defer returnHasherToPool(h)
		h.hash(accNode.Storage, true, accNode.Root[:])
	}
	return true, accNode.Root
}

func (t *Trie) EvictNode(hex []byte) {
	isCode := IsPointingToCode(hex)
	if isCode {
		hex = AddrHashFromCodeKey(hex)
	}

	nd, parent, ok, incarnation := t.getNode(hex, false)
	if !ok {
		return
	}
	if accNode, ok := parent.(*AccountNode); isCode && ok {
		// add special treatment to code nodes
		accNode.Code = nil
		return
	}

	switch nd.(type) {
	case ValueNode, *HashNode:
		return
	default:
		// can work with other nodes type
	}

	var hn common.Hash
	if nd == nil {
		fmt.Printf("nd == nil, hex %x, parent node: %T\n", hex, parent)
		return
	}
	copy(hn[:], nd.reference())
	hnode := &HashNode{hash: hn[:]}

	t.notifyUnloadRecursive(hex, incarnation, nd)

	switch p := parent.(type) {
	case nil:
		t.RootNode = hnode
	case *ShortNode:
		p.Val = hnode
	case *DuoNode:
		i1, i2 := p.childrenIdx()
		switch hex[len(hex)-1] {
		case i1:
			p.child1 = hnode
		case i2:
			p.child2 = hnode
		}
	case *FullNode:
		idx := hex[len(hex)-1]
		p.Children[idx] = hnode
	case *AccountNode:
		p.Storage = hnode
	}
}

func (t *Trie) notifyUnloadRecursive(hex []byte, incarnation uint64, nd Node) {
	switch n := nd.(type) {
	case *ShortNode:
		hex = append(hex, n.Key...)
		if hex[len(hex)-1] == 16 {
			hex = hex[:len(hex)-1]
		}
		t.notifyUnloadRecursive(hex, incarnation, n.Val)
	case *AccountNode:
		if n.Storage == nil {
			return
		}
		if _, ok := n.Storage.(*HashNode); ok {
			return
		}
		t.notifyUnloadRecursive(hex, n.Incarnation, n.Storage)
	case *FullNode:
		for i := range n.Children {
			if n.Children[i] == nil {
				continue
			}
			if _, ok := n.Children[i].(*HashNode); ok {
				continue
			}
			t.notifyUnloadRecursive(append(hex, uint8(i)), incarnation, n.Children[i])
		}
	case *DuoNode:
		i1, i2 := n.childrenIdx()
		if n.child1 != nil {
			t.notifyUnloadRecursive(append(hex, i1), incarnation, n.child1)
		}
		if n.child2 != nil {
			t.notifyUnloadRecursive(append(hex, i2), incarnation, n.child2)
		}
	default:
		// nothing to do
	}
}

func (t *Trie) TrieSize() int {
	return calcSubtreeSize(t.RootNode)
}

func (t *Trie) NumberOfAccounts() int {
	return calcSubtreeNodes(t.RootNode)
}
