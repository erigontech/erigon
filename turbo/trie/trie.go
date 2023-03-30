// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb"
)

var (
	// EmptyRoot is the known root hash of an empty trie.
	// DESCRIBED: docs/programmers_guide/guide.md#root
	EmptyRoot = libcommon.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

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
	root node

	newHasherFunc func() *hasher
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
// Deprecated
// use package turbo/trie
func New(root libcommon.Hash) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ false) },
	}
	if (root != libcommon.Hash{}) && root != EmptyRoot {
		trie.root = hashNode{hash: root[:]}
	}
	return trie
}

// NewTestRLPTrie treats all the data provided to `Update` function as rlp-encoded.
// it is usually used for testing purposes.
func NewTestRLPTrie(root libcommon.Hash) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ true) },
	}
	if (root != libcommon.Hash{}) && root != EmptyRoot {
		trie.root = hashNode{hash: root[:]}
	}
	return trie
}

// Get returns the value for key stored in the trie.
func (t *Trie) Get(key []byte) (value []byte, gotValue bool) {
	if t.root == nil {
		return nil, true
	}

	hex := keybytesToHex(key)
	return t.get(t.root, hex, 0)
}

func (t *Trie) GetAccount(key []byte) (value *accounts.Account, gotValue bool) {
	if t.root == nil {
		return nil, true
	}

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode != nil {
		var value accounts.Account
		value.Copy(&accNode.Account)
		return &value, gotValue
	}
	return nil, gotValue
}

func (t *Trie) GetAccountCode(key []byte) (value []byte, gotValue bool) {
	if t.root == nil {
		return nil, false
	}

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode != nil {
		if bytes.Equal(accNode.Account.CodeHash[:], EmptyCodeHash[:]) {
			return nil, gotValue
		}

		if accNode.code == nil {
			return nil, false
		}

		return accNode.code, gotValue
	}
	return nil, gotValue
}

func (t *Trie) GetAccountCodeSize(key []byte) (value int, gotValue bool) {
	if t.root == nil {
		return 0, false
	}

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode != nil {
		if bytes.Equal(accNode.Account.CodeHash[:], EmptyCodeHash[:]) {
			return 0, gotValue
		}

		if accNode.codeSize == codeSizeUncached {
			return 0, false
		}

		return accNode.codeSize, gotValue
	}
	return 0, gotValue
}

func (t *Trie) getAccount(origNode node, key []byte, pos int) (value *accountNode, gotValue bool) {
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case *shortNode:
		matchlen := prefixLen(key[pos:], n.Key)
		if matchlen == len(n.Key) {
			if v, ok := n.Val.(*accountNode); ok {
				return v, true
			} else {
				return t.getAccount(n.Val, key, pos+matchlen)
			}
		} else {
			return nil, true
		}
	case *duoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			return t.getAccount(n.child1, key, pos+1)
		case i2:
			return t.getAccount(n.child2, key, pos+1)
		default:
			return nil, true
		}
	case *fullNode:
		child := n.Children[key[pos]]
		return t.getAccount(child, key, pos+1)
	case hashNode:
		return nil, false

	case *accountNode:
		return n, true
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

func (t *Trie) get(origNode node, key []byte, pos int) (value []byte, gotValue bool) {
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case valueNode:
		return n, true
	case *accountNode:
		return t.get(n.storage, key, pos)
	case *shortNode:
		matchlen := prefixLen(key[pos:], n.Key)
		if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
			value, gotValue = t.get(n.Val, key, pos+matchlen)
		} else {
			value, gotValue = nil, true
		}
		return
	case *duoNode:
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
	case *fullNode:
		child := n.Children[key[pos]]
		if child == nil {
			return nil, true
		}
		return t.get(child, key, pos+1)
	case hashNode:
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
	hex := keybytesToHex(key)

	newnode := valueNode(value)

	if t.root == nil {
		t.root = NewShortNode(hex, newnode)
	} else {
		_, t.root = t.insert(t.root, hex, valueNode(value))
	}
}

func (t *Trie) UpdateAccount(key []byte, acc *accounts.Account) {
	//make account copy. There are some pointer into big.Int
	value := new(accounts.Account)
	value.Copy(acc)

	hex := keybytesToHex(key)

	var newnode *accountNode
	if value.Root == EmptyRoot || value.Root == (libcommon.Hash{}) {
		newnode = &accountNode{*value, nil, true, nil, codeSizeUncached}
	} else {
		newnode = &accountNode{*value, hashNode{hash: value.Root[:]}, true, nil, codeSizeUncached}
	}

	if t.root == nil {
		t.root = NewShortNode(hex, newnode)
	} else {
		_, t.root = t.insert(t.root, hex, newnode)
	}
}

// UpdateAccountCode attaches the code node to an account at specified key
func (t *Trie) UpdateAccountCode(key []byte, code codeNode) error {
	if t.root == nil {
		return nil
	}

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode == nil || !gotValue {
		return fmt.Errorf("account not found with key: %x, %w", key, ethdb.ErrKeyNotFound)
	}

	actualCodeHash := crypto.Keccak256(code)
	if !bytes.Equal(accNode.CodeHash[:], actualCodeHash) {
		return fmt.Errorf("inserted code mismatch account hash (acc.CodeHash=%x codeHash=%x)", accNode.CodeHash[:], actualCodeHash)
	}

	accNode.code = code
	accNode.codeSize = len(code)

	// t.insert will call the observer methods itself
	_, t.root = t.insert(t.root, hex, accNode)
	return nil
}

// UpdateAccountCodeSize attaches the code size to the account
func (t *Trie) UpdateAccountCodeSize(key []byte, codeSize int) error {
	if t.root == nil {
		return nil
	}

	hex := keybytesToHex(key)

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode == nil || !gotValue {
		return fmt.Errorf("account not found with key: %x, %w", key, ethdb.ErrKeyNotFound)
	}

	accNode.codeSize = codeSize

	// t.insert will call the observer methods itself
	_, t.root = t.insert(t.root, hex, accNode)
	return nil
}

// LoadRequestForCode Code expresses the need to fetch code from the DB (by its hash) and attach
// to a specific account leaf in the trie.
type LoadRequestForCode struct {
	t        *Trie
	addrHash libcommon.Hash // contract address hash
	codeHash libcommon.Hash
	bytecode bool // include the bytecode too
}

func (lrc *LoadRequestForCode) String() string {
	return fmt.Sprintf("rr_code{addrHash:%x,codeHash:%x,bytecode:%v}", lrc.addrHash, lrc.codeHash, lrc.bytecode)
}

func (t *Trie) NewLoadRequestForCode(addrHash libcommon.Hash, codeHash libcommon.Hash, bytecode bool) *LoadRequestForCode {
	return &LoadRequestForCode{t, addrHash, codeHash, bytecode}
}

func (t *Trie) NeedLoadCode(addrHash libcommon.Hash, codeHash libcommon.Hash, bytecode bool) (bool, *LoadRequestForCode) {
	if bytes.Equal(codeHash[:], EmptyCodeHash[:]) {
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
	return findSubTriesToLoad(t.root, nil, nil, rl, nil, 0, nil, nil, nil)
}

var bytes8 [8]byte
var bytes16 [16]byte

func findSubTriesToLoad(nd node, nibblePath []byte, hook []byte, rl RetainDecider, dbPrefix []byte, bits int, prefixes [][]byte, fixedbits []int, hooks [][]byte) (newPrefixes [][]byte, newFixedBits []int, newHooks [][]byte) {
	switch n := nd.(type) {
	case *shortNode:
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
	case *duoNode:
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
	case *fullNode:
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
	case *accountNode:
		if n.storage == nil {
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
			newPrefixes, newFixedBits, newHooks = findSubTriesToLoad(n.storage, nibblePath, hook, rl, dbPrefix, bits+64, prefixes, fixedbits, hooks)
		}
		return newPrefixes, newFixedBits, newHooks
	case hashNode:
		newPrefixes = append(prefixes, libcommon.Copy(dbPrefix))
		newFixedBits = append(fixedbits, bits)
		newHooks = append(hooks, libcommon.Copy(hook))
		return newPrefixes, newFixedBits, newHooks
	}
	return prefixes, fixedbits, hooks
}

// can pass incarnation=0 if start from root, method internally will
// put incarnation from accountNode when pass it by traverse
func (t *Trie) insert(origNode node, key []byte, value node) (updated bool, newNode node) {
	return t.insertRecursive(origNode, key, 0, value)
}

func (t *Trie) insertRecursive(origNode node, key []byte, pos int, value node) (updated bool, newNode node) {
	if len(key) == pos {
		origN, origNok := origNode.(valueNode)
		vn, vnok := value.(valueNode)
		if origNok && vnok {
			updated = !bytes.Equal(origN, vn)
			if updated {
				newNode = value
			} else {
				newNode = origN
			}
			return
		}
		origAccN, origNok := origNode.(*accountNode)
		vAccN, vnok := value.(*accountNode)
		if origNok && vnok {
			updated = !origAccN.Equals(&vAccN.Account)
			if updated {
				if !bytes.Equal(origAccN.CodeHash[:], vAccN.CodeHash[:]) {
					origAccN.code = nil
				} else if vAccN.code != nil {
					origAccN.code = vAccN.code
				}
				origAccN.Account.Copy(&vAccN.Account)
				origAccN.codeSize = vAccN.codeSize
				origAccN.rootCorrect = false
			}
			newNode = origAccN
			return
		}

		// replacing nodes except accounts
		if !origNok {
			return true, value
		}
	}

	var nn node
	switch n := origNode.(type) {
	case nil:
		return true, NewShortNode(libcommon.Copy(key[pos:]), value)
	case *accountNode:
		updated, nn = t.insertRecursive(n.storage, key, pos, value)
		if updated {
			n.storage = nn
			n.rootCorrect = false
		}
		return updated, n
	case *shortNode:
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
			var c1 node
			if len(n.Key) == matchlen+1 {
				c1 = n.Val
			} else {
				c1 = NewShortNode(libcommon.Copy(n.Key[matchlen+1:]), n.Val)
			}
			var c2 node
			if len(key) == pos+matchlen+1 {
				c2 = value
			} else {
				c2 = NewShortNode(libcommon.Copy(key[pos+matchlen+1:]), value)
			}
			branch := &duoNode{}
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
				n.Key = libcommon.Copy(key[pos : pos+matchlen])
				n.Val = branch
				n.ref.len = 0
				newNode = n
			}
			updated = true
		}
		return

	case *duoNode:
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
			var child node
			if len(key) == pos+1 {
				child = value
			} else {
				child = NewShortNode(libcommon.Copy(key[pos+1:]), value)
			}
			newnode := &fullNode{}
			newnode.Children[i1] = n.child1
			newnode.Children[i2] = n.child2
			newnode.Children[key[pos]] = child
			updated = true
			// current node leaves the generation but newnode joins it
			newNode = newnode
		}
		return

	case *fullNode:
		child := n.Children[key[pos]]
		if child == nil {
			if len(key) == pos+1 {
				n.Children[key[pos]] = value
			} else {
				n.Children[key[pos]] = NewShortNode(libcommon.Copy(key[pos+1:]), value)
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
func (t *Trie) getNode(hex []byte, doTouch bool) (node, node, bool, uint64) {
	var nd = t.root
	var parent node
	pos := 0
	var account bool
	var incarnation uint64
	for pos < len(hex) || account {
		switch n := nd.(type) {
		case nil:
			return nil, nil, false, incarnation
		case *shortNode:
			matchlen := prefixLen(hex[pos:], n.Key)
			if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
				parent = n
				nd = n.Val
				pos += matchlen
				if _, ok := nd.(*accountNode); ok {
					account = true
				}
			} else {
				return nil, nil, false, incarnation
			}
		case *duoNode:
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
		case *fullNode:
			child := n.Children[hex[pos]]
			if child == nil {
				return nil, nil, false, incarnation
			}
			parent = n
			nd = child
			pos++
		case *accountNode:
			parent = n
			nd = n.storage
			incarnation = n.Incarnation
			account = false
		case valueNode:
			return nd, parent, true, incarnation
		case hashNode:
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

func (t *Trie) hook(hex []byte, n node, hash []byte) error {
	nd, parent, ok, incarnation := t.getNode(hex, true)
	if !ok {
		return nil
	}
	if _, ok := nd.(valueNode); ok {
		return nil
	}
	if hn, ok := nd.(hashNode); ok {
		if !bytes.Equal(hn.hash, hash) {
			return fmt.Errorf("wrong hash when hooking, expected %s, sub-tree hash %x", hn, hash)
		}
	} else if nd != nil {
		return fmt.Errorf("expected hash node at %x, got %T", hex, nd)
	}

	t.touchAll(n, hex, false, incarnation)
	switch p := parent.(type) {
	case nil:
		t.root = n
	case *shortNode:
		p.Val = n
	case *duoNode:
		i1, i2 := p.childrenIdx()
		switch hex[len(hex)-1] {
		case i1:
			p.child1 = n
		case i2:
			p.child2 = n
		}
	case *fullNode:
		idx := hex[len(hex)-1]
		p.Children[idx] = n
	case *accountNode:
		p.storage = n
	}
	return nil
}

func (t *Trie) touchAll(n node, hex []byte, del bool, incarnation uint64) {
	switch n := n.(type) {
	case *shortNode:
		if _, ok := n.Val.(valueNode); !ok {
			// Don't need to compute prefix for a leaf
			h := n.Key
			// Remove terminator
			if h[len(h)-1] == 16 {
				h = h[:len(h)-1]
			}
			hexVal := concat(hex, h...)
			t.touchAll(n.Val, hexVal, del, incarnation)
		}
	case *duoNode:
		i1, i2 := n.childrenIdx()
		hex1 := make([]byte, len(hex)+1)
		copy(hex1, hex)
		hex1[len(hex)] = i1
		hex2 := make([]byte, len(hex)+1)
		copy(hex2, hex)
		hex2[len(hex)] = i2
		t.touchAll(n.child1, hex1, del, incarnation)
		t.touchAll(n.child2, hex2, del, incarnation)
	case *fullNode:
		for i, child := range n.Children {
			if child != nil {
				t.touchAll(child, concat(hex, byte(i)), del, incarnation)
			}
		}
	case *accountNode:
		if n.storage != nil {
			t.touchAll(n.storage, hex, del, n.Incarnation)
		}
	}
}

// Delete removes any existing value for key from the trie.
// DESCRIBED: docs/programmers_guide/guide.md#root
func (t *Trie) Delete(key []byte) {
	hex := keybytesToHex(key)
	_, t.root = t.delete(t.root, hex, false)
}

func (t *Trie) convertToShortNode(child node, pos uint) node {
	if pos != 16 {
		// If the remaining entry is a short node, it replaces
		// n and its key gets the missing nibble tacked to the
		// front. This avoids creating an invalid
		// shortNode{..., shortNode{...}}.  Since the entry
		// might not be loaded yet, resolve it just for this
		// check.
		if short, ok := child.(*shortNode); ok {
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

func (t *Trie) delete(origNode node, key []byte, preserveAccountNode bool) (updated bool, newNode node) {
	return t.deleteRecursive(origNode, key, 0, preserveAccountNode, 0)
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
//
// can pass incarnation=0 if start from root, method internally will
// put incarnation from accountNode when pass it by traverse
func (t *Trie) deleteRecursive(origNode node, key []byte, keyStart int, preserveAccountNode bool, incarnation uint64) (updated bool, newNode node) {
	var nn node
	switch n := origNode.(type) {
	case *shortNode:
		matchlen := prefixLen(key[keyStart:], n.Key)
		if matchlen == cmp.Min(len(n.Key), len(key[keyStart:])) || n.Key[matchlen] == 16 || key[keyStart+matchlen] == 16 {
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
						if shortChild, ok := nn.(*shortNode); ok {
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

	case *duoNode:
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

	case *fullNode:
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
				duo := &duoNode{}
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

	case valueNode:
		updated = true
		newNode = nil
		return

	case *accountNode:
		if keyStart >= len(key) || key[keyStart] == 16 {
			// Key terminates here
			h := key[:keyStart]
			if h[len(h)-1] == 16 {
				h = h[:len(h)-1]
			}
			if n.storage != nil {
				// Mark all the storage nodes as deleted
				t.touchAll(n.storage, h, true, n.Incarnation)
			}
			if preserveAccountNode {
				n.storage = nil
				n.code = nil
				n.Root = EmptyRoot
				n.rootCorrect = true
				return true, n
			}

			return true, nil
		}
		updated, nn = t.deleteRecursive(n.storage, key, keyStart, preserveAccountNode, n.Incarnation)
		if updated {
			n.storage = nn
			n.rootCorrect = false
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

	_, t.root = t.delete(t.root, hexPrefix, true)

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
func (t *Trie) Hash() libcommon.Hash {
	if t == nil || t.root == nil {
		return EmptyRoot
	}

	h := t.getHasher()
	defer returnHasherToPool(h)

	var result libcommon.Hash
	_, _ = h.hash(t.root, true, result[:])

	return result
}

func (t *Trie) Reset() {
	resetRefs(t.root)
}

func (t *Trie) getHasher() *hasher {
	return t.newHasherFunc()
}

// DeepHash returns internal hash of a node reachable by the specified key prefix.
// Note that if the prefix points into the middle of a key for a leaf node or of an extension
// node, it will return the hash of a modified leaf node or extension node, where the
// key prefix is removed from the key.
// First returned value is `true` if the node with the specified prefix is found.
func (t *Trie) DeepHash(keyPrefix []byte) (bool, libcommon.Hash) {
	hexPrefix := keybytesToHex(keyPrefix)
	accNode, gotValue := t.getAccount(t.root, hexPrefix, 0)
	if !gotValue {
		return false, libcommon.Hash{}
	}
	if accNode.rootCorrect {
		return true, accNode.Root
	}
	if accNode.storage == nil {
		accNode.Root = EmptyRoot
		accNode.rootCorrect = true
	} else {
		h := t.getHasher()
		defer returnHasherToPool(h)
		h.hash(accNode.storage, true, accNode.Root[:])
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
	if accNode, ok := parent.(*accountNode); isCode && ok {
		// add special treatment to code nodes
		accNode.code = nil
		return
	}

	switch nd.(type) {
	case valueNode, hashNode:
		return
	default:
		// can work with other nodes type
	}

	var hn libcommon.Hash
	if nd == nil {
		fmt.Printf("nd == nil, hex %x, parent node: %T\n", hex, parent)
		return
	}
	copy(hn[:], nd.reference())
	hnode := hashNode{hash: hn[:]}

	t.notifyUnloadRecursive(hex, incarnation, nd)

	switch p := parent.(type) {
	case nil:
		t.root = hnode
	case *shortNode:
		p.Val = hnode
	case *duoNode:
		i1, i2 := p.childrenIdx()
		switch hex[len(hex)-1] {
		case i1:
			p.child1 = hnode
		case i2:
			p.child2 = hnode
		}
	case *fullNode:
		idx := hex[len(hex)-1]
		p.Children[idx] = hnode
	case *accountNode:
		p.storage = hnode
	}
}

func (t *Trie) notifyUnloadRecursive(hex []byte, incarnation uint64, nd node) {
	switch n := nd.(type) {
	case *shortNode:
		hex = append(hex, n.Key...)
		if hex[len(hex)-1] == 16 {
			hex = hex[:len(hex)-1]
		}
		t.notifyUnloadRecursive(hex, incarnation, n.Val)
	case *accountNode:
		if n.storage == nil {
			return
		}
		if _, ok := n.storage.(hashNode); ok {
			return
		}
		t.notifyUnloadRecursive(hex, n.Incarnation, n.storage)
	case *fullNode:
		for i := range n.Children {
			if n.Children[i] == nil {
				continue
			}
			if _, ok := n.Children[i].(hashNode); ok {
				continue
			}
			t.notifyUnloadRecursive(append(hex, uint8(i)), incarnation, n.Children[i])
		}
	case *duoNode:
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
	return calcSubtreeSize(t.root)
}

func (t *Trie) NumberOfAccounts() int {
	return calcSubtreeNodes(t.root)
}
