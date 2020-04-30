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

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/pkg/errors"
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
type Trie struct {
	root node

	newHasherFunc func() *hasher

	Version uint8

	binary bool

	hashMap map[common.Hash]node

	observers *ObserverMux
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ false) },
		hashMap:       make(map[common.Hash]node),
		observers:     NewTrieObserverMux(),
	}
	if (root != common.Hash{}) && root != EmptyRoot {
		trie.root = hashNode(root[:])
	}
	return trie
}

func NewBinary(root common.Hash) *Trie {
	trie := New(root)
	trie.binary = true
	return trie
}

// NewTestRLPTrie treats all the data provided to `Update` function as rlp-encoded.
// it is usually used for testing purposes.
func NewTestRLPTrie(root common.Hash) *Trie {
	trie := &Trie{
		newHasherFunc: func() *hasher { return newHasher( /*valueNodesRlpEncoded = */ true) },
		hashMap:       make(map[common.Hash]node),
		observers:     NewTrieObserverMux(),
	}
	if (root != common.Hash{}) && root != EmptyRoot {
		trie.root = hashNode(root[:])
	}
	return trie
}

func (t *Trie) AddObserver(observer Observer) {
	t.observers.AddChild(observer)
}

// Get returns the value for key stored in the trie.
func (t *Trie) Get(key []byte) (value []byte, gotValue bool) {
	if t.root == nil {
		return nil, true
	}

	hex := keybytesToHex(key)
	if t.binary {
		hex = keyHexToBin(hex)
	}
	return t.get(t.root, hex, 0)
}

func (t *Trie) GetAccount(key []byte) (value *accounts.Account, gotValue bool) {
	if t.root == nil {
		return nil, true
	}

	hex := keybytesToHex(key)
	if t.binary {
		hex = keyHexToBin(hex)
	}

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
	if t.binary {
		hex = keyHexToBin(hex)
	}

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode != nil {
		if bytes.Equal(accNode.Account.CodeHash[:], EmptyCodeHash[:]) {
			return nil, gotValue
		}

		t.observers.CodeNodeTouched(hex)

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
	if t.binary {
		hex = keyHexToBin(hex)
	}

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
		t.observers.BranchNodeTouched(key[:pos])
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
		t.observers.BranchNodeTouched(key[:pos])
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
		t.observers.BranchNodeTouched(key[:pos])
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
		t.observers.BranchNodeTouched(key[:pos])
		child := n.Children[key[pos]]
		if child == nil {
			return nil, true
		}
		return t.get(child, key, pos+1)
	case hashNode:
		return n, false

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
	if t.binary {
		hex = keyHexToBin(hex)
	}

	newnode := valueNode(value)

	if t.root == nil {
		t.root = &shortNode{Key: hex, Val: newnode}
	} else {
		_, t.root = t.insert(t.root, hex, valueNode(value))
	}
}

func (t *Trie) UpdateAccount(key []byte, acc *accounts.Account) {
	//make account copy. There are some pointer into big.Int
	value := new(accounts.Account)
	value.Copy(acc)

	hex := keybytesToHex(key)
	if t.binary {
		hex = keyHexToBin(hex)
	}

	var newnode *accountNode
	if value.Root == EmptyRoot || value.Root == (common.Hash{}) {
		newnode = &accountNode{*value, nil, true, nil, codeSizeUncached}
	} else {
		newnode = &accountNode{*value, hashNode(value.Root[:]), true, nil, codeSizeUncached}
	}

	if t.root == nil {
		t.root = &shortNode{Key: hex, Val: newnode}
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
	if t.binary {
		hex = keyHexToBin(hex)
	}

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode == nil || !gotValue {
		return errors.Wrapf(ethdb.ErrKeyNotFound, "account not found with key: %x", key)
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
	if t.binary {
		hex = keyHexToBin(hex)
	}

	accNode, gotValue := t.getAccount(t.root, hex, 0)
	if accNode == nil || !gotValue {
		return errors.Wrapf(ethdb.ErrKeyNotFound, "account not found with key: %x", key)
	}

	accNode.codeSize = codeSize

	// t.insert will call the observer methods itself
	_, t.root = t.insert(t.root, hex, accNode)
	return nil
}

// ResolveRequestFor Code expresses the need to fetch code from the DB (by its hash) and attach
// to a specific account leaf in the trie.
type ResolveRequestForCode struct {
	t        *Trie
	addrHash common.Hash // contract address hash
	codeHash common.Hash
	bytecode bool // include the bytecode too
}

func (rr *ResolveRequestForCode) String() string {
	return fmt.Sprintf("rr_code{addrHash:%x,codeHash:%x,bytecode:%v}", rr.addrHash, rr.codeHash, rr.bytecode)
}

// ResolveRequest expresses the need to fetch a subtrie from the database. The location of this
// subtrie is specified by the resolveHex[:resolvePos]. The remaining part of resolveHex (if present)
// is useful to ensure that specific leaves of the trie are fully expanded (and not rolled into
// the hashes). One might think of two uses of ResolveRequests (and perhaps we need to consider
// splitting them into two types). First type is to fetch certain number of levels of a given
// subtrie, but specifying resolveHex and resolvePos such that resolvePos == len(resolveHex).
// In such situations, it want to also set topLevels field in the Resolver to a non-zero
// value, otherwise only a hashNode will be resolved.
// Second type is to fetch a subtrie in such a way that a set of keys will be fully expanded,
// so that one can perform reads, inserts, and deletes on those keys without needing to do
// any more resolving.
type ResolveRequest struct {
	t             *Trie    // trie to act upon
	contract      []byte   // contract address hash + incarnation (32+8 bytes) or nil, if the trie is the main trie
	resolveHex    []byte   // Key for which the resolution is requested
	resolvePos    int      // Position in the key for which resolution is requested
	extResolvePos int      // Internal field, does not need to be set when ResolveRequest is created
	resolveHash   hashNode // Expected hash of the resolved node (for correctness checking)
	RequiresRLP   bool     // whether to output node's RLP
	NodeRLP       []byte   // [OUT] RLP of the resolved node
}

/* add code hash there, if nil -- don't do anything with the code */

// NewResolveRequest creates a new ResolveRequest.
// contract must be either address hash + incarnation (32+8 bytes) or nil
func (t *Trie) NewResolveRequest(contract []byte, hex []byte, pos int, resolveHash []byte) *ResolveRequest {
	return &ResolveRequest{t: t, contract: contract, resolveHex: hex, resolvePos: pos, resolveHash: hashNode(resolveHash)}
}

func (rr *ResolveRequest) String() string {
	return fmt.Sprintf("rr{t:%x,resolveHex:%x,resolvePos:%d,resolveHash:%s}", rr.contract, rr.resolveHex, rr.resolvePos, rr.resolveHash)
}

func (t *Trie) NewResolveRequestForCode(addrHash common.Hash, codeHash common.Hash, bytecode bool) *ResolveRequestForCode {
	return &ResolveRequestForCode{t, addrHash, codeHash, bytecode}
}

func (t *Trie) NeedResolutonForCode(addrHash common.Hash, codeHash common.Hash, bytecode bool) (bool, *ResolveRequestForCode) {
	if bytes.Equal(codeHash[:], EmptyCodeHash[:]) {
		return false, nil
	}

	ok := false
	if bytecode {
		_, ok = t.GetAccountCode(addrHash[:])
	} else {
		_, ok = t.GetAccountCodeSize(addrHash[:])
	}
	if !ok {
		return true, t.NewResolveRequestForCode(addrHash, codeHash, bytecode)
	}

	return false, nil
}

// NeedResolution determines whether the trie needs to be extended (resolved) by fetching data
// from the database, if one were to access the key specified
// In the case of "Yes", also returns a corresponding ResolveRequest
// contract, if not empty, must be equal to the address hash
// storageKey must be the concatenation of contract's address hash and the hash of the item's key, in the KEYBYTES encoding
func (t *Trie) NeedResolution(contract []byte, storageKey []byte) (bool, *ResolveRequest) {
	var nd = t.root
	hex := keybytesToHex(storageKey)
	if t.binary {
		hex = keyHexToBin(hex)
	}
	pos := 0
	var incarnation uint64
	for {
		switch n := nd.(type) {
		case nil:
			return false, nil
		case *shortNode:
			matchlen := prefixLen(hex[pos:], n.Key)
			if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
				nd = n.Val
			} else {
				return false, nil
			}
			pos += matchlen
		case *duoNode:
			i1, i2 := n.childrenIdx()
			switch hex[pos] {
			case i1:
				nd = n.child1
				pos++
			case i2:
				nd = n.child2
				pos++
			default:
				return false, nil
			}
		case *fullNode:
			child := n.Children[hex[pos]]
			if child == nil {
				return false, nil
			}
			nd = child
			pos++
		case valueNode:
			return false, nil
		case *accountNode:
			if pos == len(hex) {
				return false, nil
			}
			nd = n.storage
			incarnation = n.Incarnation
		case hashNode:
			if contract == nil {
				l := min(len(hex), common.HashLength*2) // remove termination symbol
				return true, t.NewResolveRequest(nil, hex[:l], pos, common.CopyBytes(n))
			}
			// 8 is IncarnationLength
			if incarnation == 0 {
				//FIXME: This is a hack. Proper fix would involve figuring out proper incarnation by pre-loading
				// account, or alternatively, excluding incarnations from the keys for ResolveSet-s
				incarnation = 1
			}
			prefix := make([]byte, len(contract)+8)
			copy(prefix, contract)
			binary.BigEndian.PutUint64(prefix[len(contract):], ^incarnation)
			hexContractLen := 2 * common.HashLength // Length of 'contract' prefix in HEX encoding
			l := min(len(hex), hexContractLen*2)    // remove termination symbol
			if pos-hexContractLen < 0 {
				// when need storage resolution for non-resolved account
				return true, t.NewResolveRequest(prefix, hex[hexContractLen:l], 0, common.CopyBytes(n))
			}
			return true, t.NewResolveRequest(prefix, hex[hexContractLen:l], pos-hexContractLen, common.CopyBytes(n))

		default:
			panic(fmt.Sprintf("Unknown node: %T", n))
		}
	}
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

			if len(origAccN.code) > 0 {
				t.observers.CodeNodeSizeChanged(key[:pos-1], uint(len(origAccN.code)))
			} else {
				t.observers.CodeNodeDeleted(key[:pos-1])
			}
			return
		}

		// replacing nodes except accounts
		if !origNok {
			t.evictSubtreeFromHashMap(origNode)
			return true, value
		}
	}

	var nn node
	switch n := origNode.(type) {
	case nil:
		return true, &shortNode{Key: common.CopyBytes(key[pos:]), Val: value}
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
				t.evictNodeFromHashMap(n)
				n.Val = nn
				n.ref.len = 0
			}
			newNode = n
		} else {
			// Otherwise branch out at the index where they differ.
			t.evictNodeFromHashMap(n)
			var c1 node
			if len(n.Key) == matchlen+1 {
				c1 = n.Val
			} else {
				c1 = &shortNode{Key: common.CopyBytes(n.Key[matchlen+1:]), Val: n.Val}
			}
			var c2 node
			if len(key) == pos+matchlen+1 {
				c2 = value
			} else {
				c2 = &shortNode{Key: common.CopyBytes(key[pos+matchlen+1:]), Val: value}
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
				n.Key = common.CopyBytes(key[pos : pos+matchlen])
				n.Val = branch
				n.ref.len = 0
				newNode = n
			}
			t.observers.BranchNodeCreated(key[:pos+matchlen])
			updated = true
		}
		return

	case *duoNode:
		t.observers.BranchNodeTouched(key[:pos])
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			updated, nn = t.insertRecursive(n.child1, key, pos+1, value)
			if updated {
				t.evictNodeFromHashMap(n)
				n.child1 = nn
				n.ref.len = 0
			}
			newNode = n
		case i2:
			updated, nn = t.insertRecursive(n.child2, key, pos+1, value)
			if updated {
				t.evictNodeFromHashMap(n)
				n.child2 = nn
				n.ref.len = 0
			}
			newNode = n
		default:
			t.evictNodeFromHashMap(n)
			var child node
			if len(key) == pos+1 {
				child = value
			} else {
				child = &shortNode{Key: common.CopyBytes(key[pos+1:]), Val: value}
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
		t.observers.BranchNodeTouched(key[:pos])
		child := n.Children[key[pos]]
		if child == nil {
			t.evictNodeFromHashMap(n)
			if len(key) == pos+1 {
				n.Children[key[pos]] = value
			} else {
				n.Children[key[pos]] = &shortNode{Key: common.CopyBytes(key[pos+1:]), Val: value}
			}
			updated = true
			n.ref.len = 0
		} else {
			updated, nn = t.insertRecursive(child, key, pos+1, value)
			if updated {
				t.evictNodeFromHashMap(n)
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
			if doTouch {
				t.observers.BranchNodeTouched(hex[:pos])
			}
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
			if doTouch {
				t.observers.BranchNodeTouched(hex[:pos])
			}
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

func (t *Trie) hook(hex []byte, n node) {
	nd, parent, ok, incarnation := t.getNode(hex, true)
	if !ok {
		return
	}
	if _, ok := nd.(valueNode); ok {
		return
	}
	if _, ok := nd.(hashNode); !ok && nd != nil {
		return
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
}

func (t *Trie) touchAll(n node, hex []byte, del bool, incarnation uint64) {
	if del {
		t.evictNodeFromHashMap(n)
	} else if len(n.reference()) == common.HashLength && debug.IsGetNodeData() {
		var key common.Hash
		copy(key[:], n.reference())
		t.hashMap[key] = n
	}

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
		if del {
			t.observers.BranchNodeDeleted(hex)
		} else {
			t.observers.BranchNodeCreated(hex)
			t.observers.BranchNodeLoaded(hex, incarnation)
		}
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
		if del {
			t.observers.BranchNodeDeleted(hex)
		} else {
			t.observers.BranchNodeCreated(hex)
			t.observers.BranchNodeLoaded(hex, incarnation)
		}
		for i, child := range n.Children {
			if child != nil {
				t.touchAll(child, concat(hex, byte(i)), del, incarnation)
			}
		}
	case *accountNode:
		if del {
			t.observers.CodeNodeDeleted(hex)
		} else {
			t.observers.CodeNodeTouched(hex)
			//t.observers.BranchNodeLoaded(hex, n.Incarnation)
		}
		if n.storage != nil {
			t.touchAll(n.storage, hex, del, n.Incarnation)
		}
	}
}

// Delete removes any existing value for key from the trie.
// DESCRIBED: docs/programmers_guide/guide.md#root
func (t *Trie) Delete(key []byte) {
	hex := keybytesToHex(key)
	if t.binary {
		hex = keyHexToBin(hex)
	}
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
			t.evictNodeFromHashMap(child)
			k := make([]byte, len(short.Key)+1)
			k[0] = byte(pos)
			copy(k[1:], short.Key)
			return &shortNode{Key: k, Val: short.Val}
		}
	}
	// Otherwise, n is replaced by a one-nibble short node
	// containing the child.
	return &shortNode{Key: []byte{byte(pos)}, Val: child}
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
		if matchlen == min(len(n.Key), len(key[keyStart:])) || n.Key[matchlen] == 16 || key[keyStart+matchlen] == 16 {
			fullMatch := matchlen == len(key)-keyStart
			removeNodeEntirely := fullMatch
			if preserveAccountNode {
				removeNodeEntirely = len(key) == keyStart || matchlen == len(key[keyStart:])-1
			}

			if removeNodeEntirely {
				t.evictNodeFromHashMap(n)
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
					t.evictNodeFromHashMap(n)
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
							newNode = &shortNode{Key: concat(n.Key, shortChild.Key...), Val: shortChild.Val}
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
				t.observers.BranchNodeTouched(key[:keyStart])
			} else {
				t.evictNodeFromHashMap(n)
				if nn == nil {
					newNode = t.convertToShortNode(n.child2, uint(i2))
					t.observers.BranchNodeDeleted(key[:keyStart])
				} else {
					t.observers.BranchNodeTouched(key[:keyStart])
					n.child1 = nn
					n.ref.len = 0
					newNode = n
				}
			}
		case i2:
			updated, nn = t.deleteRecursive(n.child2, key, keyStart+1, preserveAccountNode, incarnation)
			if !updated {
				newNode = n
				t.observers.BranchNodeTouched(key[:keyStart])
			} else {
				t.evictNodeFromHashMap(n)
				if nn == nil {
					newNode = t.convertToShortNode(n.child1, uint(i1))
					t.observers.BranchNodeDeleted(key[:keyStart])
				} else {
					t.observers.BranchNodeTouched(key[:keyStart])
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
			t.observers.BranchNodeTouched(key[:keyStart])
		} else {
			t.evictNodeFromHashMap(n)
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
				t.observers.BranchNodeDeleted(key[:keyStart])
			} else if count == 2 {
				t.observers.BranchNodeTouched(key[:keyStart])
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
				t.observers.BranchNodeTouched(key[:keyStart])
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
				t.observers.CodeNodeDeleted(h)
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
	if t.binary {
		hexPrefix = keyHexToBin(hexPrefix)
	}

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
func (t *Trie) Hash() common.Hash {
	hash, _ := t.hashRoot()
	return common.BytesToHash(hash.(hashNode))
}

func (t *Trie) Reset() {
	resetRefs(t.root)
}

func (t *Trie) getHasher() *hasher {
	h := t.newHasherFunc()
	if debug.IsGetNodeData() {
		h.callback = func(key common.Hash, nd node) {
			t.hashMap[key] = nd
		}
	}
	return h
}

// DeepHash returns internal hash of a node reachable by the specified key prefix.
// Note that if the prefix points into the middle of a key for a leaf node or of an extention
// node, it will return the hash of a modified leaf node or extension node, where the
// key prefix is removed from the key.
// First returned value is `true` if the node with the specified prefix is found.
func (t *Trie) DeepHash(keyPrefix []byte) (bool, common.Hash) {
	hexPrefix := keybytesToHex(keyPrefix)
	if t.binary {
		hexPrefix = keyHexToBin(hexPrefix)
	}
	accNode, gotValue := t.getAccount(t.root, hexPrefix, 0)
	if !gotValue {
		return false, common.Hash{}
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

	t.evictSubtreeFromHashMap(nd)

	var hn common.Hash
	if nd == nil {
		fmt.Printf("nd == nil, hex %x, parent node: %T\n", hex, parent)
		return
	}
	copy(hn[:], nd.reference())
	hnode := hashNode(hn[:])
	t.observers.WillUnloadNode(hex, hn)

	switch p := parent.(type) {
	case nil:
		t.root = hnode
	case *shortNode:
		p.Val = hnode
	case *duoNode:
		t.observers.WillUnloadBranchNode(hex, hn, incarnation)
		i1, i2 := p.childrenIdx()
		switch hex[len(hex)-1] {
		case i1:
			p.child1 = hnode
		case i2:
			p.child2 = hnode
		}
	case *fullNode:
		t.observers.WillUnloadBranchNode(hex, hn, incarnation)
		idx := hex[len(hex)-1]
		p.Children[idx] = hnode
	case *accountNode:
		//t.observers.WillUnloadBranchNode(hex, hn, incarnation)
		p.storage = hnode
	}
}

func (t *Trie) TrieSize() int {
	return calcSubtreeSize(t.root)
}

func (t *Trie) NumberOfAccounts() int {
	return calcSubtreeNodes(t.root)
}

func (t *Trie) hashRoot() (node, error) {
	if t == nil || t.root == nil {
		return hashNode(EmptyRoot.Bytes()), nil
	}

	h := t.getHasher()
	defer returnHasherToPool(h)

	var hn common.Hash
	h.hash(t.root, true, hn[:])

	return hashNode(hn[:]), nil
}

// GetNodeByHash gets node's RLP by hash.
func (t *Trie) GetNodeByHash(hash common.Hash) []byte {
	nd := t.hashMap[hash]
	if nd == nil {
		return nil
	}

	h := t.getHasher()
	defer returnHasherToPool(h)

	rlp, err := h.hashChildren(nd, 0)
	if err != nil {
		log.Warn("GetNodeByHash error while producing node RLP", "err", err)
		return nil
	}

	return common.CopyBytes(rlp)
}

func (t *Trie) evictNodeFromHashMap(nd node) {
	if !debug.IsGetNodeData() || nd == nil {
		return
	}
	if len(nd.reference()) == common.HashLength {
		var key common.Hash
		copy(key[:], nd.reference())
		delete(t.hashMap, key)
	}
}

func (t *Trie) evictSubtreeFromHashMap(n node) {
	if !debug.IsGetNodeData() {
		return
	}
	t.evictNodeFromHashMap(n)

	switch n := n.(type) {
	case *shortNode:
		if _, ok := n.Val.(valueNode); !ok {
			t.evictSubtreeFromHashMap(n.Val)
		}
	case *duoNode:
		t.evictSubtreeFromHashMap(n.child1)
		t.evictSubtreeFromHashMap(n.child2)
	case *fullNode:
		for _, child := range n.Children {
			if child != nil {
				t.evictSubtreeFromHashMap(child)
			}
		}
	case *accountNode:
		if n.storage != nil {
			t.evictSubtreeFromHashMap(n.storage)
		}
	}
}

// HashMapSize returns the number of entries in trie's hash map.
func (t *Trie) HashMapSize() int {
	return len(t.hashMap)
}
