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
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)
)

// LeafCallback is a callback type invoked when a trie operation reaches a leaf
// node. It's used by state sync and commit to allow handling external references
// between account and storage tries.
type LeafCallback func(leaf []byte, parent common.Hash) error

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
type Trie struct {
	root node

	encodeToBytes bool

	joinGeneration func(gen uint64)
	leftGeneration func(gen uint64)
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash, encodeToBytes bool) *Trie {
	trie := &Trie{
		encodeToBytes:  encodeToBytes,
		joinGeneration: func(uint64) {},
		leftGeneration: func(uint64) {},
	}
	if (root != common.Hash{}) && root != emptyRoot {
		trie.root = hashNode(root[:])
	}
	return trie
}

func (t *Trie) MakeListed(joinGeneration, leftGeneration func(gen uint64)) {
	t.joinGeneration = joinGeneration
	t.leftGeneration = leftGeneration
}

// TryGet returns the value for key stored in the trie.
func (t *Trie) Get(key []byte, blockNr uint64) (value []byte, gotValue bool) {
	hex := keybytesToHex(key)
	return t.get(t.root, hex, 0, blockNr)
}

func (t *Trie) get(origNode node, key []byte, pos int, blockNr uint64) (value []byte, gotValue bool) {
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case valueNode:
		return n, true
	case *shortNode:
		var adjust bool
		nKey := compactToHex(n.Key)
		if len(key)-pos < len(nKey) || !bytes.Equal(nKey, key[pos:pos+len(nKey)]) {
			adjust = false
			value, gotValue = nil, true
		} else {
			adjust = true
			if v, ok := n.Val.(valueNode); ok {
				value, gotValue = v, true
			} else {
				value, gotValue = t.get(n.Val, key, pos+len(nKey), blockNr)
			}
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case *duoNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			adjust = n.tod(blockNr) == n.child1.tod(blockNr)
			value, gotValue = t.get(n.child1, key, pos+1, blockNr)
		case i2:
			adjust = n.tod(blockNr) == n.child2.tod(blockNr)
			value, gotValue = t.get(n.child2, key, pos+1, blockNr)
		default:
			adjust = false
			value, gotValue = nil, true
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case *fullNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[pos]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		value, gotValue = t.get(child, key, pos+1, blockNr)
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case hashNode:
		return nil, false
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
func (t *Trie) Update(key, value []byte, blockNr uint64) {
	hex := keybytesToHex(key)
	if t.root == nil {
		newnode := &shortNode{Key: hexToCompact(hex), Val: valueNode(value)}
		newnode.flags.dirty = true
		newnode.flags.t = blockNr
		newnode.adjustTod(blockNr)
		t.joinGeneration(blockNr)
		t.root = newnode
	} else {
		_, t.root = t.insert(t.root, hex, 0, valueNode(value), blockNr)
	}
}

type ResolveRequest struct {
	t             *Trie  // trie to act upon
	contract      []byte // contract address or nil, if the trie is the main trie
	resolveHex    []byte // Key for which the resolution is requested
	resolvePos    int    // Position in the key for which resolution is requested
	extResolvePos int
	resolveHash   hashNode // Expected hash of the resolved node (for correctness checking)
	resolved      node     // Node that has been resolved via Database access
	resolveParent node     // Parent node of the one needs to be resolved. nil if the root needs to be resolved
}

func (t *Trie) NewResolveRequest(contract []byte, hex []byte, pos int, resolveHash []byte) *ResolveRequest {
	return &ResolveRequest{t: t, contract: contract, resolveHex: hex, resolvePos: pos, resolveHash: hashNode(resolveHash)}
}

func (rr *ResolveRequest) String() string {
	return fmt.Sprintf("rr{t:%x,resolveHex:%x,resolvePos:%d,resolveHash:%s}", rr.contract, rr.resolveHex, rr.resolvePos, rr.resolveHash)
}

// Determines whether the trie needs to be extended (resolved) by fetching data
// from the database, if one were to access the key specified
// In the case of "Yes", also returns a corresponding ResolveRequest
func (t *Trie) NeedResolution(contract []byte, key []byte) (bool, *ResolveRequest) {
	var nd node = t.root
	var parent node = nil
	hex := keybytesToHex(key)
	pos := 0
	for {
		switch n := nd.(type) {
		case nil:
			return false, nil
		case *shortNode:
			nKey := compactToHex(n.Key)
			matchlen := prefixLen(hex[pos:], nKey)
			if matchlen == len(nKey) {
				parent = nd
				nd = n.Val
				pos += matchlen
			} else {
				return false, nil
			}
		case *duoNode:
			i1, i2 := n.childrenIdx()
			switch hex[pos] {
			case i1:
				parent = nd
				nd = n.child1
				pos++
			case i2:
				parent = nd
				nd = n.child2
				pos++
			default:
				return false, nil
			}
		case *fullNode:
			child := n.Children[hex[pos]]
			if child == nil {
				return false, nil
			} else {
				parent = nd
				nd = child
				pos++
			}
		case valueNode:
			return false, nil
		case hashNode:
			c := t.NewResolveRequest(contract, hex, pos, common.CopyBytes(n))
			c.resolveParent = parent
			return true, c
		default:
			panic(fmt.Sprintf("Unknown node: %T", n))
		}
	}
}

func (t *Trie) PopulateBlockProofData(contract []byte, key []byte, pg *ProofGenerator) {
	var nd node = t.root
	hex := keybytesToHex(key)
	pos := 0
	for {
		switch n := nd.(type) {
		case nil:
			return
		case *shortNode:
			nKey := compactToHex(n.Key)
			pg.addShort(contract, hex, pos, nKey)
			matchlen := prefixLen(hex[pos:], nKey)
			if matchlen == len(nKey) {
				nd = n.Val
				pos += matchlen
			} else {
				proofHex := make([]byte, pos+len(nKey))
				copy(proofHex, hex[:pos])
				copy(proofHex[pos:], nKey)
				if v, ok := n.Val.(valueNode); ok {
					pg.addValue(contract, proofHex, pos+len(nKey), common.CopyBytes(v))
				} else {
					pg.addSoleHash(contract, proofHex, pos+len(nKey), common.BytesToHash(n.Val.hash()))
				}
				return
			}
		case *duoNode:
			mask, hashes, m := n.hashesExcept(hex[pos])
			pg.addProof(contract, hex, pos, mask, hashes)
			if m != nil {
				for idx, s := range m {
					nKey := compactToHex(s.Key)
					proofHex := make([]byte, pos+1+len(nKey))
					copy(proofHex, hex[:pos])
					proofHex[pos] = idx
					copy(proofHex[pos+1:], nKey)
					pg.addShort(contract, proofHex, pos+1, nKey)
					if v, ok := s.Val.(valueNode); ok {
						pg.addValue(contract, proofHex, pos+1+len(nKey), common.CopyBytes(v))
					} else {
						pg.addSoleHash(contract, proofHex, pos+1+len(nKey), common.BytesToHash(s.Val.hash()))
					}
				}
			}
			i1, i2 := n.childrenIdx()
			switch hex[pos] {
			case i1:
				nd = n.child1
				pos++
			case i2:
				nd = n.child2
				pos++
			default:
				return
			}
		case *fullNode:
			mask, hashes, m := n.hashesExcept(hex[pos])
			pg.addProof(contract, hex, pos, mask, hashes)
			if m != nil {
				for idx, s := range m {
					nKey := compactToHex(s.Key)
					proofHex := make([]byte, pos+1+len(nKey))
					copy(proofHex, hex[:pos])
					proofHex[pos] = idx
					copy(proofHex[pos+1:], nKey)
					pg.addShort(contract, proofHex, pos+1, nKey)
					if v, ok := s.Val.(valueNode); ok {
						pg.addValue(contract, proofHex, pos+1+len(nKey), common.CopyBytes(v))
					} else {
						pg.addSoleHash(contract, proofHex, pos+1+len(nKey), common.BytesToHash(s.Val.hash()))
					}
				}
			}
			child := n.Children[hex[pos]]
			if child == nil {
				return
			} else {
				nd = child
				pos++
			}
		case valueNode:
			pg.addValue(contract, hex, pos, common.CopyBytes(n))
			return
		case hashNode:
			pg.addSoleHash(contract, hex, pos, common.BytesToHash(n))
			return
		default:
			panic(fmt.Sprintf("Unknown node: %T", n))
		}
	}
}

func (t *Trie) insert(origNode node, key []byte, pos int, value node, blockNr uint64) (updated bool, newNode node) {
	var nn node
	if len(key) == pos {
		if v, ok := origNode.(valueNode); ok {
			updated = !bytes.Equal(v, value.(valueNode))
			if updated {
				newNode = value
			} else {
				newNode = v
			}
			return
		}
		updated = true
		newNode = value
		return
	}
	switch n := origNode.(type) {
	case *shortNode:
		nKey := compactToHex(n.Key)
		matchlen := prefixLen(key[pos:], nKey)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(nKey) {
			updated, nn = t.insert(n.Val, key, pos+matchlen, value, blockNr)
			if updated {
				n.Val = nn
				n.flags.dirty = true
			}
			newNode = n
			n.adjustTod(blockNr)
		} else {
			// Otherwise branch out at the index where they differ.
			var c1 node
			if len(nKey) == matchlen+1 {
				c1 = n.Val
			} else {
				s1 := &shortNode{Key: hexToCompact(nKey[matchlen+1:]), Val: n.Val}
				s1.flags.dirty = true
				s1.flags.t = blockNr
				s1.adjustTod(blockNr)
				c1 = s1
				t.joinGeneration(blockNr)
			}
			var c2 node
			if len(key) == pos+matchlen+1 {
				c2 = value
			} else {
				s2 := &shortNode{Key: hexToCompact(key[pos+matchlen+1:]), Val: value}
				s2.flags.dirty = true
				s2.flags.t = blockNr
				s2.adjustTod(blockNr)
				c2 = s2
				t.joinGeneration(blockNr)
			}
			branch := &duoNode{}
			if nKey[matchlen] < key[pos+matchlen] {
				branch.child1 = c1
				branch.child2 = c2
			} else {
				branch.child1 = c2
				branch.child2 = c1
			}
			branch.mask = (1 << (nKey[matchlen])) | (1 << (key[pos+matchlen]))
			branch.flags.dirty = true
			branch.flags.t = blockNr
			branch.adjustTod(blockNr)

			// Replace this shortNode with the branch if it occurs at index 0.
			if matchlen == 0 {
				newNode = branch // current node leaves the generation, but new node branch joins it
			} else {
				// Otherwise, replace it with a short node leading up to the branch.
				n.Key = hexToCompact(key[pos : pos+matchlen])
				n.Val = branch
				t.joinGeneration(blockNr) // new branch node joins the generation
				n.flags.dirty = true
				n.flags.t = blockNr
				newNode = n
				n.adjustTod(blockNr)
			}
			updated = true
		}
		return

	case *duoNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			adjust = n.child1 != nil && n.tod(blockNr) == n.child1.tod(blockNr)
			if n.child1 == nil {
				if len(key) == pos+1 {
					n.child1 = value
				} else {
					short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
					short.flags.dirty = true
					short.flags.t = blockNr
					short.adjustTod(blockNr)
					t.joinGeneration(blockNr)
					n.child1 = short
				}
				updated = true
				n.flags.dirty = true
			} else {
				updated, nn = t.insert(n.child1, key, pos+1, value, blockNr)
				if updated {
					n.child1 = nn
					n.flags.dirty = true
				}
			}
			newNode = n
		case i2:
			adjust = n.child2 != nil && n.tod(blockNr) == n.child2.tod(blockNr)
			if n.child2 == nil {
				if len(key) == pos+1 {
					n.child2 = value
				} else {
					short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
					short.flags.dirty = true
					short.flags.t = blockNr
					short.adjustTod(blockNr)
					t.joinGeneration(blockNr)
					n.child2 = short
				}
				updated = true
				n.flags.dirty = true
			} else {
				updated, nn = t.insert(n.child2, key, pos+1, value, blockNr)
				if updated {
					n.child2 = nn
					n.flags.dirty = true
				}
			}
			newNode = n
		default:
			var child node
			if len(key) == pos+1 {
				child = value
			} else {
				short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
				short.flags.dirty = true
				short.flags.t = blockNr
				short.adjustTod(blockNr)
				t.joinGeneration(blockNr)
				child = short
			}
			newnode := &fullNode{}
			newnode.Children[i1] = n.child1
			newnode.Children[i2] = n.child2
			newnode.flags.dirty = true
			newnode.flags.t = blockNr
			newnode.adjustTod(blockNr)
			adjust = false
			newnode.Children[key[pos]] = child
			updated = true
			// current node leaves the generation but newnode joins it
			newNode = newnode
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return

	case *fullNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[pos]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		if child == nil {
			if len(key) == pos+1 {
				n.Children[key[pos]] = value
			} else {
				short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
				short.flags.dirty = true
				short.flags.t = blockNr
				short.adjustTod(blockNr)
				t.joinGeneration(blockNr)
				n.Children[key[pos]] = short
			}
			updated = true
			n.flags.dirty = true
		} else {
			updated, nn = t.insert(child, key, pos+1, value, blockNr)
			if updated {
				n.Children[key[pos]] = nn
				n.flags.dirty = true
			}
		}
		newNode = n
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	default:
		fmt.Printf("Key: %x, Prefix: %x\n", key[pos:], key[:pos])
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *Trie) Delete(key []byte, blockNr uint64) {
	hex := keybytesToHex(key)
	_, t.root = t.delete(t.root, hex, 0, blockNr)
}

func (t *Trie) convertToShortNode(key []byte, keyStart int, child node, pos uint, blockNr uint64) node {
	cnode := child
	if pos != 16 {
		// If the remaining entry is a short node, it replaces
		// n and its key gets the missing nibble tacked to the
		// front. This avoids creating an invalid
		// shortNode{..., shortNode{...}}.  Since the entry
		// might not be loaded yet, resolve it just for this
		// check.
		if short, ok := child.(*shortNode); ok {
			cnodeKey := compactToHex(short.Key)
			k := make([]byte, len(cnodeKey)+1)
			k[0] = byte(pos)
			copy(k[1:], cnodeKey)
			newshort := &shortNode{Key: hexToCompact(k)}
			t.leftGeneration(short.flags.t)
			newshort.Val = short.Val
			newshort.flags.dirty = true
			newshort.flags.t = blockNr
			newshort.adjustTod(blockNr)
			// cnode gets removed, but newshort gets added
			return newshort
		}
	}
	// Otherwise, n is replaced by a one-nibble short node
	// containing the child.
	newshort := &shortNode{Key: hexToCompact([]byte{byte(pos)})}
	newshort.Val = cnode
	newshort.flags.dirty = true
	newshort.flags.t = blockNr
	newshort.adjustTod(blockNr)
	return newshort
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(origNode node, key []byte, keyStart int, blockNr uint64) (updated bool, newNode node) {
	var nn node
	switch n := origNode.(type) {
	case *shortNode:
		nKey := compactToHex(n.Key)
		matchlen := prefixLen(key[keyStart:], nKey)
		if matchlen < len(nKey) {
			updated = false
			newNode = n // don't replace n on mismatch
		} else if matchlen == len(key)-keyStart {
			t.leftGeneration(n.flags.t)
			updated = true
			newNode = nil // remove n entirely for whole matches
		} else {
			// The key is longer than n.Key. Remove the remaining suffix
			// from the subtrie. Child can never be nil here since the
			// subtrie must contain at least two other values with keys
			// longer than n.Key.
			updated, nn = t.delete(n.Val, key, keyStart+len(nKey), blockNr)
			if !updated {
				newNode = n
			} else {
				if nn == nil {
					t.leftGeneration(n.flags.t)
					newNode = nil
				} else {
					if shortChild, ok := nn.(*shortNode); ok {
						// Deleting from the subtrie reduced it to another
						// short node. Merge the nodes to avoid creating a
						// shortNode{..., shortNode{...}}. Use concat (which
						// always creates a new slice) instead of append to
						// avoid modifying n.Key since it might be shared with
						// other nodes.
						childKey := compactToHex(shortChild.Key)
						newnode := &shortNode{Key: hexToCompact(concat(nKey, childKey...))}
						newnode.Val = shortChild.Val
						newnode.flags.dirty = true
						newnode.flags.t = blockNr
						newnode.adjustTod(blockNr)
						// We do not increase generation count here, because one short node comes, but another one
						t.leftGeneration(shortChild.flags.t) // But shortChild goes away
						newNode = newnode
					} else {
						n.Val = nn
						n.flags.dirty = true
						n.adjustTod(blockNr)
						newNode = n
					}
				}
			}
		}
		return

	case *duoNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[keyStart] {
		case i1:
			adjust = n.child1 != nil && n.tod(blockNr) == n.child1.tod(blockNr)
			updated, nn = t.delete(n.child1, key, keyStart+1, blockNr)
			if !updated {
				newNode = n
			} else {
				n.child1 = nn
				if nn == nil {
					if n.child2 == nil {
						adjust = false
						t.leftGeneration(n.flags.t)
						newNode = nil
					} else {
						newNode = t.convertToShortNode(key, keyStart, n.child2, uint(i2), blockNr)
					}
				} else {
					n.flags.dirty = true
					newNode = n
				}
			}
		case i2:
			adjust = n.child2 != nil && n.tod(blockNr) == n.child2.tod(blockNr)
			updated, nn = t.delete(n.child2, key, keyStart+1, blockNr)
			if !updated {
				newNode = n
			} else {
				n.child2 = nn
				if nn == nil {
					if n.child1 == nil {
						adjust = false
						t.leftGeneration(n.flags.t)
						newNode = nil
					} else {
						newNode = t.convertToShortNode(key, keyStart, n.child1, uint(i1), blockNr)
					}
				} else {
					n.flags.dirty = true
					newNode = n
				}
			}
		default:
			adjust = false
			updated = false
			newNode = n
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return

	case *fullNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[keyStart]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		updated, nn = t.delete(child, key, keyStart+1, blockNr)
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
			for i, cld := range &n.Children {
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
			if count == 0 {
				t.leftGeneration(n.flags.t)
				newNode = nil
			} else if count == 1 {
				newNode = t.convertToShortNode(key, keyStart, n.Children[pos1], uint(pos1), blockNr)
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
				duo.flags.dirty = true
				duo.mask = (1 << uint(pos1)) | (uint32(1) << uint(pos2))
				duo.flags.t = blockNr
				duo.adjustTod(blockNr)
				adjust = false
				newNode = duo
			} else {
				// n still contains at least three values and cannot be reduced.
				n.flags.dirty = true
				newNode = n
			}
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return

	case valueNode:
		updated = true
		newNode = nil
		return

	case nil:
		updated = false
		newNode = nil
		return

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key[:keyStart]))
	}
}

func (t *Trie) PrepareToRemove() {
	t.prepareToRemove(t.root)
}

func (t *Trie) prepareToRemove(n node) {
	switch n := n.(type) {
	case *shortNode:
		t.leftGeneration(n.flags.t)
		t.prepareToRemove(n.Val)
	case *duoNode:
		t.leftGeneration(n.flags.t)
		t.prepareToRemove(n.child1)
		t.prepareToRemove(n.child2)
	case *fullNode:
		t.leftGeneration(n.flags.t)
		for _, child := range n.Children {
			if child != nil {
				t.prepareToRemove(child)
			}
		}
	}
}

// Timestamp given node and all descendants
func (t *Trie) timestampSubTree(n node, blockNr uint64) {
	switch n := n.(type) {
	case *shortNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			t.timestampSubTree(n.Val, blockNr)
		}
	case *duoNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			t.timestampSubTree(n.child1, blockNr)
			t.timestampSubTree(n.child2, blockNr)
		}
	case *fullNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			for _, child := range n.Children {
				if child != nil {
					t.timestampSubTree(child, blockNr)
				}
			}
		}
	}
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
func (t *Trie) Hash() common.Hash {
	hash, _ := t.hashRoot()
	return common.BytesToHash(hash.(hashNode))
}

func (t *Trie) UnloadOlderThan(gen uint64, trace bool) bool {
	if hn, unloaded := unloadOlderThan(t.root, gen); unloaded {
		t.root = hn
		return true
	}
	return false
}

func unloadOlderThan(n node, gen uint64) (hashNode, bool) {
	if n == nil {
		return nil, false
	}
	switch n := (n).(type) {
	case *shortNode:
		if n.flags.tod < gen {
			if hn, unloaded := unloadOlderThan(n.Val, gen); unloaded {
				n.Val = hn
			}
		}
	case *duoNode:
		if n.flags.t < gen {
			if n.flags.dirty {
				panic(fmt.Sprintf("duoNode dirty: %s", n))
			}
			return hashNode(common.CopyBytes(n.hash())), true
		}
		if n.flags.tod < gen {
			if hn, unloaded := unloadOlderThan(n.child1, gen); unloaded {
				n.child1 = hn
			}
			if hn, unloaded := unloadOlderThan(n.child2, gen); unloaded {
				n.child2 = hn
			}
		}
	case *fullNode:
		if n.flags.t < gen {
			if n.flags.dirty {
				panic(fmt.Sprintf("fullNode dirty: %s", n))
			}
			return hashNode(common.CopyBytes(n.hash())), true
		}
		if n.flags.tod < gen {
			for i, child := range &n.Children {
				if child != nil {
					if hn, unloaded := unloadOlderThan(child, gen); unloaded {
						n.Children[i] = hn
					}
				}
			}
		}
	}
	return nil, false
}

func (t *Trie) hashRoot() (node, error) {
	if t.root == nil {
		return hashNode(emptyRoot.Bytes()), nil
	}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	var hn common.Hash
	h.hash(t.root, true, hn[:])
	return hashNode(hn[:]), nil
}
