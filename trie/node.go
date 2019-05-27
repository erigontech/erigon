// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"io"

	"github.com/ledgerwatch/turbo-geth/core/types/accounts"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type node interface {
	print(io.Writer)
	fstring(string) string
	dirty() bool
	hash() []byte
}

type (
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	fullNode struct {
		Children [17]node // Actual trie node data to encode/decode (needs custom encoder)
		flags    nodeFlag
	}
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	duoNode struct {
		mask   uint32 // Bitmask. The set bits indicate the child is not nil
		child1 node
		child2 node
		flags  nodeFlag
	}
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	shortNode struct {
		Key []byte
		Val node
	}
	hashNode  []byte
	valueNode []byte

	accountNode struct {
		accounts.Account
		storage     node
		hashCorrect bool
	}
)

// nilValueNode is used when collapsing internal trie nodes for hashing, since
// unset children need to serialize correctly.
var nilValueNode = valueNode(nil)

// EncodeRLP encodes a full node into the consensus RLP format.
func (n *fullNode) EncodeRLP(w io.Writer) error {
	var nodes [17]node

	for i, child := range &n.Children {
		if child != nil {
			nodes[i] = child
		} else {
			nodes[i] = nilValueNode
		}
	}
	return rlp.Encode(w, nodes)
}

func (n *duoNode) EncodeRLP(w io.Writer) error {
	var children [17]node
	i1, i2 := n.childrenIdx()
	children[i1] = n.child1
	children[i2] = n.child2
	for i := 0; i < 17; i++ {
		if i != int(i1) && i != int(i2) {
			children[i] = valueNode(nil)
		}
	}
	return rlp.Encode(w, children)
}

func (n *duoNode) childrenIdx() (i1 byte, i2 byte) {
	child := 1
	var m uint32 = 1
	for i := 0; i < 17; i++ {
		if (n.mask & m) > 0 {
			if child == 1 {
				i1 = byte(i)
				child = 2
			} else if child == 2 {
				i2 = byte(i)
				break
			}
		}
		m <<= 1
	}
	return i1, i2
}

func (n *fullNode) copy() *fullNode {
	c := *n
	return &c
}

func (n *fullNode) mask() uint32 {
	var m uint32
	for i, child := range n.Children {
		if child != nil {
			m |= (uint32(1) << uint(i))
		}
	}
	return m
}

func (n *fullNode) hashesExcept(idx byte) (uint32, []common.Hash, map[byte]*shortNode) {
	hashes := []common.Hash{}
	var mask uint32
	var m map[byte]*shortNode
	for i, child := range n.Children {
		if child != nil && i != int(idx) {
			short := false
			if s, ok := child.(*shortNode); ok {
				if m == nil {
					m = make(map[byte]*shortNode)
				}
				m[byte(i)] = s
				short = true
			}
			if !short {
				mask |= (uint32(1) << uint(i))
				hashes = append(hashes, common.BytesToHash(child.hash()))
			}
		}
	}
	return mask, hashes, m
}

func (n *fullNode) duoCopy() *duoNode {
	c := duoNode{}
	first := true
	for i, child := range n.Children {
		if child == nil {
			continue
		}
		if first {
			first = false
			c.mask |= (uint32(1) << uint(i))
			c.child1 = child
		} else {
			c.mask |= (uint32(1) << uint(i))
			c.child2 = child
			break
		}
	}
	if !n.flags.dirty {
		copy(c.flags.hash[:], n.flags.hash[:])
	}
	c.flags.dirty = n.flags.dirty
	return &c
}

func (n *duoNode) fullCopy() *fullNode {
	c := fullNode{}
	i1, i2 := n.childrenIdx()
	c.Children[i1] = n.child1
	c.Children[i2] = n.child2
	if !n.flags.dirty {
		copy(c.flags.hash[:], n.flags.hash[:])
	}
	c.flags.dirty = n.flags.dirty
	return &c
}

func (n *duoNode) hashesExcept(idx byte) (uint32, []common.Hash, map[byte]*shortNode) {
	i1, i2 := n.childrenIdx()
	var hash1, hash2 common.Hash
	var short1, short2 *shortNode
	if n.child1 != nil {
		if s, ok := n.child1.(*shortNode); ok {
			short1 = s
		}
		if short1 == nil {
			hash1 = common.BytesToHash(n.child1.hash())
		}
	}
	if n.child2 != nil {
		if s, ok := n.child2.(*shortNode); ok {
			short2 = s
		}
		if short2 == nil {
			hash2 = common.BytesToHash(n.child2.hash())
		}
	}
	switch idx {
	case i1:
		if short2 == nil {
			return uint32(1) << i2, []common.Hash{hash2}, nil
		} else {
			m := make(map[byte]*shortNode)
			m[i2] = short2
			return 0, []common.Hash{}, m
		}
	case i2:
		if short1 == nil {
			return uint32(1) << i1, []common.Hash{hash1}, nil
		} else {
			m := make(map[byte]*shortNode)
			m[i1] = short1
			return 0, []common.Hash{}, m
		}
	default:
		if short1 == nil {
			if short2 == nil {
				return (uint32(1) << i1) | (uint32(1) << i2), []common.Hash{hash1, hash2}, nil
			} else {
				m := make(map[byte]*shortNode)
				m[i2] = short2
				return (uint32(1) << i1), []common.Hash{hash1}, m
			}
		} else {
			if short2 == nil {
				m := make(map[byte]*shortNode)
				m[i1] = short1
				return (uint32(1) << i2), []common.Hash{hash2}, m
			} else {
				m := make(map[byte]*shortNode)
				m[i1] = short1
				m[i2] = short2
				return 0, []common.Hash{}, m
			}
		}
	}
}

func (n *duoNode) copy() *duoNode {
	c := *n
	return &c
}

func (n *shortNode) copy() *shortNode {
	c := *n
	return &c
}

// nodeFlag contains caching-related metadata about a node.
type nodeFlag struct {
	hash  common.Hash // cached hash of the node
	dirty bool        // whether the hash field represent the true hash
}

func (n hashNode) dirty() bool      { return false }
func (n valueNode) dirty() bool     { return true }
func (n *fullNode) dirty() bool     { return n.flags.dirty }
func (n *duoNode) dirty() bool      { return n.flags.dirty }
func (n *shortNode) dirty() bool    { return true }
func (an *accountNode) dirty() bool { return true }

func (n hashNode) hash() []byte      { return n }
func (n valueNode) hash() []byte     { return nil }
func (n *fullNode) hash() []byte     { return n.flags.hash[:] }
func (n *duoNode) hash() []byte      { return n.flags.hash[:] }
func (n *shortNode) hash() []byte    { return nil }
func (an *accountNode) hash() []byte { return nil }

// Pretty printing.
func (n fullNode) String() string     { return n.fstring("") }
func (n duoNode) String() string      { return n.fstring("") }
func (n shortNode) String() string    { return n.fstring("") }
func (n hashNode) String() string     { return n.fstring("") }
func (n valueNode) String() string    { return n.fstring("") }
func (an accountNode) String() string { return an.fstring("") }
