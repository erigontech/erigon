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
	"bytes"
	"io"

	"github.com/ledgerwatch/turbo-geth/core/types/accounts"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type node interface {
	print(io.Writer)
	fstring(string) string

	// if not empty, returns node's RLP or hash thereof
	reference() []byte
}

type (
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	fullNode struct {
		ref      nodeRef
		Children [17]node // Actual trie node data to encode/decode (needs custom encoder)
	}
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	duoNode struct {
		ref    nodeRef
		mask   uint32 // Bitmask. The set bits indicate the child is not nil
		child1 node
		child2 node
	}
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	shortNode struct {
		ref nodeRef
		Key []byte // HEX encoding
		Val node
	}
	hashNode  []byte
	valueNode []byte

	accountNode struct {
		accounts.Account
		storage     node
		rootCorrect bool
	}
)

// nilValueNode is used when collapsing internal trie nodes for hashing, since
// unset children need to serialize correctly.
var nilValueNode = valueNode(nil)

func EncodeAsValue(data []byte) ([]byte, error) {
	tmp := new(bytes.Buffer)
	if err := rlp.Encode(tmp, valueNode(data)); err != nil {
		return nil, err
	}
	return tmp.Bytes(), nil
}

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
	if n.ref.len > 0 {
		copy(c.ref.data[:], n.ref.data[:])
	}
	c.ref.len = n.ref.len
	return &c
}

func (n *duoNode) fullCopy() *fullNode {
	c := fullNode{}
	i1, i2 := n.childrenIdx()
	c.Children[i1] = n.child1
	c.Children[i2] = n.child2
	if n.ref.len > 0 {
		copy(c.ref.data[:], n.ref.data[:])
	}
	c.ref.len = n.ref.len
	return &c
}

func (n *duoNode) copy() *duoNode {
	c := *n
	return &c
}

func (n *shortNode) copy() *shortNode {
	c := *n
	return &c
}

func resetRefs(nd node) {
	switch n := nd.(type) {
	case *shortNode:
		n.ref.len = 0
		resetRefs(n.Val)
	case *duoNode:
		n.ref.len = 0
		resetRefs(n.child1)
		resetRefs(n.child2)
	case *fullNode:
		n.ref.len = 0
		for _, child := range n.Children {
			if child != nil {
				resetRefs(child)
			}
		}
	}
}

// nodeRef might contain node's RLP or hash thereof.
// Used instead of []byte in order to reduce GC churn.
type nodeRef struct {
	data common.Hash // cached RLP of the node or hash thereof
	len  byte        // length of the data (0 indicates invalid data)
}

func (n hashNode) reference() []byte      { return n }
func (n valueNode) reference() []byte     { return nil }
func (n *fullNode) reference() []byte     { return n.ref.data[0:n.ref.len] }
func (n *duoNode) reference() []byte      { return n.ref.data[0:n.ref.len] }
func (n *shortNode) reference() []byte    { return n.ref.data[0:n.ref.len] }
func (an *accountNode) reference() []byte { return nil }

// Pretty printing.
func (n fullNode) String() string     { return n.fstring("") }
func (n duoNode) String() string      { return n.fstring("") }
func (n shortNode) String() string    { return n.fstring("") }
func (n hashNode) String() string     { return n.fstring("") }
func (n valueNode) String() string    { return n.fstring("") }
func (an accountNode) String() string { return an.fstring("") }
