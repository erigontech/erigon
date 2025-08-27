// Copyright 2014 The go-ethereum Authors
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

package trie

import (
	"bytes"
	"io"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const codeSizeUncached = -1

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type Node interface {
	print(io.Writer)
	fstring(string) string

	// if not empty, returns node's RLP or hash thereof
	reference() []byte
}

type (
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	FullNode struct {
		ref      nodeRef
		Children [17]Node // Actual trie node data to encode/decode (needs custom encoder)
	}
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	DuoNode struct {
		ref    nodeRef
		mask   uint32 // Bitmask. The set bits indicate the child is not nil
		child1 Node
		child2 Node
	}
	// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
	ShortNode struct {
		ref nodeRef
		Key []byte // HEX encoding
		Val Node
	}
	HashNode struct {
		hash []byte
	}
	ValueNode []byte

	AccountNode struct {
		accounts.Account
		Storage     Node
		RootCorrect bool
		Code        CodeNode
		CodeSize    int
	}

	CodeNode []byte
)

// nilValueNode is used when collapsing internal trie nodes for hashing, since
// unset hasState need to serialize correctly.
var nilValueNode = ValueNode(nil)

func NewHashNode(hash []byte) *HashNode {
	return &HashNode{hash: hash}
}

func NewShortNode(key []byte, value Node) *ShortNode {
	s := &ShortNode{
		Key: key,
		Val: value,
	}

	return s
}

func EncodeAsValue(data []byte) ([]byte, error) {
	tmp := new(bytes.Buffer)
	if err := rlp.Encode(tmp, ValueNode(data)); err != nil {
		return nil, err
	}
	return tmp.Bytes(), nil
}

// EncodeRLP encodes a full node into the consensus RLP format.
func (n *FullNode) EncodeRLP(w io.Writer) error {
	var nodes [17]Node

	for i, child := range &n.Children {
		if child != nil {
			nodes[i] = child
		} else {
			nodes[i] = nilValueNode
		}
	}
	return rlp.Encode(w, nodes)
}

func (n *DuoNode) EncodeRLP(w io.Writer) error {
	var children [17]Node
	i1, i2 := n.childrenIdx()
	children[i1] = n.child1
	children[i2] = n.child2
	for i := 0; i < 17; i++ {
		if i != int(i1) && i != int(i2) {
			children[i] = ValueNode(nil)
		}
	}
	return rlp.Encode(w, children)
}

func (n *DuoNode) childrenIdx() (i1 byte, i2 byte) {
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

func resetRefs(nd Node) {
	switch n := nd.(type) {
	case *ShortNode:
		n.ref.len = 0
		resetRefs(n.Val)
	case *DuoNode:
		n.ref.len = 0
		resetRefs(n.child1)
		resetRefs(n.child2)
	case *FullNode:
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

func (n HashNode) reference() []byte      { return n.hash }
func (n ValueNode) reference() []byte     { return nil }
func (n CodeNode) reference() []byte      { return nil }
func (n *FullNode) reference() []byte     { return n.ref.data[0:n.ref.len] }
func (n *DuoNode) reference() []byte      { return n.ref.data[0:n.ref.len] }
func (n *ShortNode) reference() []byte    { return n.ref.data[0:n.ref.len] }
func (an *AccountNode) reference() []byte { return nil }

// Pretty printing.
func (n FullNode) String() string     { return n.fstring("") }
func (n DuoNode) String() string      { return n.fstring("") }
func (n ShortNode) String() string    { return n.fstring("") }
func (n HashNode) String() string     { return n.fstring("") }
func (n ValueNode) String() string    { return n.fstring("") }
func (n CodeNode) String() string     { return n.fstring("") }
func (an AccountNode) String() string { return an.fstring("") }

func CodeKeyFromAddrHash(addrHash []byte) []byte {
	return append(addrHash, 0xC0, 0xDE)
}

func CodeHexFromHex(hex []byte) []byte {
	return append(hex, 0x0C, 0x00, 0x0D, 0x0E)
}

func IsPointingToCode(key []byte) bool {
	// checking for 0xC0DE
	l := len(key)
	if l < 2 {
		return false
	}

	return key[l-2] == 0xC0 && key[l-1] == 0xDE
}

func AddrHashFromCodeKey(codeKey []byte) []byte {
	// cut off 0xC0DE
	return codeKey[:len(codeKey)-2]
}

func calcSubtreeSize(node Node) int {
	switch n := node.(type) {
	case nil:
		return 0
	case ValueNode:
		return 0
	case *ShortNode:
		return calcSubtreeSize(n.Val)
	case *DuoNode:
		return 1 + calcSubtreeSize(n.child1) + calcSubtreeSize(n.child2)
	case *FullNode:
		size := 1
		for _, child := range n.Children {
			size += calcSubtreeSize(child)
		}
		return size
	case *AccountNode:
		return len(n.Code) + calcSubtreeSize(n.Storage)
	case HashNode:
		return 0
	}
	return 0
}

func calcSubtreeNodes(node Node) int {
	switch n := node.(type) {
	case nil:
		return 0
	case ValueNode:
		return 0
	case *ShortNode:
		return calcSubtreeNodes(n.Val)
	case *DuoNode:
		return 1 + calcSubtreeNodes(n.child1) + calcSubtreeNodes(n.child2)
	case *FullNode:
		size := 1
		for _, child := range n.Children {
			size += calcSubtreeNodes(child)
		}
		return size
	case *AccountNode:
		if n.Code != nil {
			return 1 + calcSubtreeNodes(n.Storage)
		}
		return calcSubtreeNodes(n.Storage)
	case HashNode:
		return 0
	}
	return 0
}
