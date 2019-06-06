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
	"fmt"
	"io"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type node interface {
	print(io.Writer)
	fstring(string) string
	dirty() bool
	hash() []byte
	makedirty()
	tod(def uint64) uint64 // Read Touch time of the Oldest Decendant
}

type (
	fullNode struct {
		Children [17]node // Actual trie node data to encode/decode (needs custom encoder)
		flags    nodeFlag
	}
	duoNode struct {
		mask   uint32 // Bitmask. The set bits indicate the child is not nil
		child1 node
		child2 node
		flags  nodeFlag
	}
	shortNode struct {
		Key   []byte
		Val   node
		flags nodeFlag
	}
	hashNode  []byte
	valueNode []byte
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
	c.flags.t = n.flags.t
	c.flags.tod = n.flags.tod
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
	c.flags.t = n.flags.t
	c.flags.tod = n.flags.tod
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
	t     uint64      // Touch time of the node
	tod   uint64      // Touch time of the Oldest Decendent
	hash  common.Hash // cached hash of the node
	dirty bool        // whether the hash field represent the true hash
}

func (n hashNode) dirty() bool   { return false }
func (n valueNode) dirty() bool  { return true }
func (n *fullNode) dirty() bool  { return n.flags.dirty }
func (n *duoNode) dirty() bool   { return n.flags.dirty }
func (n *shortNode) dirty() bool { return n.flags.dirty }

func (n hashNode) makedirty()  {}
func (n valueNode) makedirty() {}
func (n *fullNode) makedirty() {
	n.flags.dirty = true
	for _, child := range n.Children {
		if child != nil {
			child.makedirty()
		}
	}
}
func (n *duoNode) makedirty() {
	n.flags.dirty = true
	n.child1.makedirty()
	n.child2.makedirty()
}
func (n *shortNode) makedirty() {
	n.flags.dirty = true
	n.Val.makedirty()
}

func (n hashNode) hash() []byte   { return n }
func (n valueNode) hash() []byte  { return nil }
func (n *fullNode) hash() []byte  { return n.flags.hash[:] }
func (n *duoNode) hash() []byte   { return n.flags.hash[:] }
func (n *shortNode) hash() []byte { return n.flags.hash[:] }

// Pretty printing.
func (n fullNode) String() string  { return n.fstring("") }
func (n duoNode) String() string   { return n.fstring("") }
func (n shortNode) String() string { return n.fstring("") }
func (n hashNode) String() string  { return n.fstring("") }
func (n valueNode) String() string { return n.fstring("") }

func (n hashNode) tod(def uint64) uint64   { return def }
func (n valueNode) tod(def uint64) uint64  { return def }
func (n *fullNode) tod(def uint64) uint64  { return n.flags.tod }
func (n *duoNode) tod(def uint64) uint64   { return n.flags.tod }
func (n *shortNode) tod(def uint64) uint64 { return n.flags.tod }

func (n *fullNode) updateT(t uint64, joinGeneration, leftGeneration func(uint64)) {
	if n.flags.t != t {
		leftGeneration(n.flags.t)
		joinGeneration(t)
		n.flags.t = t
	}
}

func (n *fullNode) adjustTod(def uint64) {
	tod := def
	for _, node := range &n.Children {
		if node != nil {
			nodeTod := node.tod(def)
			if nodeTod < tod {
				tod = nodeTod
			}
		}
	}
	n.flags.tod = tod
}

func (n *duoNode) updateT(t uint64, joinGeneration, leftGeneration func(uint64)) {
	if n.flags.t != t {
		leftGeneration(n.flags.t)
		joinGeneration(t)
		n.flags.t = t
	}
}

func (n *duoNode) adjustTod(def uint64) {
	tod := def
	if n.child1 != nil {
		nodeTod := n.child1.tod(def)
		if nodeTod < tod {
			tod = nodeTod
		}
	}
	if n.child2 != nil {
		nodeTod := n.child2.tod(def)
		if nodeTod < tod {
			tod = nodeTod
		}
	}
	n.flags.tod = tod
}

func (n *shortNode) updateT(t uint64, joinGeneration, leftGeneration func(uint64)) {
	if n.flags.t != t {
		leftGeneration(n.flags.t)
		joinGeneration(t)
		n.flags.t = t
	}
}

func (n *shortNode) adjustTod(def uint64) {
	tod := def
	nodeTod := n.Val.tod(def)
	if nodeTod < tod {
		tod = nodeTod
	}
	n.flags.tod = tod
}

func (n *fullNode) fstring(ind string) string {
	resp := fmt.Sprintf("full\n%s  ", ind)
	for i, node := range &n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", indices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", indices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}
func (n *fullNode) print(w io.Writer) {
	fmt.Fprintf(w, "f(")
	for i, node := range &n.Children {
		if node != nil {
			fmt.Fprintf(w, "%d:", i)
			node.print(w)
		}
	}
	fmt.Fprintf(w, ")")
}

func (n *duoNode) fstring(ind string) string {
	resp := fmt.Sprintf("duo[\n%s  ", ind)
	i1, i2 := n.childrenIdx()
	resp += fmt.Sprintf("%s: %v", indices[i1], n.child1.fstring(ind+"  "))
	resp += fmt.Sprintf("%s: %v", indices[i2], n.child2.fstring(ind+"  "))
	return resp + fmt.Sprintf("\n%s] ", ind)
}
func (n *duoNode) print(w io.Writer) {
	fmt.Fprintf(w, "d(")
	i1, i2 := n.childrenIdx()
	fmt.Fprintf(w, "%d:", i1)
	n.child1.print(w)
	fmt.Fprintf(w, "%d:", i2)
	n.child2.print(w)
	fmt.Fprintf(w, ")")
}

func (n *shortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", compactToHex(n.Key), n.Val.fstring(ind+"  "))
}
func (n *shortNode) print(w io.Writer) {
	fmt.Fprintf(w, "s(%x:", compactToHex(n.Key))
	n.Val.print(w)
	fmt.Fprintf(w, ")")
}

func (n hashNode) fstring(ind string) string {
	return fmt.Sprintf("<%x> ", []byte(n))
}
func (n hashNode) print(w io.Writer) {
	fmt.Fprintf(w, "h(%x)", []byte(n))
}

func (n valueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}
func (n valueNode) print(w io.Writer) {
	fmt.Fprintf(w, "v(%x)", []byte(n))
}

func printDiffSide(n node, w io.Writer, ind string, key string) {
	switch n := n.(type) {
	case *fullNode:
		fmt.Fprintf(w, "full(\n")
		for i, child := range &n.Children {
			if child != nil {
				fmt.Fprintf(w, "%s%s:", ind, indices[i])
				printDiffSide(child, w, "  "+ind, key+indices[i])
				fmt.Fprintf(w, "\n")
			}
		}
		fmt.Fprintf(w, "%s)\n", ind)
	case *duoNode:
		fmt.Fprintf(w, "duo(\n")
		i1, i2 := n.childrenIdx()
		fmt.Fprintf(w, "%s%s:", ind, indices[i1])
		printDiffSide(n.child1, w, "  "+ind, key+indices[i1])
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "%s%s:", ind, indices[i2])
		printDiffSide(n.child2, w, "  "+ind, key+indices[i2])
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "%s)\n", ind)
	case *shortNode:
		fmt.Fprintf(w, "short %x(", n.hash())
		keyHex := compactToHex(n.Key)
		hexV := make([]byte, len(keyHex))
		for i := 0; i < len(hexV); i++ {
			hexV[i] = []byte(indices[keyHex[i]])[0]
		}
		fmt.Fprintf(w, "%s:", string(hexV))
		printDiffSide(n.Val, w, "  "+ind, key+string(hexV))
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "%s)\n", ind)
	case hashNode:
		fmt.Fprintf(w, "hash(%x)", []byte(n))
	case valueNode:
		fmt.Fprintf(w, "value(%s %x)", key, []byte(n))
	}
}

func printDiff(n1, n2 node, w io.Writer, ind string, key string) {
	if nv1, ok := n1.(valueNode); ok {
		fmt.Fprintf(w, "value(")
		if n, ok := n2.(valueNode); ok {
			fmt.Fprintf(w, "%s %x/%x", key, []byte(nv1), []byte(n))
		} else {
			fmt.Fprintf(w, "/%T", n2)
		}
		fmt.Fprintf(w, ")")
		return
	}
	if n2 != nil && bytes.Equal(n1.hash(), n2.hash()) {
		fmt.Fprintf(w, "hash(%x)", []byte(n1.hash()))
		return
	}
	switch n1 := n1.(type) {
	case *fullNode:
		fmt.Fprintf(w, "full(\n")
		if n, ok := n2.(*fullNode); ok {
			for i, child := range &n1.Children {
				child2 := n.Children[i]
				if child == nil {
					if child2 != nil {
						fmt.Fprintf(w, "%s%s:(nil/%x %T)\n", ind, indices[i], child2.hash(), child2)
					}
				} else if child2 == nil {
					fmt.Fprintf(w, "%s%s:(%T/nil)\n", ind, indices[i], child)
					printDiffSide(child, w, ind, key+indices[i])
				} else {
					fmt.Fprintf(w, "%s%s:", ind, indices[i])
					printDiff(child, child2, w, "  "+ind, key+indices[i])
					fmt.Fprintf(w, "\n")
				}
			}
		} else {
			fmt.Fprintf(w, "%s/%T\n", ind, n2)
			printDiffSide(n1, w, ind, key)
			printDiffSide(n2, w, ind, key)
		}
		fmt.Fprintf(w, "%s)\n", ind)
	case *duoNode:
		fmt.Fprintf(w, "duo(\n")
		if n, ok := n2.(*duoNode); ok {
			i1, i2 := n1.childrenIdx()
			j1, j2 := n.childrenIdx()
			if i1 == j1 {
				fmt.Fprintf(w, "%s%s:", ind, indices[i1])
				printDiff(n1.child1, n.child1, w, "  "+ind, key+indices[i1])
				fmt.Fprintf(w, "\n")
			} else {
				fmt.Fprintf(w, "%s%s:(/%s)", ind, indices[i1], indices[j1])
			}
			if i2 == j2 {
				fmt.Fprintf(w, "%s%s:", ind, indices[i2])
				printDiff(n1.child2, n.child2, w, "  "+ind, key+indices[i2])
				fmt.Fprintf(w, "\n")
			} else {
				fmt.Fprintf(w, "%s%s:(/%s)", ind, indices[i2], indices[j2])
			}
		} else {
			fmt.Fprintf(w, "%s/%T\n", ind, n2)
			printDiffSide(n1, w, ind, key)
		}
		fmt.Fprintf(w, "%s)\n", ind)
	case *shortNode:
		fmt.Fprintf(w, "short(")
		if n, ok := n2.(*shortNode); ok {
			if bytes.Equal(n1.Key, n.Key) {
				keyHex := compactToHex(n1.Key)
				hexV := make([]byte, len(keyHex))
				for i := 0; i < len(hexV); i++ {
					hexV[i] = []byte(indices[keyHex[i]])[0]
				}
				fmt.Fprintf(w, "%s:", string(hexV))
				printDiff(n1.Val, n.Val, w, "  "+ind, key+string(hexV))
				fmt.Fprintf(w, "\n")
			} else {
				fmt.Fprintf(w, "%x:(/%x)", compactToHex(n1.Key), compactToHex(n.Key))
				fmt.Fprintf(w, "\n:LEFT\n")
				printDiffSide(n1, w, ind, key)
				fmt.Fprintf(w, "\nRIGHT\n")
				printDiffSide(n2, w, ind, key)
			}
		} else {
			fmt.Fprintf(w, "/%T\n", n2)
			printDiffSide(n1, w, ind, key)
			printDiffSide(n2, w, ind, key)
		}
		fmt.Fprintf(w, "%s)\n", ind)
	case hashNode:
		fmt.Fprintf(w, "hash(")
		if n, ok := n2.(hashNode); ok {
			fmt.Fprintf(w, "%x/%x", []byte(n1), []byte(n))
		} else {
			fmt.Fprintf(w, "hash(%x)/%T(%x)\n", []byte(n1), n2, n2.hash())
			//printDiffSide(n2, w, ind, key)
		}
		fmt.Fprintf(w, ")")
	}
}

// decodeNode parses the RLP encoding of a trie node.
func decodeNode(hash, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := decodeShort(hash, elems)
		return n, wrapError(err, "short")
	case 17:
		n, err := decodeFull(hash, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

func decodeShort(hash, elems []byte) (node, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	key := compactToHex(kbuf)
	if hasTerm(key) {
		// value node
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid value node: %v", err)
		}
		return &shortNode{Key: key, Val: append(valueNode{}, val...)}, nil
	}
	r, _, err := decodeRef(rest)
	if err != nil {
		return nil, wrapError(err, "val")
	}
	return &shortNode{Key: key, Val: r}, nil
}

func decodeFull(hash, elems []byte) (*fullNode, error) {
	n := &fullNode{}
	for i := 0; i < 16; i++ {
		cld, rest, err := decodeRef(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i], elems = cld, rest
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = append(valueNode{}, val...)
	}
	return n, nil
}

const hashLen = len(common.Hash{})

func decodeRef(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return append(hashNode{}, val...), rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

// wraps a decoding error with information about the path to the
// invalid child node (for debugging encoding issues).
type decodeError struct {
	what  error
	stack []string
}

func wrapError(err error, ctx string) error {
	if err == nil {
		return nil
	}
	if decErr, ok := err.(*decodeError); ok {
		decErr.stack = append(decErr.stack, ctx)
		return decErr
	}
	return &decodeError{err, []string{ctx}}
}

func (err *decodeError) Error() string {
	return fmt.Sprintf("%v (decode path: %s)", err.what, strings.Join(err.stack, "<-"))
}
