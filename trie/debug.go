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

// Debugging utilities for Merkle Patricia trees

package trie

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"github.com/ledgerwatch/turbo-geth/common/pool"
)

func (t *Trie) Print(w io.Writer) {
	if t.root != nil {
		t.root.print(w)
	}
	fmt.Fprintf(w, "\n")
}

func (t *Trie) PrintTrie() {
	if t.root == nil {
		fmt.Printf("nil Trie\n")
	} else {
		fmt.Printf("%s\n", t.root.fstring(""))
	}
}

func loadNode(br *bufio.Reader) (node, error) {
	nodeType, err := br.ReadString('(')
	if err != nil {
		return nil, err
	}
	switch nodeType[len(nodeType)-2:] {
	case "f(":
		return loadFull(br)
	case "d(":
		return loadDuo(br)
	case "s(":
		return loadShort(br)
	case "h(":
		return loadHash(br)
	case "v(":
		return loadValue(br)
	}
	return nil, fmt.Errorf("unknown node type: %s", nodeType)
}

func loadFull(br *bufio.Reader) (*fullNode, error) {
	n := fullNode{}
	n.flags.dirty = true
	for {
		next, err := br.Peek(1)
		if err != nil {
			return nil, err
		}
		if next[0] == ')' {
			break
		}
		idxStr, err := br.ReadBytes(':')
		if err != nil {
			return nil, err
		}
		idxStr = idxStr[:len(idxStr)-1] // chop off ":"
		idx, err := strconv.ParseInt(string(idxStr), 10, 64)
		if err != nil {
			return nil, err
		}
		n.Children[idx], err = loadNode(br)
		if err != nil {
			return nil, err
		}
	}
	if _, err := br.Discard(1); err != nil { // Discard ")"
		return nil, err
	}
	return &n, nil
}

func loadDuo(br *bufio.Reader) (*duoNode, error) {
	n := duoNode{}
	n.flags.dirty = true
	idxStr1, err := br.ReadBytes(':')
	if err != nil {
		return nil, err
	}
	idxStr1 = idxStr1[:len(idxStr1)-1] // chop off ":"
	idx1, err := strconv.ParseInt(string(idxStr1), 10, 64)
	if err != nil {
		return nil, err
	}
	n.child1, err = loadNode(br)
	if err != nil {
		return nil, err
	}
	idxStr2, err := br.ReadBytes(':')
	if err != nil {
		return nil, err
	}
	idxStr2 = idxStr2[:len(idxStr2)-1] // chop off ":"
	idx2, err := strconv.ParseInt(string(idxStr2), 10, 64)
	if err != nil {
		return nil, err
	}
	n.child2, err = loadNode(br)
	if err != nil {
		return nil, err
	}
	n.mask = (uint32(1) << uint(idx1)) | (uint32(1) << uint(idx2))
	if _, err := br.Discard(1); err != nil { // Discard ")"
		return nil, err
	}
	return &n, nil
}

func loadShort(br *bufio.Reader) (*shortNode, error) {
	n := shortNode{}
	keyHexHex, err := br.ReadBytes(':')
	if err != nil {
		return nil, err
	}
	keyHexHex = keyHexHex[:len(keyHexHex)-1]
	keyHex, err := hex.DecodeString(string(keyHexHex))
	if err != nil {
		return nil, err
	}
	n.Key = keyHex
	n.Val, err = loadNode(br)
	if err != nil {
		return nil, err
	}
	if _, err := br.Discard(1); err != nil { // Discard ")"
		return nil, err
	}
	return &n, nil
}

func loadHash(br *bufio.Reader) (hashNode, error) {
	hashHex, err := br.ReadBytes(')')
	if err != nil {
		return nil, err
	}
	hashHex = hashHex[:len(hashHex)-1]
	hash, err := hex.DecodeString(string(hashHex))
	if err != nil {
		return nil, err
	}
	return hashNode(hash), nil
}

func loadValue(br *bufio.Reader) (valueNode, error) {
	valHex, err := br.ReadBytes(')')
	if err != nil {
		return nil, err
	}
	valHex = valHex[:len(valHex)-1]
	val, err := hex.DecodeString(string(valHex))
	if err != nil {
		return nil, err
	}
	return valueNode(val), nil
}

func Load(r io.Reader) (*Trie, error) {
	br := bufio.NewReader(r)
	t := new(Trie)
	var err error
	t.root, err = loadNode(br)
	return t, err
}

func (t *Trie) PrintDiff(t2 *Trie, w io.Writer) {
	printDiff(t.root, t2.root, w, "", "0x")
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
	return fmt.Sprintf("{%x: %v} ", n.Key, n.Val.fstring(ind+"  "))
}
func (n *shortNode) print(w io.Writer) {
	fmt.Fprintf(w, "s(%x:", n.Key)
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

func (an accountNode) fstring(string) string {
	encodedAccount := pool.GetBuffer(an.EncodingLengthForHashing())
	an.EncodeForHashing(encodedAccount.B)
	defer pool.PutBuffer(encodedAccount)

	return fmt.Sprintf("%x ", encodedAccount.String())
}

func (an accountNode) print(w io.Writer) {
	encodedAccount := pool.GetBuffer(an.EncodingLengthForHashing())
	an.EncodeForHashing(encodedAccount.B)
	defer pool.PutBuffer(encodedAccount)

	fmt.Fprintf(w, "v(%x)", encodedAccount.String())
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
		keyHex := n.Key
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
	case *accountNode:
		fmt.Fprintf(w, "account(%s %x)", key, n)
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
				keyHex := n1.Key
				hexV := make([]byte, len(keyHex))
				for i := 0; i < len(hexV); i++ {
					hexV[i] = []byte(indices[keyHex[i]])[0]
				}
				fmt.Fprintf(w, "%s:", string(hexV))
				printDiff(n1.Val, n.Val, w, "  "+ind, key+string(hexV))
				fmt.Fprintf(w, "\n")
			} else {
				fmt.Fprintf(w, "%x:(/%x)", n1.Key, n.Key)
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
