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
	"bytes"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func (t *Trie) Print(w io.Writer) {
	witness, err := t.ExtractWitness(false, nil)
	if err != nil {
		panic(err)
	}
	_, err = witness.WriteInto(w)
	if err != nil {
		panic(err)
	}
}

type HexStdOutWriter struct{}

func (*HexStdOutWriter) Write(p []byte) (n int, err error) {
	fmt.Printf("%x", p)
	return len(p), nil
}

func (t *Trie) PrintTrie() {
	fmt.Printf("trie:0x")
	t.Print(&HexStdOutWriter{})
	fmt.Println("")
}

func Load(r io.Reader) (*Trie, error) {
	witness, err := NewWitnessFromReader(r, false)
	if err != nil {
		return nil, err
	}
	return BuildTrieFromWitness(witness, false)
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
	return fmt.Sprintf("<%x> ", n.hash)
}
func (n hashNode) print(w io.Writer) {
	fmt.Fprintf(w, "h(%x)", n.hash)
}

func (n valueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}
func (n valueNode) print(w io.Writer) {
	fmt.Fprintf(w, "v(%x)", []byte(n))
}

func (n codeNode) fstring(ind string) string {
	return fmt.Sprintf("code: %x ", []byte(n))
}
func (n codeNode) print(w io.Writer) {
	fmt.Fprintf(w, "code(%x)", []byte(n))
}

func (an accountNode) fstring(ind string) string {
	encodedAccount := make([]byte, an.EncodingLengthForHashing())
	an.EncodeForHashing(encodedAccount)
	if an.storage == nil {
		return fmt.Sprintf("%x", encodedAccount)
	}
	return fmt.Sprintf("%x %v", encodedAccount, an.storage.fstring(ind+" "))
}

func (an accountNode) print(w io.Writer) {
	encodedAccount := make([]byte, an.EncodingLengthForHashing())
	an.EncodeForHashing(encodedAccount)

	fmt.Fprintf(w, "v(%x)", encodedAccount)
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
		fmt.Fprintf(w, "short %x(", n.reference())
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
		fmt.Fprintf(w, "hash(%x)", n.hash)
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
	if n2 != nil && bytes.Equal(n1.reference(), n2.reference()) {
		fmt.Fprintf(w, "hash(%x)", n1.reference())
		if len(n1.reference()) == 0 {
			fmt.Fprintf(w, "%s/%s", n1.fstring(""), n2.fstring(""))
		}
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
						fmt.Fprintf(w, "%s%s:(nil/%x %T)\n", ind, indices[i], child2.reference(), child2)
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
			fmt.Fprintf(w, "%x/%x", n1.hash, n.hash)
		} else {
			fmt.Fprintf(w, "hash(%x)/%T(%x)\n", n1.hash, n2, n2.reference())
			//printDiffSide(n2, w, ind, key)
		}
		fmt.Fprintf(w, ")")
	}
}

func (t *Trie) HashOfHexKey(hexKey []byte) (libcommon.Hash, error) {
	nd := t.root
	pos := 0
	var account bool
	for pos < len(hexKey) || account {
		switch n := nd.(type) {
		case nil:
			return libcommon.Hash{}, fmt.Errorf("premature nil: pos %d, hexKey %x", pos, hexKey)
		case *shortNode:
			matchlen := prefixLen(hexKey[pos:], n.Key)
			if matchlen == len(n.Key) || n.Key[matchlen] == 16 {
				nd = n.Val
				pos += matchlen
				if _, ok := n.Val.(*accountNode); ok {
					account = true
				}
			} else {
				return libcommon.Hash{}, fmt.Errorf("too long shortNode key: pos %d, hexKey %x: %s", pos, hexKey, n.fstring(""))
			}
		case *duoNode:
			i1, i2 := n.childrenIdx()
			switch hexKey[pos] {
			case i1:
				nd = n.child1
				pos++
			case i2:
				nd = n.child2
				pos++
			default:
				return libcommon.Hash{}, fmt.Errorf("nil entry in the duoNode: pos %d, hexKey %x", pos, hexKey)
			}
		case *fullNode:
			child := n.Children[hexKey[pos]]
			if child == nil {
				return libcommon.Hash{}, fmt.Errorf("nil entry in the fullNode: pos %d, hexKey %x", pos, hexKey)
			}
			nd = child
			pos++
		case valueNode:
			return libcommon.Hash{}, fmt.Errorf("premature valueNode: pos %d, hexKey %x", pos, hexKey)
		case *accountNode:
			nd = n.storage
			account = false
		case hashNode:
			return libcommon.Hash{}, fmt.Errorf("premature hashNode: pos %d, hexKey %x", pos, hexKey)
		default:
			panic(fmt.Sprintf("Unknown node: %T", n))
		}
	}
	var hash libcommon.Hash
	if hn, ok := nd.(hashNode); ok {
		copy(hash[:], hn.hash)
	} else {
		h := t.newHasherFunc()
		defer returnHasherToPool(h)
		if _, err := h.hash(nd, len(hexKey) == 0, hash[:]); err != nil {
			return libcommon.Hash{}, err
		}
	}
	return hash, nil
}
