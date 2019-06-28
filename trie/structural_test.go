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

package trie

// Experimental code for separating data and structural information

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"

	"github.com/ledgerwatch/turbo-geth/visual"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

type TrieBuilder struct {
	key, value []byte // Next key-value pair to consume
	stack      []node
}

func (tb *TrieBuilder) setKeyValue(key, value []byte) {
	tb.key = key
	tb.value = value
}

func (tb *TrieBuilder) branch(digit int) {
	//fmt.Printf("BRANCH %d\n", digit)
	f := &fullNode{}
	f.flags.dirty = true
	n := tb.stack[len(tb.stack)-1]
	f.Children[digit] = n
	tb.stack[len(tb.stack)-1] = f
}

func (tb *TrieBuilder) hasher(digit int) {
	//fmt.Printf("HASHER %d\n", digit)
}

func (tb *TrieBuilder) leaf(length int) {
	//fmt.Printf("LEAF %d\n", length)
	s := &shortNode{Key: hexToCompact(tb.key[len(tb.key)-length:]), Val: valueNode(tb.value)}
	s.flags.dirty = true
	tb.stack = append(tb.stack, s)
}

func (tb *TrieBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	n := tb.stack[len(tb.stack)-1]
	s := &shortNode{Key: hexToCompact(key), Val: n}
	s.flags.dirty = true
	tb.stack[len(tb.stack)-1] = s
}

func (tb *TrieBuilder) add(digit int) {
	//fmt.Printf("ADD %d\n", digit)
	n := tb.stack[len(tb.stack)-1]
	tb.stack = tb.stack[:len(tb.stack)-1]
	f := tb.stack[len(tb.stack)-1].(*fullNode)
	f.Children[digit] = n
}

func (tb *TrieBuilder) hash() {
	//fmt.Printf("HASH\n")
}

func (tb *TrieBuilder) root() common.Hash {
	if len(tb.stack) == 0 {
		return emptyRoot
	}
	h := newHasher(false)
	defer returnHasherToPool(h)
	var hn common.Hash
	h.hash(tb.stack[len(tb.stack)-1], true, hn[:])
	return hn
}

func visTrie(tr *Trie, filename string, highlights [][]byte) {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	visual.StartGraph(f)
	Visual(tr, highlights, f, visual.HexIndexColors, visual.HexFontColors, false)
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func TestTrieBuilding(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 10000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := keybytesToHex(crypto.Keccak256(preimage[:])[:4])
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	var highlights [][]byte
	for _, key := range keys {
		highlights = append(highlights, hexToKeybytes([]byte(key)))
	}
	tr := New(common.Hash{}, false)
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		_, tr.root = tr.insert(tr.root, []byte(key), 0, valueNode(value), 0)
	}
	trieHash := tr.Hash()
	//fmt.Printf("trie hash: %x\n", trieHash)
	//visTrie(tr, "TestTrieBuilding_right.dot", highlights)

	var tb TrieBuilder
	var prec, curr, succ []byte
	groups := make(map[string]uint32)
	for _, key := range keys {
		prec = curr
		curr = succ
		succ = []byte(key)
		if curr != nil {
			step(false, false, prec, curr, succ, &tb, groups)
		}
		tb.setKeyValue([]byte(key), value)
	}
	prec = curr
	curr = succ
	succ = nil
	step(false, false, prec, curr, succ, &tb, groups)
	builtHash := tb.root()
	//var tr1 Trie
	//tr1.root = tb.stack[len(tb.stack)-1]
	//visTrie(&tr1, "TestTrieBuilding_wrong.dot", highlights)
	//fmt.Printf("built hash: %x\n", builtHash)
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", builtHash, trieHash)
	}
}
