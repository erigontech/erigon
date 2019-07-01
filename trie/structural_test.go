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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"

	"github.com/ledgerwatch/turbo-geth/visual"
	"golang.org/x/crypto/sha3"

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

type HashBuilder struct {
	key, value    []byte // Next key-value pair to consume
	bufferStack   []*bytes.Buffer
	digitStack    []int
	branchStack   []*fullNode
	top           *shortNode
	sha           keccakState
	encodeToBytes bool
}

func NewHashBuilder(encodeToBytes bool) *HashBuilder {
	return &HashBuilder{sha: sha3.NewLegacyKeccak256().(keccakState), encodeToBytes: encodeToBytes}
}

func (hb *HashBuilder) setKeyValue(key, value []byte) {
	hb.key = key
	hb.value = value
}

func (hb *HashBuilder) branch(digit int) {
	//fmt.Printf("BRANCH %d\n", digit)
	f := &fullNode{}
	f.flags.dirty = true
	if hb.top == nil {
		if len(hb.bufferStack) == 0 {
			n := hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
			hb.branchStack[len(hb.branchStack)-1] = f
		} else {
			// Finalise existing hasher
			hn := hb.finaliseHasher()
			f.Children[digit] = hashNode(hn[:])
			hb.branchStack = append(hb.branchStack, f)
		}
	} else {
		f.Children[digit] = hb.top
		hb.top = nil
		hb.branchStack = append(hb.branchStack, f)
	}
}

func generateStructLen(buffer []byte, l int) int {
	if l < 56 {
		buffer[0] = byte(192 + l)
		return 1
	}
	if l < 256 {
		// l can be encoded as 1 byte
		buffer[1] = byte(l)
		buffer[0] = byte(247 + 1)
		return 2
	}
	if l < 65536 {
		buffer[2] = byte(l & 255)
		buffer[1] = byte(l >> 8)
		buffer[0] = byte(247 + 2)
		return 3
	}
	buffer[3] = byte(l & 255)
	buffer[2] = byte((l >> 8) & 255)
	buffer[1] = byte(l >> 16)
	buffer[0] = byte(247 + 3)
	return 4
}

func (hb *HashBuilder) addLeafToHasher(n *shortNode, buffer *bytes.Buffer) {
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	if len(n.Key) > 1 || n.Key[0] >= 128 {
		keyPrefix[0] = byte(128 + len(n.Key))
		kp = 1
		kl = len(n.Key)
	} else {
		kl = 1
	}
	var v []byte
	switch vn := n.Val.(type) {
	case valueNode:
		if len(vn) > 1 || vn[0] >= 128 {
			if hb.encodeToBytes {
				// Wrapping into another byte array
				vp = generateByteArrayLenDouble(valPrefix[:], 0, len(vn))
			} else {
				vp = generateByteArrayLen(valPrefix[:], 0, len(vn))
			}
			vl = len(vn)
		} else {
			vl = 1
		}
		v = []byte(vn)
	case hashNode:
		valPrefix[0] = 128 + 32
		vp = 1
		vl = 32
		v = []byte(vn)
	default:
		panic("")
	}
	totalLen := kp + kl + vp + vl
	if totalLen < 32 {
		// Embedded node
		buffer.Write(keyPrefix[:kp])
		buffer.Write(n.Key)
		buffer.Write(valPrefix[:vp])
		buffer.Write(v)
	} else {
		var lenPrefix [4]byte
		pt := generateStructLen(lenPrefix[:], totalLen)
		hb.sha.Reset()
		hb.sha.Write(lenPrefix[:pt])
		hb.sha.Write(keyPrefix[:kp])
		hb.sha.Write(n.Key)
		hb.sha.Write(valPrefix[:vp])
		hb.sha.Write(v)
		var hn common.Hash
		hb.sha.Read(hn[:])
		buffer.WriteByte(128 + 32)
		buffer.Write(hn[:])
	}
}

func (hb *HashBuilder) finaliseHasher() common.Hash {
	prevDigit := hb.digitStack[len(hb.digitStack)-1]
	hb.digitStack = hb.digitStack[:len(hb.digitStack)-1]
	prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
	hb.bufferStack = hb.bufferStack[:len(hb.bufferStack)-1]
	for i := prevDigit + 1; i < 17; i++ {
		prevBuffer.WriteByte(128)
	}
	var lenPrefix [4]byte
	pt := generateStructLen(lenPrefix[:], prevBuffer.Len())
	hb.sha.Reset()
	hb.sha.Write(lenPrefix[:pt])
	hb.sha.Write(prevBuffer.Bytes())
	var hn common.Hash
	hb.sha.Read(hn[:])
	return hn
}

func (hb *HashBuilder) hasher(digit int) {
	//fmt.Printf("HASHER %d\n", digit)
	var buffer bytes.Buffer
	for i := 0; i < digit; i++ {
		buffer.WriteByte(128) // Empty array
	}
	if hb.top == nil {
		if len(hb.bufferStack) == 0 {
			panic("")
		} else {
			hn := hb.finaliseHasher()
			buffer.WriteByte(128 + 32)
			buffer.Write(hn[:])
		}
	} else {
		hb.addLeafToHasher(hb.top, &buffer)
	}
	hb.bufferStack = append(hb.bufferStack, &buffer)
	hb.top = nil
	hb.digitStack = append(hb.digitStack, digit)
}

func (hb *HashBuilder) leaf(length int) {
	//fmt.Printf("LEAF %d\n", length)
	s := &shortNode{Key: hexToCompact(hb.key[len(hb.key)-length:]), Val: valueNode(hb.value)}
	s.flags.dirty = true
	hb.top = s
}

func (hb *HashBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	s := &shortNode{Key: hexToCompact(key)}
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
		s.Val = f
	} else {
		hn := hb.finaliseHasher()
		s.Val = hashNode(hn[:])
	}
	s.flags.dirty = true
	hb.top = s
}

func (hb *HashBuilder) add(digit int) {
	//fmt.Printf("ADD %d\n", digit)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		if hb.top == nil {
			n := f
			hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
			f = hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
		} else {
			f.Children[digit] = hb.top
			hb.top = nil
		}
	} else {
		prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
		prevDigit := hb.digitStack[len(hb.digitStack)-1]
		if hb.top == nil {
			hn := hb.finaliseHasher()
			if len(hb.bufferStack) > 0 {
				prevBuffer = hb.bufferStack[len(hb.bufferStack)-1]
				prevDigit = hb.digitStack[len(hb.digitStack)-1]
				for i := prevDigit + 1; i < digit; i++ {
					prevBuffer.WriteByte(128)
				}
				prevBuffer.WriteByte(128 + 32)
				prevBuffer.Write(hn[:])
				hb.digitStack[len(hb.digitStack)-1] = digit
			} else {
				f := hb.branchStack[len(hb.branchStack)-1]
				f.Children[digit] = hashNode(hn[:])
			}
		} else {
			for i := prevDigit + 1; i < digit; i++ {
				prevBuffer.WriteByte(128)
			}
			hb.addLeafToHasher(hb.top, prevBuffer)
			hb.digitStack[len(hb.digitStack)-1] = digit
			hb.top = nil
		}
	}
}

func (hb *HashBuilder) hash() {
	//fmt.Printf("HASH\n")
}

func (hb *HashBuilder) root() common.Hash {
	var hn common.Hash
	if hb.top == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return emptyRoot
			}
			h := newHasher(hb.encodeToBytes)
			defer returnHasherToPool(h)
			h.hash(hb.branchStack[len(hb.branchStack)-1], true, hn[:])
		} else {
			return hb.finaliseHasher()
		}
	} else {
		h := newHasher(hb.encodeToBytes)
		defer returnHasherToPool(h)
		h.hash(hb.top, true, hn[:])
	}
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
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

func TestHashBuilding(t *testing.T) {
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

	hb := NewHashBuilder(false)
	var prec, curr, succ []byte
	groups := make(map[string]uint32)
	for _, key := range keys {
		prec = curr
		curr = succ
		succ = []byte(key)
		if curr != nil {
			step(true, false, prec, curr, succ, hb, groups)
		}
		hb.setKeyValue([]byte(key), value)
	}
	prec = curr
	curr = succ
	succ = nil
	step(true, false, prec, curr, succ, hb, groups)
	builtHash := hb.root()
	//var tr1 Trie
	//tr1.root = tb.stack[len(tb.stack)-1]
	//visTrie(&tr1, "TestTrieBuilding_wrong.dot", highlights)
	//fmt.Printf("built hash: %x\n", builtHash)
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}
