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
	"sort"
	"testing"

	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

type HashBuilder struct {
	hexKey, value bytes.Buffer // Next key-value pair to consume
	bufferStack   []*bytes.Buffer
	digitStack    []int
	branchStack   []*fullNode
	topKey        []byte
	topValue      []byte
	topHash       common.Hash
	topBranch     *fullNode
	sha           keccakState
	encodeToBytes bool
}

func NewHashBuilder(encodeToBytes bool) *HashBuilder {
	return &HashBuilder{
		sha:           sha3.NewLegacyKeccak256().(keccakState),
		encodeToBytes: encodeToBytes,
	}
}

// key is original key (not transformed into hex or compacted)
func (hb *HashBuilder) setKeyValue(key, value []byte) {
	// Transform key into hex representation
	hb.hexKey.Reset()
	for _, b := range key {
		hb.hexKey.WriteByte(b / 16)
		hb.hexKey.WriteByte(b % 16)
	}
	hb.hexKey.WriteByte(16)
	hb.value.Reset()
	hb.value.Write(value)
}

func (hb *HashBuilder) branch(digit int) {
	//fmt.Printf("BRANCH %d\n", digit)
	f := &fullNode{}
	f.flags.dirty = true
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			n := hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
			hb.branchStack[len(hb.branchStack)-1] = f
		} else {
			// Finalise existing hasher
			hn, _ := hb.finaliseHasher()
			f.Children[digit] = hashNode(hn[:])
			hb.branchStack = append(hb.branchStack, f)
		}
	} else {
		f.Children[digit] = hb.shortNode()
		hb.topKey = nil
		hb.topValue = nil
		hb.topBranch = nil
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

func (hb *HashBuilder) addLeafToHasher(buffer *bytes.Buffer) error {
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(hb.topKey) {
		compactLen = (len(hb.topKey)-1)/2 + 1
		if len(hb.topKey)&1 == 0 {
			compact0 = 48 + hb.topKey[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(hb.topKey)/2 + 1
		if len(hb.topKey)&1 == 1 {
			compact0 = 16 + hb.topKey[0] // Odd (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = byte(128 + compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	var v []byte
	if hb.topValue != nil {
		if len(hb.topValue) > 1 || hb.topValue[0] >= 128 {
			if hb.encodeToBytes {
				// Wrapping into another byte array
				vp = generateByteArrayLenDouble(valPrefix[:], 0, len(hb.topValue))
			} else {
				vp = generateByteArrayLen(valPrefix[:], 0, len(hb.topValue))
			}
			vl = len(hb.topValue)
		} else {
			vl = 1
		}
		v = hb.topValue
	} else if hb.topBranch != nil {
		panic("")
	} else {
		valPrefix[0] = 128 + 32
		vp = 1
		vl = 32
		v = hb.topHash[:]
	}
	totalLen := kp + kl + vp + vl
	if totalLen < 32 {
		// Embedded node
		buffer.Write(keyPrefix[:kp])
		if err := buffer.WriteByte(compact0); err != nil {
			return err
		}
		for i := 1; i < compactLen; i++ {
			if err := buffer.WriteByte(hb.topKey[ni]*16 + hb.topKey[ni+1]); err != nil {
				return err
			}
			ni += 2
		}
		buffer.Write(valPrefix[:vp])
		buffer.Write(v)
	} else {
		var lenPrefix [4]byte
		pt := generateStructLen(lenPrefix[:], totalLen)
		hb.sha.Reset()
		if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
			return err
		}
		var b [1]byte
		b[0] = compact0
		if _, err := hb.sha.Write(b[:]); err != nil {
			return err
		}
		for i := 1; i < compactLen; i++ {
			b[0] = hb.topKey[ni]*16 + hb.topKey[ni+1]
			if _, err := hb.sha.Write(b[:]); err != nil {
				return err
			}
			ni += 2
		}
		if _, err := hb.sha.Write(valPrefix[:vp]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(v); err != nil {
			return err
		}
		var hn common.Hash
		if _, err := hb.sha.Read(hn[:]); err != nil {
			return err
		}
		if err := buffer.WriteByte(128 + 32); err != nil {
			return err
		}
		if _, err := buffer.Write(hn[:]); err != nil {
			return err
		}
	}
	return nil
}

func (hb *HashBuilder) finaliseHasher() (common.Hash, error) {
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
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return common.Hash{}, err
	}
	if _, err := hb.sha.Write(prevBuffer.Bytes()); err != nil {
		return common.Hash{}, err
	}
	var hn common.Hash
	if _, err := hb.sha.Read(hn[:]); err != nil {
		return common.Hash{}, err
	}
	return hn, nil
}

func (hb *HashBuilder) hasher(digit int) {
	//fmt.Printf("HASHER %d\n", digit)
	var buffer bytes.Buffer
	for i := 0; i < digit; i++ {
		buffer.WriteByte(128) // Empty array
	}
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			panic("")
		} else {
			hn, _ := hb.finaliseHasher()
			buffer.WriteByte(128 + 32)
			buffer.Write(hn[:])
		}
	} else {
		if err := hb.addLeafToHasher(&buffer); err != nil {
			panic(err)
		}
	}
	hb.bufferStack = append(hb.bufferStack, &buffer)
	hb.topKey = nil
	hb.topValue = nil
	hb.topBranch = nil
	hb.digitStack = append(hb.digitStack, digit)
}

func (hb *HashBuilder) leaf(length int) {
	hex := hb.hexKey.Bytes()
	//fmt.Printf("LEAF %d, hex: %d\n", length, len(hex))
	hb.topKey = hex[len(hex)-length:]
	hb.topValue = hb.value.Bytes()
	hb.topBranch = nil
}

func (hb *HashBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
		hb.topBranch = f
	} else {
		hn, _ := hb.finaliseHasher()
		hb.topHash = hn
		hb.topBranch = nil
	}
	hb.topKey = key
	hb.topValue = nil
}

func (hb *HashBuilder) shortNode() *shortNode {
	if hb.topValue != nil {
		return &shortNode{Key: hexToCompact(hb.topKey), Val: valueNode(hb.topValue)}
	} else if hb.topBranch != nil {
		return &shortNode{Key: hexToCompact(hb.topKey), Val: hb.topBranch}
	}
	return &shortNode{Key: hexToCompact(hb.topKey), Val: hashNode(common.CopyBytes(hb.topHash[:]))}
}

func (hb *HashBuilder) add(digit int) {
	//fmt.Printf("ADD %d\n", digit)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		if hb.topKey == nil {
			n := f
			hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
			f = hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
		} else {
			f.Children[digit] = hb.shortNode()
			hb.topKey = nil
			hb.topValue = nil
			hb.topBranch = nil
		}
	} else {
		prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
		prevDigit := hb.digitStack[len(hb.digitStack)-1]
		if hb.topKey == nil {
			hn, _ := hb.finaliseHasher()
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
			if err := hb.addLeafToHasher(prevBuffer); err != nil {
				panic(err)
			}
			hb.digitStack[len(hb.digitStack)-1] = digit
			hb.topKey = nil
			hb.topValue = nil
			hb.topBranch = nil
		}
	}
}

func (hb *HashBuilder) hash() {
	//fmt.Printf("HASH\n")
}

func (hb *HashBuilder) root() node {
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return nil
			}
			return hb.branchStack[len(hb.branchStack)-1]
		}
		hn, _ := hb.finaliseHasher()
		return hashNode(hn[:])
	}
	return hb.shortNode()
}

func (hb *HashBuilder) rootHash() common.Hash {
	var hn common.Hash
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return emptyRoot
			}
			h := newHasher(hb.encodeToBytes)
			defer returnHasherToPool(h)
			h.hash(hb.branchStack[len(hb.branchStack)-1], true, hn[:])
		} else {
			hn, _ = hb.finaliseHasher()
			return hn
		}
	} else {
		h := newHasher(hb.encodeToBytes)
		defer returnHasherToPool(h)
		h.hash(hb.shortNode(), true, hn[:])
	}
	return hn
}
func TestHashBuilding(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 100000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])[:8]
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			fmt.Printf("Duplicate!\n")
		}
	}
	tr := New(common.Hash{}, false)
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		tr.Update([]byte(key), valueNode(value), 0)
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(false)
	var prec, curr, succ bytes.Buffer
	var groups uint64
	for _, key := range keys {
		prec.Reset()
		prec.Write(curr.Bytes())
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := []byte(key)
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			groups = step(func(prefix []byte) bool { return true }, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups)
		}
		hb.setKeyValue([]byte(key), value)
	}
	prec.Reset()
	prec.Write(curr.Bytes())
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	groups = step(func(prefix []byte) bool { return true }, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups)
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

func TestResolution(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 100000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])[:8]
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	tr := New(common.Hash{}, false)
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		tr.Update([]byte(key), valueNode(value), 0)
	}
	trieHash := tr.Hash()

	// Choose some keys to be resolved
	var rs ResolveSet
	// First, existing keys
	for i := 0; i < 1000; i += 200 {
		rs.AddKey([]byte(keys[i]))
	}
	// Next, some non-exsiting keys
	for i := 0; i < 1000; i++ {
		rs.AddKey(crypto.Keccak256([]byte(keys[i]))[:8])
	}

	hb := NewHashBuilder(false)
	var prec, curr, succ bytes.Buffer
	var groups uint64
	for _, key := range keys {
		prec.Reset()
		prec.Write(curr.Bytes())
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := []byte(key)
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			groups = step(rs.HashOnly, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups)
		}
		hb.setKeyValue([]byte(key), value)
	}
	prec.Reset()
	prec.Write(curr.Bytes())
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	groups = step(rs.HashOnly, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups)
	tr1 := New(common.Hash{}, false)
	tr1.root = hb.root()
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
	// Check the availibility of the resolved keys
	for _, hex := range rs.hexes {
		key := hexToKeybytes(hex)
		_, found := tr1.Get(key, 0)
		if !found {
			t.Errorf("Key %x was not resolved", hex)
		}
	}
}
