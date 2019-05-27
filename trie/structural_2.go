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

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/crypto/sha3"
)

// Experimental code for separating data and structural information
// Each function corresponds to an opcode
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type structInfoReceiver interface {
	leaf(length int)
	leafHash(length int)
	extension(key []byte)
	extensionHash(key []byte)
	branch(set uint32)
	branchHash(set uint32)
	hash(number int)
}

// genStructStep is one step of the algorithm that generates the structural information based on the sequence of keys.
// `hashOnly` parameter is the function that, called for a certain prefix, determines whether the trie node for that prefix needs to be
// compressed into just hash (if `true` is returned), or constructed (if `false` is returned). Usually the `hashOnly` function is
// implemented in such a way to guarantee that certain keys are always accessible in the resulting trie (see ResolveSet.HashOnly function).
// `recursive` parameter is set to true if the algorithm's step is invoked recursively, i.e. not after a freshly provided leaf.
// Recursive invocation is used to emit opcodes for non-leaf nodes.
// `prec`, `curr`, `succ` are three full keys or prefixes that are currently visible to the algorithm. By comparing these, the algorithm
// makes decisions about the local structure, i.e. the presense of the prefix groups.
// `e` parameter is a `structInfoReceiver`, an object that receives opcode messages.
// `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack.
// Whenever a `BRANCH` or `BRANCHHASH` opcode is emitted, the set of digits is taken from the corresponding `groups` item, which is
// then removed from the slice. This signifies the usage of the number of the stack items by the `BRANCH` or `BRANCHHASH` opcode.
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
func genStructStep(
	hashOnly func(prefix []byte) bool,
	recursive bool,
	prec, curr, succ []byte,
	e structInfoReceiver,
	groups []uint32,
) []uint32 {
	if !recursive && len(prec) == 0 {
		prec = nil
	}
	// Calculate the prefix of the smallest prefix group containing curr
	precLen := prefixLen(prec, curr)
	succLen := prefixLen(succ, curr)
	var maxLen int
	if precLen > succLen {
		maxLen = precLen
	} else {
		maxLen = succLen
	}
	//fmt.Printf("prec: %x, curr: %x, succ: %x, maxLen %d, prefix: %x\n", prec, curr, succ, maxLen, prefix)
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	for maxLen >= len(groups) {
		groups = append(groups, 0)
	}
	groups[maxLen] |= (uint32(1) << extraDigit)
	//fmt.Printf("groups[%d] is now %b, len(groups) %d, prefix %x\n", maxLen, groups[maxLen], len(groups), prefix)
	remainderStart := maxLen
	if len(succ) > 0 || prec != nil {
		remainderStart++
	}
	remainderLen := len(curr) - remainderStart
	// Emit LEAF or EXTENSION based on the remainder
	if recursive {
		if remainderLen > 0 {
			if hashOnly(curr[:maxLen]) {
				e.extensionHash(curr[remainderStart : remainderStart+remainderLen])
			} else {
				e.extension(curr[remainderStart : remainderStart+remainderLen])
			}
		}
	} else {
		if hashOnly(curr[:maxLen]) {
			e.leafHash(remainderLen)
		} else {
			e.leaf(remainderLen)
		}
	}
	// Check for the optional part
	if precLen <= succLen && len(succ) > 0 {
		return groups
	}
	// Close the immediately encompassing prefix group, if needed
	if len(succ) > 0 || prec != nil {
		if hashOnly(curr[:maxLen]) {
			e.branchHash(groups[maxLen])
		} else {
			e.branch(groups[maxLen])
		}
	}
	groups = groups[:maxLen]
	// Check the end of recursion
	if precLen == 0 {
		return groups
	}
	// Identify preceding key for the recursive invocation
	newCurr := curr[:precLen]
	var newPrec []byte
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
	}
	if len(groups) >= 1 {
		newPrec = curr[:len(groups)-1]
	}

	// Recursion
	return genStructStep(hashOnly, true, newPrec, newCurr, succ, e, groups)
}

// HashBuilder implements the interface `structInfoReceiver` and opcodes that the structural information of the trie
// is comprised of
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type HashBuilder struct {
	hexKey    bytes.Buffer // Next key-value pair to consume
	hashStack []byte       // Stack of sub-slices, each 33 bytes each, containing hashes (or RLP encodings, if shorter than 32 bytes)
	nodeStack []node       // Stack of nodes
	value     *bytebufferpool.ByteBuffer
	sha       keccakState                  // Keccak primitive that can absorb data (Write), and get squeezed to the hash out (Read)
	leafFunc  func(b []byte) (node, error) // Function to be called on the leafs to construct valueNode or accoutNode
}

// NewHashBuilder creates a new HashBuilder
func NewHashBuilder(leafFunc func(b []byte) (node, error)) *HashBuilder {
	return &HashBuilder{
		sha:      sha3.NewLegacyKeccak256().(keccakState),
		leafFunc: leafFunc,
	}
}

// Reset makes the HashBuilder suitable for reuse
func (hb *HashBuilder) Reset() {
	hb.hexKey.Reset()
	hb.hashStack = hb.hashStack[:0]
	hb.nodeStack = hb.nodeStack[:0]
	pool.PutBuffer(hb.value)
	hb.value = nil
}

// key is original key (not transformed into hex or compacted)
func (hb *HashBuilder) setKeyValue(skip int, key []byte, value *bytebufferpool.ByteBuffer) {
	// Transform key into hex representation
	hb.hexKey.Reset()
	i := 0
	for _, b := range key {
		if i >= skip {
			hb.hexKey.WriteByte(b / 16)
		}
		i++
		if i >= skip {
			hb.hexKey.WriteByte(b % 16)
		}
		i++
	}
	hb.hexKey.WriteByte(16)
	pool.PutBuffer(hb.value)
	hb.value = value
}

func (hb *HashBuilder) leaf(length int) {
	//fmt.Printf("LEAF %d\n", length)
	hex := hb.hexKey.Bytes()
	key := hex[len(hex)-length:]
	val, err := hb.leafFunc(hb.value.B)
	if err != nil {
		panic(err)
	}
	s := &shortNode{Key: common.CopyBytes(key), Val: val}
	hb.nodeStack = append(hb.nodeStack, s)
	hb.leafHash(length)
}

func (hb *HashBuilder) leafHash(length int) {
	//fmt.Printf("LEAFHASH %d\n", length)
	var hash [33]byte // RLP representation of hash (or un-hashes value)
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var lenPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	hex := hb.hexKey.Bytes()
	key := hex[len(hex)-length:]
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 16 + key[0] // Odd (1<<4) + first nibble
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
	val := hb.value.B
	if len(val) > 1 || val[0] >= 128 {
		vp = generateByteArrayLen(valPrefix[:], 0, len(val))
		vl = len(val)
	} else {
		vl = 1
	}
	totalLen := kp + kl + vp + vl
	pt := generateStructLen(lenPrefix[:], totalLen)
	if pt+totalLen < 32 {
		// Embedded node
		pos := 0
		copy(hash[pos:], lenPrefix[:pt])
		pos += pt
		copy(hash[pos:], keyPrefix[:kp])
		pos += kp
		hash[pos] = compact0
		pos++
		for i := 1; i < compactLen; i++ {
			hash[pos] = key[ni]*16 + key[ni+1]
			pos++
			ni += 2
		}
		copy(hash[pos:], valPrefix[:vp])
		pos += vp
		copy(hash[pos:], val)
	} else {
		hb.sha.Reset()
		if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
			panic(err)
		}
		if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
			panic(err)
		}
		var b [1]byte
		b[0] = compact0
		if _, err := hb.sha.Write(b[:]); err != nil {
			panic(err)
		}
		for i := 1; i < compactLen; i++ {
			b[0] = key[ni]*16 + key[ni+1]
			if _, err := hb.sha.Write(b[:]); err != nil {
				panic(err)
			}
			ni += 2
		}
		if _, err := hb.sha.Write(valPrefix[:vp]); err != nil {
			panic(err)
		}
		if _, err := hb.sha.Write(val); err != nil {
			panic(err)
		}
		hash[0] = byte(128 + 32)
		if _, err := hb.sha.Read(hash[1:]); err != nil {
			panic(err)
		}
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	if len(hb.hashStack) > 33*len(hb.nodeStack) {
		hb.nodeStack = append(hb.nodeStack, nil)
	}
}

func (hb *HashBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	nd := hb.nodeStack[len(hb.nodeStack)-1]
	switch n := nd.(type) {
	case nil:
		branchHash := common.CopyBytes(hb.hashStack[len(hb.hashStack)-32:])
		hb.nodeStack[len(hb.nodeStack)-1] = &shortNode{Key: common.CopyBytes(key), Val: hashNode(branchHash)}
	case *fullNode:
		hb.nodeStack[len(hb.nodeStack)-1] = &shortNode{Key: common.CopyBytes(key), Val: n}
	default:
		panic(fmt.Errorf("wrong Val type for an extension: %T", nd))
	}
	hb.extensionHash(key)
}

func (hb *HashBuilder) extensionHash(key []byte) {
	//fmt.Printf("EXTENSIONHASH %x\n", key)
	branchHash := hb.hashStack[len(hb.hashStack)-33:]
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var lenPrefix [4]byte
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 16 + key[0] // Odd (1<<4) + first nibble
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
	totalLen := kp + kl + 33
	pt := generateStructLen(lenPrefix[:], totalLen)
	hb.sha.Reset()
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		panic(err)
	}
	if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
		panic(err)
	}
	var b [1]byte
	b[0] = compact0
	if _, err := hb.sha.Write(b[:]); err != nil {
		panic(err)
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := hb.sha.Write(b[:]); err != nil {
			panic(err)
		}
		ni += 2
	}
	if _, err := hb.sha.Write(branchHash); err != nil {
		panic(err)
	}
	// Replace previous hash with the new one
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-32:]); err != nil {
		panic(err)
	}
	if _, ok := hb.nodeStack[len(hb.nodeStack)-1].(*fullNode); ok {
		panic("extensionHash cannot be emitted when a node is on top of the stack")
	}
}

func (hb *HashBuilder) branch(set uint32) {
	//fmt.Printf("BRANCH %b\n", set)
	f := &fullNode{}
	digits := bits.OnesCount32(set)
	nodes := hb.nodeStack[len(hb.nodeStack)-digits:]
	hashes := hb.hashStack[len(hb.hashStack)-33*digits:]
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint32(1) << digit) & set) != 0 {
			if nodes[i] == nil {
				f.Children[digit] = hashNode(common.CopyBytes(hashes[33*i+1 : 33*i+33]))
			} else {
				f.Children[digit] = nodes[i]
			}
			i++
		}
	}
	hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
	hb.nodeStack[len(hb.nodeStack)-1] = f
	hb.branchHash(set)
	copy(f.flags.hash[:], hb.hashStack[len(hb.hashStack)-32:])

}

func (hb *HashBuilder) branchHash(set uint32) {
	//fmt.Printf("BRANCHHASH %b\n", set)
	digits := bits.OnesCount32(set)
	hashes := hb.hashStack[len(hb.hashStack)-33*digits:]
	// Calculate the size of the resulting RLP
	totalSize := 17 // These are 17 length prefixes
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint32(1) << digit) & set) != 0 {
			if hashes[33*i] == byte(128+32) {
				totalSize += 32
			} else {
				// Embedded node
				totalSize += int(hashes[33*i]) - 192
			}
			i++
		}
	}
	hb.sha.Reset()
	var lenPrefix [4]byte
	pt := generateStructLen(lenPrefix[:], totalSize)
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		panic(err)
	}
	// Output children hashes or embedded RLPs
	i = 0
	var b [1]byte
	b[0] = 128
	for digit := uint(0); digit < 17; digit++ {
		if ((uint32(1) << digit) & set) != 0 {
			if hashes[33*i] == byte(128+32) {
				if _, err := hb.sha.Write(hashes[33*i : 33*i+33]); err != nil {
					panic(err)
				}
			} else {
				// Embedded node
				size := int(hashes[33*i]) - 192
				if _, err := hb.sha.Write(hashes[33*i : 33*i+size+1]); err != nil {
					panic(err)
				}
			}
			i++
		} else {
			if _, err := hb.sha.Write(b[:]); err != nil {
				panic(err)
			}
		}
	}
	hb.hashStack = hb.hashStack[:len(hb.hashStack)-33*digits+33]
	hb.hashStack[len(hb.hashStack)-33] = 128 + 32
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-32:]); err != nil {
		panic(err)
	}
	if 33*len(hb.nodeStack) > len(hb.hashStack) {
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
		hb.nodeStack[len(hb.nodeStack)-1] = nil
	}
}

func (hb *HashBuilder) hash(number int) {
	panic("not implemented")
}

func (hb *HashBuilder) rootHash() common.Hash {
	var hash common.Hash
	copy(hash[:], hb.hashStack[1:33])
	return hash
}

func (hb *HashBuilder) root() node {
	return hb.nodeStack[0]
}

func (hb *HashBuilder) hasRoot() bool {
	return len(hb.nodeStack) > 0
}
