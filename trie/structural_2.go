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

import "bytes"

// Experimental code for separating data and structural information (variant 2)
type emitter2 interface {
	leaf(length int)
	extension(key []byte)
	endbranch(set uint32)
	endhasher(set uint32)
	hash()
}

func step2(
	hashOnly func(prefix []byte) bool,
	recursive bool,
	prec, curr, succ []byte,
	e emitter2,
	prefix []byte, groups []uint32,
) (newPrefix []byte, newGroups []uint32) {
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
	//fmt.Printf("prec: %x, curr: %x, succ: %x, maxLen %d\n", prec, curr, succ, maxLen)
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	for maxLen >= len(groups) {
		groups = append(groups, 0)
		prefix = append(prefix, curr[len(prefix)])
	}
	groups[maxLen] |= (uint32(1) << extraDigit)
	remainderStart := maxLen
	if len(succ) > 0 || prec != nil {
		remainderStart++
	}
	remainderLen := len(curr) - remainderStart
	// Emit LEAF or EXTENSION based on the remainder
	if recursive {
		if remainderLen > 0 {
			e.extension(curr[remainderStart : remainderStart+remainderLen])
		}
	} else {
		e.leaf(remainderLen)
	}
	// Check for the optional part
	if precLen <= succLen {
		return nil, groups
	}
	// Close the immediately encompassing prefix group, if needed
	if len(succ) > 0 || prec != nil {
		if hashOnly(curr[:maxLen]) {
			e.endhasher(groups[maxLen])
		} else {
			e.endbranch(groups[maxLen])
		}
	}
	groups = groups[:maxLen]
	// Check the end of recursion
	if precLen == 0 {
		return prefix, groups
	}
	// Identify preceeding key for the recursive invocation
	newCurr := curr[:precLen]
	var newPrec []byte
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
		prefix = prefix[:len(prefix)-1]
		newPrec = prefix
	}
	// Recursion
	newPrefix, newGroups = step2(hashOnly, true, newPrec, newCurr, succ, e, prefix, groups)
	return
}

// HashBuilder impements the interface `emitter` and opcodes that the structural information of the trie
// is comprised of
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type HashBuilder2 struct {
	hexKey      bytes.Buffer // Next key-value pair to consume
	bufferStack []*bytes.Buffer
	branchStack []*fullNode
	topValue    []byte
}

func NewHashBuilder2() *HashBuilder2 {
	return &HashBuilder2{}
}

func (hb *HashBuilder2) Reset() {
	hb.hexKey.Reset()
	hb.bufferStack = hb.bufferStack[:0]
	hb.branchStack = hb.branchStack[:0]
	hb.topValue = nil
}

// key is original key (not transformed into hex or compacted)
func (hb *HashBuilder2) setKeyValue(skip int, key, value []byte) {
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
	hb.topValue = value
}
