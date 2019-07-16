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
	"sort"
)

// Experimental code for separating data and structural information

type emitter interface {
	branch(digit int)
	hasher(digit int)
	leaf(length int)
	extension(key []byte)
	add(digit int)
	hash()
}

// prefixGroups is an optimised (for allocations) storage for the mapping of prefix groups to digit sets
type prefixGroups struct {
	data    []byte   // Data making up all the prefix groups
	offsets []int    // Offsets of each prefix group in the data
	digits  []uint32 // Digit sets corresponding to the prefix group
}

// lastMatches return true if given prefix equals to the last prefix group stored
func (pg *prefixGroups) lastMatches(prefix []byte) bool {
	if len(pg.offsets) == 0 {
		return false
	}
	return bytes.Equal(pg.data[pg.offsets[len(pg.offsets)-1]:], prefix)
}

// newGroup creates a new group and places it as the last
func (pg *prefixGroups) newGroup(prefix []byte, digit byte) {
	pg.offsets = append(pg.offsets, len(pg.data))
	pg.data = append(pg.data, prefix...)
	pg.digits = append(pg.digits, uint32(1)<<digit)
}

// addToLast adds a new digit to the last group
func (pg *prefixGroups) addToLast(digit byte) {
	pg.digits[len(pg.digits)-1] |= (uint32(1) << digit)
}

// deleteLast remove the last group
func (pg *prefixGroups) deleteLast() {
	pg.data = pg.data[:pg.offsets[len(pg.offsets)-1]]
	pg.offsets = pg.offsets[:len(pg.offsets)-1]
	pg.digits = pg.digits[:len(pg.digits)-1]
}

func (pg *prefixGroups) lastLen() int {
	if len(pg.offsets) == 0 {
		return 0
	}
	return len(pg.data) - pg.offsets[len(pg.offsets)-1]
}

func step(hashOnly func(prefix []byte) bool, recursive bool, prec, curr, succ []byte, e emitter, groups *prefixGroups) {
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
	maxCommonPrefix := make([]byte, maxLen)
	copy(maxCommonPrefix, curr)
	// Look up the prefix groups's digit set
	existed := groups.lastMatches(curr[:maxLen])
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	var remainderLen int
	if recursive || len(succ) > 0 || len(prec) > 0 {
		if existed {
			groups.addToLast(extraDigit)
		} else {
			groups.newGroup(curr[:maxLen], extraDigit)
		}
		remainderLen = len(curr) - maxLen - 1
	} else {
		remainderLen = len(curr)
	}
	// Emit LEAF or EXTENSION based on the remainder
	if recursive {
		if remainderLen > 0 {
			ext := make([]byte, remainderLen)
			copy(ext, curr[maxLen+1:])
			e.extension(ext)
		}
	} else {
		e.leaf(remainderLen)
	}
	// Emit BRANCH or HASHER, or ADD
	if existed {
		e.add(int(extraDigit))
	} else if recursive || len(succ) > 0 || len(prec) > 0 {
		if hashOnly(maxCommonPrefix) {
			e.hasher(int(extraDigit))
		} else {
			e.branch(int(extraDigit))
		}
	}
	// Check for the optional part
	if precLen <= succLen {
		return
	}
	// Close the immediately encompassing prefix group
	closing := curr[:precLen]
	if precLen > succLen {
		groups.deleteLast()
	}
	// Check the end of recursion
	if precLen == 0 {
		return
	}
	// Identify preceeding key for the recursive invocation
	p := groups.lastLen()
	// Recursion
	step(hashOnly, true, curr[:p], closing, succ, e, groups)
}

type sortable [][]byte

func (s sortable) Len() int {
	return len(s)
}
func (s sortable) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}
func (s sortable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// ResolveSet encapsulates the set of keys that are required to be fully available, or resolved
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
type ResolveSet struct {
	hexes    sortable
	inited   bool // Whether keys are sorted and "LTE" and "GT" indices set
	lteIndex int  // Index of the "LTE" key in the keys slice. Next one is "GT"
}

// AddKey adds a new key to the set
func (rs *ResolveSet) AddKey(key []byte) {
	rs.hexes = append(rs.hexes, keybytesToHex(key))
}

func (rs *ResolveSet) ensureInited() {
	if rs.inited {
		return
	}
	sort.Sort(rs.hexes)
	rs.lteIndex = 0
	rs.inited = true
}

// HashOnly decides whether to emit `HASHER` or `BRANCH` for a given prefix, by
// checking if this is prefix of any of the keys added to the set
// Since keys in the set are sorted, and we expect that the prefixes will
// come in monotonically ascending order, we optimise for this, though
// the function would still work if the order is different
func (rs *ResolveSet) HashOnly(prefix []byte) bool {
	rs.ensureInited()
	// Adjust "GT" if necessary
	var gtAdjusted bool
	for rs.lteIndex < len(rs.hexes)-1 && bytes.Compare(rs.hexes[rs.lteIndex+1], prefix) <= 0 {
		rs.lteIndex++
		gtAdjusted = true
	}
	// Adjust "LTE" if necessary (normally will not be necessary)
	for !gtAdjusted && rs.lteIndex > 0 && bytes.Compare(rs.hexes[rs.lteIndex], prefix) > 0 {
		rs.lteIndex--
	}
	if rs.lteIndex < len(rs.hexes) && bytes.HasPrefix(rs.hexes[rs.lteIndex], prefix) {
		return false
	}
	if rs.lteIndex < len(rs.hexes)-1 && bytes.HasPrefix(rs.hexes[rs.lteIndex+1], prefix) {
		return false
	}
	return true
}
