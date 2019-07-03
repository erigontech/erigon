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

type emitter interface {
	branch(digit int)
	hasher(digit int)
	leaf(length int)
	extension(key []byte)
	add(digit int)
	hash()
}

func step(hashOnly bool, recursive bool, prec, curr, succ []byte, e emitter, groups map[string]uint32) {
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
	set, existed := groups[string(maxCommonPrefix)]
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	var remainderLen int
	if recursive || len(succ) > 0 || len(prec) > 0 {
		set |= (uint32(1) << extraDigit)
		remainderLen = len(curr) - maxLen - 1
		groups[string(maxCommonPrefix)] = set
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
		if hashOnly {
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
		delete(groups, string(closing))
	}
	// Check the end of recursion
	if precLen == 0 {
		return
	}
	// Identify preceeding key for the recursive invocation
	p := precLen - 1
	_, found := groups[string(curr[:p])]
	for ; p > 0 && !found; _, found = groups[string(curr[:p])] {
		p--
	}
	// Recursion
	step(hashOnly, true, curr[:p], closing, succ, e, groups)
}
