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
// Each function corresponds to an opcode
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type structInfoReceiver interface {
	leaf(length int, keyHex []byte) error
	leafHash(length int, keyHex []byte) error
	accountLeaf(length int, keyHex []byte, fieldset uint32) error
	accountLeafHash(length int, keyHex []byte, fieldset uint32) error
	extension(key []byte) error
	extensionHash(key []byte) error
	branch(set uint16) error
	branchHash(set uint16) error
	hash(number int) error
}

// GenStructStep is one step of the algorithm that generates the structural information based on the sequence of keys.
// `fieldSet` parameter specifies whether the generated leaf should be a binary string (fieldSet==0), or
// an account (in that case the opcodes `ACCOUNTLEAF`/`ACCOUNTLEAFHASH` are emitted instead of `LEAF`/`LEAFHASH`).
// `hashOnly` parameter is the function that, called for a certain prefix, determines whether the trie node for that prefix needs to be
// compressed into just hash (if `true` is returned), or constructed (if `false` is returned). Usually the `hashOnly` function is
// implemented in such a way to guarantee that certain keys are always accessible in the resulting trie (see ResolveSet.HashOnly function).
// `isHashNode` parameter is set to true if `curr` key corresponds not to a leaf but to a hash node (which is "folded" respresentation
// of a branch node).
// `recursive` is set to true if the algorithm's step is invoked recursively, i.e. not after a freshly provided leaf or hash
// `curr`, `succ` are two full keys or prefixes that are currently visible to the algorithm. By comparing these, the algorithm
// makes decisions about the local structure, i.e. the presense of the prefix groups.
// `e` parameter is the trie builder, which uses the structure information to assemble trie on the stack and compute its hash.
// `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack.
// Whenever a `BRANCH` or `BRANCHHASH` opcode is emitted, the set of digits is taken from the corresponding `groups` item, which is
// then removed from the slice. This signifies the usage of the number of the stack items by the `BRANCH` or `BRANCHHASH` opcode.
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
func GenStructStep(
	fieldSet uint32,
	hashOnly func(prefix []byte) bool,
	isHashOfNode bool,
	recursive bool,
	curr, succ []byte,
	e structInfoReceiver,
	groups []uint16,
) ([]uint16, error) {
	var precExists = len(groups) > 0
	// Calculate the prefix of the smallest prefix group containing curr
	var precLen int
	if len(groups) > 0 {
		precLen = len(groups) - 1
	}
	succLen := prefixLen(succ, curr)
	var maxLen int
	if precLen > succLen {
		maxLen = precLen
	} else {
		maxLen = succLen
	}
	//fmt.Printf("curr: %x, succ: %x, isHashOfNode: %t, maxLen %d, groups: %b, precLen: %d, succLen: %d\n", curr, succ, isHashOfNode, maxLen, groups, precLen, succLen)
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	for maxLen >= len(groups) {
		groups = append(groups, 0)
	}
	groups[maxLen] |= (uint16(1) << extraDigit)
	//fmt.Printf("groups is now %b\n", groups)
	remainderStart := maxLen
	if len(succ) > 0 || precExists {
		remainderStart++
	}
	remainderLen := len(curr) - remainderStart
	if isHashOfNode {
		if err := e.hash(1); err != nil {
			return nil, err
		}
		if remainderLen > 0 {
			if hashOnly(curr[:maxLen]) {
				if err := e.extensionHash(curr[remainderStart : remainderStart+remainderLen]); err != nil {
					return nil, err
				}
			} else {
				if err := e.extension(curr[remainderStart : remainderStart+remainderLen]); err != nil {
					return nil, err
				}
			}
		}
	} else {
		// Emit LEAF or EXTENSION based on the remainder
		if recursive {
			if remainderLen > 0 {
				if hashOnly(curr[:maxLen]) {
					if err := e.extensionHash(curr[remainderStart : remainderStart+remainderLen]); err != nil {
						return nil, err
					}
				} else {
					if err := e.extension(curr[remainderStart : remainderStart+remainderLen]); err != nil {
						return nil, err
					}
				}
			}
		} else {
			if hashOnly(curr[:maxLen]) {
				if fieldSet == 0 {
					if err := e.leafHash(remainderLen); err != nil {
						return nil, err
					}
				} else {
					if err := e.accountLeafHash(remainderLen, fieldSet); err != nil {
						return nil, err
					}
				}
			} else {
				if fieldSet == 0 {
					if err := e.leaf(remainderLen); err != nil {
						return nil, err
					}
				} else {
					if err := e.accountLeaf(remainderLen, fieldSet); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	// Check for the optional part
	if precLen <= succLen && len(succ) > 0 {
		return groups, nil
	}
	// Close the immediately encompassing prefix group, if needed
	if len(succ) > 0 || precExists {
		if hashOnly(curr[:maxLen]) {
			if err := e.branchHash(groups[maxLen]); err != nil {
				return nil, err
			}
		} else {
			if err := e.branch(groups[maxLen]); err != nil {
				return nil, err
			}
		}
	}
	groups = groups[:maxLen]
	// Check the end of recursion
	if precLen == 0 {
		return groups, nil
	}
	// Identify preceding key for the recursive invocation
	newCurr := curr[:precLen]
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
	}

	// Recursion
	return GenStructStep(fieldSet, hashOnly, false, true, newCurr, succ, e, groups)
}
