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
	"fmt"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

// Experimental code for separating data and structural information
// Each function corresponds to an opcode
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type structInfoReceiver interface {
	leaf(length int, keyHex []byte, val rlphacks.RlpSerializable) error
	leafHash(length int, keyHex []byte, val rlphacks.RlpSerializable) error
	accountLeaf(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldset uint32) error
	accountLeafHash(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldset uint32) error
	extension(key []byte) error
	extensionHash(key []byte) error
	branch(set uint16) error
	branchHash(set uint16) error
	hash(hash []byte, dataLen uint64) error
}

func calcPrecLen(groups []uint16) int {
	if len(groups) == 0 {
		return 0
	}
	return len(groups) - 1
}

type GenStructStepData interface {
	GenStructStepData()
}

type GenStructStepAccountData struct {
	FieldSet    uint32
	Balance     uint256.Int
	Nonce       uint64
	Incarnation uint64
}

func (GenStructStepAccountData) GenStructStepData() {}

type GenStructStepLeafData struct {
	Value rlphacks.RlpSerializable
}

func (GenStructStepLeafData) GenStructStepData() {}

type GenStructStepHashData struct {
	Hash    common.Hash
	DataLen uint64
}

func (GenStructStepHashData) GenStructStepData() {}

// GenStructStep is one step of the algorithm that generates the structural information based on the sequence of keys.
// `retain` parameter is the function that, called for a certain prefix, determines whether the trie node for that prefix needs to be
// compressed into just hash (if `false` is returned), or constructed (if `true` is returned). Usually the `retain` function is
// implemented in such a way to guarantee that certain keys are always accessible in the resulting trie (see RetainList.Retain function).
// `buildExtensions` is set to true if the algorithm's step is invoked recursively, i.e. not after a freshly provided leaf or hash
// `curr`, `succ` are two full keys or prefixes that are currently visible to the algorithm. By comparing these, the algorithm
// makes decisions about the local structure, i.e. the presense of the prefix groups.
// `e` parameter is the trie builder, which uses the structure information to assemble trie on the stack and compute its hash.
// `data` parameter specified if a hash or a binary string or an account should be emitted.
// `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack.
// Whenever a `BRANCH` or `BRANCHHASH` opcode is emitted, the set of digits is taken from the corresponding `groups` item, which is
// then removed from the slice. This signifies the usage of the number of the stack items by the `BRANCH` or `BRANCHHASH` opcode.
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
func GenStructStep(
	retain func(prefix []byte) bool,
	curr, succ []byte,
	e structInfoReceiver,
	data GenStructStepData,
	groups []uint16,
	trace bool,
) ([]uint16, error) {
	for precLen, buildExtensions := calcPrecLen(groups), false; precLen >= 0; precLen, buildExtensions = calcPrecLen(groups), true {
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
		if trace {
			fmt.Printf("curr: %x, succ: %x, maxLen %d, groups: %b, precLen: %d, succLen: %d, buildExtensions: %t\n", curr, succ, maxLen, groups, precLen, succLen, buildExtensions)
		}
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

		if !buildExtensions {
			switch v := data.(type) {
			case *GenStructStepHashData:
				/* building a hash */
				if err := e.hash(v.Hash[:], v.DataLen); err != nil {
					return nil, err
				}
				buildExtensions = true
			case *GenStructStepAccountData:
				if retain(curr[:maxLen]) {
					if err := e.accountLeaf(remainderLen, curr, &v.Balance, v.Nonce, v.Incarnation, v.FieldSet); err != nil {
						return nil, err
					}
				} else {
					if err := e.accountLeafHash(remainderLen, curr, &v.Balance, v.Nonce, v.Incarnation, v.FieldSet); err != nil {
						return nil, err
					}
				}
			case *GenStructStepLeafData:
				/* building leafs */
				if retain(curr[:maxLen]) {
					if err := e.leaf(remainderLen, curr, v.Value); err != nil {
						return nil, err
					}
				} else {
					if err := e.leafHash(remainderLen, curr, v.Value); err != nil {
						return nil, err
					}
				}
			default:
				panic(fmt.Errorf("unknown data type: %T", data))
			}
		}

		if buildExtensions {
			if remainderLen > 0 {
				if trace {
					fmt.Printf("Extension %x\n", curr[remainderStart:remainderStart+remainderLen])
				}
				/* building extensions */
				if retain(curr[:maxLen]) {
					if err := e.extension(curr[remainderStart : remainderStart+remainderLen]); err != nil {
						return nil, err
					}
				} else {
					if err := e.extensionHash(curr[remainderStart : remainderStart+remainderLen]); err != nil {
						return nil, err
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
			if retain(curr[:maxLen]) {
				if err := e.branch(groups[maxLen]); err != nil {
					return nil, err
				}
			} else {
				if err := e.branchHash(groups[maxLen]); err != nil {
					return nil, err
				}
			}
		}
		groups = groups[:maxLen]
		// Check the end of recursion
		if precLen == 0 {
			return groups, nil
		}
		// Identify preceding key for the buildExtensions invocation
		curr = curr[:precLen]
		for len(groups) > 0 && groups[len(groups)-1] == 0 {
			groups = groups[:len(groups)-1]
		}
	}
	return nil, nil

}
