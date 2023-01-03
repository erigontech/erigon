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

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/turbo/rlphacks"
)

// Experimental code for separating data and structural information
// Each function corresponds to an opcode
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type structInfoReceiver interface {
	leaf(length int, keyHex []byte, val rlphacks.RlpSerializable) error
	leafHash(length int, keyHex []byte, val rlphacks.RlpSerializable) error
	accountLeaf(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldset uint32, codeSize int) error
	accountLeafHash(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldset uint32) error
	extension(key []byte) error
	extensionHash(key []byte) error
	branch(set uint16) error
	branchHash(set uint16) error
	hash(hash []byte) error
	topHash() []byte
	topHashes(prefix []byte, branches, children uint16) []byte
	printTopHashes(prefix []byte, branches, children uint16)
}

// hashCollector gets called whenever there might be a need to create intermediate hash record
type HashCollector func(keyHex []byte, hash []byte) error
type StorageHashCollector func(accWithInc []byte, keyHex []byte, hash []byte) error

type HashCollector2 func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error
type StorageHashCollector2 func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error

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
	HasTree bool
}

func (GenStructStepHashData) GenStructStepData() {}

// GenStructStep is one step of the algorithm that generates the structural information based on the sequence of keys.
// `retain` parameter is the function that, called for a certain prefix, determines whether the trie node for that prefix needs to be
// compressed into just hash (if `false` is returned), or constructed (if `true` is returned). Usually the `retain` function is
// implemented in such a way to guarantee that certain keys are always accessible in the resulting trie (see RetainList.Retain function).
// `buildExtensions` is set to true if the algorithm's step is invoked recursively, i.e. not after a freshly provided leaf or hash
// `curr`, `succ` are two full keys or prefixes that are currently visible to the algorithm. By comparing these, the algorithm
// makes decisions about the local structure, i.e. the presence of the prefix groups.
// `e` parameter is the trie builder, which uses the structure information to assemble trie on the stack and compute its hash.
// `h` parameter is the hash collector, which is notified whenever branch node is constructed.
// `data` parameter specified if a hash or a binary string or an account should be emitted.
// `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. Meaning - which children of given prefix have dbutils.HashedAccount records
// `hasTree` same as `groups`, but meaning - which children of given prefix have dbutils.TrieOfAccountsBucket record
// `hasHash` same as `groups`, but meaning - which children of given prefix are branch nodes and their hashes can be saved and used on next trie resolution.
// Whenever a `BRANCH` or `BRANCHHASH` opcode is emitted, the set of digits is taken from the corresponding `groups` item, which is
// then removed from the slice. This signifies the usage of the number of the stack items by the `BRANCH` or `BRANCHHASH` opcode.
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
func GenStructStep(
	retain func(prefix []byte) bool,
	curr, succ []byte,
	e structInfoReceiver,
	h HashCollector2,
	data GenStructStepData,
	groups []uint16,
	hasTree []uint16,
	hasHash []uint16,
	trace bool,
) ([]uint16, []uint16, []uint16, error) {
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
		if trace || maxLen >= len(curr) {
			fmt.Printf("curr: %x, succ: %x, maxLen %d, groups: %b, precLen: %d, succLen: %d, buildExtensions: %t\n", curr, succ, maxLen, groups, precLen, succLen, buildExtensions)
		}

		// Add the digit immediately following the max common prefix and compute length of remainder length
		extraDigit := curr[maxLen]
		for maxLen >= len(groups) {
			groups = append(groups, 0)
		}
		groups[maxLen] |= 1 << extraDigit
		remainderStart := maxLen
		if len(succ) > 0 || precExists {
			remainderStart++
		}
		remainderLen := len(curr) - remainderStart
		for remainderStart+remainderLen >= len(hasTree) {
			hasTree = append(hasTree, 0)
			hasHash = append(hasHash, 0)
		}
		//fmt.Printf("groups is now %x,%d,%b\n", extraDigit, maxLen, groups)

		if !buildExtensions {
			switch v := data.(type) {
			case *GenStructStepHashData:
				if trace {
					fmt.Printf("HashData before: %x, %t,%b,%b,%b\n", curr, v.HasTree, hasHash, hasTree, groups)
				}
				if v.HasTree {
					hasTree[len(curr)-1] |= 1 << curr[len(curr)-1] // keep track of existing records in DB
				}
				hasHash[len(curr)-1] |= 1 << curr[len(curr)-1] // register myself in parent bitmap
				if trace {
					fmt.Printf("HashData: %x, %t,%b,%b,%b\n", curr, v.HasTree, hasHash, hasTree, groups)
				}
				/* building a hash */
				if err := e.hash(v.Hash[:]); err != nil {
					return nil, nil, nil, err
				}
				buildExtensions = true
			case *GenStructStepAccountData:
				if retain(curr[:maxLen]) {
					if err := e.accountLeaf(remainderLen, curr, &v.Balance, v.Nonce, v.Incarnation, v.FieldSet, codeSizeUncached); err != nil {
						return nil, nil, nil, err
					}
				} else {
					if err := e.accountLeafHash(remainderLen, curr, &v.Balance, v.Nonce, v.Incarnation, v.FieldSet); err != nil {
						return nil, nil, nil, err
					}
				}
			case *GenStructStepLeafData:
				/* building leafs */
				if retain(curr[:maxLen]) {
					if err := e.leaf(remainderLen, curr, v.Value); err != nil {
						return nil, nil, nil, err
					}
				} else {
					if err := e.leafHash(remainderLen, curr, v.Value); err != nil {
						return nil, nil, nil, err
					}
				}
			default:
				panic(fmt.Errorf("unknown data type: %T", data))
			}
		}

		if buildExtensions {
			if remainderLen > 0 {
				if trace {
					fmt.Printf("Extension before: %x->%x,%b, %b, %b\n", curr[:remainderStart], curr[remainderStart:remainderStart+remainderLen], hasHash, hasTree, groups)
				}
				// can't use hash of extension node
				// but must propagate hasBranch bits to keep tracking all existing DB records
				// groups bit also require propagation, but it's done automatically
				from := remainderStart
				if from == 0 {
					from = 1
				}
				hasHash[from-1] &^= 1 << curr[from-1]
				for i := from; i < remainderStart+remainderLen; i++ {
					if 1<<curr[i]&hasTree[i] != 0 {
						hasTree[from-1] |= 1 << curr[from-1]
					}
					if h != nil {
						if err := h(curr[:i], 0, 0, 0, nil, nil); err != nil {
							return nil, nil, nil, err
						}
					}
				}
				hasTree = hasTree[:from]
				hasHash = hasHash[:from]
				if trace {
					fmt.Printf("Extension: %x, %b, %b, %b\n", curr[remainderStart:remainderStart+remainderLen], hasHash, hasTree, groups)
				}
				/* building extensions */
				if retain(curr[:maxLen]) {
					if err := e.extension(curr[remainderStart : remainderStart+remainderLen]); err != nil {
						return nil, nil, nil, err
					}
				} else {
					if err := e.extensionHash(curr[remainderStart : remainderStart+remainderLen]); err != nil {
						return nil, nil, nil, err
					}
				}
			}
		}

		// Check for the optional part
		if precLen <= succLen && len(succ) > 0 {
			return groups, hasTree, hasHash, nil
		}

		var usefulHashes []byte

		if h != nil && (hasHash[maxLen] != 0 || hasTree[maxLen] != 0) { // top level must be in db
			if trace {
				fmt.Printf("why now: %x,%b,%b,%b\n", curr[:maxLen], hasHash, hasTree, groups)
			}
			usefulHashes = e.topHashes(curr[:maxLen], hasHash[maxLen], groups[maxLen])
			if maxLen != 0 {
				hasTree[maxLen-1] |= 1 << curr[maxLen-1] // register myself in parent bitmap
			}
			if maxLen > 1 {
				if err := h(curr[:maxLen], groups[maxLen], hasTree[maxLen], hasHash[maxLen], usefulHashes, nil); err != nil {
					return nil, nil, nil, err
				}
			}
		}

		// Close the immediately encompassing prefix group, if needed
		if len(succ) > 0 || precExists {
			if maxLen > 0 {
				if trace {
					fmt.Printf("Branch before: %x, %b, %b, %b\n", curr[:maxLen], hasHash, hasTree, groups)
				}
				hasHash[maxLen-1] |= 1 << curr[maxLen-1]
				if hasTree[maxLen] != 0 {
					hasTree[maxLen-1] |= 1 << curr[maxLen-1]
				}
				if trace {
					fmt.Printf("Branch: %x, %b, %b, %b\n", curr[:maxLen], hasHash, hasTree, groups)
				}
			}

			if trace {
				e.printTopHashes(curr[:maxLen], 0, groups[maxLen])
			}
			if retain(curr[:maxLen]) {
				if err := e.branch(groups[maxLen]); err != nil {
					return nil, nil, nil, err
				}
			} else {
				if err := e.branchHash(groups[maxLen]); err != nil {
					return nil, nil, nil, err
				}
			}
		}
		if h != nil {
			send := maxLen == 0 && (hasTree[maxLen] != 0 || hasHash[maxLen] != 0) // account.root - store only if have useful info
			send = send || (maxLen == 1 && groups[maxLen] != 0)                   // first level of trie_account - store in any case
			if send {
				if err := h(curr[:maxLen], groups[maxLen], hasTree[maxLen], hasHash[maxLen], usefulHashes, e.topHash()[1:]); err != nil {
					return nil, nil, nil, err
				}
			}
		}
		groups = groups[:maxLen]
		hasTree = hasTree[:maxLen]
		hasHash = hasHash[:maxLen]
		// Check the end of recursion
		if precLen == 0 {
			return groups, hasTree, hasHash, nil
		}
		// Identify preceding key for the buildExtensions invocation

		curr = curr[:precLen]
		for len(groups) > 0 && groups[len(groups)-1] == 0 {
			groups = groups[:len(groups)-1]
		}
	}
	return nil, nil, nil, nil
}

func GenStructStepOld(
	retain func(prefix []byte) bool,
	curr, succ []byte,
	e structInfoReceiver,
	h HashCollector,
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
		if trace || maxLen >= len(curr) {
			fmt.Printf("curr: %x, succ: %x, maxLen %d, groups: %b, precLen: %d, succLen: %d, buildExtensions: %t\n", curr, succ, maxLen, groups, precLen, succLen, buildExtensions)
		}
		// Add the digit immediately following the max common prefix and compute length of remainder length
		extraDigit := curr[maxLen]
		for maxLen >= len(groups) {
			groups = append(groups, 0)
		}
		groups[maxLen] |= 1 << extraDigit
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
				if err := e.hash(v.Hash[:]); err != nil {
					return nil, err
				}
				buildExtensions = true
			case *GenStructStepAccountData:
				if retain(curr[:maxLen]) {
					if err := e.accountLeaf(remainderLen, curr, &v.Balance, v.Nonce, v.Incarnation, v.FieldSet, codeSizeUncached); err != nil {
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
			if h != nil {
				if err := h(curr[:maxLen], e.topHash()[1:]); err != nil {
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
