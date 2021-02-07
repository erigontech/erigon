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

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
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

type HashCollector2 func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error
type StorageHashCollector2 func(accWithInc []byte, keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error

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
	Hash     common.Hash
	IsBranch bool
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
// `h` parameter is the hash collector, which is notified whenever branch node is constructed.
// `data` parameter specified if a hash or a binary string or an account should be emitted.
// `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack.
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
	hasBranch []uint16,
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
		groups[maxLen] |= (uint16(1) << extraDigit)
		remainderStart := maxLen
		if len(succ) > 0 || precExists {
			remainderStart++
		}
		for remainderStart >= len(hasBranch) {
			hasBranch = append(hasBranch, 0)
			hasHash = append(hasHash, 0)
		}
		remainderLen := len(curr) - remainderStart
		//fmt.Printf("groups is now %x,%b\n", extraDigit, groups)

		if !buildExtensions {
			switch v := data.(type) {
			case *GenStructStepHashData:
				//hasBranch[maxLen] |= (uint16(1) << curr[maxLen])
				hasHash[maxLen] |= (uint16(1) << curr[maxLen])
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
				if bytes.HasPrefix(curr[:maxLen], common.FromHex("04090709")) {
					fmt.Printf("ext: %x->%x\n", curr[:remainderStart], curr[remainderStart:remainderStart+remainderLen])
					if len(hasBranch) > 79 {
						fmt.Printf("ext before: %b,%d,%d\n", hasBranch[80:], remainderStart+remainderLen, maxLen)
					} else {
						fmt.Printf("ext before: %b,%d,%d\n", hasBranch, remainderStart+remainderLen, maxLen)
						fmt.Printf("ext decide: %x,%b\n", curr[remainderStart+remainderLen-1], hasBranch[remainderStart+remainderLen-1])
					}
				}
				if remainderStart > 0 {
					if (uint16(1)<<curr[remainderStart+remainderLen-1])&hasBranch[remainderStart+remainderLen-1] != 0 {
						hasBranch[remainderStart-1] |= (uint16(1) << curr[remainderStart-1])
					}
					for i := remainderStart; i < len(hasBranch); i++ {
						hasBranch[i] = 0
						hasHash[i] = 0
					}
				}
				if bytes.HasPrefix(curr[:maxLen], common.FromHex("04090709")) {
					if len(hasBranch) > 79 {
						fmt.Printf("ext after: %b\n", hasBranch[80:])
					} else {
						fmt.Printf("ext after: %b,%b,%d\n", hasBranch, groups, precLen)
					}
				}
				if trace {
					fmt.Printf("Extension %x\n", curr[remainderStart:remainderStart+remainderLen])
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
			return groups, hasBranch, hasHash, nil
		}

		var usefulHashes []byte
		// Close the immediately encompassing prefix group, if needed
		if len(succ) > 0 || precExists {
			if maxLen > 0 {
				hasHash[maxLen-1] |= (uint16(1) << curr[maxLen-1])
				if hasBranch[maxLen] != 0 {
					hasBranch[maxLen-1] |= (uint16(1) << curr[maxLen-1])
				}

				//hasBranch[succLen] |= (uint16(1) << curr[succLen])
				if bytes.HasPrefix(curr[:maxLen], common.FromHex("04090709")) {
					fmt.Printf("aa:%d,%d,%d", maxLen, succLen, precLen)
					if maxLen >= 79 {
						fmt.Printf("set bit %x, %x, %b, %b\n", curr[:maxLen-1], curr[maxLen-1], hasBranch[maxLen-1], hasBranch[80:])
					} else {
						fmt.Printf("set bit %x, %x, %b, %b\n", curr[:maxLen-1], curr[maxLen-1], hasBranch[maxLen-1], hasBranch)
					}
				}
			}
			//if bytes.HasPrefix(curr[:maxLen], common.FromHex("05070202")) {
			//if maxLen <= 3 {
			//	e.printTopHashes(curr[:maxLen], 0, groups[maxLen])
			//}
			if h != nil {
				//if err := h(curr[:maxLen], 0, 0, nil, nil); err != nil {
				//	return nil, nil, err
				//}
				//canSendHashes := bits.OnesCount16(hasBranch[maxLen]) > 1
				canSendHashes := hasHash[maxLen] != 0
				if canSendHashes {
					if bytes.HasPrefix(curr[:maxLen], common.FromHex("04090709")) {
						if len(hasBranch) > 80 {
							fmt.Printf("why now: %x,%b,%t,%b\n", curr[:maxLen], hasBranch[80:], buildExtensions, groups[80:])
							fmt.Printf("why now2: %d,%d,%d\n", maxLen, succLen, precLen)
						} else {
							fmt.Printf("why now: %x,%b,%t,%b\n", curr[:maxLen], hasBranch, buildExtensions, groups)
							fmt.Printf("why now2: %d,%d,%d\n", maxLen, succLen, precLen)
						}
					}
					usefulHashes = e.topHashes(curr[:maxLen], hasHash[maxLen], groups[maxLen])
					if maxLen != 0 && maxLen != 80 {
						hasBranch[maxLen-1] |= (uint16(1) << curr[maxLen-1])
						if err := h(curr[:maxLen], groups[maxLen], hasBranch[maxLen], hasHash[maxLen], usefulHashes, nil); err != nil {
							return nil, nil, nil, err
						}
					}
				}
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
			if h != nil && maxLen == 80 {
				//if hasBranch[maxLen] != 0 { //todo: remove this if check
				//if bits.OnesCount16(hasBranch[maxLen]) > 1 { //todo: remove this if check
				//if bytes.HasPrefix(curr[:maxLen], common.FromHex("03050b05000e070602010205080005090508060f0701070c0a000f000507080f0106060f08030c08030101050e090d07090608080003050f04060606080d0a0100000000000000000000000000000001")) {
				//	fmt.Printf("root: %x\n", curr[:maxLen])
				//}
				if err := h(curr[:maxLen], groups[maxLen], hasBranch[maxLen], hasHash[maxLen], usefulHashes, e.topHash()); err != nil {
					return nil, nil, nil, err
				}
				//} else {
				//fmt.Printf("no storage root: %x\n", curr[:maxLen])
				//}
			}
			for i := maxLen; i < len(hasBranch); i++ {
				hasBranch[i] = 0
				hasHash[i] = 0
			}
			//if bytes.HasPrefix(curr[:maxLen], common.FromHex("0c0e")) {
			//	fmt.Printf("--- %x, , %x\n", curr[:maxLen], e.topHash())
			//}
		}
		groups = groups[:maxLen]
		// Check the end of recursion
		if precLen == 0 {
			return groups, hasBranch, hasHash, nil
		}
		// Identify preceding key for the buildExtensions invocation

		if bytes.HasPrefix(curr[:maxLen], common.FromHex("04090709")) {
			fmt.Printf("cut: %x\n", curr[:maxLen])
		}
		curr = curr[:precLen]
		for len(groups) > 0 && groups[len(groups)-1] == 0 {
			groups = groups[:len(groups)-1]
			if bytes.HasPrefix(curr, common.FromHex("04090709")) {
				fmt.Printf("cut: %b,%b\n", hasBranch, groups)
			}
			//hasBranch = hasBranch[:len(hasBranch)-1]
		}
		//if len(succ) > 0 || precExists {
		//	for len(hasBranch) > 0 && hasBranch[len(hasBranch)-1] == 0 {
		//		hasBranch = hasBranch[:len(hasBranch)-1]
		//	}
		//}
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
				if err := h(curr[:maxLen], e.topHash()); err != nil {
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
