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
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
)

// RetainList encapsulates the list of keys that are required to be fully available, or loaded
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
// DESCRIBED: docs/programmers_guide/guide.md#converting-sequence-of-keys-and-value-into-a-multiproof
type RetainList struct {
	inited      bool // Whether keys are sorted and "LTE" and "GT" indices set
	binary      bool // if true, use binary encoding instead of Hex
	minLength   int  // Mininum length of prefixes for which `HashOnly` function can return `true`
	lteIndex    int  // Index of the "LTE" key in the keys slice. Next one is "GT"
	hexes       sortable
	codeTouches map[common.Hash]struct{}
}

// NewRetainList creates new RetainList
func NewRetainList(minLength int) *RetainList {
	return &RetainList{minLength: minLength, codeTouches: make(map[common.Hash]struct{})}
}

func NewBinaryRetainList(minLength int) *RetainList {
	return &RetainList{minLength: minLength, codeTouches: make(map[common.Hash]struct{}), binary: true}
}

// AddKey adds a new key (in KEY encoding) to the list
func (rl *RetainList) AddKey(key []byte) {
	var nibbles = make([]byte, 2*len(key))
	for i, b := range key {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	rl.AddHex(nibbles)
}

// AddHex adds a new key (in HEX encoding) to the list
func (rl *RetainList) AddHex(hex []byte) {
	if rl.binary {
		rl.hexes = append(rl.hexes, keyHexToBin(hex))
	} else {
		rl.hexes = append(rl.hexes, hex)
	}
}

// AddCodeTouch adds a new code touch into the resolve set
func (rl *RetainList) AddCodeTouch(codeHash common.Hash) {
	rl.codeTouches[codeHash] = struct{}{}
}

func (rl *RetainList) IsCodeTouched(codeHash common.Hash) bool {
	_, ok := rl.codeTouches[codeHash]
	return ok
}

func (rl *RetainList) ensureInited() {
	if rl.inited {
		return
	}
	if !sort.IsSorted(rl.hexes) {
		sort.Sort(rl.hexes)
	}
	rl.lteIndex = 0
	rl.inited = true
}

// Retain decides whether to emit `HASHER` or `BRANCH` for a given prefix, by
// checking if this is prefix of any of the keys added to the set
// Since keys in the set are sorted, and we expect that the prefixes will
// come in monotonically ascending order, we optimise for this, though
// the function would still work if the order is different
func (rl *RetainList) Retain(prefix []byte) bool {
	rl.ensureInited()
	if len(prefix) < rl.minLength {
		return true
	}
	// Adjust "GT" if necessary
	var gtAdjusted bool
	for rl.lteIndex < len(rl.hexes)-1 && bytes.Compare(rl.hexes[rl.lteIndex+1], prefix) <= 0 {
		rl.lteIndex++
		gtAdjusted = true
	}
	// Adjust "LTE" if necessary (normally will not be necessary)
	for !gtAdjusted && rl.lteIndex > 0 && bytes.Compare(rl.hexes[rl.lteIndex], prefix) > 0 {
		rl.lteIndex--
	}
	if rl.lteIndex < len(rl.hexes) {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex], prefix) {
			return true
		}
	}
	if rl.lteIndex < len(rl.hexes)-1 {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex+1], prefix) {
			return true
		}
	}
	return false
}

// Current returns the hex value that has been used for the latest comparison in `Retain` function
// It is only used in one edge case at the moment - to distinguish between the accounts' key
// and the storage keys of the same account
func (rl *RetainList) Current() []byte {
	return rl.hexes[rl.lteIndex]
}

// Rewind lets us reuse this list from the beginning
func (rl *RetainList) Rewind() {
	rl.lteIndex = 0
}

func (rl *RetainList) String() string {
	return fmt.Sprintf("%x", rl.hexes)
}
