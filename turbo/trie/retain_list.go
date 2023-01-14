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

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type RetainDecider interface {
	Retain([]byte) bool
	IsCodeTouched(libcommon.Hash) bool
}

type RetainDeciderWithMarker interface {
	RetainDecider
	AddKeyWithMarker(key []byte, marker bool)
	RetainWithMarker(prefix []byte) (retain bool, nextMarkedKey []byte)
}

// RetainList encapsulates the list of keys that are required to be fully available, or loaded
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
// DESCRIBED: docs/programmers_guide/guide.md#converting-sequence-of-keys-and-value-into-a-multiproof
type RetainList struct {
	inited      bool // Whether keys are sorted and "LTE" and "GT" indices set
	minLength   int  // Mininum length of prefixes for which `HashOnly` function can return `true`
	lteIndex    int  // Index of the "LTE" key in the keys slice. Next one is "GT"
	hexes       [][]byte
	markers     []bool
	codeTouches map[libcommon.Hash]struct{}
}

// NewRetainList creates new RetainList
func NewRetainList(minLength int) *RetainList {
	return &RetainList{minLength: minLength, codeTouches: make(map[libcommon.Hash]struct{})}
}

func (rl *RetainList) Len() int {
	return len(rl.hexes)
}
func (rl *RetainList) Less(i, j int) bool {
	return bytes.Compare(rl.hexes[i], rl.hexes[j]) < 0
}
func (rl *RetainList) Swap(i, j int) {
	rl.hexes[i], rl.hexes[j] = rl.hexes[j], rl.hexes[i]
	rl.markers[i], rl.markers[j] = rl.markers[j], rl.markers[i]
}

// AddKey adds a new key (in KEY encoding) to the list
func (rl *RetainList) AddKey(key []byte) {
	rl.AddKeyWithMarker(key, false)
}

func (rl *RetainList) AddKeyWithMarker(key []byte, marker bool) {
	var nibbles = make([]byte, 2*len(key))
	for i, b := range key {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	rl.AddHex(nibbles)
	rl.markers = append(rl.markers, marker)
}

// AddHex adds a new key (in HEX encoding) to the list
func (rl *RetainList) AddHex(hex []byte) {
	rl.hexes = append(rl.hexes, hex)
}

// AddCodeTouch adds a new code touch into the resolve set
func (rl *RetainList) AddCodeTouch(codeHash libcommon.Hash) {
	rl.codeTouches[codeHash] = struct{}{}
}

func (rl *RetainList) IsCodeTouched(codeHash libcommon.Hash) bool {
	_, ok := rl.codeTouches[codeHash]
	return ok
}

func (rl *RetainList) ensureInited() {
	if rl.inited {
		return
	}
	if len(rl.markers) == 0 {
		rl.markers = make([]bool, len(rl.hexes))
	}
	if !sort.IsSorted(rl) {
		sort.Sort(rl)
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

func (rl *RetainList) RetainWithMarker(prefix []byte) (bool, []byte) {
	rl.ensureInited()
	if len(prefix) < rl.minLength {
		return true, nil
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
			return true, rl.nextMarkedItem(rl.lteIndex)
		}
	}
	if rl.lteIndex < len(rl.hexes)-1 {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex+1], prefix) {
			return true, rl.nextMarkedItem(rl.lteIndex + 1)
		}
	}

	if rl.lteIndex < len(rl.hexes) {
		if bytes.Compare(prefix, rl.hexes[rl.lteIndex]) <= 0 {
			return false, rl.nextMarkedItem(rl.lteIndex)
		}
	}
	if rl.lteIndex < len(rl.hexes)-1 {
		if bytes.Compare(prefix, rl.hexes[rl.lteIndex+1]) <= 0 {
			return false, rl.nextMarkedItem(rl.lteIndex + 1)
		}
	}

	return false, nil
}

func (rl *RetainList) nextMarkedItem(index int) []byte {
	for i := index; i < len(rl.markers); i++ {
		if rl.markers[i] {
			return rl.hexes[i]
		}
	}
	return nil
}

// Rewind lets us reuse this list from the beginning
func (rl *RetainList) Rewind() {
	rl.lteIndex = 0
}

func (rl *RetainList) String() string {
	return fmt.Sprintf("%x", rl.hexes)
}
