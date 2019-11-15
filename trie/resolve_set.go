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

// ResolveSet encapsulates the set of keys that are required to be fully available, or resolved
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
// DESCRIBED: docs/programmers_guide/guide.md#converting-sequence-of-keys-and-value-into-a-multiproof
type ResolveSet struct {
	minLength int // Mininum length of prefixes for which `HashOnly` function can return `true`
	hexes     sortable
	inited    bool // Whether keys are sorted and "LTE" and "GT" indices set
	lteIndex  int  // Index of the "LTE" key in the keys slice. Next one is "GT"
}

// NewResolveSet creates new ResolveSet
func NewResolveSet(minLength int) *ResolveSet {
	return &ResolveSet{minLength: minLength}
}

// AddKey adds a new key (in KEY encoding) to the set
func (rs *ResolveSet) AddKey(key []byte) {
	rs.hexes = append(rs.hexes, keybytesToHex(key))
}

// AddHex adds a new key (in HEX encoding) to the set
func (rs *ResolveSet) AddHex(hex []byte) {
	rs.hexes = append(rs.hexes, hex)
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
	if len(prefix) < rs.minLength {
		return false
	}
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
