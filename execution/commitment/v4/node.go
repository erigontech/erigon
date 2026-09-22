// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"fmt"
	"math/bits"
	"slices"
)

type childSlot struct {
	hash   [32]byte
	node   *node
	ext    []byte
	suffix []byte
	value  []byte
}

type node struct {
	path        []byte
	slots       []childSlot
	plane       byte
	storageRoot bool

	childMask uint16
	leafMask  uint16
	hashMask  uint16
}

func fork(prefix []byte) *node {
	for _, nib := range prefix {
		if nib > 0x0f {
			panic(fmt.Sprintf("commitment v4: invalid path nibble %d", nib))
		}
	}
	return &node{path: append([]byte(nil), prefix...)}
}

const slotsInitialCap = 4

func (n *node) slotIndex(nib int) int {
	return bits.OnesCount16(n.childMask & (uint16(1)<<nib - 1))
}

func (n *node) slot(nib int) *childSlot {
	n.checkNibble(nib)
	if n.childMask&(uint16(1)<<nib) == 0 {
		return nil
	}
	return &n.slots[n.slotIndex(nib)]
}

func (n *node) child(nib int) *node {
	if s := n.slot(nib); s != nil {
		return s.node
	}
	return nil
}

func (n *node) childHashAt(nib int) []byte {
	n.checkNibble(nib)
	if n.hashMask&(uint16(1)<<nib) == 0 {
		return nil
	}
	return n.slots[n.slotIndex(nib)].hash[:]
}

func (n *node) childExtAt(nib int) []byte {
	if s := n.slot(nib); s != nil {
		return s.ext
	}
	return nil
}

func (n *node) leafSuffixAt(nib int) []byte {
	if s := n.slot(nib); s != nil {
		return s.suffix
	}
	return nil
}

func (n *node) leafValueAt(nib int) []byte {
	if s := n.slot(nib); s != nil {
		return s.value
	}
	return nil
}

func (n *node) ensureSlot(nib int) *childSlot {
	n.checkNibble(nib)
	bit := uint16(1) << nib
	index := n.slotIndex(nib)
	if n.childMask&bit == 0 {
		if n.slots == nil {
			n.slots = make([]childSlot, 0, slotsInitialCap)
		}
		n.slots = slices.Insert(n.slots, index, childSlot{})
		n.childMask |= bit
	}
	return &n.slots[index]
}

func (n *node) setLeaf(nib int, suffix, value []byte) {
	s := n.ensureSlot(nib)
	bit := uint16(1) << nib
	n.leafMask |= bit
	n.hashMask &^= bit
	s.node = nil
	s.hash = [32]byte{}
	s.ext = nil
	s.suffix = appendCopy(s.suffix, suffix)
	s.value = appendCopy(s.value, value)
}

func (n *node) setChild(nib int, child *node) {
	s := n.ensureSlot(nib)
	bit := uint16(1) << nib
	n.leafMask &^= bit
	n.hashMask &^= bit
	if child != nil && n.plane != 0 {
		child.plane = n.plane
	}
	s.node = child
	s.hash = [32]byte{}
	s.ext = nil
	s.suffix = nil
	s.value = nil
}

func (n *node) setStoredChild(nib int, hash []byte, ext []byte) {
	if len(hash) != 32 {
		panic(fmt.Sprintf("commitment v4: child hash has length %d", len(hash)))
	}
	s := n.ensureSlot(nib)
	bit := uint16(1) << nib
	n.leafMask &^= bit
	n.hashMask |= bit
	s.node = nil
	copy(s.hash[:], hash)
	s.ext = appendCopy(s.ext, ext)
	s.suffix = nil
	s.value = nil
}

func (n *node) clearChildExt(nib int) {
	if s := n.slot(nib); s != nil {
		s.ext = nil
	}
}

func (n *node) clear(nib int) {
	n.checkNibble(nib)
	bit := uint16(1) << nib
	if n.childMask&bit == 0 {
		return
	}
	index := n.slotIndex(nib)
	n.slots = slices.Delete(n.slots, index, index+1)
	n.childMask &^= bit
	n.leafMask &^= bit
	n.hashMask &^= bit
}

func (n *node) checkNibble(nib int) {
	if nib < 0 || nib > 15 {
		panic(fmt.Sprintf("commitment v4: invalid child nibble %d", nib))
	}
}

func appendCopy(dst, src []byte) []byte {
	if src == nil {
		return nil
	}
	dst = append(dst[:0], src...)
	return dst
}
