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
	"bytes"
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
	raw         []byte
	record      Record
	layout      layout
	plane       byte
	storageRoot bool
	loaded      bool

	childMask uint16
	leafMask  uint16
	hashMask  uint16
	slotMask  uint16
}

func fork(prefix []byte) *node {
	return &node{path: append([]byte(nil), prefix...)}
}

const slotsInitialCap = 4

func (n *node) slotIndex(nib int) int {
	return bits.OnesCount16(n.slotMask & (uint16(1)<<nib - 1))
}

func (n *node) slot(nib int) *childSlot {
	if n.slotMask&(uint16(1)<<nib) == 0 {
		return nil
	}
	return &n.slots[n.slotIndex(nib)]
}

func (n *node) stored(nib int) bool {
	bit := uint16(1) << nib
	return n.childMask&bit != 0 && n.slotMask&bit == 0
}

func (n *node) child(nib int) *node {
	if s := n.slot(nib); s != nil {
		return s.node
	}
	return nil
}

func (n *node) hasChildHash(nib int) bool {
	return n.hashMask&(uint16(1)<<nib) != 0
}

func (n *node) childHashAt(nib int) []byte {
	if n.hashMask&(uint16(1)<<nib) == 0 {
		return nil
	}
	if n.stored(nib) {
		return slices.Clip(n.record.slotAt(n.layout, nib))
	}
	return n.slots[n.slotIndex(nib)].hash[:]
}

func (n *node) childExtAt(nib int) []byte {
	if s := n.slot(nib); s != nil {
		return s.ext
	}
	if !n.stored(nib) {
		return nil
	}
	ext, err := decodeExtension(n.record.extAt(n.layout, nib))
	if err != nil {
		panic(fmt.Sprintf("commitment v4: validated record has a bad extension at %d: %v", nib, err))
	}
	return ext
}

func (n *node) hasChildExt(nib int) bool {
	if n.stored(nib) {
		encoded := n.record.extAt(n.layout, nib)
		return len(encoded) != 0 && encoded[0] != 0
	}
	return len(n.childExtAt(nib)) != 0
}

func (n *node) appendChildExt(out []byte, nib int) []byte {
	if n.stored(nib) {
		return append(out, n.record.extAt(n.layout, nib)...)
	}
	ext := n.childExtAt(nib)
	if len(ext) > 255 {
		panic(fmt.Sprintf("commitment v4: child extension %d is too long", nib))
	}
	out = append(out, byte(len(ext)))
	var extScratch [32]byte
	return append(out, packPath(ext, extScratch[:0])...)
}

func (n *node) leafSuffixAt(nib int) []byte {
	if s := n.slot(nib); s != nil {
		return s.suffix
	}
	if !n.stored(nib) {
		return nil
	}
	suffix, _ := n.record.leafAt(n.layout, nib)
	return slices.Clip(suffix)
}

func (n *node) leafValueAt(nib int) []byte {
	if s := n.slot(nib); s != nil {
		return s.value
	}
	if !n.stored(nib) {
		return nil
	}
	_, value := n.record.leafAt(n.layout, nib)
	return slices.Clip(value)
}

func (n *node) ensureSlot(nib int) *childSlot {
	bit := uint16(1) << nib
	index := n.slotIndex(nib)
	if n.slotMask&bit == 0 {
		if n.slots == nil {
			n.slots = make([]childSlot, 0, slotsInitialCap)
		}
		n.slots = slices.Insert(n.slots, index, childSlot{})
		n.slotMask |= bit
		n.childMask |= bit
	}
	return &n.slots[index]
}

func (n *node) setLeaf(nib int, suffix, value []byte) {
	n.setLeafShared(nib, bytes.Clone(suffix), bytes.Clone(value))
}

func (n *node) setLeafShared(nib int, suffix, value []byte) {
	s := n.ensureSlot(nib)
	bit := uint16(1) << nib
	n.leafMask |= bit
	n.hashMask &^= bit
	s.node = nil
	s.hash = [32]byte{}
	s.ext = nil
	s.suffix = slices.Clip(suffix)
	s.value = slices.Clip(value)
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
	if n.stored(nib) && n.hashMask&(uint16(1)<<nib) != 0 {
		hash := n.childHashAt(nib)
		copy(n.ensureSlot(nib).hash[:], hash)
		return
	}
	if s := n.slot(nib); s != nil {
		s.ext = nil
	}
}

func (n *node) clear(nib int) {
	bit := uint16(1) << nib
	if n.childMask&bit == 0 {
		return
	}
	if n.slotMask&bit != 0 {
		index := n.slotIndex(nib)
		n.slots = slices.Delete(n.slots, index, index+1)
	}
	n.childMask &^= bit
	n.leafMask &^= bit
	n.hashMask &^= bit
	n.slotMask &^= bit
}

func appendCopy(dst, src []byte) []byte {
	if src == nil {
		return nil
	}
	dst = append(dst[:0], src...)
	return dst
}
