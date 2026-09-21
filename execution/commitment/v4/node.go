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

import "fmt"

type node struct {
	path  []byte
	plane byte

	childMask uint16
	leafMask  uint16

	children   [16]*node
	childHash  [16][]byte
	childExt   [16][]byte
	leafSuffix [16][]byte
	leafValue  [16][]byte
}

func fork(prefix []byte) *node {
	for _, nib := range prefix {
		if nib > 0x0f {
			panic(fmt.Sprintf("commitment v4: invalid path nibble %d", nib))
		}
	}
	return &node{path: append([]byte(nil), prefix...)}
}

func join(parent *node, nib int, ref []byte) {
	if parent == nil {
		panic("commitment v4: nil parent")
	}
	parent.setStoredChild(nib, ref, nil)
}

func (n *node) setLeaf(nib int, suffix, value []byte) {
	n.checkNibble(nib)
	bit := uint16(1) << nib
	n.childMask |= bit
	n.leafMask |= bit
	n.children[nib] = nil
	n.childHash[nib] = nil
	n.childExt[nib] = nil
	n.leafSuffix[nib] = appendCopy(n.leafSuffix[nib], suffix)
	n.leafValue[nib] = appendCopy(n.leafValue[nib], value)
}

func (n *node) setChild(nib int, child *node) {
	n.checkNibble(nib)
	bit := uint16(1) << nib
	n.childMask |= bit
	n.leafMask &^= bit
	if child != nil && n.plane != 0 {
		child.plane = n.plane
	}
	n.children[nib] = child
	n.childHash[nib] = nil
	n.childExt[nib] = nil
	n.leafSuffix[nib] = nil
	n.leafValue[nib] = nil
}

func (n *node) setStoredChild(nib int, hash []byte, ext []byte) {
	n.checkNibble(nib)
	if len(hash) != 32 {
		panic(fmt.Sprintf("commitment v4: child hash has length %d", len(hash)))
	}
	bit := uint16(1) << nib
	n.childMask |= bit
	n.leafMask &^= bit
	n.children[nib] = nil
	n.childHash[nib] = appendCopy(n.childHash[nib], hash)
	n.childExt[nib] = appendCopy(n.childExt[nib], ext)
	n.leafSuffix[nib] = nil
	n.leafValue[nib] = nil
}

func (n *node) clear(nib int) {
	n.checkNibble(nib)
	bit := uint16(1) << nib
	n.childMask &^= bit
	n.leafMask &^= bit
	n.children[nib] = nil
	n.childHash[nib] = nil
	n.childExt[nib] = nil
	n.leafSuffix[nib] = nil
	n.leafValue[nib] = nil
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
