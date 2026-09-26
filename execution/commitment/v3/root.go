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

package v3

import (
	"bytes"
	"errors"
	"math/bits"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
	ErrRootPath  = errors.New("commitment v3: invalid root path")
	ErrRootShape = errors.New("commitment v3: invalid root shape")
)

func insertRoot(n *node, path, value []byte) error {
	if n == nil || len(path) != 64 {
		return ErrRootPath
	}

	if n.childMask == 0 {
		n.path = nil
		var packScratch [32]byte
		n.setLeaf(int(path[0]), packPath(path[1:], packScratch[:0]), value)
		return nil
	}
	if len(n.path) != 0 {
		common := nibbles.CommonPrefixLen(n.path, path)
		if common < len(n.path) {
			return splitRootExtension(n, path, value, common)
		}
		if child := rootExtensionChild(n); child != nil {
			return insert(child, path, value)
		}
	}
	if n.leafMask == n.childMask && bits.OnesCount16(n.childMask) == 1 {
		return insertLeafRoot(n, path, value)
	}
	return insert(n, path, value)
}

func splitRootExtension(n *node, path, value []byte, common int) error {
	if bits.OnesCount16(n.childMask) != 1 || n.leafMask != 0 {
		return ErrRootShape
	}
	nib := bits.TrailingZeros16(n.childMask)
	oldChild := n.child(nib)
	if oldChild != nil && !bytes.Equal(oldChild.path, n.path) {
		return ErrRootShape
	}
	oldPath := n.path
	branch := newBranch(path, common, value)
	branch.plane = n.plane
	if oldChild != nil {
		branch.setChild(int(oldPath[common]), oldChild)
	} else if hash := n.childHashAt(nib); len(hash) == 32 {
		branch.setStoredChild(int(oldPath[common]), hash, oldPath[common+1:])
	} else {
		return ErrRootShape
	}
	if common == 0 {
		*n = *branch
		return nil
	}
	root := fork(oldPath[:common])
	root.plane = n.plane
	root.setChild(int(oldPath[0]), branch)
	*n = *root
	return nil
}

func insertLeafRoot(n *node, path, value []byte) error {
	oldNib := bits.TrailingZeros16(n.childMask)
	oldSuffix, oldValue := n.leafAt(oldNib)
	var pathScratch [64]byte
	pathScratch[0] = byte(oldNib)
	unpackPath(oldSuffix, 63, pathScratch[1:1:64])
	oldPath := pathScratch[:]
	var packScratch [32]byte
	if bytes.Equal(oldPath, path) {
		n.setLeaf(oldNib, packPath(path[1:], packScratch[:0]), value)
		return nil
	}
	if oldNib != int(path[0]) {
		n.setLeaf(int(path[0]), packPath(path[1:], packScratch[:0]), value)
		return nil
	}
	common := nibbles.CommonPrefixLen(oldPath, path)
	branch := newBranch(path, common, value)
	branch.setLeaf(int(oldPath[common]), packPath(oldPath[common+1:], packScratch[:0]), oldValue)
	root := fork(oldPath[:common])
	root.plane = n.plane
	root.setChild(int(oldPath[0]), branch)
	*n = *root
	return nil
}

func removeRoot(n *node, path []byte) error {
	if n == nil || len(path) != 64 {
		return ErrRootPath
	}
	if child := rootExtensionChild(n); child != nil {
		state, err := remove(child, path)
		if err != nil || state.kind == removalKeep {
			return err
		}
		n.clear(bits.TrailingZeros16(n.childMask))
		n.path = nil
		switch state.kind {
		case removalLeaf:
			n.setLeaf(int(state.path[0]), packPath(state.path[1:], nil), state.value)
		case removalBranch:
			n.path = append(n.path, state.path...)
			n.setStoredChild(int(state.path[0]), state.hash, nil)
		case removalNode:
			n.path = append(n.path, state.node.path...)
			n.setChild(int(state.node.path[0]), state.node)
		}
		return nil
	}
	if _, err := remove(n, path); err != nil {
		return err
	}
	return promoteRootExtension(n)
}

func promoteRootExtension(n *node) error {
	if n == nil || len(n.path) != 0 || n.leafMask != 0 || bits.OnesCount16(n.childMask) != 1 {
		return nil
	}
	nib := bits.TrailingZeros16(n.childMask)
	if child := n.child(nib); child != nil {
		if len(child.path) == 0 || child.path[0] != byte(nib) {
			return ErrRootShape
		}
		n.path = append(n.path[:0], child.path...)
	} else {
		if !n.hasChildHash(nib) {
			return ErrRootShape
		}
		n.path = n.childPath(nib, n.path[:0])
	}
	if len(n.path) > 63 {
		return ErrRootShape
	}
	n.clearChildExt(nib)
	return nil
}
