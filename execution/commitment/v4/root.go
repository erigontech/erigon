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
	"errors"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
	ErrRootPath  = errors.New("commitment v4: invalid root path")
	ErrRootShape = errors.New("commitment v4: invalid root shape")
)

func insertRoot(n *node, path, value []byte) error {
	if n == nil || len(path) != 64 {
		return ErrRootPath
	}
	for _, nib := range path {
		if nib > 0x0f {
			return ErrRootPath
		}
	}

	if n.childMask == 0 {
		n.path = nil
		n.setLeaf(int(path[0]), packPath(path[1:], nil), value)
		return nil
	}
	if len(n.path) != 0 {
		common := nibbles.CommonPrefixLen(n.path, path)
		if common < len(n.path) {
			return splitRootExtension(n, path, value, common)
		}
	}
	if n.leafMask == n.childMask && rootBitsCount(n.childMask) == 1 {
		return insertLeafRoot(n, path, value)
	}
	return insert(n, path, packPath(path[len(n.path)+1:], nil), value)
}

func splitRootExtension(n *node, path, value []byte, common int) error {
	if common < 0 || common >= len(n.path) || rootBitsCount(n.childMask) != 1 || n.leafMask != 0 {
		return ErrRootShape
	}
	oldNib := trailingNibble(n.childMask)
	oldPath := append(append([]byte(nil), n.path...), byte(oldNib))
	oldPath = append(oldPath, n.childExt[oldNib]...)
	if len(n.childHash[oldNib]) != 32 {
		return ErrRootShape
	}
	if common >= len(path) || nibbles.CommonPrefixLen(oldPath, path) != common {
		return ErrRootPath
	}
	oldHash := append([]byte(nil), n.childHash[oldNib]...)
	oldExtension := append([]byte(nil), oldPath[common+1:]...)
	oldNib = int(oldPath[common])
	newNib := int(path[common])
	if oldNib == newNib {
		return ErrRootShape
	}

	root := fork(nil)
	root.plane = n.plane
	root.setStoredChild(oldNib, oldHash, oldExtension)
	root.setLeaf(newNib, packPath(path[1:], nil), value)
	*n = *root
	return nil
}

func insertLeafRoot(n *node, path, value []byte) error {
	oldNib := trailingNibble(n.childMask)
	oldPath := append([]byte{byte(oldNib)}, unpackPath(n.leafSuffix[oldNib], 63, nil)...)
	if bytes.Equal(oldPath, path) {
		n.setLeaf(oldNib, packPath(path[1:], nil), value)
		return nil
	}
	if oldNib != int(path[0]) {
		oldValue := append([]byte(nil), n.leafValue[oldNib]...)
		oldSuffix := append([]byte(nil), n.leafSuffix[oldNib]...)
		n.path = nil
		n.clear(oldNib)
		n.setLeaf(oldNib, oldSuffix, oldValue)
		n.setLeaf(int(path[0]), packPath(path[1:], nil), value)
		return nil
	}
	return insert(n, path, packPath(path[1:], nil), value)
}

func removeRoot(n *node, path []byte) error {
	if n == nil || len(path) != 64 {
		return ErrRootPath
	}
	if err := remove(n, path); err != nil {
		return err
	}
	return collapseRoot(n)
}

func collapseRoot(n *node) error {
	if n == nil {
		return ErrRootShape
	}
	count := rootBitsCount(n.childMask)
	if count > 1 {
		if len(n.path) != 0 {
			n.path = nil
		}
		return nil
	}
	if count == 0 {
		n.path = nil
		return nil
	}

	nib := trailingNibble(n.childMask)
	bit := uint16(1) << nib
	if n.leafMask&bit != 0 {
		fullPath := append([]byte{byte(nib)}, unpackPath(n.leafSuffix[nib], 64-len(n.path)-1, nil)...)
		value := append([]byte(nil), n.leafValue[nib]...)
		n.path = nil
		n.clear(nib)
		n.setLeaf(int(fullPath[0]), packPath(fullPath[1:], nil), value)
		return nil
	}
	if len(n.childHash[nib]) != 32 && n.children[nib] == nil {
		return ErrRootShape
	}
	if n.children[nib] != nil {
		return ErrRootShape
	}
	ext := append([]byte{byte(nib)}, n.childExt[nib]...)
	n.path = ext
	n.childExt[nib] = nil
	return nil
}

func rootBitsCount(mask uint16) int {
	count := 0
	for mask != 0 {
		mask &= mask - 1
		count++
	}
	return count
}

func trailingNibble(mask uint16) int {
	for nib := range 16 {
		if mask&(uint16(1)<<nib) != 0 {
			return nib
		}
	}
	return -1
}
