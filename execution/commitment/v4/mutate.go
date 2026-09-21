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
	"fmt"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
	ErrInsertPath        = errors.New("commitment v4: invalid insert path")
	ErrInsertSuffix      = errors.New("commitment v4: invalid insert suffix")
	ErrInsertStoredChild = errors.New("commitment v4: cannot insert below a stored child")
)

func insert(n *node, path, suffix, value []byte) error {
	if n == nil {
		return ErrInsertPath
	}
	if len(path) != 64 || len(n.path) >= len(path) || !bytes.HasPrefix(path, n.path) {
		return fmt.Errorf("%w: node depth %d, path length %d", ErrInsertPath, len(n.path), len(path))
	}
	for _, nib := range path {
		if nib > 0x0f {
			return fmt.Errorf("%w: nibble %d", ErrInsertPath, nib)
		}
	}

	depth := len(n.path)
	targetNib := int(path[depth])
	wantSuffix := packPath(path[depth+1:], nil)
	if !bytes.Equal(suffix, wantSuffix) {
		return fmt.Errorf("%w: got %d bytes, want %d", ErrInsertSuffix, len(suffix), len(wantSuffix))
	}

	bit := uint16(1) << targetNib
	if n.childMask&bit == 0 {
		n.setLeaf(targetNib, wantSuffix, value)
		return nil
	}
	if n.leafMask&bit != 0 {
		return splitLeaf(n, targetNib, path, wantSuffix, value)
	}
	if child := n.children[targetNib]; child != nil {
		return splitChild(n, targetNib, child, path, wantSuffix, value)
	}
	if len(n.childHash[targetNib]) == 32 {
		return splitStoredChild(n, targetNib, path, wantSuffix, value)
	}
	return ErrInsertPath
}

func splitLeaf(parent *node, nib int, path, suffix, value []byte) error {
	depth := len(parent.path)
	oldSuffixCount := 64 - depth - 1
	oldSuffix := unpackPath(parent.leafSuffix[nib], oldSuffixCount, nil)
	oldPath := make([]byte, 0, 64)
	oldPath = append(oldPath, parent.path...)
	oldPath = append(oldPath, byte(nib))
	oldPath = append(oldPath, oldSuffix...)
	common := nibbles.CommonPrefixLen(oldPath, path)
	if common == len(oldPath) {
		parent.setLeaf(nib, suffix, value)
		return nil
	}
	if common >= len(path) {
		return ErrInsertPath
	}

	branch := fork(oldPath[:common])
	oldNib := int(oldPath[common])
	newNib := int(path[common])
	branch.setLeaf(oldNib, packPath(oldPath[common+1:], nil), parent.leafValue[nib])
	branch.setLeaf(newNib, packPath(path[common+1:], nil), value)
	parent.setChild(nib, branch)
	return nil
}

func splitChild(parent *node, nib int, child *node, path, suffix, value []byte) error {
	common := nibbles.CommonPrefixLen(child.path, path)
	if common == len(child.path) {
		return insert(child, path, suffix, value)
	}
	if common < len(parent.path)+1 || common >= len(path) {
		return ErrInsertPath
	}

	branch := fork(child.path[:common])
	oldNib := int(child.path[common])
	newNib := int(path[common])
	branch.setChild(oldNib, child)
	branch.setLeaf(newNib, packPath(path[common+1:], nil), value)
	parent.setChild(nib, branch)
	return nil
}

func splitStoredChild(parent *node, nib int, path, suffix, value []byte) error {
	childPath := make([]byte, 0, len(parent.path)+1+len(parent.childExt[nib]))
	childPath = append(childPath, parent.path...)
	childPath = append(childPath, byte(nib))
	childPath = append(childPath, parent.childExt[nib]...)
	common := nibbles.CommonPrefixLen(childPath, path)
	if common == len(childPath) {
		return ErrInsertStoredChild
	}
	if common < len(parent.path)+1 || common >= len(path) {
		return ErrInsertPath
	}

	branch := fork(childPath[:common])
	oldNib := int(childPath[common])
	newNib := int(path[common])
	remainingExt := childPath[common+1:]
	branch.setStoredChild(oldNib, parent.childHash[nib], remainingExt)
	branch.setLeaf(newNib, packPath(path[common+1:], nil), value)
	parent.setChild(nib, branch)
	return nil
}
