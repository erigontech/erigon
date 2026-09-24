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
	"math/bits"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
	ErrInsertPath        = errors.New("commitment v4: invalid insert path")
	ErrInsertStoredChild = errors.New("commitment v4: cannot insert below a stored child")
	ErrRemovePath        = errors.New("commitment v4: invalid remove path")
	ErrRemoveNotFound    = errors.New("commitment v4: remove path not found")
	ErrRemoveStoredChild = errors.New("commitment v4: cannot remove below a stored child")
)

type removalKind byte

const (
	removalEmpty removalKind = iota
	removalKeep
	removalLeaf
	removalBranch
	removalNode
)

type removalState struct {
	kind  removalKind
	path  []byte
	hash  []byte
	value []byte
	node  *node
}

func remove(n *node, path []byte) (removalState, error) {
	if n == nil || len(path) != 64 {
		return removalState{}, fmt.Errorf("%w: node %v path length %d", ErrRemovePath, n != nil, len(path))
	}
	return removeAt(n, path)
}

func removeAt(n *node, path []byte) (removalState, error) {
	if len(n.path) >= len(path) {
		return removalState{}, fmt.Errorf("%w: node depth %d, path length %d", ErrRemovePath, len(n.path), len(path))
	}
	if !bytes.HasPrefix(path, n.path) {
		return removalState{}, ErrRemoveNotFound
	}
	depth := len(n.path)
	nib := int(path[depth])
	bit := uint16(1) << nib
	if n.childMask&bit == 0 {
		return removalState{}, ErrRemoveNotFound
	}

	if n.leafMask&bit != 0 {
		if !packedMatches(n.leafSuffixAt(nib), path[depth+1:]) {
			return removalState{}, ErrRemoveNotFound
		}
		n.clear(nib)
		return collapsedState(n), nil
	}

	child := n.child(nib)
	if child == nil {
		var childScratch [64]byte
		childPath := append(childScratch[:0], n.path...)
		childPath = append(childPath, byte(nib))
		childPath = append(childPath, n.childExtAt(nib)...)
		if !bytes.HasPrefix(path, childPath) {
			return removalState{}, ErrRemoveNotFound
		}
		return removalState{}, ErrRemoveStoredChild
	}
	state, err := removeAt(child, path)
	if err != nil {
		return removalState{}, err
	}
	applyRemoval(n, nib, state)
	return collapsedState(n), nil
}

func applyRemoval(n *node, nib int, state removalState) bool {
	switch state.kind {
	case removalEmpty:
		n.clear(nib)
	case removalLeaf:
		setLeafPath(n, nib, state.path, state.value)
	case removalBranch:
		setBranchPath(n, nib, state.path, state.hash)
	case removalNode:
		setNodePath(n, nib, state.node)
	default:
		return false
	}
	return true
}

func collapsedState(n *node) removalState {
	count := bits.OnesCount16(n.childMask)
	if count == 0 {
		return removalState{kind: removalEmpty}
	}
	if count > 1 {
		return removalState{kind: removalKeep}
	}

	nib := bits.TrailingZeros16(n.childMask)
	bit := uint16(1) << nib
	if n.leafMask&bit != 0 {
		path := append(append([]byte(nil), n.path...), byte(nib))
		path = append(path, unpackPath(n.leafSuffixAt(nib), 64-len(n.path)-1, nil)...)
		return removalState{kind: removalLeaf, path: path, value: append([]byte(nil), n.leafValueAt(nib)...)}
	}
	if child := n.child(nib); child != nil {
		if applyRemoval(n, nib, collapsedState(child)) {
			return collapsedState(n)
		}
		return removalState{kind: removalNode, path: child.path, node: child}
	}
	path := append(append([]byte(nil), n.path...), byte(nib))
	path = append(path, n.childExtAt(nib)...)
	return removalState{kind: removalBranch, path: path, hash: append([]byte(nil), n.childHashAt(nib)...)}
}

func setLeafPath(n *node, nib int, path, value []byte) {
	depth := len(n.path)
	if len(path) != 64 || len(path) <= depth || path[depth] != byte(nib) || !bytes.HasPrefix(path, n.path) {
		panic("commitment v4: invalid collapsed leaf path")
	}
	var packScratch [32]byte
	n.setLeaf(nib, packPath(path[depth+1:], packScratch[:0]), value)
}

func setNodePath(n *node, nib int, child *node) {
	depth := len(n.path)
	if child == nil || len(child.path) <= depth || child.path[depth] != byte(nib) || !bytes.HasPrefix(child.path, n.path) {
		panic("commitment v4: invalid collapsed node path")
	}
	n.setChild(nib, child)
}

func setBranchPath(n *node, nib int, path, hash []byte) {
	depth := len(n.path)
	if len(path) <= depth || path[depth] != byte(nib) || !bytes.HasPrefix(path, n.path) {
		panic("commitment v4: invalid collapsed branch path")
	}
	n.setStoredChild(nib, hash, path[depth+1:])
}

func insert(n *node, path, value []byte) error {
	if n == nil {
		return ErrInsertPath
	}
	if len(path) != 64 || len(n.path) >= len(path) || !bytes.HasPrefix(path, n.path) {
		return fmt.Errorf("%w: node depth %d, path length %d", ErrInsertPath, len(n.path), len(path))
	}

	depth := len(n.path)
	targetNib := int(path[depth])
	var packScratch [32]byte
	wantSuffix := packPath(path[depth+1:], packScratch[:0])

	bit := uint16(1) << targetNib
	if n.childMask&bit == 0 {
		n.setLeaf(targetNib, wantSuffix, value)
		return nil
	}
	if n.leafMask&bit != 0 {
		return splitLeaf(n, targetNib, path, wantSuffix, value)
	}
	if child := n.child(targetNib); child != nil {
		return splitChild(n, targetNib, child, path, value)
	}
	if n.hasChildHash(targetNib) {
		return splitStoredChild(n, targetNib, path, value)
	}
	return ErrInsertPath
}

func splitLeaf(parent *node, nib int, path, suffix, value []byte) error {
	depth := len(parent.path)
	oldSuffixCount := 64 - depth - 1
	var pathScratch [64]byte
	oldPath := append(pathScratch[:0], parent.path...)
	oldPath = append(oldPath, byte(nib))
	oldPath = oldPath[:len(oldPath)+oldSuffixCount]
	unpackPath(parent.leafSuffixAt(nib), oldSuffixCount, oldPath[depth+1:])
	common := nibbles.CommonPrefixLen(oldPath, path)
	if common == len(oldPath) {
		parent.setLeaf(nib, suffix, value)
		return nil
	}

	branch := fork(oldPath[:common])
	oldNib := int(oldPath[common])
	newNib := int(path[common])
	var packScratch [32]byte
	branch.setLeaf(oldNib, packPath(oldPath[common+1:], packScratch[:0]), parent.leafValueAt(nib))
	branch.setLeaf(newNib, packPath(path[common+1:], packScratch[:0]), value)
	parent.setChild(nib, branch)
	return nil
}

func splitChild(parent *node, nib int, child *node, path, value []byte) error {
	common := nibbles.CommonPrefixLen(child.path, path)
	if common == len(child.path) {
		return insert(child, path, value)
	}
	if common < len(parent.path)+1 {
		return ErrInsertPath
	}

	branch := fork(child.path[:common])
	oldNib := int(child.path[common])
	newNib := int(path[common])
	var packScratch [32]byte
	branch.setChild(oldNib, child)
	branch.setLeaf(newNib, packPath(path[common+1:], packScratch[:0]), value)
	parent.setChild(nib, branch)
	return nil
}

func splitStoredChild(parent *node, nib int, path, value []byte) error {
	var childScratch [64]byte
	childPath := append(childScratch[:0], parent.path...)
	childPath = append(childPath, byte(nib))
	childPath = append(childPath, parent.childExtAt(nib)...)
	common := nibbles.CommonPrefixLen(childPath, path)
	if common == len(childPath) {
		return ErrInsertStoredChild
	}
	if common < len(parent.path)+1 {
		return ErrInsertPath
	}

	branch := fork(childPath[:common])
	oldNib := int(childPath[common])
	newNib := int(path[common])
	remainingExt := childPath[common+1:]
	var packScratch [32]byte
	branch.setStoredChild(oldNib, parent.childHashAt(nib), remainingExt)
	branch.setLeaf(newNib, packPath(path[common+1:], packScratch[:0]), value)
	parent.setChild(nib, branch)
	return nil
}
