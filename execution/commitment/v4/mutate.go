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
		if suffix, _ := n.leafAt(nib); !packedMatches(suffix, path[depth+1:]) {
			return removalState{}, ErrRemoveNotFound
		}
		n.clear(nib)
		return collapsedState(n), nil
	}

	child := n.child(nib)
	if child == nil {
		var childScratch [64]byte
		if !bytes.HasPrefix(path, n.childPath(nib, childScratch[:0])) {
			return removalState{}, ErrRemoveNotFound
		}
		return removalState{}, ErrRemoveStoredChild
	}
	state, err := remove(child, path)
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
		var packScratch [32]byte
		n.setLeaf(nib, packPath(state.path[len(n.path)+1:], packScratch[:0]), state.value)
	case removalBranch:
		n.setStoredChild(nib, state.hash, state.path[len(n.path)+1:])
	case removalNode:
		n.setChild(nib, state.node)
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
		suffix, value := n.leafAt(nib)
		path := append(append([]byte(nil), n.path...), byte(nib))
		path = append(path, unpackPath(suffix, 64-len(n.path)-1, nil)...)
		return removalState{kind: removalLeaf, path: path, value: append([]byte(nil), value...)}
	}
	if child := n.child(nib); child != nil {
		if applyRemoval(n, nib, collapsedState(child)) {
			return collapsedState(n)
		}
		return removalState{kind: removalNode, path: child.path, node: child}
	}
	return removalState{kind: removalBranch, path: n.childPath(nib, nil), hash: append([]byte(nil), n.childHashAt(nib)...)}
}

func insert(n *node, path, value []byte) error {
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

func newBranch(path []byte, common int, value []byte) *node {
	branch := fork(path[:common])
	var packScratch [32]byte
	branch.setLeaf(int(path[common]), packPath(path[common+1:], packScratch[:0]), value)
	return branch
}

func splitLeaf(parent *node, nib int, path, suffix, value []byte) error {
	depth := len(parent.path)
	oldSuffixCount := 64 - depth - 1
	oldSuffix, oldValue := parent.leafAt(nib)
	var pathScratch [64]byte
	oldPath := append(pathScratch[:0], parent.path...)
	oldPath = append(oldPath, byte(nib))
	oldPath = oldPath[:len(oldPath)+oldSuffixCount]
	unpackPath(oldSuffix, oldSuffixCount, oldPath[depth+1:])
	common := nibbles.CommonPrefixLen(oldPath, path)
	if common == len(oldPath) {
		parent.setLeaf(nib, suffix, value)
		return nil
	}

	branch := newBranch(path, common, value)
	var packScratch [32]byte
	branch.setLeaf(int(oldPath[common]), packPath(oldPath[common+1:], packScratch[:0]), oldValue)
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

	branch := newBranch(path, common, value)
	branch.setChild(int(child.path[common]), child)
	parent.setChild(nib, branch)
	return nil
}

func splitStoredChild(parent *node, nib int, path, value []byte) error {
	var childScratch [64]byte
	childPath := parent.childPath(nib, childScratch[:0])
	common := nibbles.CommonPrefixLen(childPath, path)
	if common == len(childPath) {
		return ErrInsertStoredChild
	}

	branch := newBranch(path, common, value)
	branch.setStoredChild(int(childPath[common]), parent.childHashAt(nib), childPath[common+1:])
	parent.setChild(nib, branch)
	return nil
}
