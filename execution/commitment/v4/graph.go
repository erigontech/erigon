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

	"github.com/erigontech/erigon/execution/commitment"
)

type graph struct {
	plane    byte
	addrHash []byte
	errKey   error
	errNode  error
	before   *keySet
}

func accountGraph(before *keySet) graph {
	return graph{plane: planeAccount, errKey: errPhaseBKey, errNode: errPhaseBRecord, before: before}
}

func storageGraph(addrHash []byte, before *keySet) graph {
	return graph{plane: planeStorage, addrHash: addrHash, errKey: errPhaseAKey, errNode: errPhaseAStorage, before: before}
}

func (g graph) nodeKey(path, dst []byte) []byte {
	return nodeKey(g.plane, g.addrHash, path, dst)
}

func (g graph) unfoldChild(ctx commitment.PatriciaContext, path []byte) (*node, error) {
	child, err := unfold(ctx, path, g.plane, g.addrHash)
	if err != nil {
		return nil, err
	}
	if child == nil {
		return nil, fmt.Errorf("%w: missing child at depth %d", g.errNode, len(path))
	}
	child.plane = g.plane
	if g.before != nil {
		g.before.addNodeKey(g, path)
	}
	return child, nil
}

func rootExtensionChild(n *node) *node {
	if n == nil || len(n.path) == 0 || n.leafMask != 0 || bits.OnesCount16(n.childMask) != 1 {
		return nil
	}
	child := n.child(bits.TrailingZeros16(n.childMask))
	if child == nil || !bytes.Equal(child.path, n.path) {
		return nil
	}
	return child
}

func (g graph) materializeRootExtension(ctx commitment.PatriciaContext, root *node) error {
	if root == nil || len(root.path) == 0 || root.leafMask != 0 || bits.OnesCount16(root.childMask) != 1 {
		return nil
	}
	nib := bits.TrailingZeros16(root.childMask)
	if root.child(nib) != nil {
		return nil
	}
	if !root.hasChildHash(nib) {
		return g.errNode
	}
	child, err := g.unfoldChild(ctx, root.path)
	if err != nil {
		return err
	}
	root.setChild(nib, child)
	return nil
}

func (g graph) ensurePath(ctx commitment.PatriciaContext, n *node, path []byte) error {
	if n == nil {
		return fmt.Errorf("%w: nil node", g.errKey)
	}
	if len(path) != 64 || !bytes.HasPrefix(path, n.path) {
		return fmt.Errorf("%w: node path %x", g.errKey, n.path)
	}
	if len(n.path) >= 64 {
		return nil
	}
	nib := int(path[len(n.path)])
	bit := uint16(1) << nib
	if len(n.path) != 0 && bits.OnesCount16(n.childMask) == 1 && n.leafMask == 0 {
		nib = bits.TrailingZeros16(n.childMask)
		if child := n.child(nib); child != nil && bytes.Equal(child.path, n.path) {
			return g.ensurePath(ctx, child, path)
		}
		if !n.hasChildHash(nib) {
			return g.errNode
		}
		child, err := g.unfoldChild(ctx, n.path)
		if err != nil {
			return err
		}
		n.setChild(nib, child)
		return g.ensurePath(ctx, child, path)
	}
	if n.childMask&bit == 0 || n.leafMask&bit != 0 {
		return nil
	}
	if child := n.child(nib); child != nil {
		if !bytes.HasPrefix(path, child.path) {
			return nil
		}
		return g.ensurePath(ctx, child, path)
	}
	if !n.hasChildHash(nib) {
		return g.errNode
	}
	childPath := append(append([]byte(nil), n.path...), byte(nib))
	childPath = append(childPath, n.childExtAt(nib)...)
	if !bytes.HasPrefix(path, childPath) {
		return nil
	}
	child, err := g.unfoldChild(ctx, childPath)
	if err != nil {
		return err
	}
	n.setChild(nib, child)
	return g.ensurePath(ctx, child, path)
}

func storedChildPath(n *node, nib int, isRoot bool, dst []byte) []byte {
	dst = append(dst, n.path...)
	if isRoot && len(n.path) != 0 {
		return dst
	}
	dst = append(dst, byte(nib))
	return append(dst, n.childExtAt(nib)...)
}

func (g graph) reachableRecordKeys(root *node, keys *keySet) {
	var visit func(*node, bool)
	visit = func(n *node, isRoot bool) {
		if n == nil {
			return
		}
		var pathScratch [64]byte
		path := n.path
		if isRoot {
			path = nil
		}
		keys.addNodeKey(g, path)
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			if child := n.child(nib); child != nil {
				visit(child, false)
				continue
			}
			if n.hasChildHash(nib) {
				keys.addNodeKey(g, storedChildPath(n, nib, isRoot, pathScratch[:0]))
			}
		}
	}
	visit(root, true)
}

func (g graph) persistGraph(ctx commitment.PatriciaContext, root *node) error {
	if root == nil {
		return g.errNode
	}
	if err := promoteRootExtension(root); err != nil {
		return err
	}
	deltas := make([]recordDelta, 0, g.before.len()+1)
	after := new(keySet)
	var materialize func(*node) ([32]byte, error)
	materialize = func(n *node) ([32]byte, error) {
		if n == nil {
			return [32]byte{}, g.errNode
		}
		var pathScratch [64]byte
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			child := n.child(nib)
			if child == nil {
				if !n.hasChildHash(nib) {
					return [32]byte{}, g.errNode
				}
				after.addNodeKey(g, storedChildPath(n, nib, n == root, pathScratch[:0]))
				continue
			}
			childHash, err := materialize(child)
			if err != nil {
				return [32]byte{}, err
			}
			var ext []byte
			if !(n == root && len(n.path) != 0 && bytes.Equal(child.path, n.path)) {
				ext = child.path[len(n.path)+1:]
			}
			n.setStoredChild(nib, childHash[:], ext)
		}
		path := n.path
		depth := len(path)
		if n == root {
			path = nil
			depth = 0
		}
		hash, delta, err := foldAndEncodeRecord(ctx, n, depth, g.nodeKey(path, nil))
		if err != nil {
			return [32]byte{}, err
		}
		deltas = append(deltas, delta)
		return hash, nil
	}
	if _, err := materialize(root); err != nil {
		return err
	}
	for _, delta := range deltas {
		after.add(delta.key)
	}
	deltas, err := appendRemovedDeltas(ctx, deltas, g.before, after)
	if err != nil {
		return err
	}
	return applyDeltas(deltas, ctx.PutBranch)
}
