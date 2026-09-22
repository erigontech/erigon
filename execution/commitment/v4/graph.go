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
}

func accountGraph() graph {
	return graph{plane: planeAccount, errKey: errPhaseBKey, errNode: errPhaseBRecord}
}

func storageGraph(addrHash []byte) graph {
	return graph{plane: planeStorage, addrHash: addrHash, errKey: errPhaseAKey, errNode: errPhaseAStorage}
}

func (g graph) nodeKey(path []byte) []byte {
	return nodeKey(g.plane, g.addrHash, path, nil)
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
	return child, nil
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
		if child := n.children[nib]; child != nil && bytes.Equal(child.path, n.path) {
			return g.ensurePath(ctx, child, path)
		}
		if len(n.childHash[nib]) != 32 {
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
	if child := n.children[nib]; child != nil {
		return g.ensurePath(ctx, child, path)
	}
	if len(n.childHash[nib]) != 32 {
		return g.errNode
	}
	childPath := append(append([]byte(nil), n.path...), byte(nib))
	childPath = append(childPath, n.childExt[nib]...)
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

func (g graph) reachableRecordKeys(root *node) map[string]struct{} {
	keys := make(map[string]struct{})
	var visit func(*node, bool)
	visit = func(n *node, isRoot bool) {
		if n == nil {
			return
		}
		path := n.path
		if isRoot {
			path = nil
		}
		keys[string(g.nodeKey(path))] = struct{}{}
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			if child := n.children[nib]; child != nil {
				visit(child, false)
				continue
			}
			if len(n.childHash[nib]) == 32 {
				childPath := append([]byte(nil), n.path...)
				if isRoot && len(n.path) == 0 {
					childPath = append(childPath, byte(nib))
				}
				childPath = append(childPath, n.childExt[nib]...)
				keys[string(g.nodeKey(childPath))] = struct{}{}
			}
		}
	}
	visit(root, true)
	return keys
}

func (g graph) persistGraph(ctx commitment.PatriciaContext, root *node, before map[string]struct{}) error {
	if root == nil {
		return g.errNode
	}
	if err := promoteRootExtension(root); err != nil {
		return err
	}
	deltas := make([]recordDelta, 0, len(before)+1)
	var materialize func(*node) ([32]byte, error)
	materialize = func(n *node) ([32]byte, error) {
		if n == nil {
			return [32]byte{}, g.errNode
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			child := n.children[nib]
			if child == nil {
				if len(n.childHash[nib]) != 32 {
					return [32]byte{}, g.errNode
				}
				continue
			}
			childHash, err := materialize(child)
			if err != nil {
				return [32]byte{}, err
			}
			n.childHash[nib] = appendCopy(n.childHash[nib], childHash[:])
			var ext []byte
			if !(n == root && len(n.path) != 0 && bytes.Equal(child.path, n.path)) {
				ext = child.path[len(n.path)+1:]
			}
			n.childExt[nib] = appendCopy(n.childExt[nib], ext)
			n.children[nib] = nil
		}
		path := n.path
		depth := len(path)
		if n == root {
			path = nil
			depth = 0
		}
		hash, delta, err := foldAndEncodeRecord(ctx, n, depth, g.nodeKey(path))
		if err != nil {
			return [32]byte{}, err
		}
		deltas = append(deltas, delta)
		return hash, nil
	}
	if _, err := materialize(root); err != nil {
		return err
	}
	after := g.reachableRecordKeys(root)
	for _, delta := range deltas {
		after[string(delta.key)] = struct{}{}
	}
	deltas, err := appendRemovedDeltas(ctx, deltas, before, after)
	if err != nil {
		return err
	}
	return applyDeltas(deltas, ctx.PutBranch)
}
