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
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errNodeKey    = errors.New("commitment v4: invalid node key")
	errNodeRecord = errors.New("commitment v4: invalid node record")
)

type graph struct {
	plane    byte
	addrHash []byte
}

func (g graph) loadRoot(ctx commitment.PatriciaContext) (*node, error) {
	root, err := unfold(ctx, nil, g.plane, g.addrHash)
	if err != nil {
		return nil, err
	}
	if root == nil {
		root = fork(nil)
		root.loaded = true
	}
	root.plane = g.plane
	if len(root.path) == 0 || root.leafMask != 0 || bits.OnesCount16(root.childMask) != 1 {
		return root, nil
	}
	nib := bits.TrailingZeros16(root.childMask)
	if root.child(nib) != nil {
		return root, nil
	}
	if !root.hasChildHash(nib) {
		return root, errNodeRecord
	}
	child, err := g.unfoldChild(ctx, root.path)
	if err != nil {
		return root, err
	}
	root.setChild(nib, child)
	return root, nil
}

func (g graph) unfoldChild(ctx commitment.PatriciaContext, path []byte) (*node, error) {
	child, err := unfold(ctx, path, g.plane, g.addrHash)
	if err != nil {
		return nil, err
	}
	if child == nil {
		return nil, fmt.Errorf("%w: missing child at depth %d", errNodeRecord, len(path))
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

func (g graph) ensurePath(ctx commitment.PatriciaContext, n *node, path []byte) error {
	if len(path) != 64 || !bytes.HasPrefix(path, n.path) {
		return fmt.Errorf("%w: node path %x", errNodeKey, n.path)
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
			return errNodeRecord
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
		return errNodeRecord
	}
	childPath := n.childPath(nib, nil)
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

const (
	deltaChunk  = 1024
	encodeSlack = 96
)

type recordDelta = commitment.BranchDelta

type deltaParts [][]recordDelta

func (p *deltaParts) add(d recordDelta) {
	if bytes.Equal(d.Prev, d.Data) {
		return
	}
	n := len(*p)
	switch {
	case n == 0:
		*p = append(*p, nil)
	case len((*p)[n-1]) == deltaChunk:
		*p = append(*p, make([]recordDelta, 0, deltaChunk))
	}
	last := &(*p)[len(*p)-1]
	*last = append(*last, d)
}

func applyDeltas(parts deltaParts, putBranch func(key, data, prev []byte) error) error {
	for _, part := range parts {
		for _, delta := range part {
			if err := putBranch(delta.Key, delta.Data, delta.Prev); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g graph) materialize(ctx commitment.PatriciaContext, n, root *node, acc *deltaParts) ([32]byte, error) {
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.childMask&bit == 0 || n.leafMask&bit != 0 {
			continue
		}
		child := n.child(nib)
		if child == nil {
			if !n.hasChildHash(nib) {
				return [32]byte{}, errNodeRecord
			}
			continue
		}
		childHash, err := g.materialize(ctx, child, root, acc)
		if err != nil {
			return [32]byte{}, err
		}
		n.setStoredChild(nib, childHash[:], g.childExt(n, child, root))
	}
	path := n.path
	depth := len(path)
	if n == root {
		path = nil
		depth = 0
	}
	hash, err := fold(n, depth)
	if err != nil {
		return [32]byte{}, err
	}
	key := nodeKey(g.plane, g.addrHash, path, nil)
	prev := n.raw
	if !n.loaded {
		if prev, _, err = branchOwned(ctx, key); err != nil {
			return [32]byte{}, err
		}
	}
	data := encodeRecord(n, depth, make([]byte, 0, len(prev)+encodeSlack))
	if prev == nil {
		prev = []byte{}
	}
	acc.add(recordDelta{Key: key, Data: data, Prev: prev})
	return hash, nil
}

func (g graph) childExt(n, child, root *node) []byte {
	if n == root && len(n.path) != 0 && bytes.Equal(child.path, n.path) {
		return nil
	}
	return child.path[len(n.path)+1:]
}

type foldPlan struct {
	ctx       context.Context
	factory   commitment.TrieContextFactory
	workers   int
	fanOutMin int
}

func (p foldPlan) parallel() bool {
	return p.factory != nil && p.workers > 1
}

func (g graph) materializeRootChildren(root *node, plan foldPlan) ([]deltaParts, error) {
	nibs := make([]int, 0, 16)
	for nib := range 16 {
		if root.child(nib) != nil {
			nibs = append(nibs, nib)
		}
	}
	if len(nibs) < 2 {
		return nil, nil
	}

	accs := make([]deltaParts, len(nibs))
	hashes := make([][32]byte, len(nibs))
	eg, egCtx := errgroup.WithContext(plan.ctx)
	eg.SetLimit(min(plan.workers, len(nibs)))
	for k, nib := range nibs {
		eg.Go(func() error {
			workerCtx, cleanup := plan.factory(egCtx)
			if cleanup != nil {
				defer cleanup()
			}
			if workerCtx == nil {
				return errNodeRecord
			}
			hash, err := g.materialize(workerCtx, root.child(nib), root, &accs[k])
			hashes[k] = hash
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	for k, nib := range nibs {
		root.setStoredChild(nib, hashes[k][:], g.childExt(root, root.child(nib), root))
	}
	return accs, nil
}

func (g graph) persistGraph(ctx commitment.PatriciaContext, root *node, plan foldPlan) (deltaParts, error) {
	if err := promoteRootExtension(root); err != nil {
		return nil, err
	}
	var accs []deltaParts
	if plan.parallel() && len(root.path) == 0 {
		var err error
		if accs, err = g.materializeRootChildren(root, plan); err != nil {
			return nil, err
		}
	}
	var acc deltaParts
	if _, err := g.materialize(ctx, root, root, &acc); err != nil {
		return nil, err
	}
	return append(slices.Concat(accs...), acc...), nil
}
