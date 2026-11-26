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
	"math/bits"
	"slices"
	"sort"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func traceCollapsesDeletes(ctx commitment.PatriciaContext, items []feedEntry, tracer commitment.CollapseTracer) error {
	type deleteGroup struct {
		plane  byte
		owner  []byte
		paths  [][]byte
		unique map[string]struct{}
	}
	groups := make(map[string]*deleteGroup)
	deletedAccounts := make(map[string]struct{})
	for _, item := range items {
		if item.update != nil && item.update.Deleted() && len(item.plainKey) == length.Addr {
			deletedAccounts[string(item.hashedKey)] = struct{}{}
		}
	}
	for _, item := range items {
		if item.update == nil || !item.update.Deleted() {
			continue
		}
		plane, owner, path := planeAccount, []byte(nil), item.hashedKey
		if len(item.plainKey) != length.Addr {
			if len(path) < 64 {
				continue
			}
			if _, ok := deletedAccounts[string(path[:64])]; ok {
				continue
			}
			addrHash := hashAddressPath(path[:64])
			plane, owner, path = planeStorage, addrHash[:], path[64:]
		}
		groupKey := string(append([]byte{plane}, owner...))
		group := groups[groupKey]
		if group == nil {
			group = &deleteGroup{plane: plane, owner: slices.Clone(owner), unique: make(map[string]struct{})}
			groups[groupKey] = group
		}
		if _, ok := group.unique[string(path)]; ok {
			continue
		}
		group.unique[string(path)] = struct{}{}
		group.paths = append(group.paths, slices.Clone(path))
	}
	ordered := make([]*deleteGroup, 0, len(groups))
	for _, group := range groups {
		slices.SortFunc(group.paths, bytes.Compare)
		ordered = append(ordered, group)
	}
	slices.SortFunc(ordered, func(a, b *deleteGroup) int {
		if a.plane != b.plane {
			return int(a.plane) - int(b.plane)
		}
		return bytes.Compare(a.owner, b.owner)
	})
	for _, group := range ordered {
		g := graph{plane: group.plane, addrHash: group.owner}
		ownerPath := []byte(nil)
		if group.plane == planeStorage {
			ownerPath = make([]byte, 64)
			nibbles.Expand(group.owner, ownerPath)
		}
		root, err := g.loadRoot(ctx)
		if err != nil {
			return err
		}
		if root == nil {
			continue
		}
		deletes := make([][]byte, 0, len(group.paths))
		for _, path := range group.paths {
			if !bytes.HasPrefix(path, root.path) {
				continue
			}
			deletes = append(deletes, path)
		}
		if len(deletes) == 0 {
			continue
		}
		for _, path := range deletes {
			if err := g.ensurePath(ctx, root, path); err != nil {
				return err
			}
		}
		if _, err := walkDeletedNode(ctx, g, root, deletes, tracer, ownerPath); err != nil {
			return err
		}
	}
	return nil
}

func walkDeletedNode(ctx commitment.PatriciaContext, g graph, n *node, deletes [][]byte, tracer commitment.CollapseTracer, ownerPath []byte) (bool, error) {
	if n == nil || n.childMask == 0 {
		return false, nil
	}
	if n.isExtension() {
		nib := bits.TrailingZeros16(n.childMask)
		if child := n.child(nib); child != nil && bytes.Equal(child.path, n.path) {
			return walkDeletedNode(ctx, g, child, deletes, tracer, ownerPath)
		}
	}
	if n.childMask == n.leafMask && bits.OnesCount16(n.childMask) == 1 {
		nib := bits.TrailingZeros16(n.childMask)
		return len(deleteRange(deletes, deleteChildPath(n, nib), nil)) == 0, nil
	}
	survivorCount := 0
	var survivor []byte
	for mask := n.childMask; mask != 0; mask &= mask - 1 {
		nib := bits.TrailingZeros16(mask)
		childPath := deleteChildPath(n, nib)
		childDeletes := deleteRange(deletes, childPath, nil)
		live := true
		if n.leafMask&(uint16(1)<<nib) != 0 {
			live = len(childDeletes) == 0
		} else if child := n.child(nib); child != nil && len(childDeletes) != 0 {
			var err error
			live, err = walkDeletedNode(ctx, g, child, childDeletes, tracer, ownerPath)
			if err != nil {
				return false, err
			}
		}
		if live {
			survivorCount++
			survivor = childPath
			if n.leafMask&(uint16(1)<<nib) != 0 {
				var err error
				survivor, err = collapseSurvivorPath(ctx, g, n, nib, survivor)
				if err != nil {
					return false, err
				}
			}
		}
	}
	if bits.OnesCount16(n.childMask) >= 2 && survivorCount == 1 {
		tracer(slices.Concat(ownerPath, survivor), slices.Concat(ownerPath, n.path))
	}
	return survivorCount != 0, nil
}

func collapseSurvivorPath(ctx commitment.PatriciaContext, g graph, n *node, nib int, path []byte) ([]byte, error) {
	if g.plane != planeAccount || n.leafMask&(uint16(1)<<nib) == 0 {
		return path, nil
	}
	_, payload := n.leafAt(nib)
	_, _, _, storageRoot, err := decodeAccountLeaf(payload)
	if err != nil || bytes.Equal(storageRoot, empty.RootHash[:]) {
		return path, nil
	}
	addrHash := hashAddressPath(path)
	storage := graph{plane: planeStorage, addrHash: addrHash[:]}
	root, err := storage.loadRoot(ctx)
	if err != nil || root == nil || root.childMask == 0 {
		return path, err
	}
	var extension []byte
	switch {
	case root.isLeafRoot():
		child := bits.TrailingZeros16(root.childMask)
		suffix, _ := root.leafAt(child)
		extension = append([]byte{byte(child)}, unpackPath(suffix, 63, nil)...)
	case root.isExtension():
		extension = slices.Clone(root.path)
	default:
		return path, nil
	}
	return slices.Concat(path, extension), nil
}

func deleteChildPath(n *node, nib int) []byte {
	if n.leafMask&(uint16(1)<<nib) == 0 {
		return n.childPath(nib, nil)
	}
	suffix, _ := n.leafAt(nib)
	path := append(slices.Clone(n.path), byte(nib))
	return append(path, unpackPath(suffix, 63-len(n.path), nil)...)
}

func deleteRange(deletes [][]byte, prefix []byte, comparisons *int) [][]byte {
	compare := func(left, right []byte) int {
		if comparisons != nil {
			*comparisons++
		}
		return bytes.Compare(left, right)
	}
	start := sort.Search(len(deletes), func(i int) bool { return compare(deletes[i], prefix) >= 0 })
	upper := append(slices.Clone(prefix), 0x10)
	end := sort.Search(len(deletes), func(i int) bool { return compare(deletes[i], upper) >= 0 })
	return deletes[start:end]
}

func splitPrefix(prefix []byte) (plane byte, addrHash, path []byte) {
	if len(prefix) < 64 {
		return planeAccount, nil, prefix
	}
	addr := hashAddressPath(prefix[:64])
	return planeStorage, addr[:], prefix[64:]
}

func liveBranch(load func(plane byte, addrHash, path []byte) (*node, error), prefix []byte) (*node, error) {
	plane, addrHash, path := splitPrefix(prefix)
	if len(path) > 63 {
		return nil, nil
	}
	n, err := load(plane, addrHash, nil)
	if err != nil || n == nil || n.childMask == 0 || !bytes.HasPrefix(path, n.path) {
		return nil, err
	}
	if n.isLeafRoot() {
		return nil, nil
	}
	if n.isExtension() {
		if n, err = load(plane, addrHash, n.path); err != nil || n == nil {
			return nil, err
		}
	}
	for len(n.path) < len(path) {
		nib := int(path[len(n.path)])
		if bit := uint16(1) << nib; n.childMask&bit == 0 || n.leafMask&bit != 0 {
			return nil, nil
		}
		var extScratch [64]byte
		childPath := slices.Concat(n.path, []byte{byte(nib)}, n.childExtAtInto(nib, extScratch[:0]))
		if !bytes.HasPrefix(path, childPath) {
			return nil, nil
		}
		if n, err = load(plane, addrHash, childPath); err != nil || n == nil {
			return nil, err
		}
	}
	return n, nil
}

type recordCache struct {
	commitment.PatriciaContext
	recs map[string][]byte
}

func (c *recordCache) BranchOwned(prefix []byte) ([]byte, kv.Step, error) {
	if data, ok := c.recs[string(prefix)]; ok {
		return data, 0, nil
	}
	data, step, err := branchOwned(c.PatriciaContext, prefix)
	if err == nil {
		c.recs[string(prefix)] = data
	}
	return data, step, err
}

func (c *recordCache) LeafRefs(key, data []byte) *commitment.LeafRefs {
	return leafRefsOf(c.PatriciaContext, key, data)
}

type branchReader func(key []byte) ([]byte, error)

func (r branchReader) Branch(key []byte) ([]byte, kv.Step, error) {
	data, err := r(key)
	return data, 0, err
}

func (t *Trie) BranchChildCount(read func([]byte) ([]byte, error), nibblePrefix []byte) (int, error) {
	ctx := branchReader(read)
	n, err := liveBranch(func(plane byte, addrHash, path []byte) (*node, error) { return unfold(ctx, path, plane, addrHash) }, nibblePrefix)
	if err != nil || n == nil {
		return 0, err
	}
	return bits.OnesCount16(n.childMask), nil
}
